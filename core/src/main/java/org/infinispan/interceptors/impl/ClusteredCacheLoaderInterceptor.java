package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.List;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The same as a regular cache loader interceptor, except that it contains additional logic to force loading from the
 * cache loader if needed on a remote node, in certain conditions.
 *
 * @author Manik Surtani
 * @since 9.0
 */
public class ClusteredCacheLoaderInterceptor extends CacheLoaderInterceptor {

   private static final Log log = LogFactory.getLog(ClusteredActivationInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private boolean transactional;
   private ClusteringDependentLogic cdl;
   private StateTransferManager stateTransferManager;
   private boolean distributed;
   private boolean writeSkewCheck;
   private boolean totalOrder;

   @Inject
   private void injectDependencies(ClusteringDependentLogic cdl, StateTransferManager stateTransferManager) {
      this.cdl = cdl;
      this.stateTransferManager = stateTransferManager;
   }

   @Start(priority = 15)
   private void startClusteredCacheLoaderInterceptor() {
      transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
      writeSkewCheck = cacheConfiguration.transaction().lockingMode() == LockingMode.OPTIMISTIC && cacheConfiguration.locking().writeSkewCheck();
      totalOrder = cacheConfiguration.transaction().transactionProtocol().isTotalOrder();
      distributed = cacheConfiguration.clustering().cacheMode().isDistributed();
   }

   @Override
   protected boolean skipLoadForFunctionalWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      // the custom loading behaviour for functional commands happens only in DIST mode:
      if (distributed
            // TODO: functional API is not yet implemented for TX mode
            && !transactional) {
         /*
          * Functional API in DIST: if we're the originator, we load only when
          * we're the primary owner because the primary owner is responsible for
          * replicating the command to other owners - and if we're not the
          * primary owner, we forward it to one. And if we're not the
          * originator, then this is either forwarded from non-primary owner, or
          * replicated by primary owner to secondary owners, and the semantics
          * of Functional API require that we must load.
          */
         if ((ctx.isOriginLocal() ? !cdl.localNodeIsPrimaryOwner(key) : !cdl.localNodeIsOwner(key))
               // TODO Do we replicate CACHE_MODE_LOCAL commands?
               && !cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
            if (trace) {
               log.tracef("Skip load for functional command %s. This node is not an owner of %s", cmd, key);
            }
            return true;
         } else {
            // we can short-circuit here for we must load and no other condition will change it
            return false;
         }
      }
      return super.skipLoadForFunctionalWriteCommand(cmd, key, ctx);
   }

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      if (transactional) {
         if (!ctx.isOriginLocal()) {
            if (writeSkewCheck) {
               // we need to load previous value to perform the write skew check
               if (cmd.loadType() != VisitableCommand.LoadType.OWNER) {
                  if (cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
                     return false;
                  }
                  // With TOA, the prepare command is executed on origin through TOA delivery thread
                  // therefore with non-origin context. We need to load the entry again for WSC, then.
                  if (totalOrder) {
                     return !cdl.localNodeIsOwner(key);
                  } else {
                     return !cdl.localNodeIsPrimaryOwner(key);
                  }
               }
            } else if (cmd.loadType() != VisitableCommand.LoadType.OWNER){
               // without WSC, loading on non-origin is needed only when the command reads previous value on all owners
               return true;
            }
         }
         // TODO: if execution type is primary and origin is local, return true
      } else {
         switch (cmd.loadType()) {
            case DONT_LOAD:
               return true;
            case PRIMARY:
               if (cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
                  return cmd.hasFlag(Flag.SKIP_CACHE_LOAD);
               }
               if (!cdl.localNodeIsPrimaryOwner(key)) {
                  if (trace) {
                     log.tracef("Skip load for command %s. This node is not the primary owner of %s", cmd, toStr(key));
                  }
                  return true;
               }
               break;
            case OWNER:
               if (cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
                  return cmd.hasFlag(Flag.SKIP_CACHE_LOAD);
               }
               List<Address> owners = cdl.getOwners(key);
               int index = owners == null ? 0 : owners.indexOf(cdl.getAddress());
               if (index != 0 && (index < 0 || ctx.isOriginLocal())) {
                  if (trace) {
                     log.tracef("Skip load for command %s. This node is not the primary owner or backup (and not origin) of %s", cmd, toStr(key));
                  }
                  return true;
               }
               break;
         }
      }
      return super.skipLoadForWriteCommand(cmd, key, ctx);
   }

   @Override
   protected boolean canLoad(Object key) {
      // Don't load the value if we are using distributed mode and aren't in the read CH
      return stateTransferManager.isJoinComplete() && (!distributed || isKeyLocal(key));
   }

   private boolean isKeyLocal(Object key) {
      return stateTransferManager.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(cdl.getAddress(), key);
   }
}
