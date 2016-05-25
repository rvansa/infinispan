package org.infinispan.partitionhandling.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.distribution.MissingOwnerException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PartitionHandlingInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(PartitionHandlingInterceptor.class);

   private PartitionHandlingManager partitionHandlingManager;
   private Transport transport;
   private StateTransferManager stateTransferManager;
   private StateTransferLock stateTransferLock;
   private CacheMode cacheMode;

   @Inject
   void init(PartitionHandlingManager partitionHandlingManager, Transport transport,
             StateTransferManager stateTransferManager, StateTransferLock stateTransferLock,
             Configuration configuration) {
      this.partitionHandlingManager = partitionHandlingManager;
      this.transport = transport;
      this.stateTransferManager = stateTransferManager;
      this.stateTransferLock = stateTransferLock;
      this.cacheMode = configuration.clustering().cacheMode();
   }

   private boolean performPartitionCheck(InvocationContext ctx, FlagAffectedCommand command) {
      // We always perform partition check if this is a remote command
      if (!ctx.isOriginLocal()) {
         return true;
      }
      return !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   protected Object handleSingleWrite(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkWrite(command.getKey());
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         for (Object k : command.getAffectedKeys())
            partitionHandlingManager.checkWrite(k);
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkClear();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command)
         throws Throwable {
      return handleSingleWrite(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         partitionHandlingManager.checkBulkRead();
      }
      return handleDefault(ctx, command);
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleDataReadCommand(ctx, command);
   }

   private Object handleDataReadCommand(InvocationContext ctx, DataCommand command) {
      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> {
         DataCommand dataCommand = (DataCommand) rCommand;
         if (t != null) {
            if (t instanceof RpcException && performPartitionCheck(rCtx, dataCommand)) {
               // We must have received an AvailabilityException from one of the owners.
               // There is no way to verify the cause here, but there isn't any other way to get an invalid
               // get response.
               throw log.degradedModeKeyUnavailable(dataCommand.getKey());
            } else if (t instanceof MissingOwnerException) {
               return asyncValue(handleMissingOwner(Collections.singleton(dataCommand.getKey()), (MissingOwnerException) t));
            } else {
               // If all owners left and we still haven't received the availability update yet,
               // we get OutdatedTopologyException from BaseDistributionInterceptor.retrieveFromProperSource
               if (t instanceof AllOwnersLostException && performPartitionCheck(rCtx, dataCommand)) {
                  // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
                  // and we only fail the operation if _all_ owners have left the cluster.
                  // TODO Move this to the availability strategy when implementing ISPN-4624
                  CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
                  if (cacheTopology == null || cacheTopology.getTopologyId() != dataCommand.getTopologyId()) {
                     // just rethrow the exception
                     throw t;
                  }
                  List<Address> owners = cacheTopology.getReadConsistentHash().locateOwners(dataCommand.getKey());
                  if (!InfinispanCollections.containsAny(transport.getMembers(), owners)) {
                     throw log.degradedModeKeyUnavailable(dataCommand.getKey());
                  }
               }
               throw t;
            }
         }

         postOperationPartitionCheck(rCtx, dataCommand, dataCommand.getKey());
         return rv;
      });
   }

   private CompletableFuture<Void> handleMissingOwner(Collection<?> keys, MissingOwnerException moe) throws InterruptedException {
      // In scattered cache mode it is possible that the CH does not contain owner of the key
      // and therefore throws MOE.
      // At this point we may be still available, so we have to wait until happens one of
      // a) we become degraded
      // b) topology change

      // do an early check for degraded mode
      if (partitionHandlingManager.getAvailabilityMode() == AvailabilityMode.DEGRADED_MODE) {
         throw log.degradedModeKeyUnavailable(keys);
      }
      int topologyId = moe.getTopologyId();
      log.tracef("Missing owner for %s in topology %d, will wait for new topology or becoming degraded",
         keys, topologyId);
      CompletableFuture<Void> topologyFuture = stateTransferLock.topologyFuture(topologyId + 1);
      if (topologyFuture == null) {
         // newer topology is already installed, retry with this one
         throw moe; // should be handled in the same way as OutdatedTopologyException
      }
      CompletableFuture<AvailabilityMode> degraded = partitionHandlingManager.degradedFuture();
      return CompletableFuture.anyOf(degraded, topologyFuture).thenApply(result -> {
         // upon topology change the result is null (void)
         if (result == AvailabilityMode.DEGRADED_MODE) {
            throw log.degradedModeKeysUnavailable(keys);
         } else {
            throw moe;
         }
      });
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> postTxCommandCheck(((TxInvocationContext) rCtx)));
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> postTxCommandCheck(((TxInvocationContext) rCtx)));
   }

   protected void postTxCommandCheck(TxInvocationContext ctx) {
      if (ctx.hasModifications() && partitionHandlingManager.getAvailabilityMode() != AvailabilityMode.AVAILABLE && !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction())) {
         for (Object key : ctx.getAffectedKeys()) {
            partitionHandlingManager.checkWrite(key);
         }
      }
   }

   private void postOperationPartitionCheck(InvocationContext ctx, DataCommand command, Object key) throws Throwable {
      if (performPartitionCheck(ctx, command)) {
         // We do the availability check after the read, because the cache may have entered degraded mode
         // while we were reading from a remote node.
         partitionHandlingManager.checkRead(key);
      }
      // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> {
         GetAllCommand getAllCommand = (GetAllCommand) rCommand;
         if (t != null) {
            if (t instanceof RpcException && performPartitionCheck(rCtx, getAllCommand)) {
               // We must have received an AvailabilityException from one of the owners.
               // There is no way to verify the cause here, but there isn't any other way to get an invalid
               // get response.
               throw log.degradedModeKeysUnavailable(((GetAllCommand) rCommand).getKeys());
            } else if (t instanceof MissingOwnerException) {
               return asyncValue(handleMissingOwner(getAllCommand.getKeys(), (MissingOwnerException) t));
            } else {
               throw t;
            }
         }

         if (performPartitionCheck(rCtx, getAllCommand)) {
            // We do the availability check after the read, because the cache may have entered degraded mode
            // while we were reading from a remote node.
            for (Object key : getAllCommand.getKeys()) {
               partitionHandlingManager.checkRead(key);
            }

            Map<Object, Object> result = ((Map<Object, Object>) rv);
            // If all owners left and we still haven't received the availability update yet, we could return
            // an incorrect value. So we need a special check for missing results.
            if (result.size() != getAllCommand.getKeys().size()) {
               // Unlike in PartitionHandlingManager.checkRead(), here we ignore the availability status
               // and we only fail the operation if _all_ owners have left the cluster.
               // TODO Move this to the availability strategy when implementing ISPN-4624
               Set<Object> missingKeys = new HashSet<>(getAllCommand.getKeys());
               missingKeys.removeAll(result.keySet());
               for (Iterator<Object> it = missingKeys.iterator(); it.hasNext(); ) {
                  Object key = it.next();
                  if (InfinispanCollections.containsAny(transport.getMembers(), stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key))) {
                     it.remove();
                  }
               }
               if (!missingKeys.isEmpty()) {
                  throw log.degradedModeKeysUnavailable(missingKeys);
               }
            }
         }

         return rv;
         // TODO We can still return a stale value if the other partition stayed active without us and we haven't entered degraded mode yet.
      });
   }
}
