package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationExceptionFunction;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.interceptors.distribution.ConcurrentChangeException;
import org.infinispan.interceptors.distribution.ScatteredDistributionInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Used in @{link org.infinispan.configuration.cache.CacheMode#SCATTERED_SYNC scattered cache}
 * The commit is executed in {@link ScatteredDistributionInterceptor}
 * before replicating the change from primary owner.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RetryingEntryWrappingInterceptor extends EntryWrappingInterceptor {
   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationFinallyFunction handleDataWriteReturn = this::handleDataWriteReturn;
   private final InvocationExceptionFunction handleManyWriteReturn = this::handleManyWriteReturn;

   @Override
   protected Object setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext ctx, DataWriteCommand command) {
      return invokeNextAndHandle(ctx, command, handleDataWriteReturn);
   }

   private Object handleDataWriteReturn(InvocationContext ctx, VisitableCommand command, Object rv, Throwable throwable) throws Throwable {
      DataWriteCommand dataWriteCommand = (DataWriteCommand) command;
      if (throwable == null) {
         // We need to update previous values because if this write inserts prefetched value, we need to keep it
         // in context even if the main command is a retry (therefore it would revert the value).
         MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(dataWriteCommand.getKey());
         if (entry != null) {
            entry.updatePreviousValue();
         }
         return rv;
      } else if (throwable instanceof ConcurrentChangeException) {
         if (trace) {
            log.tracef(throwable, "Retrying %s after concurrent change", command);
         }
         ctx.removeLookedUpEntry(dataWriteCommand.getKey());
         return visitCommand(ctx, dataWriteCommand);
      } else {
         throw throwable;
      }
   }

   @Override
   protected Object setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command) {
      return invokeNextAndExceptionally(ctx, command, handleManyWriteReturn);
   }

   private Object handleManyWriteReturn(InvocationContext ctx, VisitableCommand command, Throwable throwable) throws Throwable {
      if (throwable instanceof ConcurrentChangeException) {
         if (trace) {
            log.tracef(throwable, "Retrying %s after concurrent change", command);
         }
         ctx.removeLookedUpEntries(((WriteCommand) command).getAffectedKeys());
         return visitCommand(ctx, command);
      } else {
         throw throwable;
      }
   }
}
