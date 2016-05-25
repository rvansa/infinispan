package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationExceptionFunction;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.interceptors.distribution.ConcurrentChangeException;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Used in @{link org.infinispan.configuration.cache.CacheMode#SCATTERED_SYNC scattered cache}
 * The commit is executed in {@link org.infinispan.interceptors.distribution.ScatteringInterceptor}
 * before replicating the change from primary owner.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RetryingEntryWrappingInterceptor extends EntryWrappingInterceptor {
   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationExceptionFunction dataWriteReturnHandler = (rCtx, rCommand, throwable) -> {
      if (throwable instanceof ConcurrentChangeException) {
         log.trace("%s has thrown %s, retrying", rCommand, throwable);
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         rCtx.removeLookedUpEntry(dataWriteCommand.getKey());
         return visitCommand(rCtx, dataWriteCommand);
      } else {
         throw throwable;
      }
   };

   private final InvocationExceptionFunction manyWriteReturnHandler = (rCtx, rCommand, throwable) -> {
      if (throwable instanceof ConcurrentChangeException) {
         log.trace("%s has thrown %s, retrying", rCommand, throwable);
         WriteCommand writeCommand = (WriteCommand) rCommand;
         for (Object key : writeCommand.getAffectedKeys()) {
            rCtx.removeLookedUpEntry(key);
         }
         return visitCommand(rCtx, writeCommand);
      } else {
         throw throwable;
      }
   };

   protected Object setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext ctx,
                                                                 DataWriteCommand command, Metadata metadata) {
      return invokeNextAndExceptionally(ctx, command, dataWriteReturnHandler);
   }

   protected Object setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command) {
      return invokeNextAndExceptionally(ctx, command, manyWriteReturnHandler);
   }
}
