package org.infinispan.interceptors.impl;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Similar to {@link DistCacheWriterInterceptor} but as commands are not forwarded from primary owner
 * so we just write to the store all the time (with non-shared interceptors).
 *
 * Uses its own locking scheme to order writes into the store - if there is a newer write into DC,
 * the older write is ignored. This could improve contented case, but has one strange property:
 * Assume CS contains V1, T2 writes V2 and T3 writes V3; if T2 receives version and stores into DC
 * before T3 but arrives here after T3 the V2 write into CS is ignored but the operation is confirmed
 * as if it was persisted correctly.
 * If T3 then (spuriously) fails to write into the store, V1 stays in CS but the error is reported
 * only to T3 - that means that T3 effectivelly rolled back V2 into V1 (and reported this as an error).
 * This may surprise some users.
 *
 * Reads can be blocked by ongoing writes, though; when T2 finishes and then the application attempts
 * to read the value, and removal from T3 is not complete yet the read would not find the value in DC
 * (because it was removed by T3) but could load V1 from cache store. Therefore, read must wait until
 * the current write (that could have interacted with previous write) finishes.
 *
 * However, blocking reads in cachestore is not something unusual; the DC lock is acquired when writing
 * the cache store during eviction/passivation, or during write skew checks in other modes as well.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredCacheWriterInterceptor extends CacheWriterInterceptor {

   private static final Log log = LogFactory.getLog(DistCacheWriterInterceptor.class);
   private DistributionManager dm;
   private DataContainer dataContainer;
   private ConcurrentHashMap<Object, CompletableFuture<Void>> locks = new ConcurrentHashMap<>();
   private TimeService timeService;
   private ScheduledExecutorService timeoutExecutor;
   private long lockTimeout;

   private final InvocationSuccessFunction dataWriteReturnHandler = (rCtx, rCommand, rv) -> {
      DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
      Object key = dataWriteCommand.getKey();
      if (!isStoreEnabled(dataWriteCommand) || !dataWriteCommand.isSuccessful())
         return rv;

      CacheEntry cacheEntry = rCtx.lookupEntry(key);
      if (cacheEntry == null) {
         throw new IllegalStateException();
      }
      Metadata metadata = cacheEntry.getMetadata();
      EntryVersion version = metadata == null ? null : metadata.version();
      // version is null only with some nasty flags, we don't care about ordering then
      if (version != null) {
         long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
         return asyncValue(checkLockAndStore(rCtx, dataWriteCommand, key, version, deadline).thenApply(nil -> rv));
      }
      storeEntry(rCtx, key, dataWriteCommand);
      if (getStatisticsEnabled())
         cacheStores.incrementAndGet();
      return rv;
   };

   private final InvocationSuccessFunction manyDataReturnHandler = (rCtx, rCommand, rv) -> {
      FlagAffectedCommand command = (FlagAffectedCommand) rCommand;
      if (!isStoreEnabled(command))
         return rv;

      long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
      List<CompletableFuture<Void>> futures = null;
      Map<Object, CacheEntry> lookedUpEntries = rCtx.getLookedUpEntries();
      for (CacheEntry cacheEntry : lookedUpEntries.values()) {
         Metadata metadata = cacheEntry.getMetadata();
         EntryVersion version = metadata == null ? null : metadata.version();
         // version is null only with some nasty flags, we don't care about ordering then
         if (version != null) {
            CompletableFuture<Void> future = checkLockAndStore(rCtx, command, cacheEntry.getKey(), version, Long.MIN_VALUE);
            if (!future.isDone()) {
               if (futures == null) {
                  futures = new ArrayList<>(lookedUpEntries.size());
               }
               futures.add(future);
            }
         } else {
            storeEntry(rCtx, cacheEntry.getKey(), command);
         }
      }
      if (futures == null) {
         if (getStatisticsEnabled())
            cacheStores.getAndAdd(lookedUpEntries.size());
         return rv;
      } else {
         CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
         scheduleTimeout(allFuture, deadline, lookedUpEntries.keySet());
         allFuture.thenRun(() -> {
            if (getStatisticsEnabled())
               cacheStores.getAndAdd(lookedUpEntries.size());
         });
         return asyncValue(allFuture.thenApply(nil -> rv));
      }
   };

   private InvocationSuccessFunction invalidateReturnHandler = (rCtx, rCommand, rv) -> {
      InvalidateVersionsCommand command = (InvalidateVersionsCommand) rCommand;
      Object[] keys = command.getKeys();
      List<CompletableFuture<Void>> futures = null;
      long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
      for (int i = 0; i < keys.length; ++i) {
         Object key = keys[i];
         if (key == null) break;
         CompletableFuture<Void> future = checkLockAndRemove(key);
         if (!future.isDone()) {
            if (futures == null) {
               futures = new ArrayList<>(keys.length);
            }
            futures.add(future);
         }
      }
      if (futures == null) {
         return null;
      } else if (futures.size() == 1) {
         return futures.get(0);
      } else {
         CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
         scheduleTimeout(allFuture, deadline, Arrays.toString(keys));
         return asyncValue(allFuture.thenApply(nil -> rv));
      }
   };

   private void scheduleTimeout(CompletableFuture<?> future, long deadline, Object keys) {
      timeoutExecutor.schedule(() -> future.completeExceptionally(
         log.unableToAcquireLock(Util.prettyPrintTime(lockTimeout, TimeUnit.NANOSECONDS), keys, null, null)),
         timeService.remainingTime(deadline, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void inject(DistributionManager dm, DataContainer dataContainer, TimeService timeService,
                      @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.dm = dm;
      this.dataContainer = dataContainer;
      this.timeService = timeService;
      this.timeoutExecutor = timeoutExecutor;
   }

   public void start() {
      super.start();
      // TODO: scattered cache cannot configure locking via XML
      this.lockTimeout = TimeUnit.MILLISECONDS.toNanos(cacheConfiguration.locking().lockAcquisitionTimeout());
   }

   private CompletableFuture<Void> checkLockAndStore(InvocationContext ctx, FlagAffectedCommand command, Object key, EntryVersion version, long deadline) {
      ByRef<CompletableFuture<Void>> lockedFuture = new ByRef<>(null);
      ByRef<CompletableFuture<Void>> waitFuture = new ByRef<>(null);
      dataContainer.compute(key, (k, oldEntry, factory) -> {
         // if there is no entry in DC, the value was either already passivated or removed by a subsequent
         // removal, or the command did not write anything (that shouldn't happen as we check if the command is successful)
         if (oldEntry != null) {
            Metadata oldMetadata;
            EntryVersion oldVersion;
            if ((oldMetadata = oldEntry.getMetadata()) == null || (oldVersion = oldMetadata.version()) == null) {
               // the record was written without version?
               lock(k, lockedFuture, waitFuture);
            } else {
               InequalVersionComparisonResult result = oldVersion.compareTo(version);
               switch (result) {
                  case AFTER:
                     // just ignore the write
                     break;
                  case EQUAL:
                     lock(k, lockedFuture, waitFuture);
                     break;
                  case BEFORE: // the actual version was not committed but we're here?
                  case CONFLICTING: // not used with numeric versions
                  default:
                     throw new IllegalStateException("DC version: " + oldVersion + ", cmd version " + version);
               }
            }
         }
         return oldEntry;
      });
      CompletableFuture<Void> wf = waitFuture.get();
      if (wf != null) {
         // multi-entry command schedule the timeout for all futures together. The composed futures can still come into
         // effect after the command throws timeout, but these will probably just find out newer versions in DC.
         if (deadline > Long.MIN_VALUE) {
            scheduleTimeout(wf, deadline, key);
         }
         return wf.thenCompose(nil -> checkLockAndStore(ctx, command, key, version, deadline));
      }
      CompletableFuture<Void> lf = lockedFuture.get();
      if (lf != null) {
         try {
            storeEntry(ctx, key, command);
            if (getStatisticsEnabled())
               cacheStores.incrementAndGet();
         } finally {
            if (!locks.remove(key, lf)) {
               throw new IllegalStateException("Noone but me should be able to replace the future");
            }
            lf.complete(null);
         }
      }
      return CompletableFutures.completedNull();
   }

   private CompletableFuture<Void> checkLockAndRemove(Object key) {
      ByRef<CompletableFuture<Void>> lockedFuture = new ByRef<>(null);
      ByRef<CompletableFuture<Void>> waitFuture = new ByRef<>(null);
      dataContainer.compute(key, (k, oldEntry, factory) -> {
         // if the entry is not null, the entry was not invalidated, or it was already written again
         if (oldEntry == null) {
            lock(k, lockedFuture, waitFuture);
         }
         return oldEntry;
      });
      CompletableFuture<Void> wf = waitFuture.get();
      if (wf != null) {
         return wf.thenCompose(nil -> checkLockAndRemove(key));
      }
      CompletableFuture<Void> lf = lockedFuture.get();
      if (lf != null) {
         try {
            DistributionInfo info = new DistributionInfo(key, dm.getWriteConsistentHash(), dm.getAddress());
            PersistenceManager.AccessMode mode = info.isPrimary() ?
                  PersistenceManager.AccessMode.BOTH : PersistenceManager.AccessMode.PRIVATE;
            persistenceManager.deleteFromAllStores(key, mode);
         } finally {
            if (!locks.remove(key, lf)) {
               throw new IllegalStateException("Noone but me should be able to replace the future");
            }
            lf.complete(null);
         }
      }
      return CompletableFutures.completedNull();
   }

   private void lock(Object key, ByRef<CompletableFuture<Void>> lockedFuture, ByRef<CompletableFuture<Void>> waitFuture) {
      CompletableFuture<Void> myFuture = new CompletableFuture<>();
      CompletableFuture<Void> prevFuture = locks.putIfAbsent(key, myFuture);
      if (prevFuture == null) {
         lockedFuture.set(myFuture);
      } else {
         waitFuture.set(prevFuture);
      }
   }

   private Object handleReadCommand(InvocationContext ctx, DataCommand command) {
      CompletableFuture<Void> wf = locks.get(command.getKey());
      if (wf != null) {
         return asyncInvokeNext(ctx, command, wf);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      List<CompletableFuture<Void>> wfs = null;
      for (Object key : command.getKeys()) {
         CompletableFuture<Void> wf = locks.get(key);
         if (wf != null) {
            if (wfs == null) wfs = new ArrayList<>();
            wfs.add(wf);
         }
      }
      if (wfs == null) {
         return invokeNext(ctx, command);
      } else if (wfs.size() == 1) {
         return asyncInvokeNext(ctx, command, wfs.get(0));
      } else {
         return asyncInvokeNext(ctx, command, CompletableFuture.allOf(wfs.toArray(new CompletableFuture[wfs.size()])));
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, dataWriteReturnHandler);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
      throws Throwable {
      return invokeNextThenApply(ctx, command, dataWriteReturnHandler);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, dataWriteReturnHandler);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, manyDataReturnHandler);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, dataWriteReturnHandler);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, dataWriteReturnHandler);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, dataWriteReturnHandler);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, dataWriteReturnHandler);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, manyDataReturnHandler);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, manyDataReturnHandler);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, manyDataReturnHandler);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, manyDataReturnHandler);
   }

   @Override
   public Object visitInvalidateVersionsCommand(InvocationContext ctx, InvalidateVersionsCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, invalidateReturnHandler);
   }

   @Override
   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      DistributionInfo info = new DistributionInfo(key, dm.getWriteConsistentHash(), dm.getAddress());
      return !info.isPrimary() || command.hasAnyFlag(FlagBitSets.SKIP_SHARED_CACHE_STORE);
   }
}
