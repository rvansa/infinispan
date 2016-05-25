package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ByRef;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClearCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This interceptor mixes several functions:
 * A) replicates changes to other nodes
 * B) commits the entry
 * C) schedules invalidation
 *
 * On primary owner, the commit is executed before the change is replicated to other node. If the command
 * reads previous value and the version of entry in {@link org.infinispan.container.DataContainer} has changed
 * during execution {@link ConcurrentChangeException} is thrown and the command has to be retried.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteringInterceptor extends ClusteringInterceptor {
   private final static Log log = LogFactory.getLog(ScatteringInterceptor.class);
   private final static boolean trace = log.isTraceEnabled();

   protected ClusteringDependentLogic cdl;
   protected ScatteredVersionManager svm;
   protected GroupManager groupManager;
   protected TimeService timeService;
   private volatile Address cachedNextMember;
   private volatile int cachedNextMemberTopology = -1;
   private RpcOptions clearSyncOptions;
   private RpcOptions getRpcOptions;

   private final InvocationSuccessAction dataWriteCommandNoReadHandler = (rCtx, rCommand, rv) -> {
      DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
      CacheEntry entry = rCtx.lookupEntry(dataWriteCommand.getKey());
      boolean committed = commitSingleEntryIfNewer(entry, rCtx, dataWriteCommand);
      if (committed && rCtx.isOriginLocal() && !dataWriteCommand.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         svm.scheduleKeyInvalidation(dataWriteCommand.getKey(), ((NumericVersion) entry.getMetadata().version()).getVersion(), entry.isRemoved());
      }
   };

   private final InvocationSuccessAction putMapCommandHandler = (rCtx, rCommand, rv) -> {
      PutMapCommand putMapCommand = (PutMapCommand) rCommand;
      for (Object key : putMapCommand.getAffectedKeys()) {
         commitSingleEntryIfNewer(rCtx.lookupEntry(key), rCtx, rCommand);
         // this handler is called only for ST or when isOriginLocal() == false so we don't have to invalidate
      }
   };

   private final InvocationSuccessAction clearHandler = (rCtx, rCommand, rv) ->
         cdl.commitEntry(ClearCacheEntry.getInstance(), null, (ClearCommand) rCommand, rCtx, null, false);

   @Inject
   public void injectDependencies(GroupManager groupManager, ClusteringDependentLogic cdl, ScatteredVersionManager svm, TimeService timeService) {
      this.groupManager = groupManager;
      this.cdl = cdl;
      this.svm = svm;
      this.timeService = timeService;
   }

   @Start
   public void start() {
      clearSyncOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
      getRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.WAIT_FOR_VALID_RESPONSE, DeliverOrder.NONE).build();
   }

   private <T extends DataWriteCommand & MetadataAwareCommand> Object handleWriteCommand(InvocationContext ctx, T command) throws Throwable {
      CacheEntry cacheEntry = ctx.lookupEntry(command.getKey());
      NumericVersion seenVersion = getVersionOrNull(cacheEntry);

      CacheTopology cacheTopology = checkTopology(command);
      if (cacheTopology == null) {
         // This happens during preload
         cacheEntry.setMetadata(command.getMetadata());
         return commitSingleEntryOnReturn(ctx, command, cacheEntry, cacheEntry.getValue(), seenVersion);
      }

      DistributionInfo info = new DistributionInfo(command.getKey(), cacheTopology.getWriteConsistentHash(), rpcManager.getAddress());
      if (info.primary() == null) {
         throw new MissingOwnerException(cacheTopology.getTopologyId());
      }

      if (isLocalModeForced(command)) {
         CacheEntry contextEntry = cacheEntry;
         if (cacheEntry == null) {
            entryFactory.wrapExternalEntry(ctx, command.getKey(), null, true);
            contextEntry = ctx.lookupEntry(command.getKey());
         }
         if (command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            contextEntry.setMetadata(command.getMetadata());
         } else if (info.isPrimary()) {
            // let's allow local-mode writes on primary owner, preserving versions
            long nextVersion = svm.incrementVersion(info.getSegmentId());
            contextEntry.setMetadata(addVersion(command.getMetadata(), new NumericVersion(nextVersion)));
         }
         return commitSingleEntryOnReturn(ctx, command, contextEntry, contextEntry.getValue(), seenVersion);
      }

      if (ctx.isOriginLocal()) {
         if (info.isPrimary()) {
            Object seenValue = cacheEntry.getValue();
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) ->
               handleWriteOnOriginPrimary(rCtx, (T) rCommand, rv, cacheEntry, seenValue, seenVersion, cacheTopology, info));
         } else { // not primary owner
            CompletableFuture<Map<Address, Response>> rpcFuture =
               rpcManager.invokeRemotelyAsync(info.owners(), command, defaultSyncOptions);
            return asyncValue(rpcFuture.thenApply(responseMap ->
                  handleWritePrimaryResponse(ctx, command, responseMap)));
         }
      } else { // remote origin
         if (info.isPrimary()) {
            Object seenValue = cacheEntry.getValue();
            // TODO: the previous value is unreliable as this could be second invocation
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
               T cmd = (T) rCommand;
               if (!cmd.isSuccessful()) {
                  if (trace) log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", cmd);
                  return rv;
               }

               long nextVersion = svm.incrementVersion(info.getSegmentId());
               Metadata metadata = addVersion(cmd.getMetadata(), new NumericVersion(nextVersion));
               cacheEntry.setMetadata(metadata);
               cmd.setMetadata(metadata);

               if (cmd.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
                  commitSingleEntryIfNoChange(seenValue, seenVersion, cacheEntry, rCtx, cmd);
               } else {
                  commitSingleEntryIfNewer(cacheEntry, rCtx, cmd);
               }

               if (cmd.isReturnValueExpected()) {
                  return new MetadataImmortalCacheValue(rv, metadata);
               } else {
                  // force return value to be sent in the response (the version)
                  command.setFlagsBitSet(command.getFlagsBitSet() & ~(FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.SKIP_REMOTE_LOOKUP));
                  return metadata.version();
               }
            });
         } else {
            // The origin is primary and we're merely backup saving the data
            assert cacheEntry == null || command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK);
            CacheEntry contextEntry;
            if (cacheEntry == null) {
               entryFactory.wrapExternalEntry(ctx, command.getKey(), null, true);
               contextEntry = ctx.lookupEntry(command.getKey());
            } else {
               contextEntry = cacheEntry;
            }
            contextEntry.setMetadata(command.getMetadata());
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
               commitSingleEntryIfNewer(contextEntry, rCtx, rCommand);
               return null;
            });
         }
      }
   }

   private <T extends DataWriteCommand & MetadataAwareCommand> Object handleWriteOnOriginPrimary(InvocationContext ctx, T command, Object rv,
                                                                                                 CacheEntry cacheEntry, Object seenValue, NumericVersion seenVersion,
                                                                                                 CacheTopology cacheTopology, DistributionInfo info) {
      if (!command.isSuccessful()) {
         if (trace)
            log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", command);
         return rv;
      }

      // increment the version
      long nextVersion = svm.incrementVersion(info.getSegmentId());
      Metadata metadata = addVersion(command.getMetadata(), new NumericVersion(nextVersion));
      cacheEntry.setMetadata(metadata);
      command.setMetadata(metadata);

      boolean committed;
      if (command.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
         committed = commitSingleEntryIfNoChange(seenValue, seenVersion, cacheEntry, ctx, command);
      } else {
         committed = commitSingleEntryIfNewer(cacheEntry, ctx, command);
      }

      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      // When replicating to backup, we'll add skip ownership check since we're now on primary owner
      // and we have already committed the entry, reading the return value. If we got OTE from remote
      // site and the command would be retried, we could fail to do the retry/return wrong value.
      command.addFlags(FlagBitSets.SKIP_OWNERSHIP_CHECK);
      // TODO: maybe we should rather create a copy of the command with modifications...
      Address backup = getNextMember(cacheTopology);
      if (backup != null) {
         // error responses throw exceptions from JGroupsTransport
         CompletableFuture<Map<Address, Response>> rpcFuture =
            rpcManager.invokeRemotelyAsync(Collections.singletonList(backup), command, defaultSyncOptions);
         rpcFuture.thenRun(() -> {
            if (committed && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               svm.scheduleKeyInvalidation(command.getKey(), ((NumericVersion) cacheEntry.getMetadata().version()).getVersion(), cacheEntry.isRemoved());
            }
         });
         // Exception responses are thrown anyway and we don't expect any return values
         return asyncValue(rpcFuture.thenApply(ignore -> rv));
      } else {
         return rv;
      }
   }

   private <T extends DataWriteCommand & MetadataAwareCommand> Object handleWritePrimaryResponse(
         InvocationContext ctx, T command, Map<Address, Response> responseMap) {
      Response response = getSingleResponse(responseMap, command.getTopologyId());
      if (!response.isSuccessful()) {
         command.fail();
         return ((UnsuccessfulResponse) response).getResponseValue();
      }

      Object responseValue = ((SuccessfulResponse) response).getResponseValue();
      NumericVersion version;
      Object value;
      if (command.isReturnValueExpected()) {
         if (!(responseValue instanceof MetadataImmortalCacheValue)) {
            throw new CacheException("Expected MetadataImmortalCacheValue as response but it is " + responseValue);
         }
         MetadataImmortalCacheValue micv = (MetadataImmortalCacheValue) responseValue;
         version = (NumericVersion) micv.getMetadata().version();
         value = micv.getValue();
      } else {
         if (!(responseValue instanceof NumericVersion)) {
            throw new CacheException("Expected NumericVersion as response but it is " + responseValue);
         }
         version = (NumericVersion) responseValue;
         value = null;
      }
      Metadata metadata = addVersion(command.getMetadata(), version);

      // TODO: skip lookup by returning form entry factory directly
      entryFactory.wrapExternalEntry(ctx, command.getKey(), null, true);
      CacheEntry cacheEntry = ctx.lookupEntry(command.getKey());
      cacheEntry.setMetadata(metadata);
      // Primary succeeded, so apply the value locally
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      return invokeNextThenApply(ctx, command, (ctx1, command1, rv) -> {
         DataWriteCommand cmd = (DataWriteCommand) command1;
         // We don't care about the local value, as we use MATCH_ALWAYS on backup
         boolean committed = commitSingleEntryIfNewer(cacheEntry, ctx1, cmd);
         if (committed && !cmd.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            svm.scheduleKeyInvalidation(cmd.getKey(), ((NumericVersion) cacheEntry.getMetadata().version()).getVersion(), cacheEntry.isRemoved());
         }
         return value;
      });
   }

   private <T extends FlagAffectedCommand & TopologyAffectedCommand> CacheTopology checkTopology(T command) {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      if (cacheTopology == null) {
         return null;
      }
      if (!command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK) && command.getTopologyId() >= 0 && command.getTopologyId() != cacheTopology.getTopologyId()) {
         throw new OutdatedTopologyException("Command topology: " + command.getTopologyId() + ", cache topology: " + cacheTopology.getTopologyId());
      } else if (trace) {
         log.tracef("%s has topology %d (current is %d)", command, command.getTopologyId(), cacheTopology.getTopologyId());
      }
      return cacheTopology;
   }

   private Object commitSingleEntryOnReturn(InvocationContext ctx, DataWriteCommand command, CacheEntry cacheEntry, Object prevValue, NumericVersion prevVersion) {
      if (command.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
            boolean committed = commitSingleEntryIfNoChange(prevValue, prevVersion, cacheEntry, rCtx, rCommand);
            if (committed && rCtx.isOriginLocal() && !dataWriteCommand.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               svm.scheduleKeyInvalidation(dataWriteCommand.getKey(), ((NumericVersion) cacheEntry.getMetadata().version()).getVersion(), cacheEntry.isRemoved());
            }
         });
      } else {
         return invokeNextThenAccept(ctx, command, dataWriteCommandNoReadHandler);
      }
   }

   private boolean commitSingleEntryIfNewer(CacheEntry entry, InvocationContext ctx, VisitableCommand command) {
      if (!entry.isChanged()) {
         if (trace) {
            log.tracef("Entry has not changed, not committing");
            return false;
         }
      }
      // ignore metadata argument and use the one from entry, as e.g. PutMapCommand passes its metadata
      // here and we need own metadata for each entry.
      // RemoveCommand does not null the entry value
      if (entry.isRemoved()) {
         entry.setValue(null);
      }

      // We cannot delegate the dataContainer.compute() to entry.commit() as we need to reliably
      // retrieve previous value and metadata, but the entry API does not provide these.
      ByRef<Object> previousValue = new ByRef<>(null);
      ByRef<Metadata> previousMetadata = new ByRef<>(null);
      ByRef.Boolean successful = new ByRef.Boolean(false);
      dataContainer.compute(entry.getKey(), (key, oldEntry, factory) -> {
         // newMetadata is null in case of local-mode write
         Metadata newMetadata = entry.getMetadata();
         if (oldEntry == null) {
            if (entry.getValue() == null && newMetadata == null) {
               if (trace) {
                  log.trace("No previous record and this is a removal, not committing anything.");
               }
               return null;
            } else {
               if (trace) {
                  log.trace("Committing new entry " + entry);
               }
               successful.set(true);
               return factory.create(entry);
            }
         }
         Metadata oldMetadata = oldEntry.getMetadata();
         InequalVersionComparisonResult comparisonResult;
         if (oldMetadata == null || oldMetadata.version() == null || newMetadata == null || newMetadata.version() == null
            || (comparisonResult = oldMetadata.version().compareTo(newMetadata.version())) == InequalVersionComparisonResult.BEFORE
            || (oldMetadata instanceof RemoteMetadata && comparisonResult == InequalVersionComparisonResult.EQUAL)) {
            previousValue.set(oldEntry.getValue());
            previousValue.set(oldMetadata);
            if (trace) {
               log.tracef("Committing entry %s, replaced %s", entry, oldEntry);
            }
            successful.set(true);
            if (entry.getValue() != null || newMetadata != null) {
               return factory.create(entry);
            } else {
               return null;
            }
         } else {
            if (trace) {
               log.tracef("Not committing %s, current entry is %s", entry, oldEntry);
            }
            return oldEntry;
         }
      });

      boolean created = entry.isCreated();
      boolean removed = entry.isRemoved();
      boolean expired = false;
      if (removed && entry instanceof MVCCEntry) {
         expired = ((MVCCEntry) entry).isExpired();
      }

      if (successful.get()) {
         cdl.notifyCommitEntry(created, removed, expired, entry, ctx, (FlagAffectedCommand) command, previousValue.get(), previousMetadata.get());
         return true;
      } else {
         // TODO: document me
         // we skip the notification, and the already executed notification skipped this (intermediate) update
         return false;
      }
   }

   private boolean commitSingleEntryIfNoChange(Object seenValue, NumericVersion seenVersion, CacheEntry entry, InvocationContext ctx, VisitableCommand command) {
      if (!entry.isChanged()) {
         if (trace) {
            log.tracef("Entry has not changed, not committing");
            return false;
         }
      }
      // ignore metadata argument and use the one from entry, as e.g. PutMapCommand passes its metadata
      // here and we need own metadata for each entry.
      // RemoveCommand does not null the entry value
      if (entry.isRemoved()) {
         entry.setValue(null);
      }

      // We cannot delegate the dataContainer.compute() to entry.commit() as we need to reliably
      // retrieve previous value and metadata, but the entry API does not provide these.
      ByRef<Object> previousValue = new ByRef<>(null);
      ByRef<Metadata> previousMetadata = new ByRef<>(null);
      ByRef.Boolean successful = new ByRef.Boolean(false);
      dataContainer.compute(entry.getKey(), (key, oldEntry, factory) -> {
         // newMetadata is null in case of local-mode write on non-primary owners
         Metadata newMetadata = entry.getMetadata();
         if (oldEntry == null) {
            if (seenValue != null) {
               if (trace) {
                  log.trace("Non-null value in context, not committing");
               }
               throw new ConcurrentChangeException();
            }
            if (entry.getValue() == null && newMetadata == null) {
               if (trace) {
                  log.trace("No previous record and this is a removal, not committing anything.");
               }
               return null;
            } else {
               if (trace) {
                  log.trace("Committing new entry " + entry);
               }
               successful.set(true);
               return factory.create(entry);
            }
         }
         Metadata oldMetadata = oldEntry.getMetadata();
         NumericVersion oldVersion = oldMetadata == null ? null : (NumericVersion) oldMetadata.version();
         if (oldVersion == null) {
            if (seenVersion != null) {
               if (trace) {
                  log.tracef("Current version is null but seen version is %s, throwing", seenVersion);
               }
               throw new ConcurrentChangeException();
            }
         } else if (seenVersion == null) {
            if (oldEntry.canExpire() && oldEntry.isExpired(timeService.wallClockTime())) {
               if (trace) {
                  log.trace("Current entry is expired and therefore we haven't seen it");
               }
            } else {
               if (trace) {
                  log.tracef("Current version is %s but seen version is null, throwing", oldVersion);
               }
               throw new ConcurrentChangeException();
            }
         } else if (seenVersion.compareTo(oldVersion) != InequalVersionComparisonResult.EQUAL) {
            if (trace) {
               log.tracef("Current version is %s but seen version is %s, throwing", oldVersion, seenVersion);
            }
            throw new ConcurrentChangeException();
         }
         InequalVersionComparisonResult comparisonResult;
         if (oldVersion == null || newMetadata == null || newMetadata.version() == null
            || (comparisonResult = oldMetadata.version().compareTo(newMetadata.version())) == InequalVersionComparisonResult.BEFORE
            || (oldMetadata instanceof RemoteMetadata && comparisonResult == InequalVersionComparisonResult.EQUAL)) {
            previousValue.set(oldEntry.getValue());
            previousValue.set(oldMetadata);
            if (trace) {
               log.tracef("Committing entry %s, replaced %s", entry, oldEntry);
            }
            successful.set(true);
            if (entry.getValue() != null || newMetadata != null) {
               return factory.create(entry);
            } else {
               return null;
            }
         } else {
            if (trace) {
               log.tracef("Not committing %s, current entry is %s", entry, oldEntry);
            }
            return oldEntry;
         }
      });

      boolean created = entry.isCreated();
      boolean removed = entry.isRemoved();
      boolean expired = false;
      if (removed && entry instanceof MVCCEntry) {
         expired = ((MVCCEntry) entry).isExpired();
      }

      if (successful.get()) {
         cdl.notifyCommitEntry(created, removed, expired, entry, ctx, (FlagAffectedCommand) command, previousValue.get(), previousMetadata.get());
         return true;
      } else {
         // TODO: document me
         // we skip the notification, and the already executed notification skipped this (intermediate) update
         return false;
      }
   }

   private NumericVersion getVersionOrNull(CacheEntry cacheEntry) {
      if (cacheEntry == null) {
         return null;
      }
      Metadata metadata = cacheEntry.getMetadata();
      if (metadata != null) {
         return (NumericVersion) metadata.version();
      }
      return null;
   }

   private static Metadata addVersion(Metadata metadata, EntryVersion nextVersion) {
      Metadata.Builder builder;
      if (metadata == null) {
         builder = new EmbeddedMetadata.Builder();
      } else {
         builder = metadata.builder();
      }
      metadata = builder.version(nextVersion).build();
      return metadata;
   }

   private Address getNextMember(CacheTopology cacheTopology) {
      if (cacheTopology.getTopologyId() == cachedNextMemberTopology) {
         return cachedNextMember;
      }
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      List<Address> members = ch.getMembers();
      Address address = rpcManager.getAddress();
      Address nextMember = null;
      if (members.size() > 1) {
         for (int i = 0; i < members.size(); ++i) {
            Address member = members.get(i);
            if (member.equals(address)) {
               if (i + 1 < members.size()) {
                  nextMember = members.get(i + 1);
               } else {
                  nextMember = members.get(0);
               }
               break;
            }
         }
      }
      cachedNextMember = nextMember;
      cachedNextMemberTopology = cacheTopology.getTopologyId();
      return nextMember;
   }

   private Object handleReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      CacheTopology cacheTopology = checkTopology(command);
      // SKIP_OWNERSHIP_CHECK is added when the entry is prefetched from remote node
      // TODO: local lookup and hinted read

      // ClusteredGetCommand invokes local-mode forced read, but we still have to check for primary owner
      // Scattered cache always uses only writeCH
      DistributionInfo info = new DistributionInfo(command.getKey(), cacheTopology.getReadConsistentHash(), rpcManager.getAddress());
      if (info.primary() == null) {
         throw new MissingOwnerException(cacheTopology.getTopologyId());
      }

      if (info.isPrimary()) {
         if (trace) {
            log.tracef("In topology %d this is primary owner", cacheTopology.getTopologyId());
         }
         return invokeNext(ctx, command);
      } else if (command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK)) {
         if (trace) {
            log.trace("Ignoring ownership");
         }
         return invokeNext(ctx, command);
      } else if (ctx.isOriginLocal()) {
         if (isLocalModeForced(command) || command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP)) {
            if (ctx.lookupEntry(command.getKey()) == null) {
               entryFactory.wrapExternalEntry(ctx, command.getKey(), NullCacheEntry.getInstance(), false);
            }
            return invokeNext(ctx, command);
         }
         ClusteredGetCommand clusteredGetCommand = cf.buildClusteredGetCommand(command.getKey(), command.getFlagsBitSet());
         CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(Collections.singletonList(info.primary()), clusteredGetCommand, getRpcOptions);
         Object key = clusteredGetCommand.getKey();
         return asyncInvokeNext(ctx, command, rpcFuture.thenAccept(responseMap -> {
            Response response = getSingleResponse(responseMap, cacheTopology.getTopologyId());
            if (response.isSuccessful()) {
               InternalCacheValue value = (InternalCacheValue) ((SuccessfulResponse) response).getResponseValue();
               if (value != null) {
                  InternalCacheEntry cacheEntry = value.toInternalCacheEntry(key);
                  entryFactory.wrapExternalEntry(ctx, key, cacheEntry, false);
               } else {
                  entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), false);
               }
            } else if (response.isValid()) {
               throw OutdatedTopologyException.getCachedInstance();
            } else {
               throw new CacheException("Invalid retrieval:" + response);
            }
         }));
      } else {
         return UnsureResponse.INSTANCE;
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      CacheTopology cacheTopology = checkTopology(command);

      Map<Object, Object> originalMap = command.getMap();
      if (command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         extractAndSetMetadata(ctx, command, originalMap);
         return invokeNextThenAccept(ctx, command, putMapCommandHandler);
      }

      if (ctx.isOriginLocal()) {
         return invokeNextThenApply(ctx, command, (returnCtx, returnCommand, rv) ->
            handlePutMapOnOrigin(returnCtx, (PutMapCommand) returnCommand, rv, originalMap, cacheTopology));
      }

      // Remote
      if (command.isForwarded()) {
         // carries entries with version to back them up
         extractAndSetMetadata(ctx, command, originalMap);
         return invokeNextThenAccept(ctx, command, putMapCommandHandler);
      } else {
         // this node should be the primary
         ConsistentHash writeCH = cacheTopology.getWriteConsistentHash();

         Map<Object, NumericVersion> versionMap = new HashMap<>(originalMap.size());
         for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
            Object key = entry.getKey();
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry == null) {
               throw new IllegalStateException("Not wrapped " + key);
            }
            NumericVersion version = new NumericVersion(svm.incrementVersion(writeCH.getSegment(key)));
            cacheEntry.setMetadata(addVersion(command.getMetadata(), version));
            versionMap.put(key, version);
         }
         // disable ignore return values as this controls isReturnValueExpected with versionMap
         command.setFlagsBitSet(command.getFlagsBitSet() & ~FlagBitSets.IGNORE_RETURN_VALUES);
         return invokeNextThenApply(ctx, command, (ctx1, command1, rv) -> {
            for (Object key : ((PutMapCommand) command1).getAffectedKeys()) {
               commitSingleEntryIfNewer(ctx1.lookupEntry(key), ctx1, command1);
            }
            return versionMap;
         });
      }
   }

   private Object handlePutMapOnOrigin(InvocationContext ctx, PutMapCommand command, Object rv, Map<Object, Object> originalMap, CacheTopology cacheTopology) {
      PutMapCommand putMapCommand = command;
      if (!putMapCommand.isSuccessful()) {
         return null;
      }
      ConsistentHash writeCH = cacheTopology.getWriteConsistentHash();

      Map<Object, CacheEntry> lookedUpEntries = ctx.getLookedUpEntries();
      Map<Address, Map<Object, Object>> remoteEntries = new HashMap<>();
      Map<Object, InternalCacheValue> localEntries = new HashMap<>();
      for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
         Object key = entry.getKey();
         DistributionInfo info = new DistributionInfo(key, writeCH, rpcManager.getAddress());
         if (info.isPrimary()) {
            CacheEntry ctxEntry = lookedUpEntries.get(key);
            if (ctxEntry == null) {
               throw new CacheException("Entry not looked up for " + key);
            }
            long version = svm.incrementVersion(writeCH.getSegment(key));
            Metadata metadata = new EmbeddedMetadata.Builder().version(new NumericVersion(version)).build();
            ctxEntry.setMetadata(metadata);
            localEntries.put(key, new MetadataImmortalCacheValue(entry.getValue(), metadata));
            commitSingleEntryIfNewer(ctxEntry, ctx, putMapCommand);
         } else {
            Map<Object, Object> currentEntries = remoteEntries.computeIfAbsent(info.primary(), k -> new HashMap<>());
            currentEntries.put(key, entry.getValue());
         }
      }

      AtomicInteger expectedResponses = new AtomicInteger(remoteEntries.size());
      CompletableFuture<Object> allFuture = new CompletableFuture<>();
      if (!localEntries.isEmpty()) {
         Address backup = getNextMember(cacheTopology);
         if (backup != null) {
            expectedResponses.incrementAndGet();
            // note: we abuse PutMapCommand a bit as we need it to transport versions as well, and it can
            // carry only single Metadata instance.
            PutMapCommand backupCommand = cf.buildPutMapCommand(localEntries, putMapCommand.getMetadata(), putMapCommand.getFlagsBitSet());
            backupCommand.setForwarded(true);
            backupCommand.addFlags(FlagBitSets.SKIP_LOCKING); // TODO: remove locking in handler
            rpcManager.invokeRemotelyAsync(Collections.singleton(backup), backupCommand, defaultSyncOptions).whenComplete((r, t) -> {
               if (t != null) {
                  allFuture.completeExceptionally(t);
               } else {
                  if (expectedResponses.decrementAndGet() == 0) {
                     allFuture.complete(rv);
                  }
                  for (Map.Entry<Object, InternalCacheValue> entry : localEntries.entrySet()) {
                     svm.scheduleKeyInvalidation(entry.getKey(), ((NumericVersion) entry.getValue().getMetadata().version()).getVersion(), false);
                  }
               }
            });
         }
      }
      for (Map.Entry<Address, Map<Object, Object>> ownerEntry : remoteEntries.entrySet()) {
         Map<Object, Object> entries = ownerEntry.getValue();
         if (entries.isEmpty()) {
            if (expectedResponses.decrementAndGet() == 0) {
               allFuture.complete(rv);
            }
            continue;
         }
         Address owner = ownerEntry.getKey();
         PutMapCommand toPrimary = cf.buildPutMapCommand(entries, putMapCommand.getMetadata(), putMapCommand.getFlagsBitSet());
         CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(
            Collections.singletonList(owner), toPrimary, defaultSyncOptions);
         rpcFuture.whenComplete((responseMap, t) -> {
            if (t != null) {
               allFuture.completeExceptionally(t);
            }
            Response response = getSingleResponse(responseMap, putMapCommand.getTopologyId());
            if (response.isSuccessful()) {
               Object responseValue = ((SuccessfulResponse) response).getResponseValue();
               if (!(responseValue instanceof Map)) {
                  allFuture.completeExceptionally(new CacheException("Reponse from " + owner + ": expected Map<?, NumericVersion> but it is " + responseValue).fillInStackTrace());
               }
               Map<Object, NumericVersion> versions = (Map<Object, NumericVersion>) responseValue;
               synchronized (ctx) {
                  for (Map.Entry<Object, NumericVersion> entry : versions.entrySet()) {
                     // we will serve as the backup
                     entryFactory.wrapExternalEntry(ctx, entry.getKey(), null, true);
                     CacheEntry cacheEntry = ctx.lookupEntry(entry.getKey());
                     Metadata metadata = addVersion(putMapCommand.getMetadata(), entry.getValue());
                     cacheEntry.setValue(originalMap.get(entry.getKey()));
                     cacheEntry.setMetadata(metadata);
                     // we don't care about setCreated() since backup owner should not fire listeners
                     cacheEntry.setChanged(true);
                     boolean committed = commitSingleEntryIfNewer(cacheEntry, ctx, putMapCommand);
                     if (committed && !putMapCommand.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
                        svm.scheduleKeyInvalidation(entry.getKey(), entry.getValue().getVersion(), false);
                     }
                  }
               }
               if (expectedResponses.decrementAndGet() == 0) {
                  allFuture.complete(rv);
               }
            } else {
               allFuture.completeExceptionally(new CacheException("Received unsuccessful response from " + owner + ": " + response));
            }
         });
      }
      return asyncValue(allFuture);
   }

   protected void extractAndSetMetadata(InvocationContext ctx, PutMapCommand command, Map<Object, Object> originalMap) {
      Map<Object, Object> valueMap = new HashMap<>(originalMap.size());
      for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
         Object key = entry.getKey();
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         if (cacheEntry == null) {
            // since this is executed on backup node (or by ST), the entry was not wrapped
            entryFactory.wrapExternalEntry(ctx, key, null, true);
            cacheEntry = ctx.lookupEntry(key);
         }
         InternalCacheValue value = (InternalCacheValue) entry.getValue();
         Metadata entryMetadata = command.getMetadata() == null ? value.getMetadata()
            : command.getMetadata().builder().version(value.getMetadata().version()).build();
         cacheEntry.setMetadata(entryMetadata);
         valueMap.put(key, value.getValue());
      }
      command.setMap(valueMap);
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
      CacheTopology cacheTopology = checkTopology(command);
      // The SKIP_OWNERSHIP_CHECK is added when the entries are prefetches from remote node

      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP | FlagBitSets.SKIP_OWNERSHIP_CHECK)) {
         return invokeNext(ctx, command);
      }

      if (ctx.isOriginLocal()) {
         Address localAddress = rpcManager.getAddress();
         ConsistentHash readCH = cacheTopology.getReadConsistentHash();

         Map<Address, List<Object>> remoteKeys = new HashMap<>();
         for (Object key : command.getKeys()) {
            DistributionInfo info = new DistributionInfo(key, readCH, localAddress);
            if (!info.isPrimary()) {
               remoteKeys.computeIfAbsent(info.primary(), k -> new ArrayList<>()).add(key);
            }
         }

         if (remoteKeys.isEmpty()) {
            return invokeNext(ctx, command);
         }
         ResponseSync sync = new ResponseSync(remoteKeys.size(), cacheTopology.getTopologyId());
         for (Map.Entry<Address, List<Object>> remote : remoteKeys.entrySet()) {
            List<Object> keys = remote.getValue();
            ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(keys, command.getFlagsBitSet(), null);
            CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(Collections.singleton(remote.getKey()), clusteredGetAllCommand, defaultSyncOptions);
            rpcFuture.whenComplete(((responseMap, throwable) -> handleGetAllResponse(responseMap, throwable, ctx, keys, sync)));
         }
         return asyncInvokeNext(ctx, command, sync);
      } else { // remote
         for (Object key : command.getKeys()) {
            if (ctx.lookupEntry(key) == null) {
               // TODO: We cannot use UnsureResponse as EntryWrappingInterceptor casts result to map for notification
               return null;
            }
         }
         return invokeNext(ctx, command);
      }
   }

   private void handleGetAllResponse(Map<Address, Response> responseMap, Throwable throwable, InvocationContext ctx,
                                     List<?> keys, ResponseSync sync) {
      if (throwable != null) {
         sync.completeExceptionally(throwable);
      } else {
         try {
            Response response = getSingleResponse(responseMap, sync.getTopologyId());
            if (response.isSuccessful()) {
               InternalCacheValue[] values = (InternalCacheValue[]) ((SuccessfulResponse) response).getResponseValue();
               if (values != null) {
                  if (keys.size() != values.length) {
                     sync.completeExceptionally(new CacheException("Request and response lengths differ: keys=" + keys + ", response=" + Arrays.toString(values)));
                     return;
                  }
                  synchronized (sync) {
                     for (int i = 0; i < values.length; ++i) {
                        Object key = keys.get(i);
                        InternalCacheValue value = values[i];
                        CacheEntry entry = value == null ? NullCacheEntry.getInstance() : value.toInternalCacheEntry(key);
                        entryFactory.wrapExternalEntry(ctx, key, entry, false);
                     }
                     if (--sync.expectedResponses == 0) {
                        sync.complete(null);
                     }
                  }
               } else {
                  // the result is null because the node is not primary owner of those entries
                  sync.completeExceptionally(OutdatedTopologyException.getCachedInstance());
               }
            } else {
               sync.completeExceptionally(new CacheException("Unsuccessful response: " + response));
            }
         } catch (RuntimeException e) {
            sync.completeExceptionally(e);
         }
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      // local mode clear will have unpredictable results
      svm.clearInvalidations();
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         RpcOptions rpcOptions = isSynchronous(command) ? clearSyncOptions : defaultAsyncOptions;
         return makeStage(asyncInvokeNext(ctx, command, rpcManager.invokeRemotelyAsync(null, command, rpcOptions)))
               .thenAccept(ctx, command, clearHandler);
      } else {
         return invokeNextThenAccept(ctx, command, clearHandler);
      }
   }

   protected static Response getSingleResponse(Map<Address, Response> responseMap, int topologyId) {
      if (responseMap.isEmpty()) {
         if (trace) log.trace("No response!");
         return null;
      } else if (responseMap.size() > 1) {
         throw new IllegalArgumentException("Expected single response, got " + responseMap);
      }
      Response response = responseMap.values().iterator().next();
      if (response.isValid()) {
         return response;
      }
      if (response instanceof CacheNotFoundResponse) {
         // This means the cache wasn't running on the primary owner, so the command wasn't executed.
         // It is also possible that the primary owner is not a member of view, as we're degraded.
         throw new MissingOwnerException(topologyId);
      }
      Throwable cause = null;
      if (response instanceof ExceptionResponse) {
         cause = ((ExceptionResponse) response).getException();
      }
      throw new CacheException("Got unsuccessful response from primary owner: " + response, cause);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public final Object visitGetKeysInGroupCommand(InvocationContext ctx,
                                                                   GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return invokeNext(ctx, command);
      }
      CompletableFuture<Void> future = rpcManager.invokeRemotelyAsync(
            Collections.singleton(groupManager.getPrimaryOwner(groupName)),
            command, defaultSyncOptions).thenAccept(responses -> {
         if (!responses.isEmpty()) {
            Response response = responses.values().iterator().next();
            if (response instanceof SuccessfulResponse) {
               //noinspection unchecked
               List<CacheEntry> cacheEntries =
                  (List<CacheEntry>) ((SuccessfulResponse) response).getResponseValue();
               for (CacheEntry entry : cacheEntries) {
                  entryFactory.wrapExternalEntry(ctx, entry.getKey(), entry, false);
               }
            }
         }
      });
      return asyncInvokeNext(ctx, command, future);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private static class ResponseSync extends CompletableFuture<Void> {
      private int expectedResponses;
      private final int topologyId;

      public ResponseSync(int expectedResponses, int topologyId) {
         this.expectedResponses = expectedResponses;
         this.topologyId = topologyId;
      }

      public int getTopologyId() {
         return topologyId;
      }
   }
}
