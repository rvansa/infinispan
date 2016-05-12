package org.infinispan.remoting.transport.tcp;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.stack.IpAddress;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.factories.KnownComponentNames.GLOBAL_MARSHALLER;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TcpTransport implements Transport {
   private static final int HEADER_SIZE = Byte.BYTES + Long.BYTES + Integer.BYTES;
   private static final byte REQUEST = 1;
   private static final byte RESPONSE = 2;
   private static final byte ASYNC_MSG = 3;
   private static final long STAGGER_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(
      Integer.getInteger("infinispan.stagger.delay", 5));
   private static final boolean FAST_RESPONSE_HANDLE = Boolean.getBoolean("infinispan.tcptransport.fastresponse");
   private static final int PORT_OFFSET = Integer.getInteger("infinispan.tcptransport.portOffset", 10000);
   private static final Log log = LogFactory.getLog(TcpTransport.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int REQUEST_ID_OFFSET = Integer.BYTES + Byte.BYTES;

   private final JGroupsTransport jgroups = new JGroupsTransport();
   private StreamingMarshaller marshaller;
   private InboundInvocationHandler globalHandler;
   private ScheduledExecutorService timeoutExecutor;
   private ConcurrentMap<Address, Connection> connections = new ConcurrentHashMap<>();
   private ConcurrentMap<IpAddress, Connection> physical = new ConcurrentHashMap<>();
   private ServerSocketChannel serverSocketChannel;
   private Selector acceptSelector;
   private Executor executor;
   private TimeService timeService;

   @Inject
   public void initialize(GlobalConfiguration configuration,
                          @ComponentName(GLOBAL_MARSHALLER) StreamingMarshaller marshaller,
                          CacheManagerNotifier notifier, GlobalComponentRegistry gcr,
                          TimeService timeService, InboundInvocationHandler globalHandler,
                          @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor,
                          @ComponentName(KnownComponentNames.REMOTE_COMMAND_EXECUTOR) ExecutorService executorService) {
      this.marshaller = marshaller;
      this.globalHandler = globalHandler;
      this.timeoutExecutor = timeoutExecutor;
      this.timeService = timeService;
      jgroups.setConfiguration(configuration);
      jgroups.initialize(marshaller, new CacheManagerNotifierMock(notifier), gcr, timeService, globalHandler, timeoutExecutor, executorService);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception {
      CompletableFuture<Map<Address, Response>> future = invokeRemotelyAsync(recipients, rpcCommand, mode,
         timeout, responseFilter, deliverOrder, anycast);
      try {
         //no need to set a timeout for the future. The rpc invocation is guaranteed to complete within the timeout milliseconds
         return CompletableFutures.await(future);
      } catch (ExecutionException e) {
         throw Util.rewrapAsCacheException(e.getCause());
      }
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception {
      if (deliverOrder != DeliverOrder.NONE) {
         return jgroups.invokeRemotelyAsync(recipients, rpcCommand, mode, timeout, responseFilter, deliverOrder, anycast);
      }
      if (trace) {
         log.tracef("Invoking %s to %s, mode %s, filter %s", rpcCommand, recipients, mode, responseFilter);
      }
      boolean broadcast = false;
      if (recipients == null) {
         recipients = connections.keySet();
         broadcast = true;
      }
      org.infinispan.commons.io.ByteBuffer buf = marshaller.objectToBuffer(rpcCommand);
      Address singleRecipient = null;
      Address[] actualRecipients = null;
      LazyCompletableFuture<Response> singleFuture = null;
      LazyCompletableFuture<Response>[] futures;
      RelayingCompletableFuture<Map<Address, Response>> aggregateFuture = null;
      if (mode == ResponseMode.ASYNCHRONOUS) {
         futures = null;
      } else if (broadcast || recipients.size() != 1) {
         futures = new LazyCompletableFuture[recipients.size() + 1];
         actualRecipients = new Address[recipients.size() + 1];
      } else {
         futures = null;
      }
      int futureIndex = 0;
      long deadline = timeService.expectedEndTime(timeout, TimeUnit.MILLISECONDS);
      for (Iterator<Address> iterator = recipients.stream().filter(a -> !a.equals(getAddress())).iterator(); iterator.hasNext(); ) {
         Address a = iterator.next();
         LazyCompletableFuture<Response> responseFuture = null;
         if (mode != ResponseMode.ASYNCHRONOUS) {
            if (futures != null) {
               if (aggregateFuture == null) {
                  aggregateFuture = new RelayingCompletableFuture<>(futures.length);
               }
               responseFuture = new LazyCompletableFuture<>(aggregateFuture);
               int index = futureIndex++;
               futures[index] = responseFuture;
               actualRecipients[index] = a;
            } else {
               singleRecipient = a;
               singleFuture = responseFuture = new LazyCompletableFuture<>();
            }
         }
         if (mode == ResponseMode.WAIT_FOR_VALID_RESPONSE) {
            if (futureIndex == 1) { // we've already increased the futureIndex
               writeAndRecordRequest(getConnection(a), buf, responseFuture, deadline, iterator.hasNext());
            }
         } else {
            writeAndRecordRequest(getConnection(a), buf, responseFuture, deadline, true);
         }
      }
      if (mode == ResponseMode.WAIT_FOR_VALID_RESPONSE) {
         staggered(buf, futures, actualRecipients, 1, deadline);
      }
      if (broadcast) {
         // send to self as well
         if (mode == ResponseMode.ASYNCHRONOUS) {
            getExecutor().execute(() -> globalHandler.handleFromCluster(getAddress(), rpcCommand, reply -> {}, DeliverOrder.NONE));
         } else {
            int index = futureIndex++;
            if (aggregateFuture == null) {
               aggregateFuture = new RelayingCompletableFuture<>(futures.length);
            }
            LazyCompletableFuture<Response> future = new LazyCompletableFuture<>(aggregateFuture);
            getExecutor().execute(() -> globalHandler.handleFromCluster(getAddress(), rpcCommand, reply -> {
               if (reply instanceof Response) {
                  future.complete((Response) reply);
               } else {
                  future.completeExceptionally(new CacheException("Unexpected response" + reply));
               }
            }, DeliverOrder.NONE));
            futures[index] = future;
            actualRecipients[index] = getAddress();
         }
      }
      if (futures != null && futureIndex == 0) {
         return CompletableFutures.returnEmptyMap();
      }
      switch (mode) {
         case ASYNCHRONOUS:
            return CompletableFutures.returnEmptyMap();
         case SYNCHRONOUS:
         case SYNCHRONOUS_IGNORE_LEAVERS:
            if (singleFuture != null) {
               return handleSingleRecipient(singleRecipient, singleFuture, responseFilter);
            } else {
               return handleAllRecipients(actualRecipients, aggregateFuture, futures, futureIndex, responseFilter);
            }
         case WAIT_FOR_VALID_RESPONSE:
            if (singleFuture != null) {
               return handleSingleRecipient(singleRecipient, singleFuture, responseFilter);
            } else {
               return handleFirstRecipient(actualRecipients, aggregateFuture, futures, futureIndex, responseFilter);
            }
         default:
            throw new IllegalArgumentException();
      }
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder, boolean anycast) throws Exception {
      return jgroups.invokeRemotely(rpcCommands, mode, timeout, usePriorityQueue, responseFilter, totalOrder, anycast);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception {
      if (rpcCommands == null || rpcCommands.isEmpty()) {
         return Collections.emptyMap();
      }
      if (deliverOrder != DeliverOrder.NONE) {
         return jgroups.invokeRemotely(rpcCommands, mode, timeout, responseFilter, deliverOrder, anycast);
      }
      if (trace) {
         log.tracef("Invoking %s, mode %s, filter %s", rpcCommands, mode, responseFilter);
      }
      Address singleRecipient = null;
      Address[] actualRecipients = null;
      CompletableFuture<Response> singleFuture = null;
      CompletableFuture<Response>[] futures;
      RelayingCompletableFuture<Map<Address, Response>> aggregateFuture = null;
      if (mode == ResponseMode.ASYNCHRONOUS) {
         futures = null;
      } else if (rpcCommands.size() != 1) {
         futures = new CompletableFuture[rpcCommands.size()];
         actualRecipients = new Address[rpcCommands.size()];
      } else {
         futures = null;
      }
      int futureIndex = 0;

      long deadline = timeService.expectedEndTime(timeout, TimeUnit.MILLISECONDS);
      for (Map.Entry<Address, ReplicableCommand> entry : rpcCommands.entrySet()) {
         if (entry.getKey().equals(getAddress())) continue;
         LazyCompletableFuture<Response> responseFuture = null;
         org.infinispan.commons.io.ByteBuffer buf = marshaller.objectToBuffer(entry.getValue());
         if (mode != ResponseMode.ASYNCHRONOUS) {
            if (futures != null) {
               if (aggregateFuture == null) {
                  aggregateFuture = new RelayingCompletableFuture<>(futures.length);
               }
               int index = futureIndex++;
               futures[index] = responseFuture = new LazyCompletableFuture<>(aggregateFuture);
               actualRecipients[index] = entry.getKey();
            } else {
               singleRecipient = entry.getKey();
               singleFuture = responseFuture = new LazyCompletableFuture<>();
            }
         }
         Connection connection = getConnection(entry.getKey());
         writeAndRecordRequest(connection, buf, responseFuture, deadline, true);
      }
      switch (mode) {
         case ASYNCHRONOUS:
            return Collections.emptyMap();
         case SYNCHRONOUS:
         case SYNCHRONOUS_IGNORE_LEAVERS:
            // even with wait for valid response we wait for all targets
         case WAIT_FOR_VALID_RESPONSE:
            if (singleFuture != null) {
               return handleSingleRecipient(singleRecipient, singleFuture, responseFilter).get();
            } else {
               return handleAllRecipients(actualRecipients, aggregateFuture, futures, futureIndex, responseFilter).get();
            }
         default:
            throw new IllegalArgumentException();
      }
   }

   private boolean staggered(org.infinispan.commons.io.ByteBuffer buf, LazyCompletableFuture<Response>[] futures, Address[] recipients, int index, long deadline) {
      assert index > 0;
      if (index >= futures.length || futures[index] == null) {
         return false;
      }
      Runnable staggeredRequest = new Runnable() {
         @Override
         public void run() {
            if (trace) {
               log.tracef("Executing staggered request %08x", System.identityHashCode(this));
            }
            boolean hasNext = TcpTransport.this.staggered(buf, futures, recipients, index + 1, deadline);
            try {
               Connection connection = TcpTransport.this.getConnection(recipients[index]);
               TcpTransport.this.writeAndRecordRequest(connection, buf, futures[index], deadline, !hasNext);
            } catch (IOException e) {
               futures[index].completeExceptionally(e);
            }
         }
      };
      ScheduledFuture<?> next = timeoutExecutor.schedule(staggeredRequest, STAGGER_DELAY_NANOS, TimeUnit.NANOSECONDS);
      if (trace) {
         log.tracef("Scheduled staggered request %08x", System.identityHashCode(staggeredRequest));
      }
      for (int i = 0; i < index; ++i) {
         futures[i].thenRun(() -> next.cancel(false));
      }
      return true;
   }

   public void writeAndRecordRequest(Connection connection, org.infinispan.commons.io.ByteBuffer buf, LazyCompletableFuture<Response> responseFuture, long deadline, boolean hasDeadline) throws IOException {
      if (trace) {
         log.tracef("%s sending request(%s) %d bytes to %s = %08x", getAddress(), responseFuture != null ? "sync" : "async", buf.getLength(), connection.channel, System.identityHashCode(connection.channel));
      }
      ByteBuffer buffer = toNioByteBuffer(buf);
      long requestId = -1;
      synchronized (connection.writeSync) {
         ByteBuffer header = connection.writeHeaderBuffer;
         try {
            header.putInt(buf.getLength());
            if (responseFuture != null) {
               requestId = connection.requestId++;
               // we have to register the request before sending out the message to the wire, or we
               // might miss the response that arrives too soon
               connection.responses.put(requestId, responseFuture);
               header.put(REQUEST);
               header.putLong(requestId);
            } else {
               header.put(ASYNC_MSG);
            }
            header.flip();
            while (header.hasRemaining()) {
               connection.channel.write(header);
            }
            while (buffer.hasRemaining()) {
               connection.channel.write(buffer);
            }
         } catch (Exception e) {
            log.error("Error writing", e);
            throw e;
         } finally {
            header.clear();
         }
      }
      if (trace) {
         log.tracef("Sent request id %d (%08x)", requestId, System.identityHashCode(responseFuture));
      }
      if (responseFuture != null) {
         if (hasDeadline) {
            final long reqId = requestId;
            long timeout = timeService.remainingTime(deadline, TimeUnit.NANOSECONDS);
            ScheduledFuture<?> scheduled = timeoutExecutor.schedule(() -> responseTimeout(connection, responseFuture, reqId),
               timeout, TimeUnit.NANOSECONDS);
            responseFuture.whenComplete((r, t) -> {
               if (trace) {
                  log.tracef("Canceling scheduled timeout for %08x", System.identityHashCode(responseFuture));
               }
               scheduled.cancel(false);
            });
         }
      }
   }

   public void responseTimeout(Connection connection, CompletableFuture<Response> responseFuture, long requestId) {
      responseFuture.completeExceptionally(new TimeoutException());
      CompletableFuture<Response> removed = connection.responses.remove(requestId);
      if (trace) {
         log.tracef("Timing out request %s/%d (%08x = %08x)", connection.channel, requestId, System.identityHashCode(responseFuture), System.identityHashCode(removed));
      }
      assert removed == null || removed == responseFuture;
   }

   private void writeResponse(Connection connection, long requestId, Object reply) {
      try {
         org.infinispan.commons.io.ByteBuffer buf = marshaller.objectToBuffer(reply);
         if (trace) {
            log.tracef("sending response reqId %d, %d bytes to %s", requestId, buf.getLength(), connection.channel);
         }
         ByteBuffer byteBuffer = toNioByteBuffer(buf);
         synchronized (connection.writeSync) {
            ByteBuffer header = connection.writeHeaderBuffer;
            try {
               header.putInt(buf.getLength());
               header.put(RESPONSE);
               header.putLong(requestId);
               header.flip();
               while (header.hasRemaining()) {
                  connection.channel.write(header);
               }
               while (byteBuffer.hasRemaining()) {
                  connection.channel.write(byteBuffer);
               }
            } finally {
               header.clear();
            }
         }
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   public CompletableFuture<Map<Address, Response>> handleFirstRecipient(Address[] recipients, RelayingCompletableFuture<Map<Address, Response>> aggregateFuture, CompletableFuture<Response>[] futures, int targets, ResponseFilter responseFilter) {
      AtomicInteger missingResponses = new AtomicInteger(targets);
      for (int i = 0; i < targets; ++i) {
         final Address recipient = recipients[i];
         futures[i].whenComplete((response, throwable) -> {
            // the filter and map can be shared among threads but are unsafe
            if (throwable != null) {
               aggregateFuture.completeExceptionally(throwable);
            } else {
               if (responseFilter != null) {
                  Map<Address, Response> map = null;
                  synchronized (responseFilter) {
                     if (responseFilter.isAcceptable(response, recipient)) {
                        map = Collections.singletonMap(recipient, response);
                     } else if (!responseFilter.needMoreResponses()) {
                        map = Collections.emptyMap();
                     }
                  }
                  if (map != null) {
                     aggregateFuture.complete(map);
                  } else if (missingResponses.decrementAndGet() == 0) {
                     aggregateFuture.complete(Collections.emptyMap());
                  }
               } else {
                  aggregateFuture.complete(Collections.singletonMap(recipient, response));
               }
            }
         });
      }
      return aggregateFuture;
   }

   public CompletableFuture<Map<Address, Response>> handleAllRecipients(Address[] recipients, RelayingCompletableFuture<Map<Address, Response>> aggregateFuture, CompletableFuture<Response>[] futures, int targets, ResponseFilter responseFilter) {
      ResponseMap map = new ResponseMap(targets);
      for (int i = 0; i < targets; ++i) {
         final Address recipient = recipients[i];
         futures[i].whenComplete((response, throwable) -> {
            // the filter and map can be shared among threads but are unsafe
            if (throwable != null) {
               aggregateFuture.completeExceptionally(throwable);
            } else {
               boolean complete;
               synchronized (map) {
                  if (map.isDone()) {
                     // no more responses required
                     return;
                  }
                  if (responseFilter == null || responseFilter.isAcceptable(response, recipient)) {
                     map.put(recipient, response);
                  }
                  if (responseFilter != null && !responseFilter.needMoreResponses()) {
                     map.complete();
                     complete = true;
                  } else {
                     complete = map.received();
                  }
               }
               // the completion is synchronous, so move it out of the synchronized block
               // we have to use separate local variable (cannot just read isDone())
               if (complete) {
                  aggregateFuture.complete(map);
               }
            }
         });
      }
      return aggregateFuture;
   }

   public CompletableFuture<Map<Address, Response>> handleSingleRecipient(Address recipient, CompletableFuture<Response> future, ResponseFilter responseFilter) {
      return future.thenApply(response -> {
         if (trace) {
            log.tracef("3 Got response %s for %08x", response, System.identityHashCode(future));
         }
         if (responseFilter == null || responseFilter.isAcceptable(response, recipient)) {
            return Collections.singletonMap(recipient, response);
         } else {
            return Collections.emptyMap();
         }
      });
   }

   public ByteBuffer toNioByteBuffer(org.infinispan.commons.io.ByteBuffer byteBuffer) {
      return ByteBuffer.wrap(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength());
   }

   private void updateMembers(List<Address> members) {
      for (Address member : members) {
         if (!getAddress().equals(member) && !connections.containsKey(member)) {
            try {
               openChannel(member);
            } catch (IOException e) {
               throw new CacheException(e);
            }
         }
      }
      HashSet<Address> existing = new HashSet<>(connections.keySet());
      existing.removeAll(members);
      for (Address member : existing) {
         Connection connection = connections.remove(member);
         if (connection != null) {
            physical.remove(connection.ipAddress);
            try {
               connection.close();
            } catch (IOException e) {
               throw new CacheException(e);
            }
         }
      }
   }

   private Connection getConnection(Address member) throws IOException {
      Connection c = connections.get(member);
      if (c == null) {
         c = openChannel(member);
      }
      if (c.channel == null) {
         synchronized (c) {
            while (c.channel == null) {
               try {
                  c.wait();
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new CacheException(e);
               }
            }
         }
      }
      return c;
   }

   private int compare(byte[] a1, byte[] a2) {
      if (a1.length < a2.length) return -1;
      if (a1.length > a2.length) return 1;
      for (int i = a1.length - 1; i >= 0; --i) {
         if (a1[i] < a2[i]) return -1;
         if (a1[i] > a2[i]) return 1;
      }
      return 0;
   }

   private Connection openChannel(Address member) throws IOException {
      org.jgroups.Address jgroupsAddress = ((JGroupsAddress) member).getJGroupsAddress();
      IpAddress ipAddress;
      if (jgroupsAddress instanceof PhysicalAddress) {
         ipAddress = (IpAddress) jgroupsAddress;
      } else {
         ipAddress = getPhysicalAddress(member);
      }
      Connection connection = new Connection(member, ipAddress);
      Connection prev = physical.putIfAbsent(ipAddress, connection);
      if (prev != null) {
         synchronized (prev) {
            if (prev.address == null) {
               prev.address = member;
               if (connections.putIfAbsent(member, prev) != null) {
                  throw new IllegalStateException();
               }
            }
         }
         return prev;
      }
      if (connections.putIfAbsent(member, connection) != null) {
         throw new IllegalStateException();
      }
      // the channel should be opened by the member with lower ip & port
      IpAddress physicalAddress = getPhysicalAddress(getAddress());
      int compare = compare(physicalAddress.getIpAddress().getAddress(), ipAddress.getIpAddress().getAddress());
      if (compare == 0) {
         compare = Integer.compare(physicalAddress.getPort(), ipAddress.getPort());
      }
      if (compare == 0) {
         throw new IllegalArgumentException("Connecting to self");
      }
      if (compare < 0) {
         return connection;
      }
      synchronized (connection) {
         if (connection.channel == null) {
            log.debug("Opening channel to " + ipAddress);
            SocketChannel channel = SocketChannel.open(new InetSocketAddress(ipAddress.getIpAddress(), ipAddress.getPort() + PORT_OFFSET));
            channel.write(ByteBuffer.wrap(physicalAddress.getIpAddress().getAddress()));
            ByteBuffer portBuffer = ByteBuffer.allocate(Short.BYTES);
            portBuffer.putShort((short) (physicalAddress.getPort()));
            portBuffer.flip();
            channel.write(portBuffer);
            channel.socket().setTcpNoDelay(true);
            channel.socket().setKeepAlive(true);
            connection.channel = channel;

            getExecutor().execute(new ReadChannel(connection));
         }
         connection.notifyAll();
      }
      return connection;
   }

   private Executor getExecutor() {
      if (executor == null) {
         executor = jgroups.getChannel().getProtocolStack().getTransport().getOOBThreadPool();
      }
      return executor;
   }

   private class CacheManagerNotifierMock implements CacheManagerNotifier {
      private final CacheManagerNotifier notifier;

      public CacheManagerNotifierMock(CacheManagerNotifier notifier) {
         this.notifier = notifier;
      }

      @Override
      public void notifyViewChange(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId) {
         updateMembers(members);
         notifier.notifyViewChange(members, oldMembers, myAddress, viewId);
      }

      @Override
      public void notifyCacheStarted(String cacheName) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void notifyCacheStopped(String cacheName) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void notifyMerge(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, List<List<Address>> subgroupsMerged) {
         updateMembers(members);
         notifier.notifyMerge(members, oldMembers, myAddress, viewId, subgroupsMerged);
      }

      @Override
      public void addListener(Object listener) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void removeListener(Object listener) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Set<Object> getListeners() {
         throw new UnsupportedOperationException();
      }
   }

   private class AcceptConnection implements Runnable {
      @Override
      public void run() {
         try {
            acceptSelector.select();
            Iterator<SelectionKey> iterator = acceptSelector.selectedKeys().iterator();
            while (iterator.hasNext()) {
               SelectionKey next = iterator.next();
               iterator.remove();
               if (next.isValid()) {
                  SocketChannel channel = serverSocketChannel.accept();
                  channel.socket().setTcpNoDelay(true);
                  channel.socket().setKeepAlive(true);
                  InetAddress remoteAddress;
                  if (channel.getRemoteAddress() instanceof InetSocketAddress) {
                     remoteAddress = ((InetSocketAddress) channel.getRemoteAddress()).getAddress();
                  } else {
                     log.error("Cannot find out who remote side is: " + channel.getRemoteAddress());
                     channel.close();
                     continue;
                  }
                  int size = (remoteAddress instanceof Inet4Address ? Global.IPV4_SIZE : Global.IPV6_SIZE);
                  ByteBuffer buffer = ByteBuffer.allocate(size);
                  while (buffer.hasRemaining()) {
                     channel.read(buffer);
                  }
                  InetAddress remoteBindAddress = InetAddress.getByAddress(buffer.array());
                  buffer.clear();
                  buffer.limit(2);
                  while (buffer.hasRemaining()) {
                     channel.read(buffer);
                  }
                  int port = buffer.getShort(0);
                  port = port < 0 ? ~port : port; // high ports got messed as we can't write unsigned shorts
                  IpAddress ipAddress = new IpAddress(remoteBindAddress, port);
                  log.debug("Accepted connection from " + ipAddress);
                  Connection connection = physical.get(ipAddress);
                  if (connection != null) {
                     synchronized (connection) {
                        if (connection.channel != null) {
                           // this is a reconnect
                           connection.close();
                        }
                        connection.channel = channel;
                        connection.notifyAll();
                     }
                  } else {
                     connection = new Connection(null, ipAddress);
                     connection.channel = channel;
                     Connection prev = physical.putIfAbsent(ipAddress, connection);
                     if (prev != null) {
                        log.debug("Connection already exist");
                        synchronized (prev) {
                           if (prev.channel != null) {
                              // this is a reconnect
                              prev.close();
                           }
                           prev.channel = channel;
                           prev.notifyAll();
                        }
                        getExecutor().execute(new ReadChannel(prev));
                        return;
                     } else {
                        synchronized (connection) {
                           connection.notifyAll();
                        }
                     }
                  }
                  getExecutor().execute(new ReadChannel(connection));
               }
            }
         } catch (IOException e) {
            log.error("Cannot select keys", e);
            throw new CacheException(e);
         } finally {
            getExecutor().execute(this);
         }
      }
   }


   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) throws Exception {
      return jgroups.backupRemotely(backups, rpcCommand);
   }

   @Override
   public boolean isCoordinator() {
      return jgroups.isCoordinator();
   }

   @Override
   public Address getCoordinator() {
      return jgroups.getCoordinator();
   }

   @Override
   public Address getAddress() {
      return jgroups.getAddress();
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      return jgroups.getPhysicalAddresses();
   }

   @Override
   public List<Address> getMembers() {
      return jgroups.getMembers();
   }

   @Override
   public boolean isMulticastCapable() {
      return jgroups.isMulticastCapable();
   }

   @Override
   public void start() {
      jgroups.start();
      IpAddress jgroupsPhysicalAddress = getPhysicalAddress(getAddress());
      try {
         serverSocketChannel = ServerSocketChannel.open();
         serverSocketChannel.configureBlocking(false);
         serverSocketChannel.socket().bind(new InetSocketAddress(jgroupsPhysicalAddress.getIpAddress(), jgroupsPhysicalAddress.getPort() + PORT_OFFSET));
         acceptSelector = Selector.open();
         serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
      } catch (IOException e) {
         throw new CacheException(e);
      }
      // we'll hijack OOB TP
      getExecutor().execute(new AcceptConnection());
      updateMembers(jgroups.getMembers());
   }

   public IpAddress getPhysicalAddress(Address address) {
      return (IpAddress) jgroups.getChannel().down(new Event(Event.GET_PHYSICAL_ADDRESS, ((JGroupsAddress) address).getJGroupsAddress()));
   }

   @Override
   public void stop() {
      jgroups.stop();
   }

   @Override
   public int getViewId() {
      return jgroups.getViewId();
   }

   @Override
   public void waitForView(int viewId) throws InterruptedException {
      jgroups.waitForView(viewId);
   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public void checkTotalOrderSupported() {
      jgroups.checkTotalOrderSupported();
   }

   public class Connection {
      private volatile SocketChannel channel;
      private Object writeSync = new Object();
      private Address address;
      private final IpAddress ipAddress;
      private ByteBuffer writeHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE);
      // TODO: this is reset during reconnect
      private long requestId;
      private final ConcurrentMap<Long, LazyCompletableFuture<Response>> responses = new ConcurrentHashMap<>();
      // TODO: better pool!
      private final ConcurrentLinkedQueue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<>();

      private Connection(Address address, IpAddress ipAddress) {
         log.debugf("Creating connection to %s = %s", address, ipAddress);
         this.address = address;
         this.ipAddress = ipAddress;
      }

      public synchronized void close() throws IOException {
         log.debugf("Closing connection to %s = %s", address, ipAddress);
         if (channel != null) {
            channel.close();
         }
         for (CompletableFuture<Response> future : responses.values()) {
            future.completeExceptionally(new CacheException("Connection closed"));
         }
         responses.clear();
      }
   }

   private class ReadChannel implements Runnable {
      private final Connection connection;

      public ReadChannel(Connection connection) {
         this.connection = connection;
      }

      @Override
      public void run() {
         try {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            while (true) {
               int size;
               byte type;
               long requestId = -1;
               ByteBuffer readBuffer = connection.bufferPool.poll();
               try {
                  header.limit(HEADER_SIZE);
                  if (!readAtLeast(header, Integer.BYTES)) return;
                  size = header.getInt(0);
                  if (readBuffer == null || readBuffer.capacity() < size) {
                     // just throw away the old small buffer, we won't keep such buffers around
                     readBuffer = ByteBuffer.allocate(Math.max(size, 8192));
                  }
                  if (!readAtLeast(header, REQUEST_ID_OFFSET)) return;
                  type = header.get(Integer.BYTES);
                  assert type > 0 && type <= ASYNC_MSG;
                  if (type != ASYNC_MSG) {
                     if (!readAtLeast(header, Integer.BYTES + Byte.BYTES + Long.BYTES)) return;
                     requestId = header.getLong(REQUEST_ID_OFFSET);
                  } else {
                     if (header.position() > REQUEST_ID_OFFSET) {
                        readBuffer.put(header.array(), header.arrayOffset() + REQUEST_ID_OFFSET, header.position() - REQUEST_ID_OFFSET);
                     }
                  }
                  header.clear();
                  readBuffer.limit(size);
                  if (!readAtLeast(readBuffer, size)) return;
                  if (FAST_RESPONSE_HANDLE && type == RESPONSE) {
                     LazyCompletableFuture<Response> future = connection.responses.remove(requestId);
                     if (future != null) {
                        ByteBuffer buffer = readBuffer;
                        long reqId = requestId;
                        future.lazyComplete(() -> {
                           try {
                              Object object = marshaller.objectFromByteBuffer(buffer.array(), buffer.arrayOffset(), size);
                              if (trace) {
                                 log.tracef("Completing %s/%d (%08x) with %s", connection.channel, reqId, System.identityHashCode(future), object);
                              }
                              if (object == null || object instanceof Response) {
                                 future.complete((Response) object);
                              } else {
                                 future.completeExceptionally(new CacheException(object + " is not a response"));
                              }
                           } catch (Exception e) {
                              future.completeExceptionally(e);
                           } finally {
                              buffer.clear();
                              connection.bufferPool.add(buffer);
                           }
                        }, getExecutor());
                     } else {
                        // this happens when we get response after timeout
                        log.tracef("No future for request %s/%d", connection.channel, requestId);
                     }
                  } else {
                     getExecutor().execute(new HandleData(type, requestId, size, readBuffer, connection));
                  }
                  if (trace) {
                     log.tracef("read %s (%d) = %d bytes from %s = %08x", type == REQUEST ? "request" : type == RESPONSE ? "response" : "async_msg", requestId, size, connection.channel, System.identityHashCode(connection.channel));
                  }
               } catch (ClosedChannelException e) {
                  log.trace("Channel closed", e);
                  return;
               }
            }
         } catch (IOException e) {
            try {
               connection.close();
            } catch (IOException e1) {
               e1.addSuppressed(e);
               throw new CacheException(e1);
            }
            throw new CacheException(e);
         }
      }

      public boolean readAtLeast(ByteBuffer buffer, int position) throws IOException {
         while (buffer.position() < position) {
            if (connection.channel.read(buffer) < 0) {
               return false;
            }
         }
         return true;
      }
   }

   private class ResponseMap extends HashMap<Address, Response> {
      private int missingResponses;

      public ResponseMap(int missingResponses) {
         super(missingResponses);
         this.missingResponses = missingResponses;
      }

      public boolean received() {
         return --missingResponses == 0;
      }

      public void complete() {
         missingResponses = 0;
      }

      public boolean isDone() {
         return missingResponses == 0;
      }
   }

   public class HandleData implements Runnable {
      private final byte type;
      private final long requestId;
      private final int size;
      private final ByteBuffer buffer;
      private final Connection connection;

      public HandleData(byte type, long requestId, int size, ByteBuffer buffer, Connection connection) {
         this.type = type;
         this.requestId = requestId;
         this.size = size;
         this.buffer = buffer;
         this.connection = connection;
      }

      @Override
      public void run() {
         try {
            Object object = marshaller.objectFromByteBuffer(buffer.array(), buffer.arrayOffset(), size);
            switch (type) {
               case REQUEST:
                  if (trace) {
                     log.trace("Handling request " + requestId);
                  }
                  globalHandler.handleFromCluster(connection.address, (ReplicableCommand) object, reply -> writeResponse(connection, requestId, reply), DeliverOrder.NONE);
                  break;
               case ASYNC_MSG:
                  globalHandler.handleFromCluster(connection.address, (ReplicableCommand) object, reply -> {}, DeliverOrder.NONE);
                  break;
               case RESPONSE:
                  CompletableFuture<Response> future = connection.responses.remove(requestId);
                  if (future != null) {
                     if (trace) {
                        log.tracef("Completing %s/%d (%08x) with %s", connection.channel, requestId, System.identityHashCode(future), object);
                     }
                     if (object == null || object instanceof Response) {
                        future.complete((Response) object);
                     } else {
                        future.completeExceptionally(new CacheException(object + " is not a response"));
                     }
                  } else {
                     // this happens when we get response after timeout
                     log.tracef("No future for request %s/%d", connection.channel, requestId);
                  }
                  break;
            }
         } catch (ClassNotFoundException e) {
            throw new CacheException(e);
         } catch (IOException e) {
            throw new CacheException(e);
         } finally {
            buffer.clear();
            connection.bufferPool.add(buffer);
         }

      }
   }
}
