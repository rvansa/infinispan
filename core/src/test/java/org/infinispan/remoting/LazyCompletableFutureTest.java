package org.infinispan.remoting;

import org.infinispan.remoting.transport.tcp.LazyCompletableFuture;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "unit", testName = "remoting.TcpTransportTest")
public class LazyCompletableFutureTest {
   private static final long DELAY = TimeUnit.MILLISECONDS.toNanos(500);
   private ExecutorService executor = Executors.newCachedThreadPool();

   private void delayedBeforeLazyComplete(LazyCompletableFuture<Thread> future, AtomicReference<Thread> completing) {
      executor.submit(() -> {
         LockSupport.parkNanos(DELAY);
         completing.set(Thread.currentThread());
         future.lazyComplete(() -> future.complete(Thread.currentThread()), executor);
      });
   }

   private void delayedAfterLazyComplete(LazyCompletableFuture<Thread> future) {
      future.lazyComplete(() -> {
         future.complete(Thread.currentThread());
         LockSupport.parkNanos(DELAY);
      }, executor);
   }

   public void testInOtherThread1() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      delayedAfterLazyComplete(future);
      assertNotEquals(Thread.currentThread(), future.get());
   }

   public void testInOtherThread2() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      AtomicReference<Thread> completing = new AtomicReference<>();
      delayedBeforeLazyComplete(future, completing);
      assertNotEquals(completing.get(), future.get());
   }

   public void testInCallingThread1() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      delayedAfterLazyComplete(future);
      executor.submit(() -> {
         try {
            assertNotEquals(Thread.currentThread(), future.get());
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }).get();
   }

   public void testInCallingThread2() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      AtomicReference<Thread> completing = new AtomicReference<>();
      delayedBeforeLazyComplete(future, completing);
      executor.submit(() -> {
         try {
            assertEquals(Thread.currentThread(), future.get());
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }).get();
   }

   public void testThenApplyInOtherThread1() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      delayedAfterLazyComplete(future);
      assertNotEquals(Thread.currentThread(), future.thenApply(Function.identity()).get());
   }

   public void testThenApplyInOtherThread2() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      AtomicReference<Thread> completing = new AtomicReference<>();
      delayedBeforeLazyComplete(future, completing);
      assertEquals(Thread.currentThread(), future.thenApply(Function.identity()).get());
   }

   public void testThenApplyInCallingThread1() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      delayedAfterLazyComplete(future);
      executor.submit(() -> {
         try {
            assertNotEquals(Thread.currentThread(), future.thenApply(Function.identity()).get());
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }).get();
   }

   public void testThenApplyInCallingThread2() throws ExecutionException, InterruptedException {
      LazyCompletableFuture<Thread> future = new LazyCompletableFuture<>();
      AtomicReference<Thread> completing = new AtomicReference<>();
      delayedBeforeLazyComplete(future, completing);
      executor.submit(() -> {
         try {
            assertEquals(Thread.currentThread(), future.thenApply(Function.identity()).get());
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }).get();
   }
}
