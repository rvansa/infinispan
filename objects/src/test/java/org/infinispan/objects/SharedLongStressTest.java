package org.infinispan.objects;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objects.interceptors.CacheObjectInterceptor;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.protocols.TP;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "stress", testName = "objects.SharedLongStressTest")
public class SharedLongStressTest extends CacheObjectTest {
   private static final String COUNTER_DATA = "counterData";
   protected static final int NUM_INCREMENTS = 1 << 16;
   final int NUM_NODES = 4;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCacheManagers(NUM_NODES);
      // This stress test requires larger thread pool (by default tests use 5 OOB threads)
      for (EmbeddedCacheManager cm : cacheManagers) {
         TP tp = (TP) ((JGroupsTransport) cm.getTransport()).getChannel().getProtocolStack().getBottomProtocol();
         ThreadPoolExecutor oobThreadPool = (ThreadPoolExecutor) tp.getOOBThreadPool();
         oobThreadPool.setMaximumPoolSize(30);
      }
   }

   public void testUncontended() throws ExecutionException, InterruptedException {
      doTest(1);
   }

   public void testLowContention() throws ExecutionException, InterruptedException {
      doTest(4);
   }

   public void testHighContention() throws ExecutionException, InterruptedException {
      doTest(16);
   }

   private void doTest(final int numThreads) throws ExecutionException, InterruptedException {
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      ArrayList<Future> futures = new ArrayList<>(numThreads);
      final CyclicBarrier startBarrier = new CyclicBarrier(numThreads + 1);
      for (int i = 0; i < numThreads; ++i) {
         final int id = i;
         futures.add(executorService.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  startBarrier.await();
               } catch (Exception e) {
                  throw new IllegalStateException(e);
               }
               SharedLong counter = Lookup.lookupOrCreateSharedLong(cache(id % NUM_NODES), COUNTER_DATA, 0);
               for (int i = 0; i < NUM_INCREMENTS / numThreads; ++i) {
                  counter.getAndIncrement();
               }
            }
         }));
      }
      try {
         startBarrier.await();
      } catch (BrokenBarrierException e) {
         throw new IllegalStateException(e);
      }
      long start = System.nanoTime();
      for (Future f : futures) {
         f.get();
      }
      long end = System.nanoTime();
      assertEquals(Lookup.lookupSharedLong(cache(0), COUNTER_DATA).get(), NUM_INCREMENTS);
      log.infof("%d increments executed using %d threads in %d ms", NUM_INCREMENTS, numThreads, TimeUnit.NANOSECONDS.toMillis(end - start));
   }
}
