package org.infinispan.objects;

import static org.testng.AssertJUnit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests implementation of {@link org.infinispan.objects.SharedLong}
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
public class SharedLongTest extends CacheObjectTest {
   private static final String COUNTER_INSTANCE = "counterInstance";
   private static final String COUNTER_DATA = "counterData";
   protected static final int NUM_OPS = 100;
   private final ExecutorService executorService = Executors.newFixedThreadPool(3);

   @AfterClass
   protected void tearDown() throws InterruptedException {
      executorService.shutdownNow();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCacheManagers(3);
   }

   public void testCommonOps() {
      SharedLong[] counters = createCounters(42);
      assertEquals(42, counters[1].getAndSet(48));
      assertEquals(48, counters[0].get());
      assertEquals(48, counters[2].get());
      assertFalse(counters[2].compareAndSet(31, 35));
      assertEquals(48, counters[1].get());
      assertTrue(counters[2].compareAndSet(48, 37));
      assertEquals(37, counters[0].get());
      assertEquals(37, counters[1].getAndAdd(3));
      assertEquals(42, counters[2].addAndGet(2));
   }

   public void testIncrementAndGet() throws Exception {
      long initialValue = 0;
      SharedLong[] counters = createCounters(initialValue);
      test(counters, new Operation() {
         public long run(SharedLong counter, long prevValue, boolean[] valuesSeen) {
            long currentValue = counter.incrementAndGet();
            assertFalse(valuesSeen[(int) currentValue - 1]);
            valuesSeen[(int) currentValue - 1] = true;
            assertTrue(prevValue + " < " + currentValue, prevValue < currentValue);
            return currentValue;
         }
      }, initialValue, (long) NUM_OPS * counters.length);
   }

   public void testGetAndIncrement() throws Exception {
      long initialValue = 0;
      SharedLong[] counters = createCounters(initialValue);
      test(counters, new Operation() {
         public long run(SharedLong counter, long prevValue, boolean[] valuesSeen) {
            long currentValue = counter.getAndIncrement();
            assertFalse(valuesSeen[(int) currentValue]);
            valuesSeen[(int) currentValue] = true;
            assertTrue(prevValue + " < " + currentValue, prevValue < currentValue);
            return currentValue;
         }
      }, initialValue - 1, (long) NUM_OPS * counters.length);
   }

   public void testGetAndDecrement() throws Exception {
      long initialValue = NUM_OPS * 3;
      SharedLong[] counters = createCounters(initialValue);
      test(counters, new Operation() {
         public long run(SharedLong counter, long prevValue, boolean[] valuesSeen) {
            long currentValue = counter.getAndDecrement();
            assertFalse(valuesSeen[(int) currentValue - 1]);
            valuesSeen[(int) currentValue - 1] = true;
            assertTrue(prevValue + " > " + currentValue, prevValue > currentValue);
            return currentValue;
         }
      }, initialValue + 1, 0);
   }

   public void testDecrementAndGet() throws Exception {
      long initialValue = NUM_OPS * 3;
      SharedLong[] counters = createCounters(initialValue);
      test(counters, new Operation() {
         public long run(SharedLong counter, long prevValue, boolean[] valuesSeen) {
            long currentValue = counter.decrementAndGet();
            assertFalse(valuesSeen[(int) currentValue]);
            valuesSeen[(int) currentValue] = true;
            assertTrue(prevValue + " > " + currentValue, prevValue > currentValue);
            return currentValue;
         }
      }, initialValue, 0);
   }

   private void test(SharedLong[] counters, final Operation operation, final long initialValue, long targetValue) throws Exception {
      final boolean[] valuesSeen = new boolean[NUM_OPS * counters.length];
      final CyclicBarrier barrier = new CyclicBarrier(3);
      Collection<Future<?>> futures = new ArrayList<>(counters.length);
      for (SharedLong c : counters) {
         final SharedLong counter = c;
         futures.add(executorService.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  barrier.await();
               } catch (Exception e) {
                  throw new RuntimeException();
               }
               long prevValue = initialValue;
               for (int i = 0; i < NUM_OPS; ++i) {
                  prevValue = operation.run(counter, prevValue, valuesSeen);
               }
            }
         }));
      }
      waitForAllFutures(futures);
      for (SharedLong counter : counters) {
         assertEquals(targetValue, counter.get());
      }
      for (boolean seen : valuesSeen) {
         assertTrue(seen);
      }
   }

   private SharedLong[] createCounters(long initialValue) {
      SharedLong counter1 = Lookup.createSharedLong(cache(0), COUNTER_DATA, initialValue);
      assertNotNull(counter1);
      SharedLong counter2 = Lookup.lookupSharedLong(cache(1), COUNTER_DATA);
      assertNotNull(counter2);
      cache(0).put(COUNTER_INSTANCE, counter1);
      SharedLong counter3 = (SharedLong) cache(2).get(COUNTER_INSTANCE);
      assertNotNull(counter3);

      // those should be different instances but equal ones
      assertFalse(counter1 == counter2);
      assertFalse(counter1 == counter3);
      assertFalse(counter2 == counter3);
      assertEquals(counter1, counter2);
      assertEquals(counter1, counter3);
      assertEquals(counter2, counter3);

      return new SharedLong[]{counter1, counter2, counter3};
   }

   private static interface Operation {
      long run(SharedLong counter, long prevValue, boolean[] valuesSeen);
   }
}
