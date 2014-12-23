package org.infinispan.objects;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.concurrent.jdk7backported.ThreadLocalRandom;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
public class AppendOnlyListTest extends CacheObjectTest {
   private static final String LIST_KEY = "list";
   protected static final int NUM_NODES = 3;
   protected static final int FRAGMENT_SIZE = 10;
   private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_NODES);
   protected static final int NUM_OPS = 1000;

   @AfterClass
   protected void tearDown() throws InterruptedException {
      executorService.shutdownNow();
      executorService.awaitTermination(FRAGMENT_SIZE, TimeUnit.SECONDS);
   }
   @Override
   protected void createCacheManagers() throws Throwable {
      createCacheManagers(NUM_NODES);
   }

   public void testAdd() throws Exception {
      test(new TaskFactory() {
         @Override
         public Callable<Void> getTask(final int node) {
            return new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  SharedAppendOnlyList<Integer> list = Lookup.lookupOrCreateAppendOnlyList(cache(node), LIST_KEY, FRAGMENT_SIZE);
                  for (int i = 0; i < NUM_OPS; ++i) {
                     list.add(NUM_OPS * node + i);
                  }
                  return null;
               }
            };
         }
      });
   }

   public void testAddAll() throws Exception {
      test(new TaskFactory() {
         @Override
         public Callable<Void> getTask(final int node) {
            return new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  SharedAppendOnlyList<Integer> list = Lookup.lookupOrCreateAppendOnlyList(cache(node), LIST_KEY, FRAGMENT_SIZE);
                  int size;
                  for (int i = 0; i < NUM_OPS; i += size) {
                     size = Math.min(NUM_OPS - i, ThreadLocalRandom.current().nextInt(1, 25));
                     ArrayList<Integer> collection = new ArrayList<>(size);
                     for (int j = 0; j < size; ++j) collection.add(NUM_OPS * node + i + j);
                     list.addAll(collection);
                  }
                  return null;
               }
            };
         }
      });
   }

   private void test(TaskFactory taskFactory) throws Exception {
      ArrayList<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < NUM_NODES; ++i) {
         futures.add(executorService.submit(taskFactory.getTask(i)));
      }
      waitForAllFutures(futures);

      SharedAppendOnlyList<Integer> list = Lookup.lookupAppendOnlyList(cache(0), LIST_KEY);
      assertEquals(list.size(), NUM_NODES * NUM_OPS);

      int maxValue[] = new int[NUM_NODES];
      Arrays.fill(maxValue, Integer.MIN_VALUE);
      for (Integer value : list) {
         int origin = value / NUM_OPS;
         assertTrue(origin < NUM_NODES);
         int op = value % NUM_OPS;
         assertTrue(op > maxValue[origin]);
         maxValue[origin] = op;
      }

      int minValue[] = new int[NUM_NODES];
      Arrays.fill(minValue, Integer.MAX_VALUE);
      for (ListIterator<Integer> it = list.listIteratorAtEnd(); it.hasPrevious(); ) {
         int value = it.previous();
         int origin = value / NUM_OPS;
         assertTrue(origin < NUM_NODES);
         int op = value % NUM_OPS;
         assertTrue(op < minValue[origin]);
         minValue[origin] = op;
      }

      ThreadLocalRandom random = ThreadLocalRandom.current();
      for (int i = 0; i < FRAGMENT_SIZE; ++i) {
         int value = random.nextInt(NUM_NODES * NUM_OPS);
         int index = list.indexOf(value);
         int valueAtIndex = list.get(index);
         assertEquals(valueAtIndex, value);
         int lastIndex = list.lastIndexOf(value);
         assertEquals(lastIndex, index);
      }
   }

   public void testIndexOf() {
      SharedAppendOnlyList<Integer> list = Lookup.createAppendOnlyList(cache(0), LIST_KEY, FRAGMENT_SIZE);
      ArrayList<Integer> control = new ArrayList<>();
      fillLists(NUM_OPS, list, control);

      for (int i = 0; i < NUM_OPS; ++i) {
         int controlValue = control.get(i);
         int actualValue = list.get(i);
         assertEquals(actualValue, controlValue,  "Error at index " + i + ": " + Arrays.toString(list.toArray()));
         assertEquals((int) list.listIterator(i).next(), controlValue);
         int index = list.indexOf(controlValue);
         int valueAtFirstIndex = list.get(index);
         assertEquals(valueAtFirstIndex, controlValue, controlValue + ": " + Arrays.toString(list.toArray()));
         int lastIndex = list.lastIndexOf(controlValue);
         int valueAtLastIndex = list.get(lastIndex);
         assertEquals(valueAtLastIndex, controlValue, controlValue + ": " + Arrays.toString(list.toArray()));
      }
   }

   private void fillLists(int ops, SharedAppendOnlyList<Integer> list, List<Integer> control) {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      for (int i = 0; i < ops; ++i) {
         // intentionally non-unique values
         int value = random.nextInt(ops);
         list.add(value);
         if (control != null) {
            control.add(value);
         }
      }
      if (control != null) {
         assertTrue(list.containsAll(control), "List: " + Arrays.toString(list.toArray()) + ", Control: " + control);
         assertTrue(control.containsAll(list), "List: " + Arrays.toString(list.toArray()) + ", Control: " + control);
      }
   }

   @Test(expectedExceptions = IndexOutOfBoundsException.class)
   public void testOutOfBounds1() {
      SharedAppendOnlyList<Integer> list = Lookup.createAppendOnlyList(cache(0), LIST_KEY, FRAGMENT_SIZE);
      list.get(0);
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testOutOfBounds2() {
      SharedAppendOnlyList<Integer> list = Lookup.createAppendOnlyList(cache(0), LIST_KEY, FRAGMENT_SIZE);
      ListIterator<Integer> iterator = list.listIterator();
      assertFalse(iterator.hasNext());
      assertFalse(iterator.hasPrevious());
      iterator.next();
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testOutOfBounds3() {
      SharedAppendOnlyList<Integer> list = Lookup.createAppendOnlyList(cache(0), LIST_KEY, FRAGMENT_SIZE);
      ListIterator<Integer> iterator = list.listIteratorAtEnd();
      assertFalse(iterator.hasNext());
      assertFalse(iterator.hasPrevious());
      iterator.previous();
   }

   @Test(expectedExceptions = IndexOutOfBoundsException.class)
   public void testOutOfBounds4() {
      SharedAppendOnlyList<Integer> list = Lookup.createAppendOnlyList(cache(0), LIST_KEY, FRAGMENT_SIZE);
      fillLists(FRAGMENT_SIZE, list, null);
      list.get(FRAGMENT_SIZE);
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testOutOfBounds5() {
      SharedAppendOnlyList<Integer> list = Lookup.createAppendOnlyList(cache(0), LIST_KEY, FRAGMENT_SIZE);
      fillLists(FRAGMENT_SIZE, list, null);
      ListIterator<Integer> iterator = list.listIterator(FRAGMENT_SIZE);
      assertFalse(iterator.hasNext());
      assertTrue(iterator.hasPrevious());
      iterator.next();
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testOutOfBounds6() {
      SharedAppendOnlyList<Integer> list = Lookup.createAppendOnlyList(cache(0), LIST_KEY, FRAGMENT_SIZE);
      fillLists(FRAGMENT_SIZE, list, null);
      ListIterator<Integer> iterator = list.listIteratorAtEnd();
      assertFalse(iterator.hasNext());
      assertTrue(iterator.hasPrevious());
      iterator.next();
   }

   private interface TaskFactory {
      Callable<Void> getTask(int node);
   }
}
