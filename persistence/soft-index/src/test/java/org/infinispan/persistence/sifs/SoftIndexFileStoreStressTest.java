package org.infinispan.persistence.sifs;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.recursiveFileRemove;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "stress", testName = "persistence.SoftIndexFileStoreStressTest")
public class SoftIndexFileStoreStressTest extends AbstractInfinispanTest {
   protected static final int THREADS = 1;
   protected static final long TEST_DURATION = TimeUnit.SECONDS.toMillis(60);

   private TestObjectStreamMarshaller marshaller;
   private InternalEntryFactory factory;
   private SoftIndexFileStore store;
   private String tmpDirectory;
   private ExecutorService executorService;
   private volatile boolean terminate;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      recursiveFileRemove(tmpDirectory);
      marshaller = new TestObjectStreamMarshaller();
      factory = new InternalEntryFactoryImpl();
      store = new SoftIndexFileStore();
      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .indexLocation(tmpDirectory).dataLocation(tmpDirectory + "/data")
            .maxFileSize(1000);

      TimeService timeService = new DefaultTimeService();
      store.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller, timeService));
      ((InternalEntryFactoryImpl) factory).injectTimeService(timeService);
      store.start();
      executorService = Executors.newFixedThreadPool(THREADS + 1);
   }

   @AfterMethod
   public void shutdown() {
      store.clear();
      store.stop();
      marshaller.stop();
      executorService.shutdown();
   }

   public void test() throws ExecutionException, InterruptedException {
      terminate = false;
      ArrayList<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < THREADS; ++i) {
         Future<?> future = executorService.submit(new TestThread());
         futures.add(future);
      }
      executorService.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Thread.sleep(TEST_DURATION);
            terminate = true;
            return null;
         }
      });
      for (Future<?> future : futures) {
         future.get();
      }
      // let's wait so that we don't store expired values in the map
      Thread.sleep(100);
      Map<Object, Object> map = new HashMap<>();
      store.process(KeyFilter.ACCEPT_ALL_FILTER, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            assertNull(map.put(marshalledEntry.getKey(), marshalledEntry.getValue()));
         }
      }, new WithinThreadExecutor(), true, false);
      store.stop();
      store.start();
      assertTrue(store.isIndexLoaded());
      // note: we can't test size() call since this provides very imprecise value when expiration is used
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
         assertEquals(map.get(entry.getKey()), entry.getValue());
      }
      store.process(KeyFilter.ACCEPT_ALL_FILTER, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            assertTrue(map.containsKey(marshalledEntry.getKey()));
         }
      }, new WithinThreadExecutor(), false, false);
   }

   private class TestThread implements Runnable {
      @Override
      public void run() {
         ThreadLocalRandom random = ThreadLocalRandom.current();
         InternalCacheEntry ice;
         while (!terminate) {
            long lifespan;
             String key = key(random);
            switch (random.nextInt(3)) {
               case 0:
                  lifespan = random.nextInt(3) == 0 ? random.nextInt(10) : -1;
                  ice = TestInternalCacheEntryFactory.<Object, Object>create(factory, key, "value", lifespan);
                  store.write(TestingUtil.marshalledEntry(ice, marshaller));
                  break;
               case 1:
                  store.delete(key);
                  break;
               case 2:
                  store.load(key);
            }
         }
      }
   }

   protected String key(ThreadLocalRandom random) {
      return "key" + random.nextInt(1000);
   }
}
