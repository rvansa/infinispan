package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

/**
 * Tests data loss during state transfer when the originator of a put operation becomes the primary owner of the
 * modified key. See https://issues.jboss.org/browse/ISPN-3357
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPutIfAbsentDuringJoinStressTest")
@CleanupAfterMethod
public class NonTxPutIfAbsentDuringJoinStressTest extends MultipleCacheManagersTest {

   private static final int NUM_WRITERS = 4;
   private static final int NUM_ORIGINATORS = 2;
   private static final int NUM_KEYS = 100;
   private final ConcurrentMap<String, String> insertedValues = CollectionFactory.makeConcurrentMap();
   private volatile boolean stop = false;

   @Override
   public Object[] factory() {
      return new Object[] {
            new NonTxPutIfAbsentDuringJoinStressTest().cacheMode(CacheMode.DIST_SYNC),
            new NonTxPutIfAbsentDuringJoinStressTest().cacheMode(CacheMode.SCATTERED_SYNC).biasAcquisition(BiasAcquisition.NEVER),
            new NonTxPutIfAbsentDuringJoinStressTest().cacheMode(CacheMode.SCATTERED_SYNC).biasAcquisition(BiasAcquisition.ON_WRITE),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      return getDefaultClusteredCacheConfig(cacheMode, false);
   }

   public void testNodeJoiningDuringPutIfAbsent() throws Exception {
      Future[] futures = new Future[NUM_WRITERS];
      for (int i = 0; i < NUM_WRITERS; i++) {
         final int writerIndex = i;
         futures[i] = fork(() -> {
            while (!stop) {
               for (int j = 0; j < NUM_KEYS; j++) {
                  Cache<Object, Object> cache = cache(writerIndex % NUM_ORIGINATORS);
                  String key = "key_" + j;
                  String value = "value_" + j + "_" + writerIndex;
                  Object oldValue = cache.putIfAbsent(key, value);
                  if (oldValue == null) {
                     // succeeded
                     log.tracef("Successfully inserted value %s for key %s", value, key);
                     Object newValue = cache.get(key);
                     assertEquals(value, newValue);
                     Object insertedValue = insertedValues.putIfAbsent(key, value);
                     assertEquals(null, insertedValue);
                  } else {
                     // failed
                     // ISPN-3918: cache.get(key) == null if another command succeeded but didn't finish
                     eventuallyEquals(oldValue, () -> cache.get(key));
                  }
               }
            }
         });
      }

      addClusterEnabledCacheManager(getConfigurationBuilder());
      waitForClusterToForm();

      addClusterEnabledCacheManager(getConfigurationBuilder());
      waitForClusterToForm();

      stop = true;

      for (int i = 0; i < NUM_WRITERS; i++) {
         futures[i].get(10, TimeUnit.SECONDS);
         for (int j = 0; j < NUM_KEYS; j++) {
            for (int k = 0; k < caches().size(); k++) {
               String key = "key_" + j;
               assertEquals(insertedValues.get(key), cache(k).get(key));
            }
         }
      }

      for (int i = 0; i < caches().size(); i++) {
         LockManager lockManager = advancedCache(i).getLockManager();
         assertEquals(0, lockManager.getNumberOfLocksHeld());
         for (int j = 0; j < NUM_KEYS; j++) {
            String key = "key_" + j;
            assertFalse(lockManager.isLocked(key));
         }
      }
   }
}
