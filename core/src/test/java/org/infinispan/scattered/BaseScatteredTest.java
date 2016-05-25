package org.infinispan.scattered;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class BaseScatteredTest extends MultipleCacheManagersTest {
   protected EmbeddedCacheManager cm1, cm2, cm3;
   protected ControlledScatteredVersionManager[] svms;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.SCATTERED_SYNC, false);
      cfg.clustering().hash().numSegments(16);
      cfg.clustering().remoteTimeout(1, TimeUnit.DAYS); // for debugging
      cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm3 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2, cm3);
      // Starting caches and rewiring SVM is not synchronized properly, so we have to first start
      // them and then rewire
      for (EmbeddedCacheManager cm : cacheManagers) cm.getCache();
      TestingUtil.waitForStableTopology(caches());

      svms = caches().stream().map(c -> {
         ControlledScatteredVersionManager csvm = new ControlledScatteredVersionManager();
         ComponentRegistry componentRegistry = c.getAdvancedCache().getComponentRegistry();
         componentRegistry.registerComponent(csvm, ScatteredVersionManager.class);
         componentRegistry.rewire();
         return csvm;
      }).toArray(ControlledScatteredVersionManager[]::new);
   }

   protected ControlledScatteredVersionManager svm(int node) {
      return svms[node];
   }

   protected void flush(boolean expectRemoval) {
      int[] regularCounters = new int[svms.length];
      int[] removeCounters = new int[svms.length];
      for (int i = 0; i < svms.length; ++i) {
         regularCounters[i] = svms[i].regularCounter.get();
         removeCounters[i] = svms[i].removeCounter.get();
         if (!svms[i].startFlush()) {
            // did not start anything
            regularCounters[i] = Integer.MIN_VALUE;
            removeCounters[i] = Integer.MIN_VALUE;
         }
      }
      for (;;) {
         boolean done = true;
         for (int i = 0; i < svms.length; ++i) {
            if (regularCounters[i] >= svms[i].regularCounter.get()) done = false;
            if (expectRemoval && removeCounters[i] >= svms[i].removeCounter.get()) done = false;
         }
         if (done) break;
         LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
      }
   }

   protected DataContainer<Object, Object> dc(int index) {
      return cache(index).getAdvancedCache().getDataContainer();
   }
}
