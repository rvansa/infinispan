package org.infinispan.objects;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objects.interceptors.CacheObjectInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * Base test class
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class CacheObjectTest extends MultipleCacheManagersTest {
   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfg.customInterceptors().addInterceptor()
            .interceptor(new CacheObjectInterceptor())
            .position(InterceptorConfiguration.Position.FIRST);
      return cfg;
   }

   protected void createCacheManagers(int numNodes) {
      for (int i = 0; i < numNodes; ++i) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(createConfiguration());
         registerCacheManager(cm);
      }
      waitForClusterToForm();
   }

   protected void waitForAllFutures(Collection<Future<?>> futures) throws InterruptedException, ExecutionException, TimeoutException {
      for (Future f : futures) {
         f.get(10, TimeUnit.SECONDS);
      }
   }
}
