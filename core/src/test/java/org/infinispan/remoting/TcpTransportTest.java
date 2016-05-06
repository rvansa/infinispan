package org.infinispan.remoting;

import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.tcp.TcpTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "remoting.TcpTransportTest")
public class TcpTransportTest extends MultipleCacheManagersTest {
   private EmbeddedCacheManager cm1, cm2, cm3;

   @Override
   protected void createCacheManagers() throws Throwable {
      cm1 = TestCacheManagerFactory.createClusteredCacheManager(getGlobalConfigurationBuilderWithTcpTransport(), getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(getGlobalConfigurationBuilderWithTcpTransport(), getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      cm3 = TestCacheManagerFactory.createClusteredCacheManager(getGlobalConfigurationBuilderWithTcpTransport(), getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      registerCacheManager(cm1, cm2, cm3);
      cm1.getCache();
      cm2.getCache();
      cm3.getCache();
   }

   private GlobalConfigurationBuilder getGlobalConfigurationBuilderWithTcpTransport() {
      System.setProperty("infinispan.transport", TcpTransport.class.getName());
      ConfigurationBuilderHolder cbh = null;
      ClassLoader classLoader = getClass().getClassLoader();
      try (InputStream input = FileLookupFactory.newInstance().lookupFileStrict("/home/rvansa/workspace/benchmarks/radargun-configs/dist-sync.xml", classLoader)) {
         cbh = new ParserRegistry(classLoader).parse(input);
      } catch (IOException e) {
         log.error("Failed to get configuration input stream", e);
      }
      return cbh.getGlobalConfigurationBuilder();
//      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
//      gcb.transport().transport(new TcpTransport());
//      return gcb;
   }

   public void test() {
      cache(0).put("key", "value1");
      caches().stream().forEach(c -> assertEquals("value1", c.get("key")));
      cache(1).put("key", "value2");
      caches().stream().forEach(c -> assertEquals("value2", c.get("key")));
      cache(2).put("key", "value3");
      caches().stream().forEach(c -> assertEquals("value3", c.get("key")));
   }

   public void testMany() {
      for (int i = 0; i < 10000; ++i) {
         cache(i % 3).put("key" + i, "value" + i);
         System.out.println("Did " + i);
      }
   }
}
