package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.jdbc.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testStringKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <string-keyed-jdbc-store xmlns=\"urn:infinispan:config:store:jdbc:7.0\" key-to-string-mapper=\"DummyKey2StringMapper\" shared=\"true\" " +
            "                                preload=\"true\" read-only=\"true\" fetch-state=\"true\" purge=\"true\" singleton=\"false\" >\n" +
            "         <connection-pool connection-url=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driver=\"org.h2.Driver\"/>\n" +
            "         <string-keyed-table prefix=\"entry\" fetch-size=\"34\" batch-size=\"128\" >\n" +
            "           <id-column name=\"id\" type=\"VARCHAR\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </string-keyed-table>\n" +
            "         <write-behind />\n" +
            "       </string-keyed-jdbc-store>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertEquals(128, store.table().batchSize());
      assertEquals(34, store.table().fetchSize());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertTrue(store.async().enabled());
      assertTrue(store.shared());
      assertFalse(store.purgeOnStartup());
      assertEquals("DummyKey2StringMapper", store.key2StringMapper());
      assertTrue(store.shared());
      assertTrue(store.preload());
      PooledConnectionFactoryConfiguration connectionFactory = (PooledConnectionFactoryConfiguration) store.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1", connectionFactory.connectionUrl());
      assertEquals("org.h2.Driver", connectionFactory.driverClass());
      assertEquals("dbuser", connectionFactory.username());
      assertEquals("dbpass", connectionFactory.password());
      assertTrue(store.ignoreModifications());
      assertTrue(store.fetchPersistentState());
      assertTrue(store.purgeOnStartup());
      assertFalse(store.singletonStore().enabled());
   }

   public void testBinaryKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <binary-keyed-jdbc-store xmlns=\"urn:infinispan:config:store:jdbc:7.0\" read-only=\"true\" singleton=\"true\">\n" +
            "         <simple-connection connection-url=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driver=\"org.h2.Driver\"/>\n" +
            "         <binary-keyed-table prefix=\"bucket\" fetch-size=\"34\" batch-size=\"128\">\n" +
            "           <id-column name=\"id\" type=\"BINARY\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </binary-keyed-table>\n" +
            "       </binary-keyed-jdbc-store>\n" +
            "       <binaryKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:6.0\" ignoreModifications=\"true\" purgeOnStartup=\"true\">\n" +
            "         <simpleConnection connectionUrl=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driverClass=\"org.h2.Driver\"/>\n" +
            "         <binaryKeyedTable prefix=\"bucket\" fetchSize=\"34\" batchSize=\"128\">\n" +
            "           <idColumn name=\"id\" type=\"BINARY\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </binaryKeyedTable>\n" +
            "         <singleton enabled=\"true\" />\n" +
            "       </binaryKeyedJdbcStore>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcBinaryStoreConfiguration store = (JdbcBinaryStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertTrue(store.ignoreModifications());
      assertEquals("bucket", store.table().tableNamePrefix());
      assertEquals(128, store.table().batchSize());
      assertEquals(34, store.table().fetchSize());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertTrue(store.singletonStore().enabled());
      assertTrue(store.purgeOnStartup());
      SimpleConnectionFactoryConfiguration connectionFactory = (SimpleConnectionFactoryConfiguration) store.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1", connectionFactory.connectionUrl());
      assertEquals("org.h2.Driver", connectionFactory.driverClass());
      assertEquals("dbuser", connectionFactory.username());
      assertEquals("dbpass", connectionFactory.password());
   }

   public void testMixedKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <mixed-keyed-jdbc-store xmlns=\"urn:infinispan:config:store:jdbc:7.0\" key-to-string-mapper=\"DummyKey2StringMapper\" singleton=\"true\" >\n" +
            "         <data-source jndi-url=\"java:MyDataSource\" />\n" +
            "         <string-keyed-table prefix=\"entry\" fetch-size=\"34\" batch-size=\"128\">\n" +
            "           <id-column name=\"id\" type=\"VARCHAR\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </string-keyed-table>\n" +
            "         <binary-keyed-table prefix=\"bucket\" fetch-size=\"44\" batch-size=\"256\">\n" +
            "           <id-column name=\"id\" type=\"BINARY\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </binary-keyed-table>\n" +
            "         <write-behind />\n" +
            "       </mixed-keyed-jdbc-store>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcMixedStoreConfiguration store = (JdbcMixedStoreConfiguration) buildCacheManagerWithCacheStore(config);

      assertEquals("entry", store.stringTable().tableNamePrefix());
      assertEquals(128, store.stringTable().batchSize());
      assertEquals(34, store.stringTable().fetchSize());
      assertEquals("BINARY", store.stringTable().dataColumnType());
      assertEquals("version", store.stringTable().timestampColumnName());

      assertEquals("bucket", store.binaryTable().tableNamePrefix());
      assertEquals(256, store.binaryTable().batchSize());
      assertEquals(44, store.binaryTable().fetchSize());
      assertEquals("BINARY", store.binaryTable().dataColumnType());
      assertEquals("version", store.binaryTable().timestampColumnName());

      assertTrue(store.async().enabled());
      assertTrue(store.singletonStore().enabled());
      assertFalse(store.purgeOnStartup());
      assertEquals("DummyKey2StringMapper", store.key2StringMapper());
   }

   private StoreConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assertEquals(1, cacheManager.getDefaultCacheConfiguration().persistence().stores().size());
      return cacheManager.getDefaultCacheConfiguration().persistence().stores().get(0);
   }
}