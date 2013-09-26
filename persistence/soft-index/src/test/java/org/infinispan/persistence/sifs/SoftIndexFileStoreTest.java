package org.infinispan.persistence.sifs;

import static org.infinispan.persistence.PersistenceUtil.internalMetadata;
import static org.infinispan.test.TestingUtil.recursiveFileRemove;

import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Low level single-file cache store tests.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "persistence.SoftIndexFileStoreTest")
public class SoftIndexFileStoreTest extends BaseStoreTest {

   SoftIndexFileStore store;
   String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      store = new SoftIndexFileStore();
      SoftIndexFileStoreConfiguration configuration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(SoftIndexFileStoreConfigurationBuilder.class)
                  .indexLocation(tmpDirectory).dataLocation(tmpDirectory + "/data")
                  .create();
      store.init(new DummyInitializationContext(configuration, getCache(), getMarshaller(), new ByteBufferFactoryImpl(),
            new MarshalledEntryFactoryImpl(getMarshaller())));
      store.start();
      return store;
   }

   public void testLoadUnload() {
      int numEntries = 10000;
      for (int i = 0; i < numEntries; ++i) {
         InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(new MarshalledEntryImpl(ice.getKey(), ice.getValue(), internalMetadata(ice), getMarshaller()));
      }
      System.out.println("Loaded all entries");
      for (int i = 0; i < numEntries; ++i) {
         if (!store.delete(key(i))) {
            AssertJUnit.fail("Key " + key(i) + " not found");
         }
      }
      store.clear();
      for (int i = 0; i < numEntries; ++i) {
         InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(new MarshalledEntryImpl(ice.getKey(), ice.getValue(), internalMetadata(ice), getMarshaller()));
      }
      for (int i = numEntries - 1; i >= 0; --i) {
         if (!store.delete(key(i))) {
            AssertJUnit.fail("Key " + key(i) + " not found");
         }
      }
   }

   private String key(int i) {
      return String.format("key%010d", i);
   }

}
