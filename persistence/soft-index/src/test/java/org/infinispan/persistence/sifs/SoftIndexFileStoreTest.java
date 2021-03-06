package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.concurrent.CompletionStages;
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
public class SoftIndexFileStoreTest extends BaseNonBlockingStoreTest {

   String tmpDirectory;
   boolean startIndex = true;
   boolean keepIndex = false;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
      return configurationBuilder.persistence()
            .addStore(SoftIndexFileStoreConfigurationBuilder.class)
               .segmented(true)
               .indexLocation(tmpDirectory).dataLocation(tmpDirectory)
               .maxFileSize(1000)
            .build();
   }

   @Override
   protected NonBlockingStore createStore() {
      return new NonBlockingSoftIndexFileStore() {
         boolean firstTime = true;

         @Override
         protected void startIndex() {
            if (startIndex) {
               super.startIndex();
            }
         }

         @Override
         public CompletionStage<Void> start(InitializationContext ctx) {
            return super.start(ctx).whenComplete((ignore, t) -> {
               if (!firstTime) {
                  assertEquals(keepIndex, isIndexLoaded());
               }
               firstTime = false;
            });
         }

         @Override
         public CompletionStage<Void> stop() {
            return super.stop()
                  .whenComplete((ignore, t) -> {
                     if (!keepIndex) {
                        Util.recursiveFileRemove(getIndexLocation().toFile());
                     }
                  });
         }
      };
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   public void testClear() {
      CompletionStages.join(store.clear());
   }

   public void testLoadUnload() {
      int numEntries = 10000;
      for (int i = 0; i < numEntries; ++i) {
         InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(MarshalledEntryUtil.create(ice, getMarshaller()));
      }
      for (int i = 0; i < numEntries; ++i) {
         assertNotNull(key(i), store.loadEntry(key(i)));
         assertTrue(key(i), store.delete(key(i)));
      }
      store.clear();
      for (int i = 0; i < numEntries; ++i) {
         InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(MarshalledEntryUtil.create(ice, getMarshaller()));
      }
      for (int i = numEntries - 1; i >= 0; --i) {
         assertNotNull(key(i), store.loadEntry(key(i)));
         assertTrue(key(i), store.delete(key(i)));
      }
   }

   // test for ISPN-5658
   public void testStopStartAndMultipleWrites() {
      MarshallableEntry<Object, Object> entry1 = marshalledEntry(internalCacheEntry("k1", "v1", -1));
      MarshallableEntry<Object, Object> entry2 = marshalledEntry(internalCacheEntry("k1", "v2", -1));

      store.write(entry1);
      store.write(entry1);
      store.write(entry1);

      store.stop();
      store.start(initializationContext);

      MarshallableEntry entry = store.loadEntry("k1");
      assertNotNull(entry);
      assertEquals("v1", entry.getValue());
      store.write(entry2);

      store.stop();
      store.start(initializationContext);

      entry = store.loadEntry("k1");
      assertNotNull(entry);
      assertEquals("v2", entry.getValue());
   }

   // test for ISPN-5743
   public void testStopStartWithRemoves() {
      String KEY = "k1";
      MarshallableEntry<Object, Object> entry1 = marshalledEntry(internalCacheEntry(KEY, "v1", -1));
      MarshallableEntry<Object, Object> entry2 = marshalledEntry(internalCacheEntry(KEY, "v2", -1));

      store.write(entry1);
      store.delete(KEY);

      store.stop();
      store.start(initializationContext);

      assertNull(store.loadEntry(KEY));
      store.write(entry2);
      store.delete(KEY);
      store.write(entry1);

      store.stop();
      startIndex = false;
      store.start(initializationContext);

      assertEquals(entry1.getValue(), store.loadEntry(KEY).getValue());
      startIndex = true;
      ((NonBlockingSoftIndexFileStore) store.delegate()).startIndex();
   }

   // test for ISPN-5753
   public void testOverrideWithExpirableAndCompaction() throws InterruptedException {
      // write immortal entry
      store.write(marshalledEntry(internalCacheEntry("key", "value1", -1)));
      writeGibberish(); // make sure that compaction happens - value1 is evacuated
      log.debug("Size :" + store.size(IntSets.immutableEmptySet()));
      store.write(marshalledEntry(internalCacheEntry("key", "value2", 1)));
      timeService.advance(2);
      writeGibberish(); // make sure that compaction happens - value2 expires
      store.stop();
      store.start(initializationContext);
      // value1 has been overwritten and value2 has expired
      MarshallableEntry entry = store.loadEntry("key");
      assertNull(entry != null ? entry.getKey() + "=" + entry.getValue() : null, entry);
   }

   public void testStopStartWithLoadDoesNotNukeValues() throws InterruptedException, PersistenceException {
      keepIndex = true;
      try {
         testStopStartDoesNotNukeValues();
      } finally {
         keepIndex = false;
      }
   }

   private int countIterator(Iterator<?> iterator) {
      int count = 0;
      while (iterator.hasNext()) {
         iterator.next();
         count++;
      }
      return count;
   }

   public void testCompactorRemovesOldFile() throws IOException {
      // Make sure no additional logs around
      store.clear();
      NonBlockingSoftIndexFileStore<Object, Object> actualStore = (NonBlockingSoftIndexFileStore<Object, Object>) store.delegate();

      FileProvider fileProvider = actualStore.getFileProvider();

      // we need to write an entry for it to create a file
      int maxWritten = 1;
      InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(maxWritten), "value" + maxWritten);
      store.write(MarshalledEntryUtil.create(ice, getMarshaller()));

      int onlyFileId;
      try (CloseableIterator<Integer> iter = fileProvider.getFileIterator()) {
         assertTrue(iter.hasNext());
         onlyFileId = iter.next();
         assertFalse(iter.hasNext());
      }
      // Keep writing until the current file fills up and we create a new one
      while (countIterator(fileProvider.getFileIterator()) == 1) {
         int i = ++maxWritten;
         ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(MarshalledEntryUtil.create(ice, getMarshaller()));
      }

      // Now we write over all the previous values, which should force the compactor to eventually remove the file
      for (int i = 0; i < maxWritten; ++i) {
         ice = TestInternalCacheEntryFactory.create(key(i), "value" + i + "-second");
         store.write(MarshalledEntryUtil.create(ice, getMarshaller()));
      }

      eventuallyEquals(Boolean.FALSE, () -> fileProvider.newFile(onlyFileId).exists());
   }

   private void writeGibberish() {
      for (int i = 0; i < 100; ++i) {
         store.write(marshalledEntry(internalCacheEntry("foo", "bar", -1)));
         store.delete("foo");
      }
   }

   private String key(int i) {
      return String.format("key%010d", i);
   }

}
