/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lucene.readlocks;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;

/**
 * Tests covering the functionality of NoopSegmentreadLocker.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.readlocks.NoopSegmentReadLockerTest")
public class NoopSegmentReadLockerTest extends DistributedSegmentReadLockerTest {

   @Override
   Directory createDirectory(Cache cache) {
      return DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
            .chunkSize(CHUNK_SIZE)
            .overrideSegmentReadLocker(new NoopSegmentReadLocker()).create();
   }

   @Test @Override
   public void testIndexWritingAndFinding() throws IOException, InterruptedException {
      verifyBoth(cache0,cache1);
      IndexOutput indexOutput = dirA.createOutput(filename, IOContext.DEFAULT);
      indexOutput.writeString("no need to write, nobody ever will read this");
      indexOutput.flush();
      indexOutput.close();
      assertFileExistsHavingRLCount(filename, 0, true);
      IndexInput firstOpenOnB = dirB.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 0, true);
      dirA.deleteFile(filename);
      assertFileExistsHavingRLCount(filename, 0, false);
      //Lucene does use clone() - lock implementation ignores it as a clone is
      //cast on locked segments and released before the close on the parent object
      IndexInput cloneOfFirstOpenOnB = (IndexInput) firstOpenOnB.clone();
      assertFileExistsHavingRLCount(filename, 0, false);
      cloneOfFirstOpenOnB.close();
      assertFileExistsHavingRLCount(filename, 0, false);
      IndexInput firstOpenOnA = dirA.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 0, false);
      IndexInput secondOpenOnA = dirA.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 0, false);
      firstOpenOnA.close();
      assertFileExistsHavingRLCount(filename, 0, false);
      secondOpenOnA.close();
      assertFileExistsHavingRLCount(filename, 0, false);
      firstOpenOnB.close();

      //As the NoopSegmentReadLocker ignores also file deletions, then verifying.
      assertFileAfterDeletion(cache0);
      assertFileAfterDeletion(cache1);

      dirA.close();
      dirB.close();
      verifyDirectoryStructure(cache0);
      verifyDirectoryStructure(cache1);
   }

   private void assertFileAfterDeletion(Cache cache) {
      Set<String> fileList = (Set<String>) cache.get(new FileListCacheKey(INDEX_NAME));
      Assert.assertNotNull(fileList);
      Assert.assertFalse(fileList.contains(filename));

      FileMetadata metadata = (FileMetadata) cache.get(new FileCacheKey(INDEX_NAME, filename));
      Assert.assertNotNull(metadata);
      long totalFileSize = metadata.getSize();
      int chunkNumbers = (int)(totalFileSize / CHUNK_SIZE);

      for(int i = 0; i < chunkNumbers; i++) {
         Assert.assertNotNull(cache.get(new ChunkCacheKey(INDEX_NAME, filename, CHUNK_SIZE, 0)));
      }

      boolean fileNameExistsInCache = false;
      for(Object key : cache.keySet()) {
         if(key instanceof FileCacheKey) {
            FileCacheKey keyObj = (FileCacheKey) key;

            if(keyObj.getFileName().contains(filename)) {
               fileNameExistsInCache = true;
            }
         }
      }

      Assert.assertTrue(fileNameExistsInCache);
   }

   private void verifyDirectoryStructure(Cache cache) {
      Set<String> fileList = (Set<String>) cache.get(new FileListCacheKey(INDEX_NAME));
      Assert.assertNotNull(fileList);
      int fileListCacheKeyInstances = 0;

      for (Object key : cache.keySet()) {
         if (key instanceof ChunkCacheKey) {
            ChunkCacheKey existingChunkKey = (ChunkCacheKey) key;
            Assert.assertEquals(existingChunkKey.getIndexName(), INDEX_NAME);
            Object value = cache.get(existingChunkKey);
            Assert.assertNotNull(value);
            Assert.assertTrue(value instanceof byte[]);
            byte[] buffer = (byte[]) cache.get(existingChunkKey);
            Assert.assertTrue(buffer.length != 0);
         } else if (key instanceof FileCacheKey) {
            FileCacheKey fileCacheKey = (FileCacheKey) key;
            Assert.assertEquals(fileCacheKey.getIndexName(), INDEX_NAME);
            String filename = fileCacheKey.getFileName();
            Object value = cache.get(fileCacheKey);
            Assert.assertNotNull(value);
            Assert.assertTrue(value instanceof FileMetadata);
            FileMetadata metadata = (FileMetadata) value;
            long totalFileSize = metadata.getSize();
            long actualFileSize = DirectoryIntegrityCheck.deepCountFileSize(fileCacheKey, cache);
            Assert.assertEquals(actualFileSize, totalFileSize);

            if(filename.contains(this.filename)) {
               Assert.assertFalse(fileList.contains(filename), fileCacheKey + " should not have existed");
            } else {
               Assert.assertTrue(fileList.contains(filename), fileCacheKey + " should not have existed");
            }
         } else if (key instanceof FileListCacheKey) {
            fileListCacheKeyInstances++;
            Assert.assertEquals(1, fileListCacheKeyInstances);
         }
      }
   }
}
