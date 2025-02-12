/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandlerWithCallback;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CorruptedFreeListException;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.LongListReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheGroupId;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2.PART_META_REUSE_LIST_ROOT_OFF;

/**
 * Tests that grid with corrupted partitions, where meta page is corrupted, fails on start with correct error.
 */
@WithSystemProperty(key = IGNITE_PDS_SKIP_CRC, value = "true")
public class PagesPossibleCorruptionDiagnosticTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 4096;

    /** */
    private static final int PAGE_STORE_VER = 2;

    /** */
    private volatile boolean correctFailure = false;

    /** */
    private FileVersionCheckingFactory storeFactory = new FileVersionCheckingFactory(
        new AsyncFileIOFactory(),
        new AsyncFileIOFactory(),
        () -> PAGE_SIZE
    ) {
        /** {@inheritDoc} */
        @Override public int latestVersion() {
            return PAGE_STORE_VER;
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            )
            .setFailureHandler(new FailureHandlerWithCallback(failureCtx ->
                correctFailure = failureCtx.error() instanceof CorruptedPartitionMetaPageException
                     && ((CorruptedDataStructureException)failureCtx.error()).pageIds().length > 0
            ));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @param ignite Ignite instance.
     * @param partId Partition id.
     * @return File page store for given partition id.
     * @throws IgniteCheckedException If failed.
     */
    private FilePageStore filePageStore(IgniteEx ignite, int partId) throws IgniteCheckedException {
        File cacheWorkDir = ignite.context().pdsFolderResolver().fileTree().cacheStorage(false, DEFAULT_CACHE_NAME);

        File partFile = new File(cacheWorkDir, format(PART_FILE_TEMPLATE, partId));

        return (FilePageStore)storeFactory.createPageStore(FLAG_DATA, partFile, a -> {});
    }

    /** Tests that node with corrupted partition fails on start. */
    @Test
    public void testCorruptedNodeFailsOnStart() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
        );

        cache.put(1, 1);

        PartitionUpdateCounter counter = counter(0, DEFAULT_CACHE_NAME, ignite.name());

        counter.update(10, 5);

        forceCheckpoint();

        FilePageStore store = filePageStore(ignite, 0);

        stopAllGrids();

        store.ensure();

        // Index of meta page is always 0.
        int metaPageIdx = 0;

        long offset = store.headerSize() + metaPageIdx * PAGE_SIZE + PART_META_REUSE_LIST_ROOT_OFF;

        writeLongToFileByOffset(store.getFileAbsolutePath(), offset, 0L);

        try {
            startGrid(0);
        }
        catch (Exception ignored) {
            /* No-op. */
        }

        assertTrue(correctFailure);
    }

    /** Tests page list pages are collected in {@link CorruptedFreeListException}. */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING, value = "true")
    public void testDiagnosticCollectedOnCorruptedPageList() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
        );

        cache.put(1, 1);

        cache.remove(1);

        int grpId = cacheGroupId(DEFAULT_CACHE_NAME, null);

        IgniteCacheOffheapManager.CacheDataStore dataStore =
            ignite.context().cache().cacheGroup(grpId).offheap().cacheDataStores().iterator().next();

        GridCacheOffheapManager.GridCacheDataStore store = (GridCacheOffheapManager.GridCacheDataStore)dataStore;

        AbstractFreeList freeList = store.getCacheStoreFreeList();

        ReuseBag bag = new LongListReuseBag();

        bag.addFreePage(pageId(0, FLAG_DATA, 10));
        bag.addFreePage(pageId(0, FLAG_DATA, 11));

        long[] pages = null;

        try {
            freeList.addForRecycle(bag);
        }
        catch (CorruptedFreeListException e) {
            pages = e.pageIds();
        }

        assertNotNull(pages);
    }

    /**
     * Write any number to any place in the file.
     *
     * @param path Path to the file.
     * @param offset Offset.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void writeLongToFileByOffset(String path, long offset, long val) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(8);

        buf.putLong(val);

        buf.rewind();

        try (FileChannel c = new RandomAccessFile(new File(path), "rw").getChannel()) {
            c.position(offset);

            c.write(buf);
        }
    }
}
