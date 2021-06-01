/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.metric;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.Person;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

/**
 * Test class for checking that index page metrics stay correct in case of page displacement.
 */
public class IndexPagesMetricsPageDisplacementTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "test";

    /** */
    private IgniteEx grid;

    /** */
    private IgniteCache<Integer, Person> cache;

    /** */
    private IndexPageCounter idxPageCounter;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();

        grid = startGrid();
        grid.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Integer, Person> cacheConfiguration = new CacheConfiguration<Integer, Person>(TEST_CACHE_NAME)
            .setIndexedTypes(Integer.class, Person.class);
        cache = grid.createCache(cacheConfiguration);

        idxPageCounter = new IndexPageCounter(grid, true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid.close();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setMaxSize(12 * 1024 * 1024) // 12 MB
                            .setPersistenceEnabled(true)
                            .setPageReplacementMode(PageReplacementMode.RANDOM_LRU)
                            .setEvictionThreshold(0.1)
                    )
            );
    }

    /**
     * Inserts data into the cache as long as it fills enough for some data pages to get dumped to the
     * storage. Since index page metrics should reflect the number of in-memory pages, the metric value is expected to
     * go down.
     */
    @Test
    public void testPageDisplacement() throws IgniteCheckedException {
        int grpId = grid.cachex(TEST_CACHE_NAME).context().groupId();

        DataRegion dataRegion = grid.context().cache().context().database().dataRegion(null);
        PageMetrics pageMetrics = dataRegion.metrics().cacheGrpPageMetrics(grpId);

        int personId = 0;
        long idxPagesOnDisk;
        long idxPagesInMemory;

        do {
            // insert data into the cache until some index pages get displaced to the storage
            for (int i = 0; i < 100; i++, personId++)
                cache.put(personId, new Person(personId, "foobar"));

            forceCheckpoint(grid);

            idxPagesOnDisk = getIdxPagesOnDisk(grpId).size();
            idxPagesInMemory = idxPageCounter.countIdxPagesInMemory(grpId);

            assertThat(pageMetrics.indexPages().value(), is(idxPagesInMemory));

        } while (idxPagesOnDisk <= idxPagesInMemory);

        // load pages back into memory and check that the metric value has increased
        touchIdxPages(grpId);

        long allIdxPagesInMemory = idxPageCounter.countIdxPagesInMemory(grpId);

        assertThat(allIdxPagesInMemory, greaterThan(idxPagesInMemory));
        assertThat(pageMetrics.indexPages().value(), is(allIdxPagesInMemory));
    }

    /**
     * Acquires all pages inside the index partition for the given cache group in order to load them back into
     * memory if some of them had been displaced to the storage earlier.
     */
    private void touchIdxPages(int grpId) throws IgniteCheckedException {
        DataRegion dataRegion = grid.context().cache().context().database().dataRegion(null);
        PageMemory pageMemory = dataRegion.pageMemory();

        for (long pageId : getIdxPagesOnDisk(grpId))
            // acquire, but not release all pages, so that they don't get displaced back to the storage
            pageMemory.acquirePage(grpId, pageId);
    }

    /**
     * Returns IDs of index pages currently residing in the storage.
     */
    private List<Long> getIdxPagesOnDisk(int grpId) throws IgniteCheckedException {
        FilePageStoreManager pageStoreMgr = (FilePageStoreManager) grid.context().cache().context().pageStore();
        FilePageStore pageStore = (FilePageStore) pageStoreMgr.getStore(grpId, PageIdAllocator.INDEX_PARTITION);

        List<Long> result = new ArrayList<>();

        ByteBuffer buf = ByteBuffer
            .allocateDirect(pageStore.getPageSize())
            .order(ByteOrder.nativeOrder());

        // Page Store contains a one-page header
        long numPages = pageStore.size() / pageStore.getPageSize() - 1;

        for (int i = 0; i < numPages; i++) {
            long pageId = PageIdUtils.pageId(PageIdAllocator.INDEX_PARTITION, (byte)0, i);

            try {
                pageStore.read(pageId, buf, false);
            } catch (IgniteDataIntegrityViolationException ignored) {
                // sometimes we try to access an invalid page, in which case this exception will be thrown.
                // We simply ignore it and try to access other pages.
            }

            if (PageIO.isIndexPage(PageIO.getType(buf)))
                result.add(PageIO.getPageId(buf));

            buf.clear();
        }

        return result;
    }
}
