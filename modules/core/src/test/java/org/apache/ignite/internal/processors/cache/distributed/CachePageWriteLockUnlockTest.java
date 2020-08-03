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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 *
 */
public class CachePageWriteLockUnlockTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTITION = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        cfg.setActiveOnStart(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(Integer.MAX_VALUE);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testPreloadPartition() throws Exception {
        try {
            IgniteEx grid0 = startGrid(0);

            grid0.cluster().active(true);

            int total = 512;

            putData(grid0, total, PARTITION);

            grid0.cache(DEFAULT_CACHE_NAME).removeAll();

            forceCheckpoint();

            stopGrid(0);

            grid0 = startGrid(0);

            grid0.cluster().active(true);

            putData(grid0, total, PARTITION); // Will use pages from reuse pool.

            forceCheckpoint();

            stopGrid(0);

            grid0 = startGrid(0);

            grid0.cluster().active(true);

            preloadPartition(grid0, DEFAULT_CACHE_NAME, PARTITION);

            Iterator<Cache.Entry<Object, Object>> it = grid0.cache(DEFAULT_CACHE_NAME).iterator();

            int c0 = 0;

            while (it.hasNext()) {
                Cache.Entry<Object, Object> entry = it.next();

                c0++;
            }

            assertEquals(total, c0);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param grid Grid.
     * @param total Total.
     * @param part Partition.
     */
    private void putData(Ignite grid, int total, int part) {
        int c = 0, k = 0;

        while (c < total) {
            if (grid(0).affinity(DEFAULT_CACHE_NAME).partition(k) == part) {
                grid.cache(DEFAULT_CACHE_NAME).put(k, k);

                c++;
            }

            k++;
        }
    }

    /**
     * Preload partition fast by iterating on all pages in disk order.
     *
     * @param grid Grid.
     * @param cacheName Cache name.
     * @param p P.
     */
    private void preloadPartition(Ignite grid, String cacheName, int p) throws IgniteCheckedException {
        GridDhtCacheAdapter<Object, Object> dht = ((IgniteKernal)grid).internalCache(cacheName).context().dht();

        GridDhtLocalPartition part = dht.topology().localPartition(p);

        assertNotNull(part);

        assertTrue(part.state() == OWNING);

        CacheGroupContext grpCtx = dht.context().group();

        if (part.state() != OWNING)
            return;

        IgnitePageStoreManager pageStoreMgr = grpCtx.shared().pageStore();

        if (pageStoreMgr instanceof FilePageStoreManager) {
            FilePageStoreManager filePageStoreMgr = (FilePageStoreManager)pageStoreMgr;

            PageStore pageStore = filePageStoreMgr.getStore(grpCtx.groupId(), part.id());

            PageMemoryEx pageMemory = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

            long pageId = pageMemory.partitionMetaPageId(grpCtx.groupId(), part.id());

            for (int pageNo = 0; pageNo < pageStore.pages(); pageId++, pageNo++) {
                long pagePointer = -1;

                try {
                    pagePointer = pageMemory.acquirePage(grpCtx.groupId(), pageId);
                }
                finally {
                    if (pagePointer != -1)
                        pageMemory.releasePage(grpCtx.groupId(), pageId, pagePointer);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }
}
