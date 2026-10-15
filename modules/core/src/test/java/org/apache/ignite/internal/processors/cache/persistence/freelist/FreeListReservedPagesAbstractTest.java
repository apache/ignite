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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Base for tests verifying that the atomic page reservation of an in-memory free list with enabled page eviction is
 * fully released after inserts.
 */
public abstract class FreeListReservedPagesAbstractTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setInitialSize(10 * 1024 * 1024L)
                .setMaxSize(20 * 1024 * 1024L)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @return Pages currently reserved by in-flight inserts on the given free list.
     */
    protected static long reservedPages(FreeList<CacheDataRow> freeList) {
        return ((AtomicLong)getFieldValue(freeList, "reservedPages")).get();
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @return The free list of the first partition data store of the named cache on the given node.
     */
    protected static FreeList<CacheDataRow> freeList(IgniteEx ignite, String cacheName) {
        return (FreeList<CacheDataRow>)ignite.context().cache().cache(cacheName).context().offheap()
            .cacheDataStores().iterator().next().rowStore().freeList();
    }

    /**
     * Asserts that {@code reservedPages} is {@code 0} on every data store of every started node, waiting up to
     * {@code timeoutMs} for the reservation to drain.
     *
     * @param cacheName Cache name.
     * @param timeoutMs Wait timeout in milliseconds.
     * @throws Exception If the reservation does not drain in time.
     */
    public static void assertAllReleased(String cacheName, long timeoutMs) throws Exception {
        for (Ignite ignite : Ignition.allGrids()) {
            for (CacheDataStore store : ((IgniteEx)ignite).context().cache().cache(cacheName).context().offheap().cacheDataStores()) {
                FreeList<CacheDataRow> freeList = (FreeList<CacheDataRow>)store.rowStore().freeList();

                if (reservedPages(freeList) == 0)
                    continue;

                assertTrue(GridTestUtils.waitForCondition(() -> reservedPages(freeList) == 0, timeoutMs));
            }
        }
    }
}
