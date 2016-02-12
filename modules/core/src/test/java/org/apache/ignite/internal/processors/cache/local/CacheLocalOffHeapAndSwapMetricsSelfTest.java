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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheLocalOffHeapAndSwapMetricsSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 1;

    /** Keys count. */
    private static final int KEYS_CNT = 1000;

    /** Max size. */
    private static final int MAX_SIZE = 100;

    /** Entry size. */
    private static final int ENTRY_SIZE = 86; // Calculated as allocated size divided on entries count.

    /** Offheap max count. */
    private static final int OFFHEAP_MAX_CNT = KEYS_CNT / 2;

    /** Offheap max size. */
    private static final int OFFHEAP_MAX_SIZE = ENTRY_SIZE * OFFHEAP_MAX_CNT;

    /** Cache. */
    private IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * @param offHeapSize Max off-heap size.
     * @param swapEnabled Swap enabled.
     */
    private void createCache(int offHeapSize, boolean swapEnabled) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setStatisticsEnabled(true);

        ccfg.setCacheMode(CacheMode.LOCAL);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);

        ccfg.setOffHeapMaxMemory(offHeapSize);
        ccfg.setSwapEnabled(swapEnabled);

        ccfg.setEvictionPolicy(new FifoEvictionPolicy(MAX_SIZE));

        cache = grid(0).getOrCreateCache(ccfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (cache != null)
            cache.destroy();
    }

    /**
     * @throws Exception if failed.
     */
    public void testOffHeapMetrics() throws Exception {
        createCache(0, false);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(0f, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100f, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT * 3, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(100 / 3.0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100 - (100 / 3.0), cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - MAX_SIZE) / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());
    }

    /**
     * @throws Exception if failed.
     */
    public void testSwapMetrics() throws Exception {
        createCache(-1, true);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(0f, cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100f, cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT * 3, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(100 / 3.0, cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100 - (100 / 3.0), cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - MAX_SIZE) / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());
    }

    /**
     * @throws Exception if failed.
     */
    public void testOffHeapAndSwapMetrics() throws Exception {
        createCache(OFFHEAP_MAX_SIZE, true);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(0f, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100f, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(0f, cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100f, cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(0.0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100.0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions() - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT * 3, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(0.0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 3, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100.0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions() - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT * 3, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(100 / 3.0, cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100 - (100 / 3.0), cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions(), cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits());
        assertEquals(100 * OFFHEAP_MAX_CNT / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 4 - OFFHEAP_MAX_CNT - MAX_SIZE, cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses());
        assertEquals(100 * (KEYS_CNT * 4 - OFFHEAP_MAX_CNT - MAX_SIZE) / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage(), 0.1);
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions() - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions(), cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        assertEquals(KEYS_CNT * 2 - MAX_SIZE - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - MAX_SIZE - OFFHEAP_MAX_CNT) / (KEYS_CNT * 4.0 - MAX_SIZE - OFFHEAP_MAX_CNT),
            cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - MAX_SIZE - OFFHEAP_MAX_CNT),
            cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - MAX_SIZE - OFFHEAP_MAX_CNT, cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());

        assertEquals(0, cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());
    }

    /**
     * Prints stats.
     */
    protected void printStat() {
        System.out.println("!!! -------------------------------------------------------");
        System.out.println("!!! Puts: cache = " + cache.metrics(grid(0).cluster().forLocal()).getCachePuts() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapPuts() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapPuts());
        System.out.println("!!! Gets: cache = " + cache.metrics(grid(0).cluster().forLocal()).getCacheGets() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapGets() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapGets());
        System.out.println("!!! Removes: cache = " + cache.metrics(grid(0).cluster().forLocal()).getCacheRemovals() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapRemovals() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapRemovals());
        System.out.println("!!! Evictions: cache = " + cache.metrics(grid(0).cluster().forLocal()).getCacheEvictions() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapEvictions() +
            ", swap = none" );
        System.out.println("!!! Hits: cache = " + cache.metrics(grid(0).cluster().forLocal()).getCacheHits() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapHits() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapHits());
        System.out.println("!!! Hit(%): cache = " + cache.metrics(grid(0).cluster().forLocal()).getCacheHitPercentage() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapHitPercentage() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapHitPercentage());
        System.out.println("!!! Misses: cache = " + cache.metrics(grid(0).cluster().forLocal()).getCacheMisses() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapMisses() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapMisses());
        System.out.println("!!! Miss(%): cache = " + cache.metrics(grid(0).cluster().forLocal()).getCacheMissPercentage() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapMissPercentage() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapMissPercentage());
        System.out.println("!!! Entries: cache = " + cache.metrics(grid(0).cluster().forLocal()).getSize() +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapEntriesCount() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapEntriesCount());
        System.out.println("!!! Size: cache = none" +
            ", offheap = " + cache.metrics(grid(0).cluster().forLocal()).getOffHeapAllocatedSize() +
            ", swap = " + cache.metrics(grid(0).cluster().forLocal()).getSwapSize());
        System.out.println();
    }
}