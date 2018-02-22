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
public class LocalCacheOffHeapAndSwapMetricsSelfTest extends GridCommonAbstractTest {
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
     * @param memoryMode Cache memory mode.
     * @param offHeapSize Max off-heap size.
     * @param swapEnabled Swap enabled.
     */
    private void createCache(CacheMemoryMode memoryMode, int offHeapSize, boolean swapEnabled) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setStatisticsEnabled(true);

        ccfg.setCacheMode(CacheMode.LOCAL);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setMemoryMode(memoryMode);

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
        createCache(CacheMemoryMode.ONHEAP_TIERED, 0, false);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0f, cache.localMetrics().getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapMisses());
        assertEquals(100f, cache.localMetrics().getOffHeapMissPercentage());
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapHits());
        assertEquals(100 / 3.0, cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 - (100 / 3.0), cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(KEYS_CNT - MAX_SIZE, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE, cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.localMetrics().getOffHeapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - MAX_SIZE) / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(0, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());
    }

    /**
     * @throws Exception if failed.
     */
    public void testSwapMetrics() throws Exception {
        createCache(CacheMemoryMode.ONHEAP_TIERED, -1, true);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapGets());
        assertEquals(0, cache.localMetrics().getSwapHits());
        assertEquals(0f, cache.localMetrics().getSwapHitPercentage());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapMisses());
        assertEquals(100f, cache.localMetrics().getSwapMissPercentage());
        assertEquals(0, cache.localMetrics().getSwapRemovals());

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE, cache.localMetrics().getSwapEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 / 3.0, cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapMisses());
        assertEquals(100 - (100 / 3.0), cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE, cache.localMetrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.localMetrics().getSwapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - MAX_SIZE) / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - MAX_SIZE, cache.localMetrics().getSwapRemovals());

        assertEquals(0, cache.localMetrics().getSwapEntriesCount());
    }

    /**
     * @throws Exception if failed.
     */
    public void testOffHeapAndSwapMetrics() throws Exception {
        createCache(CacheMemoryMode.ONHEAP_TIERED, OFFHEAP_MAX_SIZE, true);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0f, cache.localMetrics().getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapMisses());
        assertEquals(100f, cache.localMetrics().getOffHeapMissPercentage());
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE - OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapGets());
        assertEquals(0, cache.localMetrics().getSwapHits());
        assertEquals(0f, cache.localMetrics().getSwapHitPercentage());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapMisses());
        assertEquals(100f, cache.localMetrics().getSwapMissPercentage());
        assertEquals(0, cache.localMetrics().getSwapRemovals());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0.0, cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapMisses());
        assertEquals(100.0, cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(cache.localMetrics().getCacheEvictions() - OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0.0, cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getOffHeapMisses());
        assertEquals(100.0, cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(cache.localMetrics().getCacheEvictions() - OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 / 3.0, cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapMisses());
        assertEquals(100 - (100 / 3.0), cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(KEYS_CNT - MAX_SIZE - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE, cache.localMetrics().getOffHeapGets());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapHits());
        assertEquals(100 * OFFHEAP_MAX_CNT / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 4 - OFFHEAP_MAX_CNT - MAX_SIZE, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 * (KEYS_CNT * 4 - OFFHEAP_MAX_CNT - MAX_SIZE) / (KEYS_CNT * 4.0 - MAX_SIZE),
            cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapRemovals());

        assertEquals(cache.localMetrics().getCacheEvictions() - OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEvictions());
        assertEquals(0, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 4 - MAX_SIZE - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT * 2 - MAX_SIZE - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - MAX_SIZE - OFFHEAP_MAX_CNT) / (KEYS_CNT * 4.0 - MAX_SIZE - OFFHEAP_MAX_CNT),
            cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - MAX_SIZE - OFFHEAP_MAX_CNT),
            cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - MAX_SIZE - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(0, cache.localMetrics().getSwapEntriesCount());
    }

    /**
     * @throws Exception if failed.
     */
    public void testOffHeapMetricsInOffHeapTiered() throws Exception {
        createCache(CacheMemoryMode.OFFHEAP_TIERED, 0, false);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapPuts());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapGets());
        assertEquals(0, cache.metrics().getOffHeapHits());
        assertEquals(0f, cache.metrics().getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapMisses());
        assertEquals(100f, cache.metrics().getOffHeapMissPercentage());
        assertEquals(0, cache.metrics().getOffHeapRemovals());

        assertEquals(0, cache.metrics().getOffHeapEvictions());
        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapEntriesCount());
        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 2, cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.metrics().getOffHeapRemovals());

        assertEquals(0, cache.metrics().getOffHeapEvictions());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapEntriesCount());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 3, cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapHits());
        assertEquals(100 / 3.0, cache.metrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics().getOffHeapMisses());
        assertEquals(100 - (100 / 3.0), cache.metrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.metrics().getOffHeapRemovals());

        assertEquals(0, cache.metrics().getOffHeapEvictions());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapEntriesCount());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 4, cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 2, cache.metrics().getOffHeapHits());
        assertEquals(100 * (KEYS_CNT * 2.0) / (KEYS_CNT * 4.0),
            cache.metrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics().getOffHeapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0),
            cache.metrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapRemovals());

        assertEquals(0, cache.metrics().getOffHeapEvictions());
        assertEquals(0, cache.metrics().getOffHeapEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());
    }

    /**
     * @throws Exception if failed.
     */
    public void testOffHeapAndSwapMetricsInOffHeapTiered() throws Exception {
        createCache(CacheMemoryMode.OFFHEAP_TIERED, OFFHEAP_MAX_SIZE, true);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapGets());
        assertEquals(0, cache.metrics().getOffHeapHits());
        assertEquals(0f, cache.metrics().getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.metrics().getOffHeapMisses());
        assertEquals(100f, cache.metrics().getOffHeapMissPercentage());
        assertEquals(0, cache.metrics().getOffHeapRemovals());

        assertEquals(KEYS_CNT - OFFHEAP_MAX_CNT, cache.metrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics().getOffHeapEvictions(), cache.metrics().getSwapPuts());
        assertEquals(KEYS_CNT, cache.metrics().getSwapGets());
        assertEquals(0, cache.metrics().getSwapHits());
        assertEquals(0f, cache.metrics().getSwapHitPercentage());
        assertEquals(KEYS_CNT, cache.metrics().getSwapMisses());
        assertEquals(100f, cache.metrics().getSwapMissPercentage());
        assertEquals(0, cache.metrics().getSwapRemovals());

        assertEquals(cache.metrics().getOffHeapEvictions(), cache.metrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 2, cache.metrics().getOffHeapGets());
        assertEquals(0, cache.metrics().getOffHeapHits());
        assertEquals(0.0, cache.metrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics().getOffHeapMisses());
        assertEquals(100.0, cache.metrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.metrics().getOffHeapRemovals());

        assertEquals(cache.metrics().getCacheEvictions() - OFFHEAP_MAX_CNT, cache.metrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics().getOffHeapEvictions(), cache.metrics().getSwapPuts());
        assertEquals(KEYS_CNT * 2, cache.metrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.metrics().getSwapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.metrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics().getSwapRemovals());

        assertEquals(KEYS_CNT - OFFHEAP_MAX_CNT, cache.metrics().getSwapEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 3, cache.metrics().getOffHeapGets());
        assertEquals(0, cache.metrics().getOffHeapHits());
        assertEquals(0.0, cache.metrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 3, cache.metrics().getOffHeapMisses());
        assertEquals(100.0, cache.metrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.metrics().getOffHeapRemovals());

        assertEquals(cache.metrics().getCacheEvictions() - OFFHEAP_MAX_CNT - KEYS_CNT,
            cache.metrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics().getOffHeapEvictions(), cache.metrics().getSwapPuts());
        assertEquals(KEYS_CNT * 3, cache.metrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.metrics().getSwapHits());
        assertEquals(100 / 3.0, cache.metrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics().getSwapMisses());
        assertEquals(100 - (100 / 3.0), cache.metrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.metrics().getSwapRemovals());

        assertEquals(KEYS_CNT - OFFHEAP_MAX_CNT, cache.metrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.metrics().getCacheEvictions(), cache.metrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 4, cache.metrics().getOffHeapGets());
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapHits());
        assertEquals(100 * OFFHEAP_MAX_CNT / (KEYS_CNT * 4.0),
            cache.metrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 4 - OFFHEAP_MAX_CNT, cache.metrics().getOffHeapMisses());
        assertEquals(100 * (KEYS_CNT * 4 - OFFHEAP_MAX_CNT) / (KEYS_CNT * 4.0),
            cache.metrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(OFFHEAP_MAX_CNT, cache.metrics().getOffHeapRemovals());

        assertEquals(cache.metrics().getCacheEvictions() - OFFHEAP_MAX_CNT - 2 * KEYS_CNT, cache.metrics().getOffHeapEvictions());
        assertEquals(0, cache.metrics().getOffHeapEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.metrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.metrics().getOffHeapEvictions(), cache.metrics().getSwapPuts());
        assertEquals(KEYS_CNT * 4 - OFFHEAP_MAX_CNT, cache.metrics().getSwapGets());
        assertEquals(KEYS_CNT * 2 - OFFHEAP_MAX_CNT, cache.metrics().getSwapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - OFFHEAP_MAX_CNT) / (KEYS_CNT * 4.0 - OFFHEAP_MAX_CNT),
            cache.metrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.metrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - OFFHEAP_MAX_CNT),
            cache.metrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - OFFHEAP_MAX_CNT, cache.metrics().getSwapRemovals());

        assertEquals(0, cache.metrics().getSwapEntriesCount());
    }

    /**
     * Prints stats.
     */
    protected void printStat() {
        System.out.println("!!! -------------------------------------------------------");
        System.out.println("!!! Puts: cache = " + cache.localMetrics().getCachePuts() +
            ", offheap = " + cache.localMetrics().getOffHeapPuts() +
            ", swap = " + cache.localMetrics().getSwapPuts());
        System.out.println("!!! Gets: cache = " + cache.localMetrics().getCacheGets() +
            ", offheap = " + cache.localMetrics().getOffHeapGets() +
            ", swap = " + cache.localMetrics().getSwapGets());
        System.out.println("!!! Removes: cache = " + cache.localMetrics().getCacheRemovals() +
            ", offheap = " + cache.localMetrics().getOffHeapRemovals() +
            ", swap = " + cache.localMetrics().getSwapRemovals());
        System.out.println("!!! Evictions: cache = " + cache.localMetrics().getCacheEvictions() +
            ", offheap = " + cache.localMetrics().getOffHeapEvictions() +
            ", swap = none" );
        System.out.println("!!! Hits: cache = " + cache.localMetrics().getCacheHits() +
            ", offheap = " + cache.localMetrics().getOffHeapHits() +
            ", swap = " + cache.localMetrics().getSwapHits());
        System.out.println("!!! Hit(%): cache = " + cache.localMetrics().getCacheHitPercentage() +
            ", offheap = " + cache.localMetrics().getOffHeapHitPercentage() +
            ", swap = " + cache.localMetrics().getSwapHitPercentage());
        System.out.println("!!! Misses: cache = " + cache.localMetrics().getCacheMisses() +
            ", offheap = " + cache.localMetrics().getOffHeapMisses() +
            ", swap = " + cache.localMetrics().getSwapMisses());
        System.out.println("!!! Miss(%): cache = " + cache.localMetrics().getCacheMissPercentage() +
            ", offheap = " + cache.localMetrics().getOffHeapMissPercentage() +
            ", swap = " + cache.localMetrics().getSwapMissPercentage());
        System.out.println("!!! Entries: cache = " + cache.localMetrics().getSize() +
            ", offheap = " + cache.localMetrics().getOffHeapEntriesCount() +
            ", swap = " + cache.localMetrics().getSwapEntriesCount());
        System.out.println("!!! Size: cache = none" +
            ", offheap = " + cache.localMetrics().getOffHeapAllocatedSize() +
            ", swap = " + cache.localMetrics().getSwapSize());
        System.out.println();
    }
}