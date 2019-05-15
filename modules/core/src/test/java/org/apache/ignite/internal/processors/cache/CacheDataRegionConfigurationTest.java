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
package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class CacheDataRegionConfigurationTest extends GridCommonAbstractTest {
    /** */
    private volatile CacheConfiguration ccfg;

    /** */
    private volatile DataStorageConfiguration memCfg;

    /** */
    private static final long DFLT_MEM_PLC_SIZE = 10L * 1024 * 1024;

    /** */
    private static final long BIG_MEM_PLC_SIZE = 1024L * 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (memCfg != null)
            cfg.setDataStorageConfiguration(memCfg);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    private void checkStartGridException(Class<? extends Throwable> ex, String message) {
        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                startGrid(0);
                return null;
            }
        }, ex, message);
    }

    /**
     * Verifies that proper exception is thrown when DataRegion is misconfigured for cache.
     */
    @Test
    public void testMissingDataRegion() {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setDataRegionName("nonExistingMemPlc");

        checkStartGridException(IgniteCheckedException.class, "Requested DataRegion is not configured");
    }

    /**
     * Verifies that {@link IgniteOutOfMemoryException} is thrown when cache is configured with too small DataRegion.
     */
    @Test
    public void testTooSmallDataRegion() throws Exception {
        memCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        dfltPlcCfg.setMaxSize(DFLT_MEM_PLC_SIZE);

        DataRegionConfiguration bigPlcCfg = new DataRegionConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(BIG_MEM_PLC_SIZE);

        memCfg.setDataRegionConfigurations(bigPlcCfg);
        memCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        boolean oomeThrown = false;

        try {
            for (int i = 0; i < 500_000; i++)
                cache.put(i, "abc");
        }
        catch (Exception e) {
            Throwable cause = e;

            do {
                if (cause instanceof IgniteOutOfMemoryException) {
                    oomeThrown = true;
                    break;
                }

                if (cause == null)
                    break;

                if (cause.getSuppressed() == null || cause.getSuppressed().length == 0)
                    cause = cause.getCause();
                else
                    cause = cause.getSuppressed()[0];
            }
            while (true);
        }

        if (!oomeThrown)
            fail("OutOfMemoryException hasn't been thrown");
    }

    /**
     * Verifies that with enough memory allocated adding values to cache doesn't cause any exceptions.
     */
    @Test
    public void testProperlySizedMemoryPolicy() throws Exception {
        memCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        dfltPlcCfg.setMaxSize(DFLT_MEM_PLC_SIZE);

        DataRegionConfiguration bigPlcCfg = new DataRegionConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(BIG_MEM_PLC_SIZE);

        memCfg.setDataRegionConfigurations(bigPlcCfg);
        memCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setDataRegionName("bigPlc");

        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        try {
            for (int i = 0; i < 500_000; i++)
                cache.put(i, "abc");
        }
        catch (Exception e) {
            fail("With properly sized DataRegion no exceptions are expected to be thrown.");
        }
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when swap and persistence are enabled at the same time
     * for a data region.
     */
    @Test
    public void testSetPersistenceAndSwap() {
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        // Enabling the persistence.
        invCfg.setPersistenceEnabled(true);
        // Enabling the swap space.
        invCfg.setSwapPath("/path/to/some/directory");

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setDataRegionName("ccfg");

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when page eviction threshold is less than 0.5.
     */
    @Test
    public void testSetSmallInvalidEviction() {
        final double SMALL_EVICTION_THRESHOLD = 0.1D;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the page eviction threshold less than 0.5
        invCfg.setEvictionThreshold(SMALL_EVICTION_THRESHOLD);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when page eviction threshold is greater than 0.999.
     */
    @Test
    public void testSetBigInvalidEviction() {
        final double BIG_EVICTION_THRESHOLD = 1.0D;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the page eviction threshold greater than 0.999
        invCfg.setEvictionThreshold(BIG_EVICTION_THRESHOLD);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when empty pages pool size is less than 10
     */
    @Test
    public void testInvalidSmallEmptyPagesPoolSize() {
        final int SMALL_PAGES_POOL_SIZE = 5;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the pages pool size less than 10
        invCfg.setEmptyPagesPoolSize(SMALL_PAGES_POOL_SIZE);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when empty pages pool size is greater than
     * DataRegionConfiguration.getMaxSize() / DataStorageConfiguration.getPageSize() / 10.
     */
    @Test
    public void testInvalidBigEmptyPagesPoolSize() {
        final int DFLT_PAGE_SIZE = 1024;
        long expectedMaxPoolSize;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);
        memCfg.setPageSize(DFLT_PAGE_SIZE);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        expectedMaxPoolSize = invCfg.getMaxSize() / memCfg.getPageSize() / 10;

        if (expectedMaxPoolSize < Integer.MAX_VALUE) {
            // Setting the empty pages pool size greater than
            // DataRegionConfiguration.getMaxSize() / DataStorageConfiguration.getPageSize() / 10
            invCfg.setEmptyPagesPoolSize((int)expectedMaxPoolSize + 1);
            memCfg.setDataRegionConfigurations(invCfg);
            checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
        }
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when IgniteCheckedException if validation of
     * memory metrics properties fails. Metrics rate time interval must not be less than 1000ms.
     */
    @Test
    public void testInvalidMetricsProperties() {
        final long SMALL_RATE_TIME_INTERVAL_MS = 999;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the metrics rate time less then 1000ms
        invCfg.setMetricsRateTimeInterval(SMALL_RATE_TIME_INTERVAL_MS);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when IgniteCheckedException if validation of
     * memory metrics properties fails. Metrics sub interval count must be positive.
     */
    @Test
    public void testInvalidSubIntervalCount() {
        final int NEG_SUB_INTERVAL_COUNT = -1000;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the metrics sub interval count as negative
        invCfg.setMetricsSubIntervalCount(NEG_SUB_INTERVAL_COUNT);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }
}
