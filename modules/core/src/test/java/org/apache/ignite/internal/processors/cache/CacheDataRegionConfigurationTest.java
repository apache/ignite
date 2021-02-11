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

import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Objects.nonNull;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
public class CacheDataRegionConfigurationTest extends GridCommonAbstractTest {
    /** */
    private volatile CacheConfiguration ccfg;

    /** */
    private volatile DataStorageConfiguration memCfg;

    /** Failure handler. */
    @Nullable private FailureHandler failureHnd;

    /** */
    private IgniteLogger logger;

    /** */
    private static final long DFLT_MEM_PLC_SIZE = 10L * 1024 * 1024;

    /** */
    private static final long BIG_MEM_PLC_SIZE = 1024L * 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (nonNull(logger))
            cfg.setGridLogger(logger);

        if (nonNull(failureHnd))
            cfg.setFailureHandler(failureHnd);

        if (gridName.contains("client")) {
            cfg.setClientMode(true);

            return cfg;
        }

        if (nonNull(memCfg))
            cfg.setDataStorageConfiguration(memCfg);

        if (nonNull(ccfg))
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    private void checkStartGridException(Class<? extends Throwable> ex, String message) {
        assertThrows(log(), () -> {
            startGrid(0);

            return null;
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
     * Filter to exclude the node from affinity nodes by its name.
     */
    private static class NodeNameNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final String filteredNode;

        /**
         * @param node Node.
         */
        private NodeNameNodeFilter(String node) {
            filteredNode = node;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.attribute("org.apache.ignite.ignite.name").toString().contains(filteredNode);
        }
    }

    /**
     * Verifies that warning message is printed to the logs if user tries to start a static cache in data region which
     * overhead (e.g. metapages for partitions) occupies more space of the region than a defined threshold (15%)
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWarningIfStaticCacheOverheadExceedsThreshold() throws Exception {
        DataRegionConfiguration smallRegionCfg = new DataRegionConfiguration();
        int numOfPartitions = 512;
        int partitionsMetaMemoryChunk = U.sizeInMegabytes(512 * DFLT_PAGE_SIZE);

        smallRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setPersistenceEnabled(true);
        smallRegionCfg.setName("smallRegion");

        memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(smallRegionCfg);
        //one hour to guarantee that checkpoint will be triggered by 'dirty pages amount' trigger
        memCfg.setCheckpointFrequency(60 * 60 * 1000);

        CacheConfiguration<Object, Object> manyPartitionsCache = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        //512 partitions are enough only if primary and backups count
        manyPartitionsCache.setAffinity(new RendezvousAffinityFunction(false, numOfPartitions));
        manyPartitionsCache.setBackups(1);

        ccfg = manyPartitionsCache;

        ListeningTestLogger srv0Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr0 = matches("Cache group 'default' brings high overhead").build();
        LogListener dataRegLsnr0 = matches("metainformation in data region 'smallRegion'").build();
        LogListener partsInfoLsnr0 = matches(numOfPartitions + " partitions, " +
            DFLT_PAGE_SIZE +
            " bytes per partition, " + partitionsMetaMemoryChunk + " MBs total").build();
        srv0Logger.registerAllListeners(cacheGrpLsnr0, dataRegLsnr0, partsInfoLsnr0);
        logger = srv0Logger;

        IgniteEx ignite0 = startGrid("srv0");

        ListeningTestLogger srv1Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr1 = matches("Cache group 'default' brings high overhead").build();
        LogListener dataRegLsnr1 = matches("metainformation in data region 'smallRegion'").build();
        LogListener partsInfoLsnr1 = matches(numOfPartitions + " partitions, " +
            DFLT_PAGE_SIZE +
            " bytes per partition, " + partitionsMetaMemoryChunk + " MBs total").build();
        srv1Logger.registerAllListeners(cacheGrpLsnr1, dataRegLsnr1, partsInfoLsnr1);
        logger = srv1Logger;

        startGrid("srv1");

        ignite0.cluster().active(true);

        //srv0 and srv1 print warning into the log as the threshold for cache in default cache group is broken
        assertTrue(cacheGrpLsnr0.check());
        assertTrue(dataRegLsnr0.check());
        assertTrue(partsInfoLsnr0.check());

        assertTrue(cacheGrpLsnr1.check());
        assertTrue(dataRegLsnr1.check());
        assertTrue(partsInfoLsnr1.check());
    }

    /**
     * Verifies that warning message is printed to the logs if user tries to start a dynamic cache in data region which
     * overhead (e.g. metapages for partitions) occupies more space of the region than a defined threshold.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWarningIfDynamicCacheOverheadExceedsThreshold() throws Exception {
        String filteredSrvName = "srv2";
        int numOfPartitions = 512;
        int partitionsMetaMemoryChunk = U.sizeInMegabytes(512 * DFLT_PAGE_SIZE);

        DataRegionConfiguration smallRegionCfg = new DataRegionConfiguration();

        smallRegionCfg.setName("smallRegion");
        smallRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setPersistenceEnabled(true);

        //explicit default data region configuration to test possible NPE case
        DataRegionConfiguration defaultRegionCfg = new DataRegionConfiguration();
        defaultRegionCfg.setName("defaultRegion");
        defaultRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setPersistenceEnabled(true);

        memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(defaultRegionCfg);
        memCfg.setDataRegionConfigurations(smallRegionCfg);
        //one hour to guarantee that checkpoint will be triggered by 'dirty pages amount' trigger
        memCfg.setCheckpointFrequency(60 * 60 * 1000);

        ListeningTestLogger srv0Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr0 = matches("Cache group 'default' brings high overhead").build();
        LogListener dataRegLsnr0 = matches("metainformation in data region 'defaultRegion'").build();
        LogListener partsInfoLsnr0 = matches(numOfPartitions + " partitions, " +
            DFLT_PAGE_SIZE +
            " bytes per partition, " + partitionsMetaMemoryChunk + " MBs total").build();
        srv0Logger.registerAllListeners(cacheGrpLsnr0, dataRegLsnr0, partsInfoLsnr0);
        logger = srv0Logger;

        IgniteEx ignite0 = startGrid("srv0");

        ListeningTestLogger srv1Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr1 = matches("Cache group 'default' brings high overhead").build();
        LogListener dataRegLsnr1 = matches("metainformation in data region 'defaultRegion'").build();
        LogListener partsInfoLsnr1 = matches(numOfPartitions + " partitions, " +
            DFLT_PAGE_SIZE +
            " bytes per partition, " + partitionsMetaMemoryChunk + " MBs total").build();
        srv1Logger.registerAllListeners(cacheGrpLsnr1, dataRegLsnr1, partsInfoLsnr1);
        logger = srv1Logger;

        startGrid("srv1");

        ListeningTestLogger srv2Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr2 = matches("Cache group 'default' brings high overhead").build();
        srv2Logger.registerListener(cacheGrpLsnr2);
        logger = srv2Logger;

        startGrid("srv2");

        ignite0.cluster().active(true);

        IgniteEx cl = startGrid("client01");

        CacheConfiguration<Object, Object> manyPartitionsCache = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        manyPartitionsCache.setAffinity(new RendezvousAffinityFunction(false, numOfPartitions));
        manyPartitionsCache.setNodeFilter(new NodeNameNodeFilter(filteredSrvName));
        manyPartitionsCache.setBackups(1);

        cl.createCache(manyPartitionsCache);

        //srv0 and srv1 print warning into the log as the threshold for cache in default cache group is broken
        assertTrue(cacheGrpLsnr0.check());
        assertTrue(dataRegLsnr0.check());
        assertTrue(partsInfoLsnr0.check());

        assertTrue(cacheGrpLsnr1.check());
        assertTrue(dataRegLsnr1.check());
        assertTrue(partsInfoLsnr1.check());

        //srv2 doesn't print the warning as it is filtered by node filter from affinity nodes
        assertFalse(cacheGrpLsnr2.check());
    }

    /**
     * Verifies that warning is printed out to logs if after removing nodes from baseline
     * some caches reach or cross dangerous limit of metainformation overhead per data region.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWarningOnBaselineTopologyChange() throws Exception {
        DataRegionConfiguration defaultRegionCfg = new DataRegionConfiguration();
        defaultRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setPersistenceEnabled(true);

        memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(defaultRegionCfg);
        //one hour to guarantee that checkpoint will be triggered by 'dirty pages amount' trigger
        memCfg.setCheckpointFrequency(60 * 60 * 1000);

        ListeningTestLogger srv0Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr0 = matches("Cache group 'default' brings high overhead").build();
        srv0Logger.registerListener(cacheGrpLsnr0);
        logger = srv0Logger;

        IgniteEx ignite0 = startGrid("srv0");

        ListeningTestLogger srv1Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr1 = matches("Cache group 'default' brings high overhead").build();
        srv1Logger.registerListener(cacheGrpLsnr1);
        logger = srv1Logger;

        startGrid("srv1");

        ignite0.cluster().active(true);

        ignite0.createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setDataRegionName(defaultRegionCfg.getName())
                .setCacheMode(CacheMode.PARTITIONED)
                .setAffinity(new RendezvousAffinityFunction(false, 512))
        );

        assertFalse(cacheGrpLsnr0.check());
        assertFalse(cacheGrpLsnr1.check());

        stopGrid("srv1");

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        long topVer = ignite0.cluster().topologyVersion();

        ignite0.cluster().setBaselineTopology(topVer);

        awaitPartitionMapExchange();

        assertTrue(cacheGrpLsnr0.check());
    }

    /**
     * Negative test: verifies that no warning is printed to logs if user starts static and dynamic caches
     * in data region with enough capacity to host these caches;
     * in other words, no thresholds for metapages ration are broken.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoWarningIfCacheConfigurationDoesntBreakThreshold() throws Exception {
        DataRegionConfiguration defaultRegionCfg = new DataRegionConfiguration();
        defaultRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setPersistenceEnabled(true);

        memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(defaultRegionCfg);
        //one hour to guarantee that checkpoint will be triggered by 'dirty pages amount' trigger
        memCfg.setCheckpointFrequency(60 * 60 * 1000);

        CacheConfiguration<Object, Object> fewPartitionsCache = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        //512 partitions are enough only if primary and backups count
        fewPartitionsCache.setAffinity(new RendezvousAffinityFunction(false, 16));
        fewPartitionsCache.setBackups(1);

        ccfg = fewPartitionsCache;

        ListeningTestLogger srv0Logger = new ListeningTestLogger(false, null);
        LogListener cacheGrpLsnr0 = matches("Cache group 'default' brings high overhead").build();
        LogListener dynamicGrpLsnr = matches("Cache group 'dynamicCache' brings high overhead").build();
        srv0Logger.registerListener(cacheGrpLsnr0);
        srv0Logger.registerListener(dynamicGrpLsnr);
        logger = srv0Logger;

        IgniteEx ignite0 = startGrid("srv0");

        ignite0.cluster().active(true);

        assertFalse(cacheGrpLsnr0.check());

        ignite0.createCache(new CacheConfiguration<>("dynamicCache")
            .setAffinity(new RendezvousAffinityFunction(false, 16))
        );

        assertFalse(dynamicGrpLsnr.check());
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

    /**
     * Test checks that nodes will not fall if you receive a request
     * to create a cache with an unknown data region.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoFailNodeIfUnknownDataRegion() throws Exception {
        failureHnd = new StopNodeOrHaltFailureHandler();
        ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        LogListener logLsnr = matches("Possible failure suppressed accordingly to a configured handler").build();
        logger = new ListeningTestLogger(false, log, logLsnr);

        IgniteEx srvNode = startGrid(0);

        String dataRegionName = "region";

        IgniteConfiguration clientCfg = getConfiguration(getTestIgniteInstanceName(1))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(new DataRegionConfiguration().setName(dataRegionName))
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );

        IgniteEx clientNode = startClientGrid(optimize(clientCfg));

        srvNode.cluster().active(true);

        assertThrows(log, () -> {
            clientNode.getOrCreateCache(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + 1).setDataRegionName(dataRegionName)
            );

            return null;
        }, CacheException.class, null);

        assertThrows(log, () -> {
            clientNode.getOrCreateCache(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + 1).setDataRegionName(UUID.randomUUID().toString())
            );

            return null;
        }, CacheException.class, null);

        IgniteCache<Object, Object> cacheSrv = srvNode.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cacheClient = clientNode.cache(DEFAULT_CACHE_NAME);

        cacheSrv.put(1, 1);
        assertEquals(1, cacheSrv.get(1));

        cacheClient.put(2, 2);
        assertEquals(2, cacheClient.get(2));

        assertFalse(logLsnr.check());
    }
}
