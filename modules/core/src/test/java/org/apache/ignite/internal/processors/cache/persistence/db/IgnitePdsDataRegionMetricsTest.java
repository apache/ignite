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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;

/**
 *
 */
public class IgnitePdsDataRegionMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long INIT_REGION_SIZE = 10 << 20;

    /** */
    private static final long MAX_REGION_SIZE = INIT_REGION_SIZE * 10;

    /** */
    private static final int ITERATIONS = 3;

    /** */
    private static final int BATCHES = 5;

    /** */
    private static final int BATCH_SIZE_LOW = 100;

    /** */
    private static final int BATCH_SIZE_HIGH = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(INIT_REGION_SIZE)
                    .setMaxSize(MAX_REGION_SIZE)
                    .setPersistenceEnabled(true)
                    .setMetricsEnabled(true))
            .setCheckpointFrequency(1000);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    public void testMemoryUsageSingleNode() throws Exception {
        DataRegionMetrics initMetrics = null;

        for (int iter = 0; iter < ITERATIONS; iter++) {
            final IgniteEx node = startGrid(0);

            node.cluster().active(true);

            DataRegionMetrics currMetrics = getDfltRegionMetrics(node);

            if (initMetrics == null)
                initMetrics = currMetrics;

            assertTrue(currMetrics.getTotalAllocatedPages() >= currMetrics.getPhysicalMemoryPages());

            final IgniteCache<String, String> cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);

            Map<String, String> map = new HashMap<>();

            for (int batch = 0; batch < BATCHES; batch++) {
                int nPuts = BATCH_SIZE_LOW + ThreadLocalRandom.current().nextInt(BATCH_SIZE_HIGH - BATCH_SIZE_LOW);

                for (int i = 0; i < nPuts; i++)
                    map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

                cache.putAll(map);

                forceCheckpoint();

                checkMetricsConsistency(node, DEFAULT_CACHE_NAME);
            }

            currMetrics = getDfltRegionMetrics(node);

            // Make sure metrics are rising
            assertTrue(currMetrics.getPhysicalMemoryPages() > initMetrics.getPhysicalMemoryPages());
            assertTrue(currMetrics.getTotalAllocatedPages() > initMetrics.getTotalAllocatedPages());

            stopGrid(0, true);
        }
    }

    /** */
    public void testMemoryUsageMultipleNodes() throws Exception {
        IgniteEx node0 = startGrid(0);
        IgniteEx node1 = startGrid(1);

        node0.cluster().active(true);

        final IgniteCache<Integer, String> cache = node0.getOrCreateCache(DEFAULT_CACHE_NAME);

        Map<Integer, String> map = new HashMap<>();

        for (int i = 0; i < 10_000; i++)
            map.put(i, UUID.randomUUID().toString());

        cache.putAll(map);

        awaitPartitionMapExchange(true, true, null);

        forceCheckpoint();

        checkMetricsConsistency(node0, DEFAULT_CACHE_NAME);
        checkMetricsConsistency(node1, DEFAULT_CACHE_NAME);

        IgniteEx node2 = startGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        forceCheckpoint();

        checkMetricsConsistency(node0, DEFAULT_CACHE_NAME);
        checkMetricsConsistency(node1, DEFAULT_CACHE_NAME);
        checkMetricsConsistency(node2, DEFAULT_CACHE_NAME);

        stopGrid(1, true);

        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        forceCheckpoint();

        checkMetricsConsistency(node0, DEFAULT_CACHE_NAME);
        checkMetricsConsistency(node2, DEFAULT_CACHE_NAME);
    }

    /**
     * Test for check checkpoint size metric.
     *
     * @throws Exception If failed.
     */
    public void testCheckpointBufferSize() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        DataRegionMetricsImpl regionMetrics = ig.cachex(DEFAULT_CACHE_NAME)
            .context().group().dataRegion().memoryMetrics();

        Assert.assertTrue(regionMetrics.getCheckpointBufferSize() != 0);
        Assert.assertTrue(regionMetrics.getCheckpointBufferSize() <= MAX_REGION_SIZE);
    }

    /**
     * Test for check used checkpoint size metric.
     *
     * @throws Exception If failed.
     */
    public void testUsedCheckpointBuffer() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        final DataRegionMetricsImpl regionMetrics = ig.cachex(DEFAULT_CACHE_NAME)
            .context().group().dataRegion().memoryMetrics();

        Assert.assertEquals(0, regionMetrics.getUsedCheckpointBufferPages());
        Assert.assertEquals(0, regionMetrics.getUsedCheckpointBufferSize());

        load(ig);

        GridCacheDatabaseSharedManager psMgr = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        GridFutureAdapter<T2<Long, Long>> metricsResult = new GridFutureAdapter<>();

        IgniteInternalFuture chpBeginFut = psMgr.wakeupForCheckpoint(null);

        chpBeginFut.listen((f) -> {
            load(ig);

            metricsResult.onDone(new T2<>(
                regionMetrics.getUsedCheckpointBufferPages(),
                regionMetrics.getUsedCheckpointBufferSize()
            ));
        });

        metricsResult.get();

        Assert.assertTrue(metricsResult.get().get1() > 0);
        Assert.assertTrue(metricsResult.get().get2() > 0);
    }

    /**
     * @param ig Ignite.
     */
    private void load(Ignite ig) {
        IgniteCache<Integer, byte[]> cache = ig.cache(DEFAULT_CACHE_NAME);

        Random rnd = new Random();

        for (int i = 0; i < 1000; i++) {
            byte[] payload = new byte[128];

            rnd.nextBytes(payload);

            cache.put(i, payload);
        }
    }

    /** */
    private static DataRegionMetrics getDfltRegionMetrics(Ignite node) {
        for (DataRegionMetrics m : node.dataRegionMetrics())
            if (DFLT_DATA_REG_DEFAULT_NAME.equals(m.getName()))
                return m;

        throw new RuntimeException("No metrics found for default data region");
    }

    /** */
    private void checkMetricsConsistency(final IgniteEx node, String cacheName) throws Exception {
        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)node.context().cache().context().pageStore();

        assert pageStoreMgr != null : "Persistence is not enabled";

        File cacheWorkDir = pageStoreMgr.cacheWorkDir(
            node.getOrCreateCache(cacheName).getConfiguration(CacheConfiguration.class)
        );

        long totalPersistenceSize = 0;

        try (DirectoryStream<Path> files = newDirectoryStream(
            cacheWorkDir.toPath(), entry -> entry.toFile().getName().endsWith(".bin"))
        ) {
            for (Path path : files) {
                File file = path.toFile();

                FilePageStore store = (FilePageStore)pageStoreMgr.getStore(CU.cacheId(cacheName), partId(file));

                totalPersistenceSize += path.toFile().length() - store.headerSize();
            }
        }

        long totalAllocatedPagesFromMetrics = node.context().cache().context()
            .cacheContext(CU.cacheId(DEFAULT_CACHE_NAME))
            .group()
            .dataRegion()
            .memoryMetrics()
            .getTotalAllocatedPages();

        assertEquals(totalPersistenceSize / pageStoreMgr.pageSize(), totalAllocatedPagesFromMetrics);
    }

    /**
     * @param partFile Partition file.
     */
    private static int partId(File partFile) {
        String name = partFile.getName();

        if (name.equals(FilePageStoreManager.INDEX_FILE_NAME))
            return PageIdAllocator.INDEX_PARTITION;

        if (name.startsWith(FilePageStoreManager.PART_FILE_PREFIX))
            return Integer.parseInt(name.substring(FilePageStoreManager.PART_FILE_PREFIX.length(), name.indexOf('.')));

        throw new IllegalStateException("Illegal partition file name: " + name);
    }
}
