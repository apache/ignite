/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheStartInParallelTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHES_COUNT = 40;
    /** */
    private static final String STATIC_CACHE_PREFIX = "static-cache-";
    /** */
    private static final String DYNAMIC_CACHE_PREFIX = "dynamic-cache-";
    /** */
    private static boolean isStaticCache = true;
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSystemThreadPoolSize(10);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        long sz = 100 * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(sz).setMaxSize(sz))
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

        cfg.setDataStorageConfiguration(memCfg);

        if (isStaticCache) {
            ArrayList<Object> staticCaches = new ArrayList<>(CACHES_COUNT);

            for (int i = 0; i < CACHES_COUNT; i++)
                staticCaches.add(cacheConfiguration(STATIC_CACHE_PREFIX + i));

            cfg.setCacheConfiguration(staticCaches.toArray(new CacheConfiguration[CACHES_COUNT]));
        }

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(cacheName);
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanupTestData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanupTestData();
    }

    /** */
    private void cleanupTestData() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL);

        isStaticCache = true;
    }

    /**
     * Checking that start static caches in parallel faster than consistenly.
     *
     * @throws Exception if fail.
     */
    public void testParallelizationAcceleratesStartOfStaticCaches() throws Exception {
        //start caches consistently.
        System.setProperty(IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL, "false");

        long startTime = System.currentTimeMillis();

        IgniteEx igniteEx = startGrid(0);

        igniteEx.cluster().active(true);

        long totalStartTimeConsistently = System.currentTimeMillis() - startTime;

        //check cache started.
        for (int i = 0; i < CACHES_COUNT; i++)
            igniteEx.cache(STATIC_CACHE_PREFIX + i).put(i, i);

        stopAllGrids();

        //start caches in parallel.
        System.setProperty(IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL, "true");

        startTime = System.currentTimeMillis();

        igniteEx = startGrid(0);

        igniteEx.cluster().active(true);

        long totalStartTimeInParallel = System.currentTimeMillis() - startTime;

        for (int i = 0; i < CACHES_COUNT; i++)
            igniteEx.cache(STATIC_CACHE_PREFIX + i).put(i, i);

        stopAllGrids();

        assertTrue("Consistently cache stat time : " + totalStartTimeConsistently +
                "Parallelization cache stat time : " + totalStartTimeInParallel,
            totalStartTimeConsistently > totalStartTimeInParallel);
    }

    /**
     * Checking that start dynamic caches in parallel faster than consistenly.
     *
     * @throws Exception if fail.
     */
    public void testParallelizationAcceleratesStartOfCaches2() throws Exception {
        //prepare dynamic caches.
        isStaticCache = false;

        IgniteEx igniteEx = startGrid(0);

        igniteEx.cluster().active(true);

        for (int i = 0; i < CACHES_COUNT; i++)
            igniteEx.getOrCreateCache(DYNAMIC_CACHE_PREFIX + i);

        stopAllGrids();

        //start caches consistently.
        System.setProperty(IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL, "false");

        igniteEx = startGrid(0);
        long startTime = System.currentTimeMillis();

        igniteEx.cluster().active(true);

        long totalStartTimeConsistently = System.currentTimeMillis() - startTime;

        for (int i = 0; i < CACHES_COUNT; i++)
            igniteEx.cache(DYNAMIC_CACHE_PREFIX + i);

        stopAllGrids();

        //start caches in parallel.
        System.setProperty(IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL, "true");

        startTime = System.currentTimeMillis();

        igniteEx = startGrid(0);

        igniteEx.cluster().active(true);

        long totalStartTimeInParallel = System.currentTimeMillis() - startTime;

        for (int i = 0; i < CACHES_COUNT; i++)
            igniteEx.cache(DYNAMIC_CACHE_PREFIX + i).put(i, i);

        stopAllGrids();

        assertTrue("Consistently cache stat time : " + totalStartTimeConsistently +
                "Parallelization cache stat time : " + totalStartTimeInParallel,
            totalStartTimeConsistently > totalStartTimeInParallel);
    }
}
