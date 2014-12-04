package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Checks that GridCacheProjection.reload() operations are performed correctly.
 */
public class GridCacheReloadSelfTest extends GridCommonAbstractTest {

    /** Maximum allowed number of cache entries. */
    public static final int MAX_CACHE_ENTRIES = 500;

    /** Number of entries to load from store. */
    public static final int N_ENTRIES = 5000;

    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Cache mode. */
    private GridCacheMode cacheMode;

    /** Near enabled flag. */
    private boolean nearEnabled = true;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cacheMode = null;
        nearEnabled = true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();
        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setEvictionPolicy(new GridCacheLruEvictionPolicy(MAX_CACHE_ENTRIES));
        cacheCfg.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cacheCfg.setStore(new GridCacheStoreAdapter<Integer, Integer>() {
            @Override public Integer load(@Nullable GridCacheTx tx, Integer key) {
                return key;
            }

            @Override public void put(@Nullable GridCacheTx tx, Integer key,
                @Nullable Integer val) {
                //No-op.
            }

            @Override public void remove(@Nullable GridCacheTx tx, Integer key) {
                //No-op.
            }
        });

        if (cacheMode == PARTITIONED)
            cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * Checks that eviction works with reload() on local cache.
     *
     * @throws Exception If error occurs.
     */
    public void testReloadEvictionLocalCache() throws Exception {
        cacheMode = GridCacheMode.LOCAL;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on partitioned cache
     * with near enabled.
     *
     * @throws Exception If error occurs.
     */
    //TODO: Active when ticket GG-3926 will be ready.
    public void _testReloadEvictionPartitionedCacheNearEnabled() throws Exception {
        cacheMode = PARTITIONED;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on partitioned cache
     * with near disabled.
     *
     * @throws Exception If error occurs.
     */
    public void testReloadEvictionPartitionedCacheNearDisabled() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on replicated cache.
     *
     * @throws Exception If error occurs.
     */
    public void testReloadEvictionReplicatedCache() throws Exception {
        cacheMode = GridCacheMode.REPLICATED;

        doTest();
    }

    /**
     * Actual test logic.
     *
     * @throws Exception If error occurs.
     */
    private void doTest() throws Exception {
        Ignite ignite = startGrid();

        try {
            GridCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < N_ENTRIES; i++)
                cache.reload(i);

            assertEquals(MAX_CACHE_ENTRIES, cache.size());
        }
        finally {
            stopGrid();
        }
    }
}
