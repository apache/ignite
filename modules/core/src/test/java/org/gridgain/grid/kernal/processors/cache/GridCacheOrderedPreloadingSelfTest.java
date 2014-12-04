/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Checks ordered preloading.
 */
public class GridCacheOrderedPreloadingSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Number of grids in test. */
    private static final int GRID_CNT = 4;

    /** First cache name. */
    public static final String FIRST_CACHE_NAME = "first";

    /** Second cache name. */
    public static final String SECOND_CACHE_NAME = "second";

    /** First cache mode. */
    private GridCacheMode firstCacheMode;

    /** Second cache mode. */
    private GridCacheMode secondCacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(
            cacheConfig(firstCacheMode, 1, FIRST_CACHE_NAME),
            cacheConfig(secondCacheMode, 2, SECOND_CACHE_NAME));

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @param cacheMode Cache mode.
     * @param preloadOrder Preload order.
     * @param name Cache name.
     * @return Cache configuration.
     */
    private GridCacheConfiguration cacheConfig(GridCacheMode cacheMode, int preloadOrder, String name) {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);
        cfg.setCacheMode(cacheMode);
        cfg.setPreloadOrder(preloadOrder);
        cfg.setPreloadMode(ASYNC);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderPartitionedPartitioned() throws Exception {
        checkPreloadOrder(PARTITIONED, PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderReplicatedReplicated() throws Exception {
        checkPreloadOrder(REPLICATED, REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderPartitionedReplicated() throws Exception {
        checkPreloadOrder(PARTITIONED, REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderReplicatedPartitioned() throws Exception {
        checkPreloadOrder(REPLICATED, PARTITIONED);
    }

    /**
     * @param first First cache mode.
     * @param second Second cache mode.
     * @throws Exception If failed.
     */
    private void checkPreloadOrder(GridCacheMode first, GridCacheMode second) throws Exception {
        firstCacheMode = first;
        secondCacheMode = second;

        Ignite g = startGrid(0);

        try {
            GridCache<Object, Object> cache = g.cache("first");

            // Put some data into cache.
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            for (int i = 1; i < GRID_CNT; i++)
                startGrid(i);

            // For first node in topology replicated preloader gets completed right away.
            for (int i = 1; i < GRID_CNT; i++) {
                GridKernal kernal = (GridKernal)grid(i);

                GridFutureAdapter<?> fut1 = (GridFutureAdapter<?>)kernal.internalCache(FIRST_CACHE_NAME).preloader()
                    .syncFuture();
                GridFutureAdapter<?> fut2 = (GridFutureAdapter<?>)kernal.internalCache(SECOND_CACHE_NAME).preloader()
                    .syncFuture();

                fut1.get();
                fut2.get();

                assertTrue("[i=" + i + ", fut1=" + fut1 + ", fut2=" + fut2 + ']', fut1.endTime() <= fut2.endTime());
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
