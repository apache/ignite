/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Multi-node cache test.
 */
public class GridCacheMultiNodeLoadTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "partitioned";

    /** Elements count. */
    public static final int ELEMENTS_COUNT = 200000;

    /** Grid 1. */
    private static Grid grid1;

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(PARTITIONED_ONLY);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setStartSize(10);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cacheCfg.setEvictionPolicy(new GridCacheLruEvictionPolicy(100000));
        cacheCfg.setBackups(1);

        cacheCfg.setPreloadMode(SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        grid1 = startGrid(1);
        startGrid(2);

        grid1.cache(CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        grid1 = null;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMany() throws Exception {
        grid1.compute().execute(GridCacheLoadPopulationTask.class, null);
    }
}
