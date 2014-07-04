/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;

import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests ensuring that GGFS data and meta caches are not "visible" through public API.
 */
public class GridGgfsCacheSelfTest extends GridGgfsCommonAbstractTest {
    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "meta";

    /** Data cache name. */
    private static final String DATA_CACHE_NAME = null;

    /** Regular cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(META_CACHE_NAME), cacheConfiguration(DATA_CACHE_NAME),
            cacheConfiguration(CACHE_NAME));

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setMetaCacheName(META_CACHE_NAME);
        ggfsCfg.setDataCacheName(DATA_CACHE_NAME);
        ggfsCfg.setName("ggfs");

        cfg.setGgfsConfiguration(ggfsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    protected GridCacheConfiguration cacheConfiguration(String cacheName) {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        if (META_CACHE_NAME.equals(cacheName)) {
            cacheCfg.setCacheMode(REPLICATED);
        }
        else {
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);

            cacheCfg.setBackups(0);
            cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
        }

        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setQueryIndexEnabled(false);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test cache.
     *
     * @throws Exception If failed.
     */
    public void testCache() throws Exception {
        final Grid g = grid();

        assert g.caches().size() == 1;

        assert CACHE_NAME.equals(g.caches().iterator().next().name());

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                g.cache(META_CACHE_NAME);

                return null;
            }
        }, IllegalStateException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                g.cache(DATA_CACHE_NAME);

                return null;
            }
        }, IllegalStateException.class, null);

        assert g.cache(CACHE_NAME) != null;
    }
}
