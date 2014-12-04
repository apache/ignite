/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test for cache checkpoint SPI with second cache configured.
 */
public class GridCacheCheckpointSpiSecondCacheSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Data cache name. */
    private static final String DATA_CACHE = null;

    /** Checkpoints cache name. */
    private static final String CP_CACHE = "checkpoints";

    /** Starts grid. */
    public GridCacheCheckpointSpiSecondCacheSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName(DATA_CACHE);
        cacheCfg1.setCacheMode(REPLICATED);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);

        GridCacheConfiguration cacheCfg2 = defaultCacheConfiguration();

        cacheCfg2.setName(CP_CACHE);
        cacheCfg2.setCacheMode(REPLICATED);
        cacheCfg2.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        GridCacheCheckpointSpi cp = new GridCacheCheckpointSpi();

        cp.setCacheName(CP_CACHE);

        cfg.setCheckpointSpi(cp);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSecondCachePutRemove() throws Exception {
        GridCache<Integer, Integer> data = grid().cache(DATA_CACHE);
        GridCache<Integer, String> cp = grid().cache(CP_CACHE);

        assertTrue(data.putx(1, 1));
        assertTrue(cp.putx(1, "1"));

        Integer v = data.get(1);

        assertNotNull(v);
        assertEquals(Integer.valueOf(1), data.get(1));

        assertTrue(data.removex(1));

        assertNull(data.get(1));

        assertTrue(data.isEmpty());

        assertEquals(1, cp.size());
        assertEquals("1", cp.get(1));

    }
}
