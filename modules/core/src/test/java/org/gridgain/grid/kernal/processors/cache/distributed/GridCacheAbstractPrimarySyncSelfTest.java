/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test ensuring that PRIMARY_SYNC mode works correctly.
 */
public abstract class GridCacheAbstractPrimarySyncSelfTest extends GridCommonAbstractTest {
    /** Grids count. */
    private static final int GRID_CNT = 3;

    /** IP_FINDER. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setBackups(1);
        ccfg.setPreloadMode(SYNC);
        ccfg.setDistributionMode(distributionMode());

        cfg.setCacheConfiguration(ccfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assert GRID_CNT > 1;

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Distribution mode.
     */
    protected abstract GridCacheDistributionMode distributionMode();

    /**
     * @throws Exception If failed.
     */
    public void testPrimarySync() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            for (int j = 0; j < GRID_CNT; j++) {
                GridCache<Integer, Integer> cache = grid(j).cache(null);

                if (cache.entry(i).primary()) {
                    try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.put(i, i);

                        tx.commit();
                    }

                    assert cache.get(i) == i;

                    break;
                }
            }
        }
    }
}
