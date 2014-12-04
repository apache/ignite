/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test to check slow TX warning timeout defined by
 * {@link GridSystemProperties#GG_SLOW_TX_WARN_TIMEOUT}
 * system property.
 */
public class GridCacheSlowTxWarnTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc1 = defaultCacheConfiguration();

        cc1.setName("partitioned");
        cc1.setCacheMode(PARTITIONED);
        cc1.setBackups(1);

        GridCacheConfiguration cc2 = defaultCacheConfiguration();

        cc2.setName("replicated");
        cc2.setCacheMode(REPLICATED);

        GridCacheConfiguration cc3 = defaultCacheConfiguration();

        cc3.setName("local");
        cc3.setCacheMode(LOCAL);

        c.setCacheConfiguration(cc1, cc2, cc3);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWarningOutput() throws Exception {
        try {
            GridKernal g = (GridKernal)startGrid(1);

            info(">>> Slow tx timeout is not set, long-live txs simulated.");

            checkCache(g, "partitioned", true, false);
            checkCache(g, "replicated", true, false);
            checkCache(g, "local", true, false);

            info(">>> Slow tx timeout is set, long-live tx simulated.");

            checkCache(g, "partitioned", true, true);
            checkCache(g, "replicated", true, true);
            checkCache(g, "local", true, true);

            info(">>> Slow tx timeout is set, no long-live txs.");

            checkCache(g, "partitioned", false, true);
            checkCache(g, "replicated", false, true);
            checkCache(g, "local", false, true);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param g Grid.
     * @param cacheName Cache.
     * @param simulateTimeout Simulate timeout.
     * @param configureTimeout Alter configuration of TX manager.
     * @throws Exception If failed.
     */
    private void checkCache(Ignite g, String cacheName, boolean simulateTimeout,
        boolean configureTimeout) throws Exception {
        if (configureTimeout) {
            GridCacheAdapter<Integer, Integer> cache = ((GridKernal)g).internalCache(cacheName);

            cache.context().tm().slowTxWarnTimeout(500);
        }

        GridCache<Object, Object> cache1 = g.cache(cacheName);

        GridCacheTx tx = cache1.txStart();

        try {
            cache1.put(1, 1);

            if (simulateTimeout)
                Thread.sleep(800);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache1.txStart();

        try {
            cache1.put(1, 1);

            if (simulateTimeout)
                Thread.sleep(800);

            tx.rollback();
        }
        finally {
            tx.close();
        }
    }
}
