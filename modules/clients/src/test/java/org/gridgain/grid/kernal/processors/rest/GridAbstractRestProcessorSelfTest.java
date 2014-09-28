/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Abstract class for REST protocols tests.
 */
abstract class GridAbstractRestProcessorSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Local host. */
    protected static final String LOC_HOST = "127.0.0.1";

    /**
     * @return Grid count.
     */
    protected abstract int gridCount();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        assert grid(0).nodes().size() == gridCount();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache().clearAll();

        assertTrue(cache().isEmpty());
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(LOC_HOST);

        assert cfg.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        cfg.setClientConnectionConfiguration(clientCfg);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Cache.
     */
    @Override protected <K, V> GridCache<K, V> cache() {
        return grid(0).cache(null);
    }
}
