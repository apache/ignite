/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jdbc;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.concurrent.*;

/**
 * Connection test.
 */
public class GridJdbcConnectionSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Custom cache name. */
    private static final String CUSTOM_CACHE_NAME = "custom-cache";

    /** Custom REST TCP port. */
    private static final int CUSTOM_PORT = 11212;

    /** URL prefix. */
    private static final String URL_PREFIX = "jdbc:gridgain://";

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration(CUSTOM_CACHE_NAME));

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        assert cfg.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        if (!gridName.endsWith("0"))
            clientCfg.setRestTcpPort(CUSTOM_PORT);

        cfg.setClientConnectionConfiguration(clientCfg);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private GridCacheConfiguration cacheConfiguration(@Nullable String name) throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);

        Class.forName("org.gridgain.jdbc.GridJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaults() throws Exception {
        String url = URL_PREFIX + HOST;

        assert DriverManager.getConnection(url) != null;
        assert DriverManager.getConnection(url + "/") != null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeId() throws Exception {
        String url = URL_PREFIX + HOST + "/?nodeId=" + grid(0).localNode().id();

        assert DriverManager.getConnection(url) != null;

        url = URL_PREFIX + HOST + "/" + CUSTOM_CACHE_NAME + "?nodeId=" + grid(0).localNode().id();

        assert DriverManager.getConnection(url) != null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomCache() throws Exception {
        String url = URL_PREFIX + HOST + "/" + CUSTOM_CACHE_NAME;

        assert DriverManager.getConnection(url) != null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomPort() throws Exception {
        String url = URL_PREFIX + HOST + ":" + CUSTOM_PORT;

        assert DriverManager.getConnection(url) != null;
        assert DriverManager.getConnection(url + "/") != null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomCacheNameAndPort() throws Exception {
        String url = URL_PREFIX + HOST + ":" + CUSTOM_PORT + "/" + CUSTOM_CACHE_NAME;

        assert DriverManager.getConnection(url) != null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWrongCache() throws Exception {
        final String url = URL_PREFIX + HOST + "/wrongCacheName";

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection(url);

                    return null;
                }
            },
            SQLException.class,
            "Client is invalid. Probably cache name is wrong."
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testWrongPort() throws Exception {
        final String url = URL_PREFIX + HOST + ":33333";

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection(url);

                    return null;
                }
            },
            SQLException.class,
            "Failed to establish connection."
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testClose() throws Exception {
        String url = URL_PREFIX + HOST;

        final Connection conn = DriverManager.getConnection(url);

        assert conn != null;
        assert !conn.isClosed();

        conn.close();

        assert conn.isClosed();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    conn.isValid(2);

                    return null;
                }
            },
            SQLException.class,
            "Connection is closed."
        );
    }
}
