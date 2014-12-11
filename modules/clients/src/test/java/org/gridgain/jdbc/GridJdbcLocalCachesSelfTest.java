/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jdbc;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.testframework.junits.common.*;

import java.sql.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.jdbc.GridJdbcDriver.*;

/**
 * Test JDBC with several local caches.
 */
public class GridJdbcLocalCachesSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** URL. */
    private static final String URL = "jdbc:gridgain://127.0.0.1/" + CACHE_NAME;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(CACHE_NAME);
        cache.setCacheMode(LOCAL);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        GridCacheQueryConfiguration qryCfg = new GridCacheQueryConfiguration();

        qryCfg.setIndexPrimitiveKey(true);

        cache.setQueryConfiguration(qryCfg);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setRestEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);

        GridCache<Object, Object> cache1 = grid(0).cache(CACHE_NAME);

        assert cache1 != null;

        assert cache1.putx("key1", 1);
        assert cache1.putx("key2", 2);

        GridCache<Object, Object> cache2 = grid(1).cache(CACHE_NAME);

        assert cache2 != null;

        assert cache2.putx("key1", 3);
        assert cache2.putx("key2", 4);

        Class.forName("org.gridgain.jdbc.GridJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCache1() throws Exception {
        Properties cfg = new Properties();

        cfg.setProperty(PROP_NODE_ID, grid(0).localNode().id().toString());

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(URL, cfg);

            ResultSet rs = conn.createStatement().executeQuery("select _val from Integer order by _val");

            int cnt = 0;

            while (rs.next())
                assertEquals(++cnt, rs.getInt(1));

            assertEquals(2, cnt);
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCache2() throws Exception {
        Properties cfg = new Properties();

        cfg.setProperty(PROP_NODE_ID, grid(1).localNode().id().toString());

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(URL, cfg);

            ResultSet rs = conn.createStatement().executeQuery("select _val from Integer order by _val");

            int cnt = 0;

            while (rs.next())
                assertEquals(++cnt + 2, rs.getInt(1));

            assertEquals(2, cnt);
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }
}
