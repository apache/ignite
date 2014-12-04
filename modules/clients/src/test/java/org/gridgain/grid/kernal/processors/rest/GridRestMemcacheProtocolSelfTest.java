/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import junit.framework.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * TCP protocol test.
 */
@SuppressWarnings("unchecked")
public class GridRestMemcacheProtocolSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final String HOST = "127.0.0.1";

    /** */
    private static final int PORT = 11212;

    /** */
    private GridTestMemcacheClient client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = client();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        client.shutdown();

        grid().cache(null).clearAll();
        grid().cache(CACHE_NAME).clearAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(HOST);

        assert cfg.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        clientCfg.setRestTcpPort(PORT);

        cfg.setClientConnectionConfiguration(clientCfg);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private GridCacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(LOCAL);
        cfg.setName(cacheName);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return Client.
     * @throws GridException In case of error.
     */
    private GridTestMemcacheClient client() throws GridException {
        return new GridTestMemcacheClient(HOST, PORT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        assertTrue(client.cachePut(null, "key1", "val1"));
        assertEquals("val1", grid().cache(null).get("key1"));

        assertTrue(client.cachePut(CACHE_NAME, "key1", "val1"));
        assertEquals("val1", grid().cache(CACHE_NAME).get("key1"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        assertTrue(grid().cache(null).putx("key", "val"));

        Assert.assertEquals("val", client.cacheGet(null, "key"));

        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));

        Assert.assertEquals("val", client.cacheGet(CACHE_NAME, "key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        assertTrue(grid().cache(null).putx("key", "val"));

        assertTrue(client.cacheRemove(null, "key"));
        assertFalse(client.cacheRemove(null, "wrongKey"));

        assertNull(grid().cache(null).get("key"));

        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));

        assertTrue(client.cacheRemove(CACHE_NAME, "key"));
        assertFalse(client.cacheRemove(CACHE_NAME, "wrongKey"));

        assertNull(grid().cache(CACHE_NAME).get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAdd() throws Exception {
        assertTrue(client.cacheAdd(null, "key", "val"));
        assertEquals("val", grid().cache(null).get("key"));
        assertFalse(client.cacheAdd(null, "key", "newVal"));
        assertEquals("val", grid().cache(null).get("key"));

        assertTrue(client.cacheAdd(CACHE_NAME, "key", "val"));
        assertEquals("val", grid().cache(CACHE_NAME).get("key"));
        assertFalse(client.cacheAdd(CACHE_NAME, "key", "newVal"));
        assertEquals("val", grid().cache(CACHE_NAME).get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplace() throws Exception {
        assertFalse(client.cacheReplace(null, "key1", "val1"));
        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(client.cacheReplace(null, "key1", "val2"));

        assertFalse(client.cacheReplace(null, "key2", "val1"));
        assertTrue(grid().cache(null).putx("key2", "val1"));
        assertTrue(client.cacheReplace(null, "key2", "val2"));

        grid().cache(null).clearAll();

        assertFalse(client.cacheReplace(CACHE_NAME, "key1", "val1"));
        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(client.cacheReplace(CACHE_NAME, "key1", "val2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetrics() throws Exception {
        grid().cache(null).resetMetrics();
        grid().cache(CACHE_NAME).resetMetrics();

        grid().cache(null).putx("key1", "val");
        grid().cache(null).putx("key2", "val");
        grid().cache(null).putx("key2", "val");

        grid().cache(null).get("key1");
        grid().cache(null).get("key2");
        grid().cache(null).get("key2");

        grid().cache(CACHE_NAME).putx("key1", "val");
        grid().cache(CACHE_NAME).putx("key2", "val");
        grid().cache(CACHE_NAME).putx("key2", "val");

        grid().cache(CACHE_NAME).get("key1");
        grid().cache(CACHE_NAME).get("key2");
        grid().cache(CACHE_NAME).get("key2");

        Map<String, Long> m = client.cacheMetrics(null);

        assertNotNull(m);
        assertEquals(7, m.size());
        assertEquals(3, m.get("reads").longValue());
        assertEquals(3, m.get("writes").longValue());

        m = client.cacheMetrics(CACHE_NAME);

        assertNotNull(m);
        assertEquals(7, m.size());
        assertEquals(3, m.get("reads").longValue());
        assertEquals(3, m.get("writes").longValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrement() throws Exception {
        assertEquals(15L, client().cacheIncrement(null, "key", 10L, 5L));
        assertEquals(15L, grid().cache(null).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(18L, client().cacheIncrement(null, "key", 20L, 3L));
        assertEquals(18L, grid().cache(null).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(20L, client().cacheIncrement(null, "key", null, 2L));
        assertEquals(20L, grid().cache(null).dataStructures().atomicLong("key", 0, true).get());

        assertEquals(15L, client().cacheIncrement(CACHE_NAME, "key", 10L, 5L));
        assertEquals(15L, grid().cache(CACHE_NAME).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(18L, client().cacheIncrement(CACHE_NAME, "key", 20L, 3L));
        assertEquals(18L, grid().cache(CACHE_NAME).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(20L, client().cacheIncrement(CACHE_NAME, "key", null, 2L));
        assertEquals(20L, grid().cache(CACHE_NAME).dataStructures().atomicLong("key", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecrement() throws Exception {
        assertEquals(15L, client().cacheDecrement(null, "key", 20L, 5L));
        assertEquals(15L, grid().cache(null).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(12L, client().cacheDecrement(null, "key", 20L, 3L));
        assertEquals(12L, grid().cache(null).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(10L, client().cacheDecrement(null, "key", null, 2L));
        assertEquals(10L, grid().cache(null).dataStructures().atomicLong("key", 0, true).get());

        assertEquals(15L, client().cacheDecrement(CACHE_NAME, "key", 20L, 5L));
        assertEquals(15L, grid().cache(CACHE_NAME).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(12L, client().cacheDecrement(CACHE_NAME, "key", 20L, 3L));
        assertEquals(12L, grid().cache(CACHE_NAME).dataStructures().atomicLong("key", 0, true).get());
        assertEquals(10L, client().cacheDecrement(CACHE_NAME, "key", null, 2L));
        assertEquals(10L, grid().cache(CACHE_NAME).dataStructures().atomicLong("key", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppend() throws Exception {
        assertFalse(client.cacheAppend(null, "wrongKey", "_suffix"));
        assertFalse(client.cacheAppend(CACHE_NAME, "wrongKey", "_suffix"));

        assertTrue(grid().cache(null).putx("key", "val"));
        assertTrue(client.cacheAppend(null, "key", "_suffix"));
        assertEquals("val_suffix", grid().cache(null).get("key"));

        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));
        assertTrue(client.cacheAppend(CACHE_NAME, "key", "_suffix"));
        assertEquals("val_suffix", grid().cache(CACHE_NAME).get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepend() throws Exception {
        assertFalse(client.cachePrepend(null, "wrongKey", "prefix_"));
        assertFalse(client.cachePrepend(CACHE_NAME, "wrongKey", "prefix_"));

        assertTrue(grid().cache(null).putx("key", "val"));
        assertTrue(client.cachePrepend(null, "key", "prefix_"));
        assertEquals("prefix_val", grid().cache(null).get("key"));

        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));
        assertTrue(client.cachePrepend(CACHE_NAME, "key", "prefix_"));
        assertEquals("prefix_val", grid().cache(CACHE_NAME).get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersion() throws Exception {
        assertNotNull(client.version());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoop() throws Exception {
        client.noop();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQuit() throws Exception {
        client.quit();
    }
}
