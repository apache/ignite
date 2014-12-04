/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * TCP protocol test.
 */
@SuppressWarnings("unchecked")
public class GridRestBinaryProtocolSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final String HOST = "127.0.0.1";

    /** */
    private static final int PORT = 11212;

    /** */
    private GridTestBinaryClient client;

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
    private GridTestBinaryClient client() throws GridException {
        return new GridTestBinaryClient(HOST, PORT);
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
    public void testPutAll() throws Exception {
        client.cachePutAll(null, F.asMap("key1", "val1", "key2", "val2"));

        Map<String, String> map = grid().<String, String>cache(null).getAll(Arrays.asList("key1", "key2"));

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));

        client.cachePutAll(CACHE_NAME, F.asMap("key1", "val1", "key2", "val2"));

        map = grid().<String, String>cache(CACHE_NAME).getAll(Arrays.asList("key1", "key2"));

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        assertTrue(grid().cache(null).putx("key", "val"));

        assertEquals("val", client.cacheGet(null, "key"));

        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));

        assertEquals("val", client.cacheGet(CACHE_NAME, "key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailure() throws Exception {
        GridKernal kernal = ((GridKernal)grid());

        GridRestProcessor proc = kernal.context().rest();

        // Clearing handlers collection to force failure.
        Field hndField = proc.getClass().getDeclaredField("handlers");

        hndField.setAccessible(true);

        Map handlers = (Map)hndField.get(proc);

        Map cp = new HashMap(handlers);

        handlers.clear();

        try {
            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return client.cacheGet(null, "key");
                    }
                },
                GridException.class,
                "Failed to process client request: Failed to find registered handler for command: CACHE_GET");
        }
        finally {
            handlers.putAll(cp);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(grid().cache(null).putx("key2", "val2"));

        Map<String, String> map = client.cacheGetAll(null, "key1", "key2");

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));

        assertTrue(grid().cache(null).putx("key3", "val3"));
        assertTrue(grid().cache(null).putx("key4", "val4"));

        map = client.cacheGetAll(null, "key3", "key4");

        assertEquals(2, map.size());
        assertEquals("val3", map.get("key3"));
        assertEquals("val4", map.get("key4"));

        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(grid().cache(CACHE_NAME).putx("key2", "val2"));

        map = client.cacheGetAll(CACHE_NAME, "key1", "key2");

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));

        assertTrue(grid().cache(CACHE_NAME).putx("key3", "val3"));
        assertTrue(grid().cache(CACHE_NAME).putx("key4", "val4"));

        map = client.cacheGetAll(CACHE_NAME, "key3", "key4");

        assertEquals(2, map.size());
        assertEquals("val3", map.get("key3"));
        assertEquals("val4", map.get("key4"));
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
    public void testRemoveAll() throws Exception {
        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(grid().cache(null).putx("key2", "val2"));
        assertTrue(grid().cache(null).putx("key3", "val3"));
        assertTrue(grid().cache(null).putx("key4", "val4"));

        client.cacheRemoveAll(null, "key1", "key2");

        assertNull(grid().cache(null).get("key1"));
        assertNull(grid().cache(null).get("key2"));
        assertNotNull(grid().cache(null).get("key3"));
        assertNotNull(grid().cache(null).get("key4"));

        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(grid().cache(CACHE_NAME).putx("key2", "val2"));
        assertTrue(grid().cache(CACHE_NAME).putx("key3", "val3"));
        assertTrue(grid().cache(CACHE_NAME).putx("key4", "val4"));

        client.cacheRemoveAll(CACHE_NAME, "key1", "key2");

        assertNull(grid().cache(CACHE_NAME).get("key1"));
        assertNull(grid().cache(CACHE_NAME).get("key2"));
        assertNotNull(grid().cache(CACHE_NAME).get("key3"));
        assertNotNull(grid().cache(CACHE_NAME).get("key4"));
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
    public void testCompareAndSet() throws Exception {
        assertFalse(client.cacheCompareAndSet(null, "key", null, null));
        assertTrue(grid().cache(null).putx("key", "val"));
        assertTrue(client.cacheCompareAndSet(null, "key", null, null));
        assertNull(grid().cache(null).get("key"));

        assertFalse(client.cacheCompareAndSet(null, "key", null, "val"));
        assertTrue(grid().cache(null).putx("key", "val"));
        assertFalse(client.cacheCompareAndSet(null, "key", null, "wrongVal"));
        assertEquals("val", grid().cache(null).get("key"));
        assertTrue(client.cacheCompareAndSet(null, "key", null, "val"));
        assertNull(grid().cache(null).get("key"));

        assertTrue(client.cacheCompareAndSet(null, "key", "val", null));
        assertEquals("val", grid().cache(null).get("key"));
        assertFalse(client.cacheCompareAndSet(null, "key", "newVal", null));
        assertEquals("val", grid().cache(null).get("key"));
        assertTrue(grid().cache(null).removex("key"));

        assertFalse(client.cacheCompareAndSet(null, "key", "val1", "val2"));
        assertTrue(grid().cache(null).putx("key", "val2"));
        assertFalse(client.cacheCompareAndSet(null, "key", "val1", "wrongVal"));
        assertEquals("val2", grid().cache(null).get("key"));
        assertTrue(client.cacheCompareAndSet(null, "key", "val1", "val2"));
        assertEquals("val1", grid().cache(null).get("key"));
        assertTrue(grid().cache(null).removex("key"));

        assertFalse(client.cacheCompareAndSet(CACHE_NAME, "key", null, null));
        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));
        assertTrue(client.cacheCompareAndSet(CACHE_NAME, "key", null, null));
        assertNull(grid().cache(CACHE_NAME).get("key"));

        assertFalse(client.cacheCompareAndSet(CACHE_NAME, "key", null, "val"));
        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));
        assertFalse(client.cacheCompareAndSet(CACHE_NAME, "key", null, "wrongVal"));
        assertEquals("val", grid().cache(CACHE_NAME).get("key"));
        assertTrue(client.cacheCompareAndSet(CACHE_NAME, "key", null, "val"));
        assertNull(grid().cache(CACHE_NAME).get("key"));

        assertTrue(client.cacheCompareAndSet(CACHE_NAME, "key", "val", null));
        assertEquals("val", grid().cache(CACHE_NAME).get("key"));
        assertFalse(client.cacheCompareAndSet(CACHE_NAME, "key", "newVal", null));
        assertEquals("val", grid().cache(CACHE_NAME).get("key"));
        assertTrue(grid().cache(CACHE_NAME).removex("key"));

        assertFalse(client.cacheCompareAndSet(CACHE_NAME, "key", "val1", "val2"));
        assertTrue(grid().cache(CACHE_NAME).putx("key", "val2"));
        assertFalse(client.cacheCompareAndSet(CACHE_NAME, "key", "val1", "wrongVal"));
        assertEquals("val2", grid().cache(CACHE_NAME).get("key"));
        assertTrue(client.cacheCompareAndSet(CACHE_NAME, "key", "val1", "val2"));
        assertEquals("val1", grid().cache(CACHE_NAME).get("key"));
        assertTrue(grid().cache(CACHE_NAME).removex("key"));
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
    public void testAppend() throws Exception {
        grid().cache(null).remove("key");
        grid().cache(CACHE_NAME).remove("key");

        assertFalse(client.cacheAppend(null, "key", ".val"));
        assertFalse(client.cacheAppend(CACHE_NAME, "key", ".val"));

        grid().cache(null).put("key", "orig");
        grid().cache(CACHE_NAME).put("key", "orig");

        assertTrue(client.cacheAppend(null, "key", ".val"));
        assertEquals("orig.val", grid().cache(null).get("key"));
        assertTrue(client.cacheAppend(null, "key", ".newVal"));
        assertEquals("orig.val.newVal", grid().cache(null).get("key"));

        assertTrue(client.cacheAppend(CACHE_NAME, "key", ".val"));
        assertEquals("orig.val", grid().cache(CACHE_NAME).get("key"));
        assertTrue(client.cacheAppend(CACHE_NAME, "key", ".newVal"));
        assertEquals("orig.val.newVal", grid().cache(CACHE_NAME).get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepend() throws Exception {
        grid().cache(null).remove("key");
        grid().cache(CACHE_NAME).remove("key");

        assertFalse(client.cachePrepend(null, "key", ".val"));
        assertFalse(client.cachePrepend(CACHE_NAME, "key", ".val"));

        grid().cache(null).put("key", "orig");
        grid().cache(CACHE_NAME).put("key", "orig");

        assertTrue(client.cachePrepend(null, "key", "val."));
        assertEquals("val.orig", grid().cache(null).get("key"));
        assertTrue(client.cachePrepend(null, "key", "newVal."));
        assertEquals("newVal.val.orig", grid().cache(null).get("key"));

        assertTrue(client.cachePrepend(CACHE_NAME, "key", "val."));
        assertEquals("val.orig", grid().cache(CACHE_NAME).get("key"));
        assertTrue(client.cachePrepend(CACHE_NAME, "key", "newVal."));
        assertEquals("newVal.val.orig", grid().cache(CACHE_NAME).get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecute() throws Exception {
        GridClientTaskResultBean res = client.execute(TestTask.class.getName(),
            Arrays.asList("executing", 3, "test", 5, "task"));

        assertTrue(res.isFinished());
        assertEquals(25, res.getResult());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNode() throws Exception {
        assertNull(client.node(UUID.randomUUID(), false, false));
        assertNull(client.node("wrongHost", false, false));

        GridClientNodeBean node = client.node(grid().localNode().id(), true, true);

        assertNotNull(node);
        assertFalse(node.getAttributes().isEmpty());
        assertNotNull(node.getMetrics());
        assertNotNull(node.getTcpAddresses());
        assertEquals(PORT,  node.getTcpPort());
        assertEquals(grid().localNode().id(), node.getNodeId());

        node = client.node(grid().localNode().id(), false, false);

        assertNotNull(node);
        assertNull(node.getAttributes());
        assertNull(node.getMetrics());
        assertNotNull(node.getTcpAddresses());
        assertEquals(PORT,  node.getTcpPort());
        assertEquals(grid().localNode().id(), node.getNodeId());

        node = client.node(HOST, true, true);

        assertNotNull(node);
        assertFalse(node.getAttributes().isEmpty());
        assertNotNull(node.getMetrics());
        assertNotNull(node.getTcpAddresses());
        assertEquals(PORT,  node.getTcpPort());
        assertEquals(grid().localNode().id(), node.getNodeId());

        node = client.node(HOST, false, false);

        assertNotNull(node);
        assertNull(node.getAttributes());
        assertNull(node.getMetrics());
        assertNotNull(node.getTcpAddresses());
        assertEquals(PORT,  node.getTcpPort());
        assertEquals(grid().localNode().id(), node.getNodeId());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopology() throws Exception {
        List<GridClientNodeBean> top = client.topology(true, true);

        assertNotNull(top);
        assertEquals(1, top.size());

        GridClientNodeBean node = F.first(top);

        assertNotNull(node);
        assertFalse(node.getAttributes().isEmpty());
        assertNotNull(node.getMetrics());
        assertNotNull(node.getTcpAddresses());
        assertEquals(grid().localNode().id(), node.getNodeId());

        top = client.topology(false, false);

        assertNotNull(top);
        assertEquals(1, top.size());

        node = F.first(top);

        assertNotNull(node);
        assertNull(node.getAttributes());
        assertNull(node.getMetrics());
        assertNotNull(node.getTcpAddresses());
        assertEquals(grid().localNode().id(), node.getNodeId());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLog() throws Exception {
        String path = "work/log/gridgain.log." + System.currentTimeMillis();

        File file = new File(U.getGridGainHome(), path);

        assert !file.exists();

        FileWriter writer = new FileWriter(file);

        String sep = System.getProperty("line.separator");

        writer.write("Line 1" + sep);
        writer.write(sep);
        writer.write("Line 2" + sep);
        writer.write("Line 3" + sep);

        writer.flush();
        writer.close();

        List<String> log = client.log(path, 0, 10);

        assertNotNull(log);
        assertEquals(4, log.size());

        file.delete();

        GridTestUtils.assertThrows(
            log(),
            new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    client.log("wrong/path", 0, 10);

                    return null;
                }
            },
            GridException.class,
            null
        );
    }

    /**
     * Test task.
     */
    private static class TestTask extends GridComputeTaskSplitAdapter<List<Object>, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, List<Object> args)
            throws GridException {
            Collection<GridComputeJobAdapter> jobs = new ArrayList<>(args.size());

            for (final Object arg : args) {
                jobs.add(new GridComputeJobAdapter() {
                    @SuppressWarnings("OverlyStrongTypeCast")
                    @Override public Object execute() {
                        try {
                            return ((String)arg).length();
                        }
                        catch (ClassCastException ignored) {
                            assert arg instanceof Integer;

                            return arg;
                        }
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            int sum = 0;

            for (GridComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }
}
