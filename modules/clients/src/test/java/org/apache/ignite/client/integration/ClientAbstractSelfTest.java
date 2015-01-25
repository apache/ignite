/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.integration;

import junit.framework.*;
import net.sf.json.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.GridCache;
import org.apache.ignite.cache.affinity.consistenthash.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.client.*;
import org.apache.ignite.client.ssl.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.testframework.GridTestUtils.*;

/**
 * Tests for Java client.
 */
@SuppressWarnings("deprecation")
public abstract class ClientAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    public static final String HOST = "127.0.0.1";

    /** */
    public static final int JETTY_PORT = 8080;

    /** */
    public static final int BINARY_PORT = 11212;

   /** Path to jetty config. */
    public static final String REST_JETTY_CFG = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

    /** Need to be static because configuration inits only once per class. */
    private static final ConcurrentMap<Object, Object> INTERCEPTED_OBJECTS = new ConcurrentHashMap<>();

    /** */
    private static final Map<String, HashMapStore> cacheStores = new HashMap<>();

    /** Path to test log. */
    private static final String TEST_LOG_PATH = "modules/core/src/test/resources/log/gridgain.log.tst";

    /** */
    public static final String ROUTER_LOG_CFG = "modules/core/src/test/config/log4j-test.xml";

    /** */
    private static final String INTERCEPTED_SUF = "intercepted";

    /** */
    private static final String[] TASK_ARGS = new String[] {"executing", "test", "task"};

    /** Flag indicating whether intercepted objects should be overwritten. */
    private static volatile boolean overwriteIntercepted;

    /** */
    private ExecutorService exec;

    /** */
    protected GridClient client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(GG_JETTY_PORT, Integer.toString(JETTY_PORT));

        startGrid();

        System.clearProperty(GG_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        exec = Executors.newCachedThreadPool();

        client = client();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        exec.shutdown();
        exec = null;

        GridClientFactory.stop(client.id(), true);

        client = null;

        for (HashMapStore cacheStore : cacheStores.values())
            cacheStore.map.clear();

        grid().cache(null).clearLocally();
        grid().cache(CACHE_NAME).clearLocally();

        INTERCEPTED_OBJECTS.clear();
    }

    /**
     * Gets protocol which should be used in client connection.
     *
     * @return Protocol.
     */
    protected abstract GridClientProtocol protocol();

    /**
     * Gets server address to which client should connect.
     *
     * @return Server address in format "host:port".
     */
    protected abstract String serverAddress();

    /**
     * @return Whether SSL should be used in test.
     */
    protected abstract boolean useSsl();

    /**
     * @return SSL context factory used in test.
     */
    protected abstract GridSslContextFactory sslContextFactory();

    /**
     * Get task name.
     *
     * @return Task name.
     */
    protected String getTaskName() {
        return TestTask.class.getName();
    }

    /**
     * @return name of the sleep task for current test.
     */
    protected String getSleepTaskName() {
        return SleepTestTask.class.getName();
    }

    /**
     * Get task argument.
     *
     * @return Task argument.
     */
    protected Object getTaskArgument() {
        return Arrays.asList(TASK_ARGS);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(HOST);

        assert cfg.getClientConnectionConfiguration() == null;

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        clientCfg.setRestTcpPort(BINARY_PORT);

        clientCfg.setRestAccessibleFolders(
            U.getGridGainHome() + "/work/log",
            U.resolveGridGainPath("modules/core/src/test/resources/log").getAbsolutePath());

        if (useSsl()) {
            clientCfg.setRestTcpSslEnabled(true);

            clientCfg.setRestTcpSslContextFactory(sslContextFactory());
        }

        cfg.setClientConnectionConfiguration(clientCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration("replicated"),
            cacheConfiguration("partitioned"), cacheConfiguration(CACHE_NAME));

        clientCfg.setClientMessageInterceptor(new ClientMessageInterceptor() {
            @Override public Object onReceive(@Nullable Object obj) {
                if (obj != null)
                    INTERCEPTED_OBJECTS.put(obj, obj);

                return overwriteIntercepted && obj instanceof String ?
                    obj + INTERCEPTED_SUF : obj;
            }

            @Override public Object onSend(Object obj) {
                if (obj != null)
                    INTERCEPTED_OBJECTS.put(obj, obj);

                return obj;
            }
        });

        // Specify swap SPI, otherwise test fails on windows.
        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(cacheName == null || CACHE_NAME.equals(cacheName) ? LOCAL : "replicated".equals(cacheName) ?
            REPLICATED : PARTITIONED);
        cfg.setName(cacheName);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        HashMapStore cacheStore = cacheStores.get(cacheName);

        if (cacheStore == null)
            cacheStores.put(cacheName, cacheStore = new HashMapStore());

        cfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(cacheStore));
        cfg.setWriteThrough(true);
        cfg.setReadThrough(true);
        cfg.setLoadPreviousValue(true);

        cfg.setSwapEnabled(true);

        if (cfg.getCacheMode() == PARTITIONED)
            cfg.setBackups(1);

        return cfg;
    }

    /**
     * @return Client.
     * @throws GridClientException In case of error.
     */
    protected GridClient client() throws GridClientException {
        return GridClientFactory.start(clientConfiguration());
    }

    /**
     * @return Test client configuration.
     */
    protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        GridClientDataConfiguration nullCache = new GridClientDataConfiguration();

        GridClientDataConfiguration cache = new GridClientDataConfiguration();

        cache.setName(CACHE_NAME);

        cfg.setDataConfigurations(Arrays.asList(nullCache, cache));

        cfg.setProtocol(protocol());
        cfg.setServers(Arrays.asList(serverAddress()));

        // Setting custom executor, to avoid failures on client shutdown.
        // And applying custom naming scheme to ease debugging.
        cfg.setExecutorService(Executors.newCachedThreadPool(new ThreadFactory() {
            private AtomicInteger cntr = new AtomicInteger();

            @SuppressWarnings("NullableProblems")
            @Override public Thread newThread(Runnable r) {
                return new Thread(r, "client-worker-thread-" + cntr.getAndIncrement());
            }
        }));

        if (useSsl())
            cfg.setSslContextFactory(sslContextFactory());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectable() throws Exception {
        GridClient client = client();

        List<GridClientNode> nodes = client.compute().refreshTopology(false, false);

        assertTrue(F.first(nodes).connectable());
    }

    /**
     * Check async API methods don't generate exceptions.
     *
     * @throws Exception If failed.
     */
    public void testNoAsyncExceptions() throws Exception {
        GridClient client = client();

        GridClientData data = client.data(CACHE_NAME);
        GridClientCompute compute = client.compute().projection(new GridClientPredicate<GridClientNode>() {
            @Override public boolean apply(GridClientNode e) {
                return false;
            }
        });

        Map<String, GridClientFuture<?>> futs = new LinkedHashMap<>();

        futs.put("exec", compute.executeAsync("taskName", "taskArg"));
        futs.put("affExec", compute.affinityExecuteAsync("taskName", "cacheName", "affKey", "taskArg"));
        futs.put("refreshById", compute.refreshNodeAsync(UUID.randomUUID(), true, true));
        futs.put("refreshByIP", compute.refreshNodeAsync("nodeIP", true, true));
        futs.put("refreshTop", compute.refreshTopologyAsync(true, true));
        futs.put("log", compute.logAsync(-1, -1));
        futs.put("logForPath", compute.logAsync("path/to/log", -1, -1));

        GridClientFactory.stop(client.id(), false);

        futs.put("put", data.putAsync("key", "val"));
        futs.put("putAll", data.putAllAsync(F.asMap("key", "val")));
        futs.put("get", data.getAsync("key"));
        futs.put("getAll", data.getAllAsync(Arrays.asList("key")));
        futs.put("remove", data.removeAsync("key"));
        futs.put("removeAll", data.removeAllAsync(Arrays.asList("key")));
        futs.put("replace", data.replaceAsync("key", "val"));
        futs.put("cas", data.casAsync("key", "val", "val2"));
        futs.put("metrics", data.metricsAsync());

        for (Map.Entry<String, GridClientFuture<?>> e : futs.entrySet()) {
            try {
                e.getValue().get();

                info("Expects '" + e.getKey() + "' fails with grid client exception.");
            }
            catch (GridServerUnreachableException |GridClientClosedException ignore) {
                // No op: compute projection is empty.
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGracefulShutdown() throws Exception {
        GridClientCompute compute = client.compute();

        Object taskArg = getTaskArgument();
        String taskName = getSleepTaskName();

        GridClientFuture<Object> fut = compute.executeAsync(taskName, taskArg);
        GridClientFuture<Object> fut2 = compute.executeAsync(taskName, taskArg);

        GridClientFactory.stop(client.id(), true);

        Assert.assertEquals(17, fut.get());
        Assert.assertEquals(17, fut2.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testForceShutdown() throws Exception {
        GridClientCompute compute = client.compute();

        Object taskArg = getTaskArgument();
        String taskName = getSleepTaskName();

        GridClientFuture<Object> fut = compute.executeAsync(taskName, taskArg);

        GridClientFactory.stop(client.id(), false);

        try {
            fut.get();
        }
        catch (GridClientClosedException ignored) {
            return;
        }

        Assert.fail("Expected GridClientClosedException.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testShutdown() throws Exception {
        GridClient c = client();

        GridClientCompute compute = c.compute();

        String taskName = getTaskName();
        Object taskArg = getTaskArgument();

        Collection<GridClientFuture<Object>> futs = new ArrayList<>();

        // Validate connection works.
        compute.execute(taskName, taskArg);

        info(">>> First task executed successfully, running batch.");

        for (int i = 0; i < 100; i++)
            futs.add(compute.executeAsync(taskName, taskArg));

        // Stop client.
        GridClientFactory.stop(c.id(), true);

        info(">>> Completed stop request.");

        int failed = 0;

        for (GridClientFuture<Object> fut : futs) {
            try {
                assertEquals(17, fut.get());
            }
            catch (GridClientException e) {
                failed++;

                log.warning("Task execution failed.", e);
            }
        }

        assertEquals(0, failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        GridCache<String, String> dfltCache = grid().cache(null);
        GridCache<Object, Object> namedCache = grid().cache(CACHE_NAME);

        GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        assertTrue(dfltData.put("key1", "val1"));
        assertEquals("val1", dfltCache.get("key1"));

        assertTrue(dfltData.putAsync("key2", "val2").get());
        assertEquals("val2", dfltCache.get("key2"));

        assertTrue(namedData.put("key1", "val1"));
        assertEquals("val1", namedCache.get("key1"));

        assertTrue(namedData.putAsync("key2", "val2").get());
        assertEquals("val2", namedCache.get("key2"));

        assertTrue(dfltData.put("", ""));
        assertEquals("", dfltCache.get(""));

        assertTrue(namedData.put("", ""));
        assertEquals("", namedCache.get(""));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheFlags() throws Exception {
        /* Note! Only 'SKIP_STORE' flag is validated. */
        final GridClientData data = client.data(CACHE_NAME);
        final GridClientData readData = data.flagsOn(GridClientCacheFlag.SKIP_STORE);
        final GridClientData writeData = readData.flagsOff(GridClientCacheFlag.SKIP_STORE);

        assertEquals(Collections.singleton(GridClientCacheFlag.SKIP_STORE), readData.flags());
        assertTrue(writeData.flags().isEmpty());

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();
            Object val = UUID.randomUUID().toString();

            // Put entry into cache & store.
            assertTrue(writeData.put(key, val));

            assertEquals(val, readData.get(key));
            assertEquals(val, writeData.get(key));

            // Remove from cache, skip store.
            assertTrue(readData.remove(key));

            assertNull(readData.get(key));
            assertEquals(val, writeData.get(key));
            assertEquals(val, readData.get(key));

            // Remove from cache and from store.
            assertTrue(writeData.remove(key));

            assertNull(readData.get(key));
            assertNull(writeData.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        dfltData.putAll(F.asMap("key1", "val1", "key2", "val2"));

        Map<String, String> map = grid().<String, String>cache(null).getAll(F.asList("key1", "key2"));

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));

        dfltData.putAllAsync(F.asMap("key3", "val3", "key4", "val4")).get();

        map = grid().<String, String>cache(null).getAll(F.asList("key3", "key4"));

        assertEquals(2, map.size());
        assertEquals("val3", map.get("key3"));
        assertEquals("val4", map.get("key4"));

        namedData.putAll(F.asMap("key1", "val1", "key2", "val2"));

        map = grid().<String, String>cache(CACHE_NAME).getAll(F.asList("key1", "key2"));

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));

        namedData.putAllAsync(F.asMap("key3", "val3", "key4", "val4")).get();

        map = grid().<String, String>cache(CACHE_NAME).getAll(F.asList("key3", "key4"));

        assertEquals(2, map.size());
        assertEquals("val3", map.get("key3"));
        assertEquals("val4", map.get("key4"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllWithCornerCases() throws Exception {
        final GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        dfltData.putAll(F.asMap("", "val1"));

        assertEquals(F.asMap("", "val1"), grid().<String, String>cache(null).getAll(F.asList("")));

        GridClientProtocol proto = clientConfiguration().getProtocol();

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                dfltData.putAll(Collections.singletonMap("key3", null));

                return null;
            }
        }, proto == GridClientProtocol.TCP ? GridClientException.class : IllegalArgumentException.class, null);

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                dfltData.putAll(Collections.singletonMap(null, "val2"));

                return null;
            }
        }, proto == GridClientProtocol.TCP ? GridClientException.class : IllegalArgumentException.class, null);

        dfltData.getAll(Collections.singleton(null));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        assertTrue(grid().cache(null).putx("key", "val"));

        Assert.assertEquals("val", dfltData.get("key"));
        Assert.assertEquals("val", dfltData.getAsync("key").get());

        assertTrue(grid().cache(CACHE_NAME).putx("key", "val"));

        Assert.assertEquals("val", namedData.get("key"));
        Assert.assertEquals("val", namedData.getAsync("key").get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(grid().cache(null).putx("key2", "val2"));

        Map<String, String> map = dfltData.getAll(F.asList("key1", "key2"));

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));

        assertTrue(grid().cache(null).putx("key3", "val3"));
        assertTrue(grid().cache(null).putx("key4", "val4"));

        map = dfltData.getAll(F.asList("key3", "key4"));

        assertEquals(2, map.size());
        assertEquals("val3", map.get("key3"));
        assertEquals("val4", map.get("key4"));

        map = dfltData.getAll(F.asList("key1"));

        assertEquals(1, map.size());
        assertEquals("val1", map.get("key1"));

        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(grid().cache(CACHE_NAME).putx("key2", "val2"));

        map = namedData.getAll(F.asList("key1", "key2"));

        assertEquals(2, map.size());
        assertEquals("val1", map.get("key1"));
        assertEquals("val2", map.get("key2"));

        assertTrue(grid().cache(CACHE_NAME).putx("key3", "val3"));
        assertTrue(grid().cache(CACHE_NAME).putx("key4", "val4"));

        map = namedData.getAll(F.asList("key3", "key4"));

        assertEquals(2, map.size());
        assertEquals("val3", map.get("key3"));
        assertEquals("val4", map.get("key4"));

        map = namedData.getAll(F.asList("key1"));

        assertEquals(1, map.size());
        assertEquals("val1", map.get("key1"));

    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(grid().cache(null).putx("key2", "val2"));

        assertTrue(dfltData.remove("key1"));
        assertTrue(dfltData.removeAsync("key2").get());
        assertFalse(dfltData.remove("wrongKey"));
        assertFalse(dfltData.removeAsync("wrongKey").get());

        assertNull(grid().cache(null).get("key1"));
        assertNull(grid().cache(null).get("key2"));

        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(grid().cache(CACHE_NAME).putx("key2", "val2"));

        assertTrue(namedData.remove("key1"));
        assertTrue(namedData.removeAsync("key2").get());
        assertFalse(namedData.remove("wrongKey"));
        assertFalse(namedData.removeAsync("wrongKey").get());

        assertNull(grid().cache(CACHE_NAME).get("key1"));
        assertNull(grid().cache(CACHE_NAME).get("key2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSkipStoreFlag() throws Exception {
        GridClientData namedData = client.data(CACHE_NAME).flagsOn(GridClientCacheFlag.SKIP_STORE);

        // test keyA
        assertTrue(grid().cache(CACHE_NAME).putx("keyA", "valA"));
        assertTrue(namedData.remove("keyA"));
        assertEquals("valA", cacheStores.get(CACHE_NAME).map.get("keyA"));
        assertNull(namedData.get("keyA"));

        // test keyX
        assertTrue(namedData.put("keyX", "valX"));
        assertEquals("valX", namedData.get("keyX"));
        assertNull(cacheStores.get(CACHE_NAME).map.get("keyX"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSkipSwapFlag() throws Exception {
        GridClientData namedData = client.data(CACHE_NAME);

        assertTrue(namedData.put("k", "v"));

        assertTrue(grid().cache(CACHE_NAME).evict("k"));

        assertNull(namedData.flagsOn(GridClientCacheFlag.SKIP_SWAP, GridClientCacheFlag.SKIP_STORE).get("k"));
        assertEquals("v", namedData.flagsOn(GridClientCacheFlag.SKIP_STORE).get("k"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAll() throws Exception {
        GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(grid().cache(null).putx("key2", "val2"));
        assertTrue(grid().cache(null).putx("key3", "val3"));
        assertTrue(grid().cache(null).putx("key4", "val4"));

        dfltData.removeAll(F.asList("key1", "key2"));
        dfltData.removeAllAsync(F.asList("key3", "key4")).get();

        assertNull(grid().cache(null).get("key1"));
        assertNull(grid().cache(null).get("key2"));
        assertNull(grid().cache(null).get("key3"));
        assertNull(grid().cache(null).get("key4"));

        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(grid().cache(CACHE_NAME).putx("key2", "val2"));
        assertTrue(grid().cache(CACHE_NAME).putx("key3", "val3"));
        assertTrue(grid().cache(CACHE_NAME).putx("key4", "val4"));

        namedData.removeAll(F.asList("key1", "key2"));
        namedData.removeAllAsync(F.asList("key3", "key4")).get();

        assertNull(grid().cache(CACHE_NAME).get("key1"));
        assertNull(grid().cache(CACHE_NAME).get("key2"));
        assertNull(grid().cache(CACHE_NAME).get("key3"));
        assertNull(grid().cache(CACHE_NAME).get("key4"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplace() throws Exception {
        GridClientData dfltData = client.data();

        assertNotNull(dfltData);

        GridClientData namedData = client.data(CACHE_NAME);

        assertNotNull(namedData);

        assertFalse(dfltData.replace("key1", "val1"));
        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(dfltData.replace("key1", "val2"));
        assertEquals("val2", grid().cache(null).get("key1"));

        assertFalse(dfltData.replace("key2", "val1"));
        assertTrue(grid().cache(null).putx("key2", "val1"));
        assertTrue(dfltData.replace("key2", "val2"));
        assertEquals("val2", grid().cache(null).get("key2"));

        grid().cache(null).removeAll(F.asList("key1", "key2"));

        assertFalse(dfltData.replaceAsync("key1", "val1").get());
        assertTrue(grid().cache(null).putx("key1", "val1"));
        assertTrue(dfltData.replaceAsync("key1", "val2").get());
        assertEquals("val2", grid().cache(null).get("key1"));

        assertFalse(dfltData.replaceAsync("key2", "val1").get());
        assertTrue(grid().cache(null).putx("key2", "val1"));
        assertTrue(dfltData.replaceAsync("key2", "val2").get());
        assertEquals("val2", grid().cache(null).get("key2"));

        assertFalse(namedData.replace("key1", "val1"));
        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(namedData.replace("key1", "val2"));
        assertEquals("val2", grid().cache(CACHE_NAME).get("key1"));

        assertFalse(namedData.replaceAsync("key2", "val1").get());
        assertTrue(grid().cache(CACHE_NAME).putx("key2", "val1"));
        assertTrue(namedData.replaceAsync("key2", "val2").get());
        assertEquals("val2", grid().cache(CACHE_NAME).get("key2"));

        grid().cache(CACHE_NAME).removeAll(F.asList("key1", "key2"));

        assertFalse(namedData.replaceAsync("key1", "val1").get());
        assertTrue(grid().cache(CACHE_NAME).putx("key1", "val1"));
        assertTrue(namedData.replaceAsync("key1", "val2").get());
        assertEquals("val2", grid().cache(CACHE_NAME).get("key1"));

        assertFalse(namedData.replaceAsync("key2", "val1").get());
        assertTrue(grid().cache(CACHE_NAME).putx("key2", "val1"));
        assertTrue(namedData.replaceAsync("key2", "val2").get());
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullableProblems")
    public void testCompareAndSet() throws Exception {
        GridClientData[] datas = new GridClientData[] {
            client.data(),
            client.data(CACHE_NAME)
        };

        assertNotNull(datas[0]);
        assertNotNull(datas[1]);

        GridCache[] caches = new GridCache[] {
            grid().cache(null),
            grid().cache(CACHE_NAME)
        };

        for (int i = 0; i < datas.length; i++) {
            GridClientData data = datas[i];
            GridCache<String, String> cache = (GridCache<String, String>)caches[i];

            assertFalse(data.cas("key", null, null));
            assertTrue(cache.putx("key", "val"));
            assertTrue(data.cas("key", null, null));
            assertNull(cache.get("key"));

            assertFalse(data.cas("key", null, "val"));
            assertTrue(cache.putx("key", "val"));
            assertFalse(data.cas("key", null, "wrongVal"));
            assertEquals("val", cache.get("key"));
            assertTrue(data.cas("key", null, "val"));
            assertNull(cache.get("key"));

            assertTrue(data.cas("key", "val", null));
            assertEquals("val", cache.get("key"));
            assertFalse(data.cas("key", "newVal", null));
            assertEquals("val", cache.get("key"));
            assertTrue(cache.removex("key"));

            assertFalse(data.cas("key", "val1", "val2"));
            assertTrue(cache.putx("key", "val2"));
            assertFalse(data.cas("key", "val1", "wrongVal"));
            assertEquals("val2", cache.get("key"));
            assertTrue(data.cas("key", "val1", "val2"));
            assertEquals("val1", cache.get("key"));
            assertTrue(cache.removex("key"));

            assertFalse(data.casAsync("key", null, null).get());
            assertTrue(cache.putx("key", "val"));
            assertTrue(data.casAsync("key", null, null).get());
            assertNull(cache.get("key"));

            assertFalse(data.casAsync("key", null, "val").get());
            assertTrue(cache.putx("key", "val"));
            assertFalse(data.casAsync("key", null, "wrongVal").get());
            assertEquals("val", cache.get("key"));
            assertTrue(data.casAsync("key", null, "val").get());
            assertNull(cache.get("key"));

            assertTrue(data.casAsync("key", "val", null).get());
            assertEquals("val", cache.get("key"));
            assertFalse(data.casAsync("key", "newVal", null).get());
            assertEquals("val", cache.get("key"));
            assertTrue(cache.removex("key"));

            assertFalse(data.casAsync("key", "val1", "val2").get());
            assertTrue(cache.putx("key", "val2"));
            assertFalse(data.casAsync("key", "val1", "wrongVal").get());
            assertEquals("val2", cache.get("key"));
            assertTrue(data.casAsync("key", "val1", "val2").get());
            assertEquals("val1", cache.get("key"));
            assertTrue(cache.removex("key"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetrics() throws Exception {
        GridClientData dfltData = client.data();
        GridClientData namedData = client.data(CACHE_NAME);

        grid().cache(null).resetMetrics();
        grid().cache(CACHE_NAME).resetMetrics();

        grid().cache(null).putx("key1", "val1");
        grid().cache(null).putx("key2", "val2");
        grid().cache(null).putx("key2", "val3");

        assertEquals("val1", grid().cache(null).get("key1"));
        assertEquals("val3", grid().cache(null).get("key2"));
        assertEquals("val3", grid().cache(null).get("key2"));

        grid().cache(CACHE_NAME).putx("key1", "val1");
        grid().cache(CACHE_NAME).putx("key2", "val2");
        grid().cache(CACHE_NAME).putx("key2", "val3");

        assertEquals("val1", grid().cache(CACHE_NAME).get("key1"));
        assertEquals("val3", grid().cache(CACHE_NAME).get("key2"));
        assertEquals("val3", grid().cache(CACHE_NAME).get("key2"));

        GridClientDataMetrics m = dfltData.metrics();

        CacheMetrics metrics = grid().cache(null).metrics();

        assertNotNull(m);
        assertEquals(metrics.reads(), m.reads());
        assertEquals(metrics.writes(), m.writes());

        m = dfltData.metricsAsync().get();

        assertNotNull(m);
        assertEquals(metrics.reads(), m.reads());
        assertEquals(metrics.writes(), m.writes());

        m = namedData.metrics();

        metrics = grid().cache(CACHE_NAME).metrics();

        assertNotNull(m);
        assertEquals(metrics.reads(), m.reads());
        assertEquals(metrics.writes(), m.writes());

        m = namedData.metricsAsync().get();

        assertNotNull(m);
        assertEquals(metrics.reads(), m.reads());
        assertEquals(metrics.writes(), m.writes());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendPrepend() throws Exception {
        List<GridClientData> datas = Arrays.asList(client.data(), client.data(CACHE_NAME));

        String key = UUID.randomUUID().toString();

        for (GridClientData data : datas) {
            assertNotNull(data);

            data.remove(key);

            assertFalse(data.append(key, ".suffix"));
            assertTrue(data.put(key, "val"));
            assertTrue(data.append(key, ".suffix"));
            assertEquals("val.suffix", data.get(key));
            assertTrue(data.remove(key));
            assertFalse(data.append(key, ".suffix"));

            data.remove(key);

            assertFalse(data.prepend(key, "postfix."));
            assertTrue(data.put(key, "val"));
            assertTrue(data.prepend(key, "postfix."));
            assertEquals("postfix.val", data.get(key));
            assertTrue(data.remove(key));
            assertFalse(data.prepend(key, "postfix."));
        }

        // TCP protocol supports work with collections.
        if (protocol() != GridClientProtocol.TCP)
            return;

        List<String> origList = new ArrayList<>(Arrays.asList("1", "2")); // This list should be modifiable.
        List<String> newList = Arrays.asList("3", "4");

        Map<String, String> origMap = F.asMap("1", "a1", "2", "a2");
        Map<String, String> newMap = F.asMap("2", "b2", "3", "b3");

        for (GridClientData data : datas) {
            assertNotNull(data);

            data.remove(key);

            assertFalse(data.append(key, newList));
            assertTrue(data.put(key, origList));
            assertTrue(data.append(key, newList));
            assertEquals(Arrays.asList("1", "2", "3", "4"), data.get(key));

            data.remove(key);

            assertFalse(data.prepend(key, newList));
            assertTrue(data.put(key, origList));
            assertTrue(data.prepend(key, newList));
            assertEquals(Arrays.asList("3", "4", "1", "2"), data.get(key));

            data.remove(key);

            assertFalse(data.append(key, newMap));
            assertTrue(data.put(key, origMap));
            assertTrue(data.append(key, newMap));
            assertEquals(F.asMap("1", "a1", "2", "b2", "3", "b3"), data.get(key));

            data.remove(key);

            assertFalse(data.prepend(key, newMap));
            assertTrue(data.put(key, origMap));
            assertTrue(data.prepend(key, newMap));
            assertEquals(F.asMap("1", "a1", "2", "a2", "3", "b3"), data.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecute() throws Exception {
        String taskName = getTaskName();
        Object taskArg = getTaskArgument();

        GridClientCompute compute = client.compute();

        Assert.assertEquals(17, compute.execute(taskName, taskArg));
        Assert.assertEquals(17, compute.executeAsync(taskName, taskArg).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNode() throws Exception {
        GridClientCompute compute = client.compute();

        assertNull(compute.refreshNode(UUID.randomUUID(), true, false));
        assertNull(compute.refreshNode(UUID.randomUUID(), false, false));

        GridClientNode node = compute.refreshNode(grid().localNode().id(), true, false);

        assertNotNull(node);
        assertFalse(node.attributes().isEmpty());
        assertTrue(node.metrics() == null);
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
        assertEquals(4, node.caches().size());

        Integer replica = grid().localNode().attribute(CacheConsistentHashAffinityFunction.DFLT_REPLICA_COUNT_ATTR_NAME);

        if (replica == null)
            replica = CacheConsistentHashAffinityFunction.DFLT_REPLICA_COUNT;

        assertEquals((int)replica, node.replicaCount());

        Map<String, GridClientCacheMode> caches = node.caches();

        for (Map.Entry<String, GridClientCacheMode> e : caches.entrySet()) {
            if (e.getKey() == null || CACHE_NAME.equals(e.getKey()))
                assertEquals(GridClientCacheMode.LOCAL, e.getValue());
            else if ("replicated".equals(e.getKey()))
                assertEquals(GridClientCacheMode.REPLICATED, e.getValue());
            else if ("partitioned".equals(e.getKey()))
                assertEquals(GridClientCacheMode.PARTITIONED, e.getValue());
            else
                fail("Unexpected cache name: " + e.getKey());
        }

        node = compute.refreshNode(grid().localNode().id(), false, false);

        assertNotNull(node);
        assertTrue(node.attributes().isEmpty());
        assertTrue(node.metrics() == null);
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
        assertEquals(4, node.caches().size());

        caches = node.caches();

        for (Map.Entry<String, GridClientCacheMode> e : caches.entrySet()) {
            if (e.getKey() == null || CACHE_NAME.equals(e.getKey()))
                assertEquals(GridClientCacheMode.LOCAL, e.getValue());
            else if ("replicated".equals(e.getKey()))
                assertEquals(GridClientCacheMode.REPLICATED, e.getValue());
            else if ("partitioned".equals(e.getKey()))
                assertEquals(GridClientCacheMode.PARTITIONED, e.getValue());
            else
                fail("Unexpected cache name: " + e.getKey());
        }

        node = compute.refreshNode(grid().localNode().id(), false, true);

        assertNotNull(node);
        assertTrue(node.attributes().isEmpty());
        assertFalse(node.metrics() == null);
        assertTrue(node.metrics().getCurrentActiveJobs() != -1);
        assertTrue(node.metrics().getCurrentIdleTime() != -1);
        assertTrue(node.metrics().getLastUpdateTime() != -1);
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
        assertEquals(4, node.caches().size());

        caches = node.caches();

        for (Map.Entry<String, GridClientCacheMode> e : caches.entrySet()) {
            if (e.getKey() == null || CACHE_NAME.equals(e.getKey()))
                assertEquals(GridClientCacheMode.LOCAL, e.getValue());
            else if ("replicated".equals(e.getKey()))
                assertEquals(GridClientCacheMode.REPLICATED, e.getValue());
            else if ("partitioned".equals(e.getKey()))
                assertEquals(GridClientCacheMode.PARTITIONED, e.getValue());
            else
                fail("Unexpected cache name: " + e.getKey());
        }

        assertNull(compute.refreshNodeAsync(UUID.randomUUID(), true, false).get());
        assertNull(compute.refreshNodeAsync(UUID.randomUUID(), false, false).get());

        node = compute.refreshNodeAsync(grid().localNode().id(), true, false).get();

        assertNotNull(node);
        assertFalse(node.attributes().isEmpty());
        assertTrue(node.metrics() == null);
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
        assertEquals(4, node.caches().size());

        caches = node.caches();

        for (Map.Entry<String, GridClientCacheMode> e : caches.entrySet()) {
            if (e.getKey() == null || CACHE_NAME.equals(e.getKey()))
                assertEquals(GridClientCacheMode.LOCAL, e.getValue());
            else if ("replicated".equals(e.getKey()))
                assertEquals(GridClientCacheMode.REPLICATED, e.getValue());
            else if ("partitioned".equals(e.getKey()))
                assertEquals(GridClientCacheMode.PARTITIONED, e.getValue());
            else
                fail("Unexpected cache name: " + e.getKey());
        }

        node = compute.refreshNodeAsync(grid().localNode().id(), false, false).get();

        assertNotNull(node);
        assertTrue(node.attributes().isEmpty());
        assertTrue(node.metrics() == null);
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
        assertEquals(4, node.caches().size());

        caches = node.caches();

        for (Map.Entry<String, GridClientCacheMode> e : caches.entrySet()) {
            if (e.getKey() == null || CACHE_NAME.equals(e.getKey()))
                assertEquals(GridClientCacheMode.LOCAL, e.getValue());
            else if ("replicated".equals(e.getKey()))
                assertEquals(GridClientCacheMode.REPLICATED, e.getValue());
            else if ("partitioned".equals(e.getKey()))
                assertEquals(GridClientCacheMode.PARTITIONED, e.getValue());
            else
                fail("Unexpected cache name: " + e.getKey());
        }

        node = compute.refreshNodeAsync(grid().localNode().id(), false, true).get();

        assertNotNull(node);
        assertTrue(node.attributes().isEmpty());
        assertFalse(node.metrics() == null);
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
        assertEquals(4, node.caches().size());

        caches = node.caches();

        for (Map.Entry<String, GridClientCacheMode> e : caches.entrySet()) {
            if (e.getKey() == null || CACHE_NAME.equals(e.getKey()))
                assertEquals(GridClientCacheMode.LOCAL, e.getValue());
            else if ("replicated".equals(e.getKey()))
                assertEquals(GridClientCacheMode.REPLICATED, e.getValue());
            else if ("partitioned".equals(e.getKey()))
                assertEquals(GridClientCacheMode.PARTITIONED, e.getValue());
            else
                fail("Unexpected cache name: " + e.getKey());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopology() throws Exception {
        GridClientCompute compute = client.compute();

        List<GridClientNode> top = compute.refreshTopology(true, true);

        assertNotNull(top);
        assertEquals(1, top.size());

        GridClientNode node = F.first(top);

        assertNotNull(node);
        assertFalse(node.attributes().isEmpty());
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
        assertNotNull(node.metrics());

        top = compute.refreshTopology(false, false);

        node = F.first(top);

        assertNotNull(top);
        assertEquals(1, top.size());
        assertNull(node.metrics());
        assertTrue(node.attributes().isEmpty());

        node = F.first(top);

        assertNotNull(node);
        assertTrue(node.attributes().isEmpty());
        assertNull(node.metrics());
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());

        top = compute.refreshTopologyAsync(true, true).get();

        assertNotNull(top);
        assertEquals(1, top.size());

        node = F.first(top);

        assertNotNull(node);
        assertFalse(node.attributes().isEmpty());
        assertNotNull(node.metrics());
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());

        top = compute.refreshTopologyAsync(false, false).get();

        assertNotNull(top);
        assertEquals(1, top.size());

        node = F.first(top);

        assertNotNull(node);
        assertTrue(node.attributes().isEmpty());
        assertNull(node.metrics());
        assertNotNull(node.tcpAddresses());
        assertEquals(grid().localNode().id(), node.nodeId());
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testLog() throws Exception {
        final GridClientCompute compute = client.compute();

        /* Usually this log file is created by log4j, but some times it doesn't exists. */
        new File(U.getGridGainHome(), "work/log/gridgain.log").createNewFile();

        List<String> log = compute.log(6, 7);
        assertNotNull(log);

        log = compute.log(-7, -6);
        assertNotNull(log);

        log = compute.log(-6, -7);
        assertNotNull(log);
        assertTrue(log.isEmpty());

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

        log = compute.log(path, -1, -1);
        assertNotNull(log);
        assertEquals(1, log.size());
        assertEquals("Line 3", log.get(0));

        // Indexing from 0.
        log = compute.log(path, 2, 3);
        assertNotNull(log);
        assertEquals(2, log.size());
        assertEquals("Line 2", log.get(0));
        assertEquals("Line 3", log.get(1));

        // Backward reading.
        log = compute.log(path, -3, -1);
        assertNotNull(log);
        assertEquals(3, log.size());
        assertEquals("", log.get(0));
        assertEquals("Line 2", log.get(1));
        assertEquals("Line 3", log.get(2));

        log = compute.log(path, -4, -3);
        assertNotNull(log);
        assertEquals(2, log.size());
        assertEquals("Line 1", log.get(0));
        assertEquals("", log.get(1));

        log = compute.log(path, -5, -8);
        assertNotNull(log);
        assertEquals(0, log.size());

        assert file.delete();

        log = compute.log(TEST_LOG_PATH, -9, -5);
        assertNotNull(log);
        assertEquals(5, log.size());

        log = compute.log(TEST_LOG_PATH, -1, -1);
        assertNotNull(log);
        assertEquals(1, log.size());
        assertEquals("[14:23:34,336][INFO ][main][GridTaskContinuousMapperSelfTest] >>> Stopping test: " +
            "testContinuousMapperNegative in 2633 ms <<<",
            log.get(0));

        log = compute.log(TEST_LOG_PATH, -13641, -13640);
        assertNotNull(log);
        assertEquals(1, log.size());
        assertEquals("[14:14:22,515][INFO ][main][GridListenActorSelfTest] ", log.get(0));

        assertThrows(
            log(),
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    compute.log("wrong/path", -1, -1);

                    return null;
                }
            },
            GridClientException.class,
            null
        );

        assertThrows(
            log(),
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    new File(U.getGridGainHome(), "work/security.log").createNewFile();

                    compute.log("work/log/../security.log", -1, -1);

                    return null;
                }
            },
            GridClientException.class,
            null
        );
    }

    /**
     * Test if all user objects passed interception.
     *
     * @throws Exception If failed.
     */
    public void testInterception() throws Exception {
        grid().cache(null).put("rem1", "rem1");

        GridClientData data = client.data();

        assertNotNull(data);

        overwriteIntercepted = true;

        data.put("key1", "val1");
        data.putAll(F.asMap("key2", "val2", "key3", "val3"));
        data.remove("rem1");
        data.replace("key1", "nval1");

        client.compute().execute(getTaskName(), getTaskArgument());

        for (Object obj : Arrays.asList(
            "rem1", "rem1", "key1", "key2", "key2", "val2", "key3", "val3", "rem1", "key1", "nval1",
            getTaskArgument())) {

            assert INTERCEPTED_OBJECTS.containsKey(obj);
        }

        assert ("nval1" + INTERCEPTED_SUF).equals(grid().cache(null).get("key1" + INTERCEPTED_SUF));
        assert ("val2" + INTERCEPTED_SUF).equals(grid().cache(null).get("key2" + INTERCEPTED_SUF));
        assert "rem1".equals(grid().cache(null).get("rem1"));

        overwriteIntercepted = false;
    }

    /**
     * Test task.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<List<Object>, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, List<Object> list)
            throws IgniteCheckedException {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>();

            if (list != null)
                for (final Object val : list)
                    jobs.add(new ComputeJobAdapter() {
                        @Override public Object execute() {
                            try {
                                Thread.sleep(1);
                            }
                            catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            }

                            return val == null ? 0 : val.toString().length();
                        }
                    });

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            int sum = 0;

            for (ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }

    /**
     * Test task that sleeps 5 seconds.
     */
    private static class SleepTestTask extends ComputeTaskSplitAdapter<List<Object>, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, List<Object> list)
            throws IgniteCheckedException {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>();

            if (list != null)
                for (final Object val : list)
                    jobs.add(new ComputeJobAdapter() {
                        @Override public Object execute() {
                            try {
                                Thread.sleep(5000);

                                return val == null ? 0 : val.toString().length();
                            }
                            catch (InterruptedException ignored) {
                                return -1;
                            }
                        }
                    });

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            int sum = 0;

            for (ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }

    /**
     * Http test task with restriction to string arguments only.
     */
    protected static class HttpTestTask extends ComputeTaskSplitAdapter<String, Integer> {
        private final TestTask delegate = new TestTask();

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) throws IgniteCheckedException {
            if (arg.endsWith("intercepted"))
                arg = arg.substring(0, arg.length() - 11);

            JSON json = JSONSerializer.toJSON(arg);

            List list = json.isArray() ? JSONArray.toList((JSONArray)json, String.class, new JsonConfig()) : null;

            return delegate.split(gridSize, list);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return delegate.reduce(results);
        }
    }

    /**
     * Http wrapper for sleep task.
     */
    protected static class SleepHttpTestTask extends ComputeTaskSplitAdapter<String, Integer> {
        private final SleepTestTask delegate = new SleepTestTask();

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) throws IgniteCheckedException {
            JSON json = JSONSerializer.toJSON(arg);

            List list = json.isArray() ? JSONArray.toList((JSONArray)json, String.class, new JsonConfig()) : null;

            return delegate.split(gridSize, list);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return delegate.reduce(results);
        }
    }

    /**
     * Simple HashMap based cache store emulation.
     */
    private static class HashMapStore extends CacheStoreAdapter<Object, Object> {
        /** Map for cache store. */
        private final Map<Object, Object> map = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry e : map.entrySet()) {
                clo.apply(e.getKey(), e.getValue());
            }
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Object, ? extends Object> e) {
            map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }
}
