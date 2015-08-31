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

package org.apache.ignite.internal.client.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import junit.framework.Assert;
import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONSerializer;
import net.sf.json.JsonConfig;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorMessageInterceptor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
        System.setProperty(IGNITE_JETTY_PORT, Integer.toString(JETTY_PORT));

        startGrid();

        System.clearProperty(IGNITE_JETTY_PORT);
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
        U.shutdownNow(ClientAbstractSelfTest.class, exec, log);

        exec = null;

        if (client != null)
            GridClientFactory.stop(client.id(), true);

        client = null;

        synchronized (cacheStores) {
            for (HashMapStore cacheStore : cacheStores.values())
                cacheStore.map.clear();
        }

        grid().cache(null).clear();
        grid().cache(CACHE_NAME).clear();

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

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setPort(BINARY_PORT);

        if (useSsl()) {
            clientCfg.setSslEnabled(true);

            clientCfg.setSslContextFactory(sslContextFactory());
        }

        cfg.setConnectorConfiguration(clientCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration("replicated"),
            cacheConfiguration("partitioned"), cacheConfiguration(CACHE_NAME));

        clientCfg.setMessageInterceptor(new ConnectorMessageInterceptor() {
            @Override
            public Object onReceive(@Nullable Object obj) {
                if (obj != null)
                    INTERCEPTED_OBJECTS.put(obj, obj);

                return overwriteIntercepted && obj instanceof String ?
                    obj + INTERCEPTED_SUF : obj;
            }

            @Override
            public Object onSend(Object obj) {
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
    private  static CacheConfiguration cacheConfiguration(@Nullable final String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(cacheName == null || CACHE_NAME.equals(cacheName) ? LOCAL : "replicated".equals(cacheName) ?
            REPLICATED : PARTITIONED);
        cfg.setName(cacheName);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheStoreFactory(new Factory<CacheStore>() {
            @Override public CacheStore create() {
                synchronized (cacheStores) {
                    HashMapStore cacheStore = cacheStores.get(cacheName);

                    if (cacheStore == null)
                        cacheStores.put(cacheName, cacheStore = new HashMapStore());

                    return cacheStore;
                }
            }
        });

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

        for (int i = 0; i < 10; i++)
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
    public void testExecute() throws Exception {
        String taskName = getTaskName();
        Object taskArg = getTaskArgument();

        GridClientCompute compute = client.compute();

        Assert.assertEquals(new Integer(17), compute.execute(taskName, taskArg));
        Assert.assertEquals(new Integer(17), compute.executeAsync(taskName, taskArg).get());
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
     * Test task.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<List<Object>, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, List<Object> list) {
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
        @Override public Integer reduce(List<ComputeJobResult> results) {
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
            {
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
        @Override public Integer reduce(List<ComputeJobResult> results) {
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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            if (arg.endsWith("intercepted"))
                arg = arg.substring(0, arg.length() - 11);

            JSON json = JSONSerializer.toJSON(arg);

            List list = json.isArray() ? JSONArray.toList((JSONArray)json, String.class, new JsonConfig()) : null;

            return delegate.split(gridSize, list);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            JSON json = JSONSerializer.toJSON(arg);

            List list = json.isArray() ? JSONArray.toList((JSONArray)json, String.class, new JsonConfig()) : null;

            return delegate.split(gridSize, list);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
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