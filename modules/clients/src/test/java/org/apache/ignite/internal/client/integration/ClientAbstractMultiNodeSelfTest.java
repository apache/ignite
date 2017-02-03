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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPartitionAffinity;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.GridClientTopologyListener;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.balancer.GridClientRoundRobinBalancer;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests basic client behavior with multiple nodes.
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public abstract class ClientAbstractMultiNodeSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Partitioned cache name. */
    private static final String PARTITIONED_CACHE_NAME = "partitioned";

    /** Replicated cache name. */
    private static final String REPLICATED_CACHE_NAME = "replicated";

    /** Replicated async cache name. */
    private static final String REPLICATED_ASYNC_CACHE_NAME = "replicated_async";

    /** Nodes count. */
    public static final int NODES_CNT = 5;

    /**
     * Topology update frequency.
     * Set it longer than router's, so we wouldn't receive failures
     * caused only by obsolete topology on router.
     */
    static final int TOP_REFRESH_FREQ = 2500;

    /** Path to jetty config. */
    public static final String REST_JETTY_CFG = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

    /** Path to jetty config with SSl enabled. */
    public static final String REST_JETTY_SSL_CFG = "modules/clients/src/test/resources/jetty/rest-jetty-ssl.xml";

    /** Host. */
    public static final String HOST = "127.0.0.1";

    /** Base for tcp rest ports. */
    public static final int REST_TCP_PORT_BASE = 12345;

    /** Base for http rest ports, defined in {@link #REST_JETTY_CFG}. */
    public static final int REST_HTTP_PORT_BASE = 11080;

    /** Base for https rest ports, defined in {@link #REST_JETTY_SSL_CFG}. */
    public static final int REST_HTTPS_PORT_BASE = 11443;

    /** */
    private static volatile boolean commSpiEnabled;

    /** Flag to enable REST in node configuration. */
    private boolean restEnabled = true;

    /** Client instance for each test. */
    private GridClient client;

    /**
     * @return Client protocol that should be used.
     */
    protected abstract GridClientProtocol protocol();

    /**
     * @return Server address to create first connection.
     */
    protected abstract String serverAddress();

    /**
     * @return SSL context factory to use if SSL or {@code null} to disable SSL usage.
     */
    @Nullable protected GridSslContextFactory sslContextFactory() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setLocalHost(HOST);

        assert c.getConnectorConfiguration() == null;

        if (restEnabled) {
            ConnectorConfiguration clientCfg = new ConnectorConfiguration();

            clientCfg.setPort(REST_TCP_PORT_BASE);

            GridSslContextFactory sslCtxFactory = sslContextFactory();

            if (sslCtxFactory != null) {
                clientCfg.setSslEnabled(true);
                clientCfg.setSslContextFactory(sslCtxFactory);
            }

            c.setConnectorConfiguration(clientCfg);
        }

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        TestCommunicationSpi spi = new TestCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));

        c.setCommunicationSpi(spi);

        c.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration(PARTITIONED_CACHE_NAME),
            cacheConfiguration(REPLICATED_CACHE_NAME), cacheConfiguration(REPLICATED_ASYNC_CACHE_NAME));

        c.setPublicThreadPoolSize(40);

        c.setSystemThreadPoolSize(40);

        return c;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setAtomicityMode(TRANSACTIONAL);

        if (cacheName == null)
            cfg.setCacheMode(LOCAL);
        else if (PARTITIONED_CACHE_NAME.equals(cacheName)) {
            cfg.setCacheMode(PARTITIONED);

            cfg.setBackups(0);
        }
        else
            cfg.setCacheMode(REPLICATED);

        cfg.setName(cacheName);

        cfg.setWriteSynchronizationMode(REPLICATED_ASYNC_CACHE_NAME.equals(cacheName) ? FULL_ASYNC : FULL_SYNC);

        cfg.setAffinity(new RendezvousAffinityFunction());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        info("Stopping grids.");

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = GridClientFactory.start(clientConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (client != null) {
            GridClientFactory.stop(client.id(), false);

            client = null;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEmptyProjections() throws Exception {
        final GridClientCompute dflt = client.compute();

        Collection<? extends GridClientNode> nodes = dflt.nodes();

        assertEquals(NODES_CNT, nodes.size());

        Iterator<? extends GridClientNode> iter = nodes.iterator();

        final GridClientCompute singleNodePrj = dflt.projection(Collections.singletonList(iter.next()));

        final GridClientNode second = iter.next();

        final GridClientPredicate<GridClientNode> targetFilter = new GridClientPredicate<GridClientNode>() {
            @Override public boolean apply(GridClientNode node) {
                return node.nodeId().equals(second.nodeId());
            }
        };

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return singleNodePrj.projection(second);
            }
        }, GridClientException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return singleNodePrj.projection(targetFilter);
            }
        }, GridClientException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjectionRun() throws Exception {
        GridClientCompute dflt = client.compute();

        Collection<? extends GridClientNode> nodes = dflt.nodes();

        assertEquals(NODES_CNT, nodes.size());

        for (int i = 0; i < NODES_CNT; i++) {
            Ignite g = grid(i);

            assert g != null;

            GridClientNode clientNode = dflt.node(g.cluster().localNode().id());

            assertNotNull("Client node for " + g.cluster().localNode().id() + " was not found", clientNode);

            GridClientCompute prj = dflt.projection(clientNode);

            String res = prj.execute(TestTask.class.getName(), null);

            assertNotNull(res);

            assertEquals(g.cluster().localNode().id().toString(), res);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyListener() throws Exception {
        final Collection<UUID> added = new ArrayList<>(1);
        final Collection<UUID> rmvd = new ArrayList<>(1);

        final CountDownLatch addedLatch = new CountDownLatch(1);
        final CountDownLatch rmvLatch = new CountDownLatch(1);

        assertEquals(NODES_CNT, client.compute().refreshTopology(false, false).size());

        GridClientTopologyListener lsnr = new GridClientTopologyListener() {
            @Override public void onNodeAdded(GridClientNode node) {
                added.add(node.nodeId());

                addedLatch.countDown();
            }

            @Override public void onNodeRemoved(GridClientNode node) {
                rmvd.add(node.nodeId());

                rmvLatch.countDown();
            }
        };

        client.addTopologyListener(lsnr);

        try {
            Ignite g = startGrid(NODES_CNT + 1);

            UUID id = g.cluster().localNode().id();

            assertTrue(addedLatch.await(2 * TOP_REFRESH_FREQ, MILLISECONDS));

            assertEquals(1, added.size());
            assertEquals(id, F.first(added));

            stopGrid(NODES_CNT + 1);

            assertTrue(rmvLatch.await(2 * TOP_REFRESH_FREQ, MILLISECONDS));

            assertEquals(1, rmvd.size());
            assertEquals(id, F.first(rmvd));
        }
        finally {
            client.removeTopologyListener(lsnr);

            stopGrid(NODES_CNT + 1);
        }
    }

    /**
     * @return Client configuration for the test.
     */
    protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setBalancer(getBalancer());

        cfg.setTopologyRefreshFrequency(TOP_REFRESH_FREQ);

        cfg.setProtocol(protocol());
        cfg.setServers(Arrays.asList(serverAddress()));
        cfg.setSslContextFactory(sslContextFactory());

        GridClientDataConfiguration loc = new GridClientDataConfiguration();

        GridClientDataConfiguration partitioned = new GridClientDataConfiguration();

        partitioned.setName(PARTITIONED_CACHE_NAME);
        partitioned.setAffinity(new GridClientPartitionAffinity());

        GridClientDataConfiguration replicated = new GridClientDataConfiguration();
        replicated.setName(REPLICATED_CACHE_NAME);

        GridClientDataConfiguration replicatedAsync = new GridClientDataConfiguration();
        replicatedAsync.setName(REPLICATED_ASYNC_CACHE_NAME);

        cfg.setDataConfigurations(Arrays.asList(loc, partitioned, replicated, replicatedAsync));

        return cfg;
    }

    /**
     * Gets client load balancer.
     *
     * @return Load balancer.
     */
    protected GridClientLoadBalancer getBalancer() {
        return new GridClientRoundRobinBalancer();
    }

    /**
     * Test task. Returns a tuple in which first component is id of node that has split the task,
     * and second component is count of nodes that executed jobs.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Object, String> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Count of tasks this job was split to. */
        private int gridSize;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            this.gridSize = gridSize;

            final String locNodeId = ignite.cluster().localNode().id().toString();

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    @SuppressWarnings("OverlyStrongTypeCast")
                    @Override public Object execute() {
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }

                        return new IgniteBiTuple<>(locNodeId, 1);
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) {
            int sum = 0;

            String locNodeId = null;

            for (ComputeJobResult res : results) {
                IgniteBiTuple<String, Integer> part = res.getData();

                if (locNodeId == null)
                    locNodeId = part.get1();

                Integer i = part.get2();

                if (i != null)
                    sum += i;
            }

            assert gridSize == sum;

            return locNodeId;
        }
    }

    /**
     * Communication SPI which checks cache flags.
     */
    @SuppressWarnings("unchecked")
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            checkSyncFlags((GridIoMessage)msg);

            super.sendMessage(node, msg, ackClosure);
        }

        /**
         * Check if flags in correct state.
         *
         * @param msg Message.
         */
        private void checkSyncFlags(GridIoMessage msg) {
            if (!commSpiEnabled)
                return;

            Object o = msg.message();

            if (!(o instanceof GridDistributedLockRequest))
                return;

            IgniteKernal g = (IgniteKernal)G.ignite(ignite.configuration().getNodeId());

            GridCacheContext<Object, Object> cacheCtx = g.internalCache(REPLICATED_ASYNC_CACHE_NAME).context();

            IgniteTxManager tm = cacheCtx.tm();

            GridCacheVersion v = ((GridCacheVersionable)o).version();

            IgniteInternalTx t = tm.tx(v);

            if (t.hasWriteKey(cacheCtx.txKey(cacheCtx.toCacheKeyObject("x1"))))
                assertEquals("Invalid tx flags: " + t, FULL_ASYNC, t.syncMode());
            else if (t.hasWriteKey(cacheCtx.txKey(cacheCtx.toCacheKeyObject("x2"))))
                assertEquals("Invalid tx flags: " + t, FULL_SYNC, t.syncMode());
            else if (t.hasWriteKey(cacheCtx.txKey(cacheCtx.toCacheKeyObject("x3"))))
                assertEquals("Invalid tx flags: " + t, FULL_ASYNC, t.syncMode());
            else if (t.hasWriteKey(cacheCtx.txKey(cacheCtx.toCacheKeyObject("x4"))))
                assertEquals("Invalid tx flags: " + t, FULL_SYNC, t.syncMode());
        }
    }
}
