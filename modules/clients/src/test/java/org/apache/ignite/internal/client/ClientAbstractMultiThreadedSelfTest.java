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

package org.apache.ignite.internal.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.balancer.GridClientRoundRobinBalancer;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class ClientAbstractMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Partitioned cache name. */
    protected static final String PARTITIONED_CACHE_NAME = "partitioned";

    /** Partitioned cache with async commit and backup name. */
    protected static final String PARTITIONED_ASYNC_BACKUP_CACHE_NAME = "partitioned-async-backup";

    /** Replicated cache name. */
    private static final String REPLICATED_CACHE_NAME = "replicated";

    /** Replicated cache  with async commit name. */
    private static final String REPLICATED_ASYNC_CACHE_NAME = "replicated-async";

    /** Nodes count. */
    protected static final int NODES_CNT = 5;

    /** Thread count to run tests. */
    private static final int THREAD_CNT = 20;

    /** Count of tasks to run. */
    private static final int TASK_EXECUTION_CNT = 50000;

    /** Count of cache puts in tests. */
    private static final int CACHE_PUT_CNT = 10000;

    /** Topology update frequency. */
    private static final int TOP_REFRESH_FREQ = 1000;

    /** Info messages will be printed each 5000 iterations. */
    private static final int STATISTICS_PRINT_STEP = 5000;

    /** Host. */
    public static final String HOST = "127.0.0.1";

    /** Base for tcp rest ports. */
    public static final int REST_TCP_PORT_BASE = 12345;

    static {
        System.setProperty("CLIENTS_MODULE_PATH", U.resolveIgnitePath("modules/clients").getAbsolutePath());
    }

    /** Client instance for each test. */
    protected GridClient client;

    /**
     * @return Client protocol that should be used.
     */
    protected abstract GridClientProtocol protocol();

    /**
     * @return Server address to create first connection.
     */
    protected abstract String serverAddress();

    /**
     * @return Whether SSL should be used.
     */
    protected abstract boolean useSsl();

    /**
     * @return SSL context factory to use if SSL is enabled.
     */
    protected abstract GridSslContextFactory sslContextFactory();

    /**
     * @return Topology refresh frequency interval.
     */
    protected int topologyRefreshFrequency() {
        return TOP_REFRESH_FREQ;
    }

    /**
     * @return Max connection idle time.
     */
    protected int maxConnectionIdleTime() {
        return 5000;
    }

    /**
     * @return Number of tasks that should be executed during test.
     */
    protected int taskExecutionCount() {
        return TASK_EXECUTION_CNT;
    }

    /**
     * @return Number of puts to the cache.
     */
    protected int cachePutCount() {
        return CACHE_PUT_CNT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setLocalHost(HOST);

        assert c.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setPort(REST_TCP_PORT_BASE);

        if (useSsl()) {
            clientCfg.setSslEnabled(true);

            clientCfg.setSslContextFactory(sslContextFactory());
        }

        c.setConnectorConfiguration(clientCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration(PARTITIONED_CACHE_NAME),
            cacheConfiguration(REPLICATED_CACHE_NAME), cacheConfiguration(PARTITIONED_ASYNC_BACKUP_CACHE_NAME),
            cacheConfiguration(REPLICATED_ASYNC_CACHE_NAME));

        return c;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setAffinity(new RendezvousAffinityFunction());

        cfg.setAtomicityMode(TRANSACTIONAL);

        if (cacheName == null)
            cfg.setCacheMode(LOCAL);
        else if (PARTITIONED_CACHE_NAME.equals(cacheName)) {
            cfg.setCacheMode(PARTITIONED);

            cfg.setBackups(0);
        }
        else if (PARTITIONED_ASYNC_BACKUP_CACHE_NAME.equals(cacheName)) {
            cfg.setCacheMode(PARTITIONED);

            cfg.setBackups(1);
        }
        else
            cfg.setCacheMode(REPLICATED);

        cfg.setName(cacheName);

        if (cacheName != null && !cacheName.contains("async"))
            cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = GridClientFactory.start(clientConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridClientFactory.stop(client.id(), false);

        client = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedTaskRun() throws Exception {
        final AtomicLong cnt = new AtomicLong();

        final AtomicReference<GridClientException> err = new AtomicReference<>();

        final ConcurrentLinkedQueue<String> execQueue = new ConcurrentLinkedQueue<>();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override
            public void run() {
                long processed;

                while ((processed = cnt.getAndIncrement()) < taskExecutionCount()) {
                    try {
                        if (processed > 0 && processed % STATISTICS_PRINT_STEP == 0)
                            info(">>>>>>> " + processed + " tasks finished.");

                        String res = client.compute().execute(TestTask.class.getName(), null);

                        execQueue.add(res);
                    }
                    catch (GridClientException e) {
                        err.compareAndSet(null, e);
                    }
                }
            }
        }, THREAD_CNT, "client-task-request");

        fut.get();

        if (err.get() != null)
            throw new Exception(err.get());

        assertEquals(taskExecutionCount(), execQueue.size());

        // With round-robin balancer each node must receive equal count of task requests.
        Collection<String> executionIds = new HashSet<>(execQueue);

        assertTrue(executionIds.size() == NODES_CNT);

        Map<String, AtomicInteger> statisticsMap = new HashMap<>();

        for (String id : executionIds)
            statisticsMap.put(id, new AtomicInteger());

        for (String id : execQueue)
            statisticsMap.get(id).incrementAndGet();

        info(">>>>>>> Execution statistics per node:");

        for (Map.Entry<String, AtomicInteger> e : statisticsMap.entrySet())
            info(">>>>>>> " + e.getKey() + " run " + e.getValue().get() + " tasks");
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000;
    }

    /**
     * Creates client that will try to connect to only first node in grid.
     *
     * @return Client.
     */
    private GridClientConfiguration clientConfiguration() {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setTopologyRefreshFrequency(topologyRefreshFrequency());
        cfg.setMaxConnectionIdleTime(maxConnectionIdleTime());

        cfg.setProtocol(protocol());
        cfg.setServers(Arrays.asList(serverAddress()));
        cfg.setBalancer(new GridClientRoundRobinBalancer());

        if (useSsl())
            cfg.setSslContextFactory(sslContextFactory());

        GridClientDataConfiguration loc = new GridClientDataConfiguration();

        GridClientDataConfiguration partitioned = new GridClientDataConfiguration();
        partitioned.setName(PARTITIONED_CACHE_NAME);
        partitioned.setAffinity(new GridClientPartitionAffinity());

        GridClientDataConfiguration partitionedAsyncBackup = new GridClientDataConfiguration();
        partitionedAsyncBackup.setName(PARTITIONED_ASYNC_BACKUP_CACHE_NAME);
        partitionedAsyncBackup.setAffinity(new GridClientPartitionAffinity());

        GridClientDataConfiguration replicated = new GridClientDataConfiguration();
        replicated.setName(REPLICATED_CACHE_NAME);

        GridClientDataConfiguration replicatedAsync = new GridClientDataConfiguration();
        replicatedAsync.setName(REPLICATED_ASYNC_CACHE_NAME);

        cfg.setDataConfigurations(Arrays.asList(loc, partitioned, replicated, replicatedAsync, partitionedAsyncBackup));

        return cfg;
    }

    /**
     * Test task. Returns a tuple in which first component is id of node that has split the task,
     * and second component is count of nodes that executed jobs.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Object, String> {
        /** Injected grid. */
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
                    @Override public Object execute() {
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
}