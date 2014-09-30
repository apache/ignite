/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.client.balancer.*;
import org.gridgain.client.impl.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.testframework.GridTestUtils.*;

/**
 *
 */
public abstract class GridClientAbstractMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

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
        System.setProperty("CLIENTS_MODULE_PATH", U.resolveGridGainPath("os/modules/clients").getAbsolutePath());
    }

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
     * @return Whether SSL should be used.
     */
    protected abstract boolean useSsl();

    /**
     * @return SSL context factory to use if SSL is enabled.
     */
    protected abstract GridSslContextFactory sslContextFactory();

    /**
     * @return Count of iterations for sync commit test.
     */
    protected int syncCommitIterCount() {
        return 1000;
    }

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
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        c.setLocalHost(HOST);

        assert c.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        clientCfg.setRestTcpPort(REST_TCP_PORT_BASE);

        if (useSsl()) {
            clientCfg.setRestTcpSslEnabled(true);

            clientCfg.setRestTcpSslContextFactory(sslContextFactory());
        }

        c.setClientConnectionConfiguration(clientCfg);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

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
    private GridCacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setDistributionMode(NEAR_PARTITIONED);
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
    public void testSyncCommitFlagReplicated() throws Exception {
        doTestSyncCommitFlag(client.data(REPLICATED_ASYNC_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSyncCommitFlagPartitioned() throws Exception {
        doTestSyncCommitFlag(client.data(PARTITIONED_ASYNC_BACKUP_CACHE_NAME));
    }

    /**
     * Extracts array from given iterator.
     *
     * @param nodes Iterator of nodes.
     * @return Nodes array.
     */
    private GridClientNode[] toArray(Iterator<? extends GridClientNode> nodes) {
        ArrayList<GridClientNode> res = new ArrayList<>();

        while (nodes.hasNext())
            res.add(nodes.next());

        return res.toArray(new GridClientNode[res.size()]);
    }

    /**
     * Runs test on SYNC_COMMIT flag.
     *
     * @param data Client data to run test on.
     * @throws Exception If failed.
     */
    private void doTestSyncCommitFlag(final GridClientData data) throws Exception {
        final String key = "k0";

        Collection<UUID> affNodesIds = F.viewReadOnly(
            grid(0).cache(data.cacheName()).affinity().mapKeyToPrimaryAndBackups(key),
            F.node2id());

        final GridClientData dataFirst = data.pinNodes(F.first(client.compute().nodes()));

        List<GridClientNode> affNodes = new ArrayList<>();

        for (GridClientNode node : client.compute().nodes()) {
            if (affNodesIds.contains(node.nodeId()))
                affNodes.add(node);
        }

        Assert.assertFalse(affNodes.isEmpty());

        Iterator<? extends GridClientNode> it = affNodes.iterator();

        final GridClientData dataOthers = data.pinNodes(it.next(), toArray(it));

        for (int i = 0; i < syncCommitIterCount(); i++) {
            final CountDownLatch l = new CountDownLatch(1);

            final String val = "v" + i;

            GridFuture<?> f = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    l.await();

                    assertEquals(val, dataOthers.get(key));

                    return null;
                }
            }, THREAD_CNT);

            dataFirst.flagsOn(GridClientCacheFlag.SYNC_COMMIT).put(key, val);

            l.countDown();

            f.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedTaskRun() throws Exception {
        final AtomicLong cnt = new AtomicLong();

        final AtomicReference<GridClientException> err = new AtomicReference<>();

        final ConcurrentLinkedQueue<String> execQueue = new ConcurrentLinkedQueue<>();

        GridFuture<?> fut = multithreadedAsync(new Runnable() {
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

    /**
     * @throws Exception If failed.
     */
    public void test6Affinity() throws Exception {
        GridClientData cache = client.data(PARTITIONED_CACHE_NAME);
        UUID nodeId = cache.affinity("6");

        info("Affinity node: " + nodeId);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedCachePut() throws Exception {
        final AtomicLong keyCnt = new AtomicLong();

        final AtomicReference<Exception> err = new AtomicReference<>();

        final ConcurrentMap<String, T2<UUID, String>> puts = new ConcurrentHashMap<>();

        final Map<UUID, Grid> gridMap = new HashMap<>();

        for (int i = 0; i < NODES_CNT; i++) {
            Grid g = grid(i);

            gridMap.put(g.localNode().id(), g);
        }

        final Grid grid = F.first(gridMap.values());

        assertEquals(NODES_CNT, client.compute().refreshTopology(false, false).size());

        GridFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings("OverlyStrongTypeCast")
            @Override public void run() {
                try {
                    GridClientData cache = client.data(PARTITIONED_CACHE_NAME);

                    assertEquals(NODES_CNT, ((GridClientDataImpl)cache).projectionNodes().size());

                    long rawKey;

                    while ((rawKey = keyCnt.getAndIncrement()) < cachePutCount()) {
                        String key = String.valueOf(rawKey);

                        UUID nodeId = cache.affinity(key);
                        UUID srvNodeId = grid.mapKeyToNode(PARTITIONED_CACHE_NAME, key).id();

                        if (!nodeId.equals(srvNodeId)) {
                            //GridClientDataAffinity clAff =
                            //    ((GridClientConfiguration)getFieldValue(client, "cfg")).
                            //        getDataConfiguration(PARTITIONED_CACHE_NAME).getAffinity();

                            //printAffinityState(gridMap.values());
                            //info("Client affinity: " + clAff);

                            info("Got wrong client mapping [key=" + key + ", exp=" + srvNodeId +
                                ", actual=" + nodeId + "]");
                        }

                        String val = "val" + rawKey;

                        if (cache.put(key, val)) {
                            T2<UUID, String> old = puts.putIfAbsent(key, new T2<>(nodeId, val));

                            assert old == null : "Map contained entry [key=" + rawKey + ", entry=" + old + ']';
                        }
                    }
                }
                catch (Exception e) {
                    err.compareAndSet(null, e);
                }
            }
        }, THREAD_CNT, "client-cache-put");

        fut.get();

        if (err.get() != null)
            throw new Exception(err.get());

        assertEquals(cachePutCount(), puts.size());

        // Now check that all puts went to primary nodes.
        for (long i = 0; i < cachePutCount(); i++) {
            String key = String.valueOf(i);

            GridNode node = grid.mapKeyToNode(PARTITIONED_CACHE_NAME, key);

            if (!puts.get(key).get2().equals(gridMap.get(node.id()).cache(PARTITIONED_CACHE_NAME).peek(key))) {
                // printAffinityState(gridMap.values());

                failNotEquals("Node don't have value for key [nodeId=" + node.id() + ", key=" + key + "]",
                    puts.get(key).get2(), gridMap.get(node.id()).cache(PARTITIONED_CACHE_NAME).peek(key));
            }

            // Assert that client has properly determined affinity node.
            if (!node.id().equals(puts.get(key).get1())) {
                //GridClientDataAffinity clAff =
                //    ((GridClientConfiguration)getFieldValue(client, "cfg")).
                //        getDataConfiguration(PARTITIONED_CACHE_NAME).getAffinity();

                //printAffinityState(gridMap.values());
                //info("Client affinity: " + clAff);

                UUID curAffNode = client.data(PARTITIONED_CACHE_NAME).affinity(key);

                failNotEquals(
                    "Got different mappings [key=" + key + ", currId=" + curAffNode + "]",
                    node.id(), puts.get(key).get1());
            }

            // Check that no other nodes see this key.
            for (UUID id : F.view(gridMap.keySet(), F.notEqualTo(node.id())))
                assertNull("Got value in near cache.", gridMap.get(id).cache(PARTITIONED_CACHE_NAME).peek(key));
        }

        for (Grid g : gridMap.values())
            g.cache(PARTITIONED_CACHE_NAME).clearAll();
    }

    /**
     * @param grids Collection for Grids to print affinity info.
     */
    private void printAffinityState(Iterable<Grid> grids) {
        for (Grid g : grids) {
            GridAffinityAssignmentCache affCache = getFieldValue(
                ((GridKernal)g).internalCache(PARTITIONED_CACHE_NAME).context().affinity(),
                "aff");

            GridCacheAffinityFunction aff = getFieldValue(affCache, "aff");

            info("Affinity [nodeId=" + g.localNode().id() + ", affinity=" + aff + "]");
        }
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
    private static class TestTask extends GridComputeTaskSplitAdapter<Object, String> {
        /** Injected grid. */
        @GridInstanceResource
        private Grid grid;

        /** Count of tasks this job was split to. */
        private int gridSize;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg)
            throws GridException {
            Collection<GridComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            this.gridSize = gridSize;

            final String locNodeId = grid.localNode().id().toString();

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new GridComputeJobAdapter() {
                    @Override public Object execute() {
                        return new GridBiTuple<>(locNodeId, 1);
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<GridComputeJobResult> results) throws GridException {
            int sum = 0;

            String locNodeId = null;

            for (GridComputeJobResult res : results) {
                GridBiTuple<String, Integer> part = res.getData();

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
