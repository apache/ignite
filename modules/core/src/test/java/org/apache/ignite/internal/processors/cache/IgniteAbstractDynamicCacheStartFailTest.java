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

package org.apache.ignite.internal.processors.cache;

import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests the recovery after a dynamic cache start failure.
 */
public abstract class IgniteAbstractDynamicCacheStartFailTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

    /** */
    private static final String CLIENT_GRID_NAME = "client";

    /** */
    protected static final String EXISTING_CACHE_NAME = "existing-cache";

    /** */
    private static final int PARTITION_COUNT = 16;

    /** Failure MBean server. */
    private static FailureMBeanServer mbSrv;

    /** Coordinator node index. */
    private int crdIdx = 0;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * Returns {@code true} if persistence is enabled.
     *
     * @return {@code true} if persistence is enabled.
     */
    protected boolean persistenceEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBrokenAffinityFunStartOnServerFailedOnClient() throws Exception {
        final String clientName = CLIENT_GRID_NAME + "testBrokenAffinityFunStartOnServerFailedOnClient";

        Ignite client = startClientGrid(clientName, getConfiguration(clientName));

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(DYNAMIC_CACHE_NAME + "-server-1");
        cfg.setAffinity(new BrokenAffinityFunction(false, clientName));

        try {
            IgniteCache cache = ignite(0).getOrCreateCache(cfg);
        }
        catch (CacheException e) {
            fail("Exception should not be thrown.");
        }

        stopGrid(clientName);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBrokenAffinityFunStartOnServerFailedOnServer() throws Exception {
        final String clientName = CLIENT_GRID_NAME + "testBrokenAffinityFunStartOnServerFailedOnServer";

        Ignite client = startClientGrid(clientName, getConfiguration(clientName));

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(DYNAMIC_CACHE_NAME + "-server-2");
        cfg.setAffinity(new BrokenAffinityFunction(false, getTestIgniteInstanceName(0)));

        try {
            IgniteCache cache = ignite(0).getOrCreateCache(cfg);

            fail("Expected exception was not thrown.");
        }
        catch (CacheException e) {
        }

        stopGrid(clientName);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBrokenAffinityFunStartOnClientFailOnServer() throws Exception {
        final String clientName = CLIENT_GRID_NAME + "testBrokenAffinityFunStartOnClientFailOnServer";

        Ignite client = startClientGrid(clientName, getConfiguration(clientName));

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(DYNAMIC_CACHE_NAME + "-client-2");
        cfg.setAffinity(new BrokenAffinityFunction(false, getTestIgniteInstanceName(0)));

        try {
            IgniteCache cache = client.getOrCreateCache(cfg);

            fail("Expected exception was not thrown.");
        }
        catch (CacheException e) {
        }

        stopGrid(clientName);
    }

    /**
     * Test cache start with broken affinity function that throws an exception on all nodes.
     */
    @Test
    public void testBrokenAffinityFunOnAllNodes() {
        final boolean failOnAllNodes = true;
        final int unluckyNode = 0;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 0;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFun(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Test cache start with broken affinity function that throws an exception on initiator node.
     */
    @Test
    public void testBrokenAffinityFunOnInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 1;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFun(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Test cache start with broken affinity function that throws an exception on non-initiator node.
     */
    @Test
    public void testBrokenAffinityFunOnNonInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 2;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFun(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Test cache start with broken affinity function that throws an exception on coordinator node.
     */
    @Test
    public void testBrokenAffinityFunOnCoordinatorDiffInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = (crdIdx + 1) % gridCount();

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFun(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Test cache start with broken affinity function that throws an exception on initiator node.
     */
    @Test
    public void testBrokenAffinityFunOnCoordinator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = crdIdx;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFun(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Tests cache start with node filter and broken affinity function that throws an exception on initiator node.
     */
    @Test
    public void testBrokenAffinityFunWithNodeFilter() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 0;
        final int unluckyCfg = 0;
        final int numOfCaches = 1;
        final int initiator = 0;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFun(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, true),
            initiator);
    }

    /**
     * Tests cache start with broken cache store that throws an exception on all nodes.
     */
    @Test
    public void testBrokenCacheStoreOnAllNodes() {
        final boolean failOnAllNodes = true;
        final int unluckyNode = 0;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 0;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Tests cache start with broken cache store that throws an exception on initiator node.
     */
    @Test
    public void testBrokenCacheStoreOnInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 1;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Tests cache start that throws an Ignite checked exception on initiator node.
     */
    @Test
    public void testThrowsIgniteCheckedExceptionOnInitiator() {
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 1;

        testDynamicCacheStart(
            createCacheConfigsWithFailureMbServer(unluckyNode, unluckyCfg, numOfCaches),
            initiator);
    }

    /**
     * Tests cache start with broken cache store that throws an exception on non-initiator node.
     */
    @Test
    public void testBrokenCacheStoreOnNonInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 2;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     * Tests cache start that throws an Ignite checked exception on non-initiator node.
     */
    @Test
    public void testThrowsIgniteCheckedExceptionOnNonInitiator() {
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = 2;

        testDynamicCacheStart(
            createCacheConfigsWithFailureMbServer(unluckyNode, unluckyCfg, numOfCaches),
            initiator);
    }

    /**
     *  Tests cache start with broken cache store that throws an exception on initiator node.
     */
    @Test
    public void testBrokenCacheStoreOnCoordinatorDiffInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = (crdIdx + 1) % gridCount();

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     *  Tests cache start that throws an Ignite checked exception on coordinator node
     *  that doesn't initiator node.
     */
    @Test
    public void testThrowsIgniteCheckedExceptionOnCoordinatorDiffInitiator() {
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = (crdIdx + 1) % gridCount();

        testDynamicCacheStart(
            createCacheConfigsWithFailureMbServer(unluckyNode, unluckyCfg, numOfCaches),
            initiator);
    }

    /**
     *  Tests cache start with broken cache store that throws an exception on coordinator node.
     */
    @Test
    public void testBrokenCacheStoreFunOnCoordinator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = crdIdx;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numOfCaches, false),
            initiator);
    }

    /**
     *  Tests cache start that throws an Ignite checked exception on coordinator node.
     */
    @Test
    public void testThrowsIgniteCheckedExceptionOnCoordinator() {
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numOfCaches = 3;
        final int initiator = crdIdx;

        testDynamicCacheStart(
            createCacheConfigsWithFailureMbServer(unluckyNode, unluckyCfg, numOfCaches),
            initiator);
    }

    /**
     *  Tests multiple creation of cache with broken affinity function.
     */
    @Test
    public void testCreateCacheMultipleTimes() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 0;
        final int numOfAttempts = 15;

        CacheConfiguration cfg = createCacheConfigsWithBrokenAffinityFun(
            failOnAllNodes, unluckyNode, unluckyCfg, 1, false).get(0);

        for (int i = 0; i < numOfAttempts; ++i) {
            try {
                IgniteCache cache = ignite(0).getOrCreateCache(cfg);

                fail("Expected exception was not thrown");
            }
            catch (CacheException e) {
            }
        }
    }

    /**
     * Tests that a cache with the same name can be started after failure if cache configuration is corrected.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testCacheStartAfterFailure() throws Exception {
        CacheConfiguration cfg = createCacheConfigsWithBrokenAffinityFun(
            false, 1, 0, 1, false).get(0);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(0).getOrCreateCache(cfg);
                return null;
            }
        }, CacheException.class, null);

        // Correct the cache configuration. Default constructor creates a good affinity function.
        cfg.setAffinity(new BrokenAffinityFunction());

        checkCacheOperations(grid(0).getOrCreateCache(cfg));
    }

    /**
     * Tests that other cache (existed before the failed start) is still operable after the failure.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testExistingCacheAfterFailure() throws Exception {
        IgniteCache<Integer, Value> cache = grid(0).getOrCreateCache(createCacheConfiguration(EXISTING_CACHE_NAME));

        CacheConfiguration cfg = createCacheConfigsWithBrokenAffinityFun(
            false, 1, 0, 1, false).get(0);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(0).getOrCreateCache(cfg);
                return null;
            }
        }, CacheException.class, null);

        checkCacheOperations(cache);
    }

    /**
     * Tests that other cache works as expected after the failure and further topology changes.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testTopologyChangesAfterFailure() throws Exception {
        final String clientName = "testTopologyChangesAfterFailure";

        IgniteCache<Integer, Value> cache = grid(0).getOrCreateCache(createCacheConfiguration(EXISTING_CACHE_NAME));

        checkCacheOperations(cache);

        CacheConfiguration cfg = createCacheConfigsWithBrokenAffinityFun(
            false, 0, 0, 1, false).get(0);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(0).getOrCreateCache(cfg);
                return null;
            }
        }, CacheException.class, null);

        awaitPartitionMapExchange();

        checkCacheOperations(cache);

        // Start a new server node and check cache operations.
        Ignite serverNode = startGrid(gridCount() + 1);

        if (persistenceEnabled()) {
            // Start a new client node to perform baseline change.
            // TODO: This change is workaround:
            // Sometimes problem with caches configuration deserialization from test thread arises.
            final String clientName1 = "baseline-changer";

            Ignite clientNode = startClientGrid(clientName1, getConfiguration(clientName1));

            clientNode.cluster().baselineAutoAdjustEnabled(false);

            List<BaselineNode> baseline = new ArrayList<>(grid(0).cluster().currentBaselineTopology());

            baseline.add(serverNode.cluster().localNode());

            clientNode.cluster().setBaselineTopology(baseline);
        }

        awaitPartitionMapExchange();

        checkCacheOperations(serverNode.cache(EXISTING_CACHE_NAME));

        // Start a new client node and check cache operations.
        Ignite clientNode = startClientGrid(clientName, getConfiguration(clientName));

        checkCacheOperations(clientNode.cache(EXISTING_CACHE_NAME));
    }

    @Test
    public void testConcurrentClientNodeJoins() throws Exception {
        final int clientCnt = 3;
        final int numberOfAttempts = 5;

        IgniteCache<Integer, Value> cache = grid(0).getOrCreateCache(createCacheConfiguration(EXISTING_CACHE_NAME));

        final AtomicInteger attemptCnt = new AtomicInteger();
        final CountDownLatch stopLatch = new CountDownLatch(clientCnt);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                String clientName = Thread.currentThread().getName();

                try {
                    for (int i = 0; i < numberOfAttempts; ++i) {
                        int uniqueCnt = attemptCnt.getAndIncrement();

                        final Ignite clientNode = startClientGrid(clientName, getConfiguration(clientName + uniqueCnt));

                        CacheConfiguration cfg = new CacheConfiguration();

                        cfg.setName(clientName + uniqueCnt);

                        String instanceName = getTestIgniteInstanceName(uniqueCnt % gridCount());

                        cfg.setAffinity(new BrokenAffinityFunction(false, instanceName));

                        GridTestUtils.assertThrows(log, new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                clientNode.getOrCreateCache(cfg);
                                return null;
                            }
                        }, CacheException.class, null);

                        stopGrid(clientName, true);
                    }
                }
                catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                }
                finally {
                    stopLatch.countDown();
                }

                return null;
            }
        }, clientCnt, "start-client-thread");

        stopLatch.await();

        assertEquals(numberOfAttempts * clientCnt, attemptCnt.get());

        checkCacheOperations(cache);
    }

    protected void testDynamicCacheStart(final Collection<CacheConfiguration> cfgs, final int initiatorId) {
        assert initiatorId < gridCount();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(initiatorId).getOrCreateCaches(cfgs);
                return null;
            }
        }, CacheException.class, null);

        for (CacheConfiguration cfg: cfgs) {
            IgniteCache cache = grid(initiatorId).cache(cfg.getName());

            assertNull(cache);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration res = super.getConfiguration(igniteInstanceName);

        if (mbSrv == null)
            mbSrv = new FailureMBeanServer(res.getMBeanServer());

        res.setMBeanServer(mbSrv);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        mbSrv.clear();

        for (String cacheName : grid(0).cacheNames()) {
            if (!(EXISTING_CACHE_NAME.equals(cacheName) || DEFAULT_CACHE_NAME.equals(cacheName)))
                grid(0).cache(cacheName).destroy();
        }
    }

    /**
     * Creates new cache configuration with the given name.
     *
     * @param cacheName Cache name.
     * @return New cache configuration.
     */
    protected CacheConfiguration createCacheConfiguration(String cacheName) {
        CacheConfiguration cfg = new CacheConfiguration()
            .setName(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new BrokenAffinityFunction());

        return cfg;
    }

    /**
     * Create list of cache configurations.
     *
     * @param failOnAllNodes {@code true} if affinity function should be broken on all nodes.
     * @param unluckyNode Node, where exception is raised.
     * @param unluckyCfg Unlucky cache configuration number.
     * @param cacheNum Number of caches.
     * @param useFilter {@code true} if NodeFilter should be used.
     *
     * @return List of cache configurations.
     */
    protected List<CacheConfiguration> createCacheConfigsWithBrokenAffinityFun(
        boolean failOnAllNodes,
        int unluckyNode,
        final int unluckyCfg,
        int cacheNum,
        boolean useFilter
    ) {
        assert unluckyCfg >= 0 && unluckyCfg < cacheNum;

        final UUID uuid = ignite(unluckyNode).cluster().localNode().id();

        List<CacheConfiguration> cfgs = new ArrayList<>();

        for (int i = 0; i < cacheNum; ++i) {
            CacheConfiguration cfg = createCacheConfiguration(DYNAMIC_CACHE_NAME + "-" + i);

            if (i == unluckyCfg)
                cfg.setAffinity(new BrokenAffinityFunction(failOnAllNodes, getTestIgniteInstanceName(unluckyNode)));

            if (useFilter)
                cfg.setNodeFilter(new NodeFilter(uuid));

            cfgs.add(cfg);
        }

        return cfgs;
    }

    /**
     * Create list of cache configurations.
     *
     * @param failOnAllNodes {@code true} if cache store should be broken on all nodes.
     * @param unluckyNode Node, where exception is raised.
     * @param unluckyCfg Unlucky cache configuration number.
     * @param cacheNum Number of caches.
     * @param useFilter {@code true} if NodeFilter should be used.
     *
     * @return List of cache configurations.
     */
    protected List<CacheConfiguration> createCacheConfigsWithBrokenCacheStore(
        boolean failOnAllNodes,
        int unluckyNode,
        int unluckyCfg,
        int cacheNum,
        boolean useFilter
    ) {
        assert unluckyCfg >= 0 && unluckyCfg < cacheNum;

        final UUID uuid = ignite(unluckyNode).cluster().localNode().id();

        List<CacheConfiguration> cfgs = new ArrayList<>();

        for (int i = 0; i < cacheNum; ++i) {
            CacheConfiguration cfg = new CacheConfiguration();

            cfg.setName(DYNAMIC_CACHE_NAME + "-" + i);

            if (i == unluckyCfg)
                cfg.setCacheStoreFactory(new BrokenStoreFactory(failOnAllNodes, getTestIgniteInstanceName(unluckyNode)));

            if (useFilter)
                cfg.setNodeFilter(new NodeFilter(uuid));

            cfgs.add(cfg);
        }

        return cfgs;
    }

    /**
     * Create list of cache configurations.
     *
     * @param unluckyNode Node, where exception is raised.
     * @param unluckyCfg Unlucky cache configuration number.
     * @param cacheNum Number of caches.
     *
     * @return List of cache configurations.
     */
    private List<CacheConfiguration> createCacheConfigsWithFailureMbServer(
        int unluckyNode,
        int unluckyCfg,
        int cacheNum
    ) {
        assert unluckyCfg >= 0 && unluckyCfg < cacheNum;

        List<CacheConfiguration> cfgs = new ArrayList<>();

        for (int i = 0; i < cacheNum; ++i) {
            CacheConfiguration cfg = new CacheConfiguration();

            String cacheName = DYNAMIC_CACHE_NAME + "-" + i;

            cfg.setName(cacheName);

            if (i == unluckyCfg)
                mbSrv.cache(cacheName);

            cfgs.add(cfg);
        }

        mbSrv.node(getTestIgniteInstanceName(unluckyNode));

        return cfgs;
    }

    /**
     * Test the basic cache operations.
     *
     * @param cache Cache.
     * @throws Exception If test failed.
     */
    protected void checkCacheOperations(IgniteCache<Integer, Value> cache) throws Exception {
        int cnt = 1000;

        // Check cache operations.
        for (int i = 0; i < cnt; ++i)
            cache.put(i, new Value(i));

        for (int i = 0; i < cnt; ++i) {
            Value v = cache.get(i);

            assertNotNull(v);
            assertEquals(i, v.getValue());
        }

        // Check Data Streamer functionality.
        try (IgniteDataStreamer<Integer, Value> streamer = grid(0).dataStreamer(cache.getName())) {
            for (int i = 0; i < 10_000; ++i)
                streamer.addData(i, new Value(i));
        }
    }

    /**
     *
     */
    public static class Value {
        @QuerySqlField
        private final int fieldVal;

        public Value(int fieldVal) {
            this.fieldVal = fieldVal;
        }

        public int getValue() {
            return fieldVal;
        }
    }

    /**
     * Filter specifying on which node the cache should be started.
     */
    public static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** Cache should be created node with certain UUID. */
        public UUID uuid;

        /**
         * @param uuid node ID.
         */
        public NodeFilter(UUID uuid) {
            this.uuid = uuid;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return clusterNode.id().equals(uuid);
        }
    }

    /**
     * Affinity function that throws an exception when affinity nodes are calculated on the given node.
     */
    public static class BrokenAffinityFunction extends RendezvousAffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Exception should arise on all nodes. */
        private boolean eOnAllNodes = false;

        /** Exception should arise on node with certain name. */
        private String gridName;

        /**
         * Constructs a good affinity function.
         */
        public BrokenAffinityFunction() {
            super(false, PARTITION_COUNT);
            // No-op.
        }

        /**
         * @param eOnAllNodes {@code True} if exception should be thrown on all nodes.
         * @param gridName Exception should arise on node with certain name.
         */
        public BrokenAffinityFunction(boolean eOnAllNodes, String gridName) {
            super(false, PARTITION_COUNT);

            this.eOnAllNodes = eOnAllNodes;
            this.gridName = gridName;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            if (eOnAllNodes || ignite.name().equals(gridName))
                throw new IllegalStateException("Simulated exception [locNodeId="
                    + ignite.cluster().localNode().id() + "]");
            else
                return super.assignPartitions(affCtx);
        }
    }

    /**
     * Factory that throws an exception is got created.
     */
    public static class BrokenStoreFactory implements Factory<CacheStore<Integer, String>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Exception should arise on all nodes. */
        boolean eOnAllNodes = true;

        /** Exception should arise on node with certain name. */
        public static String gridName;

        /**
         * @param eOnAllNodes {@code True} if exception should be thrown on all nodes.
         * @param gridName Exception should arise on node with certain name.
         */
        public BrokenStoreFactory(boolean eOnAllNodes, String gridName) {
            this.eOnAllNodes = eOnAllNodes;

            this.gridName = gridName;
        }

        /** {@inheritDoc} */
        @Override public CacheStore<Integer, String> create() {
            if (eOnAllNodes || ignite.name().equals(gridName))
                throw new IllegalStateException("Simulated exception [locNodeId="
                    + ignite.cluster().localNode().id() + "]");
            else
                return null;
        }
    }

    /** Failure MBean server. */
    private class FailureMBeanServer implements MBeanServer {
        /** */
        private final MBeanServer origin;

        /** Set of caches that must be failure. */
        private final Set<String> caches = new HashSet<>();

        /** Set of nodes that must be failure. */
        private final Set<String> nodes = new HashSet<>();

        /** */
        private FailureMBeanServer(MBeanServer origin) {
            this.origin = origin;
        }

        /** Add cache name to failure set. */
        void cache(String cache) {
            caches.add('\"' + cache + '\"');
        }

        /** Add node name to failure set. */
        void node(String node) {
            nodes.add(node);
        }

        /** Clear failure set of caches and set of nodes. */
        void clear() {
            caches.clear();
            nodes.clear();
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance registerMBean(Object obj, ObjectName name)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
            String node = name.getKeyProperty("igniteInstanceName");

            if (nodes.contains(node) && caches.contains(name.getKeyProperty("group")))
                throw new MBeanRegistrationException(new Exception("Simulate exception [node=" + node + ']'));

            return origin.registerMBean(obj, name);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanException, NotCompliantMBeanException {
            return origin.createMBean(clsName, name);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name, ObjectName ldrName)
            throws ReflectionException, InstanceAlreadyExistsException,
            MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
            return origin.createMBean(clsName, name, ldrName);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name, Object[] params,
            String[] signature) throws ReflectionException, InstanceAlreadyExistsException,
            MBeanException, NotCompliantMBeanException {
            return origin.createMBean(clsName, name, params, signature);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance createMBean(String clsName, ObjectName name, ObjectName ldrName,
            Object[] params, String[] signature) throws ReflectionException, InstanceAlreadyExistsException,
            MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
            return origin.createMBean(clsName, name, ldrName, params, signature);
        }

        /** {@inheritDoc} */
        @Override public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
            origin.unregisterMBean(name);
        }

        /** {@inheritDoc} */
        @Override public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
            return origin.getObjectInstance(name);
        }

        /** {@inheritDoc} */
        @Override public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp qry) {
            return origin.queryMBeans(name, qry);
        }

        /** {@inheritDoc} */
        @Override public Set<ObjectName> queryNames(ObjectName name, QueryExp qry) {
            return origin.queryNames(name, qry);
        }

        /** {@inheritDoc} */
        @Override public boolean isRegistered(ObjectName name) {
            return origin.isRegistered(name);
        }

        /** {@inheritDoc} */
        @Override public Integer getMBeanCount() {
            return origin.getMBeanCount();
        }

        /** {@inheritDoc} */
        @Override public Object getAttribute(ObjectName name, String attribute)
            throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
            return origin.getAttribute(name, attribute);
        }

        /** {@inheritDoc} */
        @Override public AttributeList getAttributes(ObjectName name,
            String[] attrs) throws InstanceNotFoundException, ReflectionException {
            return origin.getAttributes(name, attrs);
        }

        /** {@inheritDoc} */
        @Override public void setAttribute(ObjectName name,
            Attribute attribute) throws InstanceNotFoundException, AttributeNotFoundException,
            InvalidAttributeValueException, MBeanException, ReflectionException {
            origin.setAttribute(name, attribute);
        }

        /** {@inheritDoc} */
        @Override public AttributeList setAttributes(ObjectName name,
            AttributeList attrs) throws InstanceNotFoundException, ReflectionException {
            return origin.setAttributes(name, attrs);
        }

        /** {@inheritDoc} */
        @Override public Object invoke(ObjectName name, String operationName, Object[] params,
            String[] signature) throws InstanceNotFoundException, MBeanException, ReflectionException {
            return origin.invoke(name, operationName, params, signature);
        }

        /** {@inheritDoc} */
        @Override public String getDefaultDomain() {
            return origin.getDefaultDomain();
        }

        /** {@inheritDoc} */
        @Override public String[] getDomains() {
            return origin.getDomains();
        }

        /** {@inheritDoc} */
        @Override public void addNotificationListener(ObjectName name, NotificationListener lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException {
            origin.addNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public void addNotificationListener(ObjectName name, ObjectName lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException {
            origin.addNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name,
            ObjectName lsnr) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name, ObjectName lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name,
            NotificationListener lsnr) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr);
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ObjectName name, NotificationListener lsnr,
            NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
            origin.removeNotificationListener(name, lsnr, filter, handback);
        }

        /** {@inheritDoc} */
        @Override public MBeanInfo getMBeanInfo(
            ObjectName name) throws InstanceNotFoundException, IntrospectionException, ReflectionException {
            return origin.getMBeanInfo(name);
        }

        /** {@inheritDoc} */
        @Override public boolean isInstanceOf(ObjectName name, String clsName) throws InstanceNotFoundException {
            return origin.isInstanceOf(name, clsName);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName) throws ReflectionException, MBeanException {
            return origin.instantiate(clsName);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName,
            ObjectName ldrName) throws ReflectionException, MBeanException, InstanceNotFoundException {
            return origin.instantiate(clsName, ldrName);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName, Object[] params,
            String[] signature) throws ReflectionException, MBeanException {
            return origin.instantiate(clsName, params, signature);
        }

        /** {@inheritDoc} */
        @Override public Object instantiate(String clsName, ObjectName ldrName, Object[] params,
            String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
            return origin.instantiate(clsName, ldrName, params, signature);
        }

        /** {@inheritDoc} */
        @Override @Deprecated public ObjectInputStream deserialize(ObjectName name, byte[] data)
            throws OperationsException {
            return origin.deserialize(name, data);
        }

        /** {@inheritDoc} */
        @Override @Deprecated public ObjectInputStream deserialize(String clsName, byte[] data)
            throws OperationsException, ReflectionException {
            return origin.deserialize(clsName, data);
        }

        /** {@inheritDoc} */
        @Override @Deprecated public ObjectInputStream deserialize(String clsName, ObjectName ldrName, byte[] data)
            throws OperationsException, ReflectionException {
            return origin.deserialize(clsName, ldrName, data);
        }

        /** {@inheritDoc} */
        @Override public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
            return origin.getClassLoaderFor(mbeanName);
        }

        /** {@inheritDoc} */
        @Override public ClassLoader getClassLoader(ObjectName ldrName) throws InstanceNotFoundException {
            return origin.getClassLoader(ldrName);
        }

        /** {@inheritDoc} */
        @Override public ClassLoaderRepository getClassLoaderRepository() {
            return origin.getClassLoaderRepository();
        }
    }
}
