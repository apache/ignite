package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

public class IgniteAbstractDynamicCacheStartFailTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

    /** */
    private static final String CLIENT_GRID_NAME = "client";

    /** Coordinator node index. */
    private int crdIdx = 0;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testBrokenAffinityFunStartOnServerFailedOnClient() throws Exception {
        final String clientName = CLIENT_GRID_NAME + "testBrokenAffinityFunStartOnServerFailedOnClient";

        IgniteConfiguration clientCfg = getConfiguration(clientName);

        clientCfg.setClientMode(true);

        Ignite client = startGrid(clientName, clientCfg);

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
    public void testBrokenAffinityFunStartOnServerFailedOnServer() throws Exception {
        final String clientName = CLIENT_GRID_NAME + "testBrokenAffinityFunStartOnServerFailedOnServer";

        IgniteConfiguration clientCfg = getConfiguration(clientName);

        clientCfg.setClientMode(true);

        Ignite client = startGrid(clientName, clientCfg);

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
//    @Ignore
//    @IgniteIgnore(value = "", forceFailure = true)
//    public void testBrokenAffinityFunStartOnClientFailOnClient() throws Exception {
//        final String clientName = CLIENT_GRID_NAME + "testBrokenAffinityFunStartOnClientFailOnClient";
//
//        IgniteConfiguration clientCfg = getConfiguration(clientName);
//
//        clientCfg.setClientMode(true);
//
//        Ignite client = startGrid(clientName, clientCfg);
//
//        CacheConfiguration cfg = new CacheConfiguration();
//
//        cfg.setName(DYNAMIC_CACHE_NAME + "-client-1");
//
//        cfg.setAffinity(new BrokenAffinityFunction(false, clientName));
//
//        try {
//            IgniteCache cache = client.getOrCreateCache(cfg);
//
//            fail("Expected exception was not thrown.");
//        }
//        catch (CacheException e) {
//        }
//
//        stopGrid(clientName);
//    }

    /**
     * @throws Exception If failed.
     */
    public void testBrokenAffinityFunStartOnClientFailOnServer() throws Exception {
        final String clientName = CLIENT_GRID_NAME + "testBrokenAffinityFunStartOnClientFailOnServer";

        IgniteConfiguration clientCfg = getConfiguration(clientName);

        clientCfg.setClientMode(true);

        Ignite client = startGrid(clientName, clientCfg);

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
     * Tests cache start with broken cache store that throws an exception on non-initiator node.
     */
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
     *  Tests cache start with broken cache store that throws an exception on initiator node.
     */
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
     *  Tests cache start with broken cache store that throws an exception on coordinator node.
     */
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
     *  Tests multiple creation of cache with broken affinity function.
     */
    public void testCreateCacheMultipleTimes() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 0;
        final int numOfAttempts = 100;

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

    private List<CacheConfiguration> createCacheConfigsWithBrokenAffinityFun(
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
            CacheConfiguration cfg = new CacheConfiguration();

            cfg.setName(DYNAMIC_CACHE_NAME + "-" + i);

            if (i == unluckyCfg)
                cfg.setAffinity(new BrokenAffinityFunction(failOnAllNodes, getTestIgniteInstanceName(unluckyNode)));

            if (useFilter)
                cfg.setNodeFilter(new NodeFilter(uuid));

            cfgs.add(cfg);
        }

        return cfgs;
    }

    private Collection<CacheConfiguration> createCacheConfigsWithBrokenCacheStore(
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

    private void testDynamicCacheStart(final Collection<CacheConfiguration> cfgs, final int initiatorId) {
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

    /**
     * Filter specifying on which node the cache should be started.
     */
    private static class NodeFilter implements IgnitePredicate<ClusterNode> {
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
     * Factory that throws an exception is got created.
     */
    private static class BrokenAffinityFunction extends RendezvousAffinityFunction {
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
         * Default constructor.
         */
        public BrokenAffinityFunction() {
            // No-op.
        }

        /**
         * @param eOnAllNodes {@code True} if exception should be thrown on all nodes.
         * @param gridName Exception should arise on node with certain name.
         */
        public BrokenAffinityFunction(boolean eOnAllNodes, String gridName) {
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
    private static class BrokenStoreFactory implements Factory<CacheStore<Integer, String>> {
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
}
