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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * TODO: try start from client and server.
 * TODO: case when failed on client and did not failed on server and vice versa.
 */
@SuppressWarnings("unchecked")
public class IgniteDynamicCacheStartFailSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

    /** Coordinator node. */
    private Ignite crd;

    /** Coordinator node index. */
    private int crdIdx;

    /**
     * @return Number of nodes for this test.
     */
    public int nodeCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        crdIdx = 0;
        crd = startGrid(crdIdx);

        for (int i = 1; i < nodeCount(); ++i)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    public void testBrokenAffinityFunctionOnAllNodes() {
        final boolean failOnAllNodes = true;
        final int unluckyNode = 0;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = 0;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFunction(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenAffinityFunctionOnInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = 1;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFunction(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenAffinityFunctionOnNonInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = 2;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFunction(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenAffinityFunctionOnCoordinatorDiffInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = (crdIdx + 1) % nodeCount();

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFunction(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenAffinityFunctionOnCoordinator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = crdIdx;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFunction(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenAffinityFunctionWithNodeFilter() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 0;
        final int unluckyCfg = 0;
        final int numberOfCaches = 1;
        final int initiator = 0;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenAffinityFunction(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, true),
            initiator);
    }

    public void testBrokenCacheStoreOnAllNodes() {
        final boolean failOnAllNodes = true;
        final int unluckyNode = 0;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = 0;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenCacheStoreOnInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = 1;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenCacheStoreOnNonInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = 2;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenCacheStoreOnCoordinatorDiffInitiator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = (crdIdx + 1) % nodeCount();

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testBrokenCacheStoreFunctionOnCoordinator() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = crdIdx;
        final int unluckyCfg = 1;
        final int numberOfCaches = 3;
        final int initiator = crdIdx;

        testDynamicCacheStart(
            createCacheConfigsWithBrokenCacheStore(
                failOnAllNodes, unluckyNode, unluckyCfg, numberOfCaches, false),
            initiator);
    }

    public void testCreateCacheMultipleTimes() {
        final boolean failOnAllNodes = false;
        final int unluckyNode = 1;
        final int unluckyCfg = 0;
        final int numOfAttempts = 100;

        CacheConfiguration cfg = createCacheConfigsWithBrokenAffinityFunction(
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

    private List<CacheConfiguration> createCacheConfigsWithBrokenAffinityFunction(
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
                cfg.setAffinity(new BrokenAffinityFunction(failOnAllNodes, getTestGridName(unluckyNode)));

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
                cfg.setCacheStoreFactory(new BrokenStoreFactory(failOnAllNodes, getTestGridName(unluckyNode)));

            if (useFilter)
                cfg.setNodeFilter(new NodeFilter(uuid));

            cfgs.add(cfg);
        }

        return cfgs;
    }

    private void testDynamicCacheStart(final Collection<CacheConfiguration> cfgs, final int initiatorId) {
        assert initiatorId < nodeCount();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(initiatorId).getOrCreateCaches(cfgs);
                return null;
            }
        }, CacheException.class, null);

        for (CacheConfiguration cfg : cfgs)
            assertNull("initiatorId=" + initiatorId, grid(initiatorId).cache(cfg.getName()));
    }

    private static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** Cache should be created node with certain UUID. */
        public UUID uuid;

        public NodeFilter() {
        }

        public NodeFilter(UUID uuid) {
            this.uuid = uuid;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return clusterNode.id().equals(uuid);
        }
    }

    private static class BrokenAffinityFunction extends RendezvousAffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Exception should arise on all nodes. */
        private boolean exceptionOnAllNodes = false;

        /** Exception should arise on node with certain name. */
        private String gridName;

        public BrokenAffinityFunction() {
        }

        public BrokenAffinityFunction(boolean exceptionOnAllNodes, String gridName) {
            this.exceptionOnAllNodes = exceptionOnAllNodes;
            this.gridName = gridName;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            if (exceptionOnAllNodes || ignite.name().equals(gridName))
                throw new IllegalStateException("Simulated exception [locNodeId="
                    + ignite.cluster().localNode().id() + "]");
            else
                return super.assignPartitions(affCtx);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeBoolean(exceptionOnAllNodes);
            out.writeObject(gridName);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            exceptionOnAllNodes = in.readBoolean();
            gridName = (String)in.readObject();
        }
    }

    private static class BrokenStoreFactory implements Factory<CacheStore<Integer, String>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Exception should arise on all nodes. */
        boolean exceptionOnAllNodes = true;

        /** Exception should arise on node with certain name. */
        public static String gridName;

        public BrokenStoreFactory() {
        }

        public BrokenStoreFactory(boolean exceptionOnAllNodes, String gridName) {
            this.exceptionOnAllNodes = exceptionOnAllNodes;
            this.gridName = gridName;
        }

        /** {@inheritDoc} */
        @Override public CacheStore<Integer, String> create() {
            if (exceptionOnAllNodes || ignite.name().equals(gridName))
                throw new IllegalStateException("Simulated exception [locNodeId="
                    + ignite.cluster().localNode().id() + "]");
            else
                return null;
        }
    }
}
