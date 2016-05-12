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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.lang.IgniteProductVersion.fromString;

/**
 *  GridDiscovery self test.
 */
public class GridDiscoverySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static Ignite ignite;

    /** Nodes count. */
    private static final int NODES_CNT = 5;

    /** Maximum timeout when remote nodes join/left the topology */
    private static final int MAX_TIMEOUT_IN_MINS = 5;

    /** */
    public GridDiscoverySelfTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        //cacheCfg.setName(null);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = G.ignite(getTestGridName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemoteNodes() throws Exception {
        Collection<ClusterNode> nodes = ignite.cluster().forRemotes().nodes();

        printNodes(nodes);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllNodes() throws Exception {
        Collection<ClusterNode> nodes = ignite.cluster().nodes();

        printNodes(nodes);

        assert nodes != null;
        assert !nodes.isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTopologyHash() throws Exception {
        int hashCnt = 5000;

        Random rand = new Random();

        Collection<Long> hashes = new HashSet<>(hashCnt, 1.0f);

        for (int i = 0; i < hashCnt; i++) {
            // Max topology of 10 nodes.
            int size = rand.nextInt(10) + 1;

            Collection<ClusterNode> nodes = new ArrayList<>(size);

            for (int j = 0; j < size; j++)
                nodes.add(new GridDiscoveryTestNode());

            @SuppressWarnings("deprecation")
            long hash = ((IgniteKernal) ignite).context().discovery().topologyHash(nodes);

            boolean isHashed = hashes.add(hash);

            assert isHashed : "Duplicate hash [hash=" + hash + ", topSize=" + size + ", iteration=" + i + ']';
        }

        info("No duplicates found among '" + hashCnt + "' hashes.");
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    public void testGetLocalNode() throws Exception {
        ClusterNode node = ignite.cluster().localNode();

        assert node != null;

        Collection<ClusterNode> nodes = ignite.cluster().nodes();

        assert nodes != null;
        assert nodes.contains(node);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPingNode() throws Exception {
        ClusterNode node = ignite.cluster().localNode();

        assert node != null;

        boolean pingRes = ignite.cluster().pingNode(node.id());

        assert pingRes : "Failed to ping local node.";
    }

    /**
     * @throws Exception If failed.
     */
    public void testDiscoveryListener() throws Exception {
        ClusterNode node = ignite.cluster().localNode();

        assert node != null;

        final AtomicInteger cnt = new AtomicInteger();

        /** Joined nodes counter. */
        final CountDownLatch joinedCnt = new CountDownLatch(NODES_CNT);

        /** Left nodes counter. */
        final CountDownLatch leftCnt = new CountDownLatch(NODES_CNT);

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (EVT_NODE_JOINED == evt.type()) {
                    cnt.incrementAndGet();

                    joinedCnt.countDown();
                }
                else if (EVT_NODE_LEFT == evt.type()) {
                    int i = cnt.decrementAndGet();

                    assert i >= 0;

                    leftCnt.countDown();
                }
                else
                    assert false;

                return true;
            }
        };

        ignite.events().localListen(lsnr, EVT_NODE_LEFT, EVT_NODE_JOINED);

        try {
            for (int i = 0; i < NODES_CNT; i++)
                startGrid(i);

            joinedCnt.await(MAX_TIMEOUT_IN_MINS, MINUTES);

            assert cnt.get() == NODES_CNT;

            for (int i = 0; i < NODES_CNT; i++)
                stopGrid(i);

            leftCnt.await(MAX_TIMEOUT_IN_MINS, MINUTES);

            assert cnt.get() == 0;

            ignite.events().stopLocalListen(lsnr);

            assert cnt.get() == 0;
        }
        finally {
            for (int i = 0; i < NODES_CNT; i++)
                stopAndCancelGrid(i);
        }
    }

    /**
     * Test cache nodes resolved correctly from topology history.
     *
     * @throws Exception In case of any exception.
     */
    public void testCacheNodes() throws Exception {
        // Validate only original node is available.
        GridDiscoveryManager discoMgr = ((IgniteKernal) ignite).context().discovery();

        Collection<ClusterNode> nodes = discoMgr.allNodes();

        assert nodes.size() == 1 : "Expects only original node is available: " + nodes;

        final long topVer0 = discoMgr.topologyVersion();

        assert topVer0 > 0 : "Unexpected initial topology version: " + topVer0;

        List<UUID> uuids = new ArrayList<>(NODES_CNT);

        UUID locId = ignite.cluster().localNode().id();

        try {
            // Start nodes.
            for (int i = 0; i < NODES_CNT; i++)
                uuids.add(startGrid(i).cluster().localNode().id());

            // Stop nodes.
            for (int i = 0; i < NODES_CNT; i++)
                stopGrid(i);

            final long topVer = discoMgr.topologyVersion();

            assert topVer == topVer0 + NODES_CNT * 2 : "Unexpected topology version: " + topVer;

            for (long ver = topVer0; ver <= topVer; ver++) {
                Collection<UUID> exp = new ArrayList<>();

                exp.add(locId);

                for (int i = 0; i < NODES_CNT && i < ver - topVer0; i++)
                    exp.add(uuids.get(i));

                for (int i = 0; i < ver - topVer0 - NODES_CNT; i++)
                    exp.remove(uuids.get(i));

                // Cache nodes by topology version (e.g. NODE_CNT == 3).
                //            0 1 2 3 (node id)
                // 1 (topVer) +       - only local node
                // 2          + +
                // 3          + + +
                // 4          + + + +
                // 5          +   + +
                // 6          +     +
                // 7          +       - only local node

                Collection<ClusterNode> cacheNodes = discoMgr.cacheNodes(null, new AffinityTopologyVersion(ver));

                Collection<UUID> act = new ArrayList<>(F.viewReadOnly(cacheNodes, new C1<ClusterNode, UUID>() {
                    @Override public UUID apply(ClusterNode n) {
                        return n.id();
                    }
                }));

                assertEquals("Expects correct cache nodes for topology version: " + ver, exp, act);
            }
        }
        finally {
            for (int i = 0; i < NODES_CNT; i++)
                stopAndCancelGrid(i);
        }
    }

    /**
     * @param nodes Nodes.
     */
    private void printNodes(Collection<ClusterNode> nodes) {
        StringBuilder buf = new StringBuilder();

        if (nodes != null && !nodes.isEmpty()) {
            buf.append("Found nodes [nodes={");

            int i = 0;

            for (Iterator<ClusterNode> iter = nodes.iterator(); iter.hasNext(); i++) {
                ClusterNode node = iter.next();

                buf.append(node.id());

                if (i + 1 != nodes.size())
                    buf.append(", ");
            }

            buf.append("}]");
        }
        else
            buf.append("Found no nodes.");

        if (log().isDebugEnabled())
            log().debug(buf.toString());
    }

    /**
     *
     */
    private static class GridDiscoveryTestNode extends GridMetadataAwareAdapter implements ClusterNode {
        /** */
        private static AtomicInteger consistentIdCtr = new AtomicInteger();

        /** */
        private UUID nodeId = UUID.randomUUID();

        /** */
        private Object consistentId = consistentIdCtr.incrementAndGet();

        /** {@inheritDoc} */
        @Override public long order() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public IgniteProductVersion version() {
            return fromString("99.99.99");
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public Object consistentId() {
            return consistentId;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Nullable @Override public <T> T attribute(String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public ClusterMetrics metrics() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<String, Object> attributes() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> addresses() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isLocal() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isDaemon() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isClient() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> hostNames() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return F.eqNodes(this, o);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id().hashCode();
        }
    }
}