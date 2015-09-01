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

import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test for {@link ClusterGroup}.
 */
@GridCommonTest(group = "Kernal Self")
public class ClusterGroupSelfTest extends ClusterGroupAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 4;

    /** Projection node IDs. */
    private static Collection<UUID> ids;

    /** */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT > 2;

        ids = new LinkedList<>();

        try {
            for (int i = 0; i < NODES_CNT; i++) {
                Ignition.setClientMode(i > 1);

                Ignite g = startGrid(i);

                ids.add(g.cluster().localNode().id());

                if (i == 0)
                    ignite = g;
            }
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (int i = 0; i < NODES_CNT; i++)
            stopGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected ClusterGroup projection() {
        return grid(0).cluster().forPredicate(F.nodeForNodeIds(ids));
    }

    /** {@inheritDoc} */
    @Override protected UUID localNodeId() {
        return grid(0).localNode().id();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandom() throws Exception {
        assertTrue(ignite.cluster().nodes().contains(ignite.cluster().forRandom().node()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldest() throws Exception {
        ClusterGroup oldest = ignite.cluster().forOldest();

        ClusterNode node = null;

        long minOrder = Long.MAX_VALUE;

        for (ClusterNode n : ignite.cluster().nodes()) {
            if (n.order() < minOrder) {
                node = n;

                minOrder = n.order();
            }
        }

        assertEquals(oldest.node(), ignite.cluster().forNode(node).node());
    }

    /**
     * @throws Exception If failed.
     */
    public void testYoungest() throws Exception {
        ClusterGroup youngest = ignite.cluster().forYoungest();

        ClusterNode node = null;

        long maxOrder = Long.MIN_VALUE;

        for (ClusterNode n : ignite.cluster().nodes()) {
            if (n.order() > maxOrder) {
                node = n;

                maxOrder = n.order();
            }
        }

        assertEquals(youngest.node(), ignite.cluster().forNode(node).node());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNewNodes() throws Exception {
        ClusterGroup youngest = ignite.cluster().forYoungest();
        ClusterGroup oldest = ignite.cluster().forOldest();

        ClusterNode old = oldest.node();
        ClusterNode last = youngest.node();

        assertNotNull(last);

        try (Ignite g = startGrid(NODES_CNT)) {
            ClusterNode n = g.cluster().localNode();

            ClusterNode latest = youngest.node();

            assertNotNull(latest);
            assertEquals(latest.id(), n.id());
            assertEquals(oldest.node(), old);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testForPredicate() throws Exception {
        IgnitePredicate<ClusterNode> evenP = new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return node.order() % 2 == 0;
            }
        };

        IgnitePredicate<ClusterNode> oddP = new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return node.order() % 2 == 1;
            }
        };

        ClusterGroup remotes = ignite.cluster().forRemotes();

        ClusterGroup evenYoungest = remotes.forPredicate(evenP).forYoungest();
        ClusterGroup evenOldest = remotes.forPredicate(evenP).forOldest();

        ClusterGroup oddYoungest = remotes.forPredicate(oddP).forYoungest();
        ClusterGroup oddOldest = remotes.forPredicate(oddP).forOldest();

        int clusterSize = ignite.cluster().nodes().size();

        assertEquals(grid(gridMaxOrder(clusterSize, true)).localNode().id(), evenYoungest.node().id());
        assertEquals(grid(1).localNode().id(), evenOldest.node().id());

        assertEquals(grid(gridMaxOrder(clusterSize, false)).localNode().id(), oddYoungest.node().id());
        assertEquals(grid(2).localNode().id(), oddOldest.node().id());

        try (Ignite g4 = startGrid(NODES_CNT);
            Ignite g5 = startGrid(NODES_CNT + 1))
        {
            clusterSize = g4.cluster().nodes().size();

            assertEquals(grid(gridMaxOrder(clusterSize, true)).localNode().id(), evenYoungest.node().id());
            assertEquals(grid(1).localNode().id(), evenOldest.node().id());

            assertEquals(grid(gridMaxOrder(clusterSize, false)).localNode().id(), oddYoungest.node().id());
            assertEquals(grid(2).localNode().id(), oddOldest.node().id());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAgeClusterGroupSerialization() throws Exception {
        Marshaller marshaller = getConfiguration().getMarshaller();

        ClusterGroup grp = ignite.cluster().forYoungest();
        ClusterNode node = grp.node();

        byte[] arr = marshaller.marshal(grp);

        ClusterGroup obj = marshaller.unmarshal(arr, null);

        assertEquals(node.id(), obj.node().id());

        try (Ignite ignore = startGrid()) {
            obj = marshaller.unmarshal(arr, null);

            assertEquals(grp.node().id(), obj.node().id());
            assertFalse(node.id().equals(obj.node().id()));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientServer() throws Exception {
        ClusterGroup srv = ignite.cluster().forServers();

        assertEquals(2, srv.nodes().size());
        assertTrue(srv.nodes().contains(ignite(0).cluster().localNode()));
        assertTrue(srv.nodes().contains(ignite(1).cluster().localNode()));

        ClusterGroup cli = ignite.cluster().forClients();

        assertEquals(2, srv.nodes().size());
        assertTrue(cli.nodes().contains(ignite(2).cluster().localNode()));
        assertTrue(cli.nodes().contains(ignite(3).cluster().localNode()));
    }

    /**
     * @param cnt Count.
     * @param even Even.
     */
    private static int gridMaxOrder(int cnt, boolean even) {
        assert cnt > 2;

        cnt = cnt - (cnt % 2);

        return even ? cnt - 1 : cnt - 2;
    }
}