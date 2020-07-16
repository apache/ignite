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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

/**
 * Checks cluster groups for thin client.
 */
public class ClusterGroupTest extends AbstractThinClientTest {
    /** Grid index attribute name. */
    private static final String GRID_IDX_ATTR_NAME = "GRID_IDX";

    /** Some custom attribute name. */
    private static final String CUSTOM_ATTR_NAME = "CUSTOM_ATTR";

    /** Custom attribute value. */
    private static final String CUSTOM_ATTR_VAL = "VAL";

    /** Client. */
    private static IgniteClient client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0, false, null);
        startGrid(1, false, null);
        startGrid(2, false, F.asMap(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL));
        startGrid(3, true, null);
        startGrid(4, true, F.asMap(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL));

        client = startClient(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        client.close();
    }

    /**
     * Test that thin client ClusterNode instances have the same field values as server ClusterNode instances.
     */
    @Test
    public void testClusterNodeFields() {
        for (Ignite ignite : G.allGrids()) {
            ClusterNode igniteNode = ignite.cluster().localNode();

            ClusterNode clientNode = client.cluster().node(igniteNode.id());

            assertEquals(igniteNode.id(), clientNode.id());
            assertEquals(igniteNode.consistentId(), clientNode.consistentId());
            // Some attribute values identical by content, but not equals after marshalling/unmarshalling
            // (for example values of type byte[]), so we can't compare whole attribute maps using assertEquals.
            assertEquals(igniteNode.attributes().keySet(), clientNode.attributes().keySet());
            assertEquals((Integer)igniteNode.attribute(GRID_IDX_ATTR_NAME), clientNode.attribute(GRID_IDX_ATTR_NAME));
            assertEquals(igniteNode.attribute(CUSTOM_ATTR_NAME), clientNode.attribute(CUSTOM_ATTR_NAME));
            assertEquals(new HashSet<>(igniteNode.addresses()), new HashSet<>(clientNode.addresses()));
            assertEquals(new HashSet<>(igniteNode.hostNames()), new HashSet<>(clientNode.hostNames()));
            assertEquals(igniteNode.order(), clientNode.order());
            assertEquals(igniteNode.version(), clientNode.version());
            assertEquals(igniteNode.isDaemon(), clientNode.isDaemon());
            assertEquals(igniteNode.isClient(), clientNode.isClient());
        }
    }

    /**
     * Test forNode(), forNodes() methods.
     */
    @Test
    public void testForNodes() {
        ClusterNode node0 = client.cluster().node(grid(0).localNode().id());
        ClusterNode node1 = client.cluster().node(grid(1).localNode().id());

        assertNodes(client.cluster().forNode(node0, node1), 0, 1);
        assertNodes(client.cluster().forNode(node0), 0);
        assertNodes(client.cluster().forNode(node1), 1);
        assertNodes(client.cluster().forNodes(F.asList(node0, node1)), 0, 1);
        assertNodes(client.cluster().forNodes(F.asList(node0)), 0);
        assertNodes(client.cluster().forNodes(F.asList(node0, node1)).forNode(node0), 0);
        assertNodes(client.cluster().forNode(node0, node1).forNodes(F.asList(node0, node1)), 0, 1);
        assertNodes(client.cluster().forNode(node0).forNodes(F.asList(node0, node1)), 0);
        assertNodes(client.cluster().forNode(node0).forNodes(F.asList(node1)));
    }

    /**
     * Test forOthers() methods.
     */
    @Test
    public void testForOthers() {
        ClusterNode node0 = client.cluster().node(grid(0).localNode().id());
        ClusterNode node1 = client.cluster().node(grid(1).localNode().id());

        assertNodes(client.cluster().forOthers(node0, node1), 2, 3, 4);
        assertNodes(client.cluster().forOthers(client.cluster().forOthers(node0, node1)), 0, 1);
        assertNodes(client.cluster().forOthers(node0).forOthers(node1), 2, 3, 4);
        assertNodes(client.cluster().forOthers(node0, node1).forOthers(node1), 2, 3, 4);
        assertNodes(client.cluster().forOthers(node0).forOthers(client.cluster().forOthers(node0, node1)), 1);
    }

    /**
     * Test forNodeId(), forNodeIds() methods.
     */
    @Test
    public void testForNodeIds() {
        UUID id0 = nodeId(0);
        UUID id1 = nodeId(1);

        assertNodes(client.cluster().forNodeId(id0, id1), 0, 1);
        assertNodes(client.cluster().forNodeId(id0), 0);
        assertNodes(client.cluster().forNodeId(id1), 1);
        assertNodes(client.cluster().forNodeIds(F.asList(id0, id1)), 0, 1);
        assertNodes(client.cluster().forNodeIds(F.asList(id0)), 0);
        assertNodes(client.cluster().forNodeIds(F.asList(id0, id1)).forNodeId(id0), 0);
        assertNodes(client.cluster().forNodeId(id0, id1).forNodeIds(F.asList(id0, id1)), 0, 1);
        assertNodes(client.cluster().forNodeId(id0).forNodeIds(F.asList(id0, id1)), 0);
        assertNodes(client.cluster().forNodeId(id0).forNodeIds(F.asList(id1)));
        assertNodes(client.cluster().forNodeId(UUID.randomUUID()));
        assertNodes(client.cluster().forNodeIds(F.asList(UUID.randomUUID())));
    }

    /**
     * Test forPredicate() method.
     */
    @Test
    public void testForPredicate() {
        assertNodes(client.cluster().forPredicate(ClusterNode::isClient), 3, 4);
        assertNodes(client.cluster().forPredicate(n -> n.order() == 1), 0);
        assertNodes(client.cluster().forPredicate(ClusterNode::isClient).forPredicate(n -> !n.isClient()));
        assertNodes(client.cluster().forPredicate(ClusterNode::isClient).forPredicate(n -> n.order() == 5), 4);
    }

    /**
     * Test forAttribute() method.
     */
    @Test
    public void testForAttribute() {
        assertNodes(client.cluster().forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL), 2, 4);
        assertNodes(client.cluster().forAttribute(CUSTOM_ATTR_NAME, null), 2, 4);
        assertNodes(client.cluster().forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL)
            .forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL), 2, 4);
        assertNodes(client.cluster().forAttribute(CUSTOM_ATTR_NAME, null)
            .forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL), 2, 4);
        assertNodes(client.cluster().forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL)
            .forAttribute(CUSTOM_ATTR_NAME, null), 2, 4);
        assertNodes(client.cluster().forAttribute(GRID_IDX_ATTR_NAME, 0), 0);
        assertNodes(client.cluster().forAttribute(GRID_IDX_ATTR_NAME, 0).forAttribute(GRID_IDX_ATTR_NAME, 1));
        assertNodes(client.cluster().forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL)
            .forAttribute(GRID_IDX_ATTR_NAME, 2), 2);
    }

    /**
     * Test forServers()/forClients() methods.
     */
    @Test
    public void testForServersForClients() {
        assertNodes(client.cluster().forServers(), 0, 1, 2);
        assertNodes(client.cluster().forClients(), 3, 4);
        assertNodes(client.cluster().forServers().forServers(), 0, 1, 2);
        assertNodes(client.cluster().forClients().forClients(), 3, 4);
        assertNodes(client.cluster().forServers().forClients());
    }

    /**
     * Test forRandom()/forOldest()/forYoungest() methods.
     */
    @Test
    public void testOneNodeFilters() {
        assertEquals(1, client.cluster().forRandom().nodes().size());
        assertNodes(client.cluster().forOldest(), 0);
        assertNodes(client.cluster().forYoungest(), 4);
        assertNodes(client.cluster().forOldest().forYoungest(), 0);
        assertNodes(client.cluster().forYoungest().forOldest(), 4);
        assertNodes(client.cluster().forOldest().forYoungest().forRandom(), 0);

        assertEquals(grid(0).localNode(), client.cluster().forOldest().node());
        assertEquals(grid(4).localNode(), client.cluster().forYoungest().node());
    }

    /**
     * Test forHost() methods.
     */
    @Test
    public void testForHost() {
        ClusterNode node0 = grid(0).localNode();
        String hostName = F.first(node0.hostNames());

        assertNodes(client.cluster().forHost(node0), 0, 1, 2, 3, 4);
        assertNodes(client.cluster().forHost(hostName), 0, 1, 2, 3, 4);
        assertNodes(client.cluster().forHost(node0).forHost(hostName), 0, 1, 2, 3, 4);
        assertNodes(client.cluster().forHost(hostName, hostName), 0, 1, 2, 3, 4);
        assertNodes(client.cluster().forHost("-"));
        assertNodes(client.cluster().forHost("-", hostName), 0, 1, 2, 3, 4);
        assertNodes(client.cluster().forHost("-").forHost(node0));
    }

    /**
     * Test combination of different for... methods.
     */
    @Test
    public void testForFiltersCombinations() {
        assertNodes(client.cluster(), 0, 1, 2, 3, 4);

        assertNodes(client.cluster().forServers().forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL), 2);
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(1), nodeId(4))
            .forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL).forPredicate(ClusterNode::isClient), 4);
        assertNodes(client.cluster().forClients().forOthers(grid(4).localNode()), 3);

        // Check resultFilter chaining.
        assertNodes(client.cluster().forOldest().forClients());
        assertNodes(client.cluster().forNodeId(nodeId(0)).forOldest().forNodeId(nodeId(0)), 0);
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(1)).forOldest()
            .forNodeId(nodeId(1)));
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forOldest().forClients());
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forYoungest().forClients(), 4);
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forOldest()
            .forPredicate(ClusterNode::isClient));
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forYoungest()
            .forPredicate(ClusterNode::isClient), 4);
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forOldest()
            .forAttribute(CUSTOM_ATTR_NAME, null));
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forYoungest()
            .forAttribute(CUSTOM_ATTR_NAME, null), 4);
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forOldest()
            .forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL));
        assertNodes(client.cluster().forNodeId(nodeId(0), nodeId(4)).forYoungest()
            .forAttribute(CUSTOM_ATTR_NAME, CUSTOM_ATTR_VAL), 4);
    }

    /**
     * Test that ClusterNode doesn't requested each time from the server.
     */
    @Test
    public void testClusterNodeCaching() {
        ClientClusterGroup grp = client.cluster().forServers();

        Collection<ClusterNode> nodes1 = grp.nodes();
        Collection<ClusterNode> nodes2 = grp.nodes();

        assertEquals(3, nodes1.size());
        assertEquals(3, nodes2.size());

        int foundCnt = 0;

        for (ClusterNode node1 : nodes1) {
            for (ClusterNode node2 : nodes2) {
                if (F.eq(node1.id(), node2.id())) {
                    assertTrue(node1 == node2); // Should be exactly the same instance.

                    foundCnt++;
                }
            }
        }

        assertEquals(3, foundCnt);
    }

    /**
     * Test node(UUID) method.
     */
    @Test
    public void testNodeById() {
        UUID rndId = UUID.randomUUID();
        ClusterNode node0 = client.cluster().node(nodeId(0));

        assertNotNull(node0);
        assertNull(client.cluster().node(rndId));
        assertNull(client.cluster().forNodeId(rndId).node(rndId));

        assertEquals(node0, client.cluster().forNodeId(nodeId(0)).node(nodeId(0)));
        assertNull(client.cluster().forNodeId(nodeId(0)).node(nodeId(1)));

        assertEquals(node0, client.cluster().forNode(node0).node(nodeId(0)));
        assertNull(client.cluster().forNode(node0).node(nodeId(1)));

        assertEquals(node0, client.cluster().forServers().node(nodeId(0)));
        assertNull(client.cluster().forServers().node(nodeId(3)));

        assertEquals(node0, client.cluster().forAttribute(GRID_IDX_ATTR_NAME, 0).node(nodeId(0)));
        assertNull(client.cluster().forAttribute(GRID_IDX_ATTR_NAME, 0).node(nodeId(1)));

        assertEquals(node0, client.cluster().forOldest().node(nodeId(0)));
        assertNull(client.cluster().forOldest().node(nodeId(1)));

        assertEquals(node0, client.cluster().forServers().forNodeId(nodeId(0), nodeId(1),
            nodeId(3)).forOldest().node(nodeId(0)));
        assertNull(client.cluster().forServers().forNodeId(nodeId(0), nodeId(1), nodeId(3))
            .forOldest().node(nodeId(1)));
    }

    /**
     * Test nodeIds() method.
     */
    @Test
    public void testNodeIds() {
        // Check default projection.
        assertNull(((ClientClusterImpl)client.cluster()).defaultClusterGroup().nodeIds());

        // Check filter only by node id.
        Collection<UUID> nodeIds = ((ClientClusterGroupImpl)client.cluster().forNodeId(nodeId(0),
            nodeId(1), UUID.randomUUID())).nodeIds();

        assertTrue(nodeIds.contains(nodeId(0)));
        assertTrue(nodeIds.contains(nodeId(1)));
        assertFalse(nodeIds.contains(nodeId(2)));
        assertFalse(nodeIds.contains(nodeId(3)));
        assertFalse(nodeIds.contains(nodeId(4)));

        // Check server-side filter.
        nodeIds = ((ClientClusterGroupImpl)client.cluster().forClients()).nodeIds();

        assertEquals(new HashSet<>(F.asList(nodeId(3), nodeId(4))),
            new HashSet<>(nodeIds));

        // Check client-side filters.
        nodeIds = ((ClientClusterGroupImpl)client.cluster().forPredicate(ClusterNode::isClient)).nodeIds();

        assertEquals(new HashSet<>(F.asList(nodeId(3), nodeId(4))), new HashSet<>(nodeIds));

        nodeIds = ((ClientClusterGroupImpl)client.cluster().forOldest()).nodeIds();

        assertEquals(Collections.singleton(nodeId(0)), new HashSet<>(nodeIds));

        // Check filters combination.
        nodeIds = ((ClientClusterGroupImpl)client.cluster().forServers().forClients()).nodeIds();

        assertTrue(nodeIds.isEmpty());

        nodeIds = ((ClientClusterGroupImpl)client.cluster().forServers().forNodeId(nodeId(0),
            nodeId(1))).nodeIds();

        assertEquals(new HashSet<>(F.asList(nodeId(0), nodeId(1))),
            new HashSet<>(nodeIds));

        nodeIds = ((ClientClusterGroupImpl)client.cluster().forNodeId(nodeId(1), nodeId(2),
            nodeId(3), UUID.randomUUID()).forServers().forYoungest()).nodeIds();

        assertEquals(Collections.singleton(nodeId(2)), new HashSet<>(nodeIds));
    }

    /**
     * Asserts that cluster group has the same nodes set as expected.
     *
     * @param grp Group.
     * @param nodeIdxs Node idxs.
     */
    private void assertNodes(ClientClusterGroup grp, int... nodeIdxs) {
        Collection<ClusterNode> expNodes = U.newHashSet(nodeIdxs.length);

        if (nodeIdxs == null) {
            assertTrue(F.isEmpty(grp.nodes()));

            return;
        }

        for (int nodeIdx : nodeIdxs)
            expNodes.add(grid(nodeIdx).localNode());

        assertEquals(expNodes, new HashSet<>(grp.nodes()));
    }

    /**
     * Start grid node with defined parameters.
     */
    private void startGrid(int idx, boolean client, Map<String, Object> attrs) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        if (attrs == null)
            attrs = F.asMap(GRID_IDX_ATTR_NAME, idx);
        else
            attrs.put(GRID_IDX_ATTR_NAME, idx);

        cfg.setUserAttributes(attrs);
        cfg.setClientMode(client);
        cfg.setDiscoverySpi(new TestDiscoverySpi().setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder()));

        startGrid(cfg);
    }

    /**
     * SPI to override hosts for ClusterNodes.
     */
    private static class TestDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void initLocalNode(int srvPort, boolean addExtAddrAttr) {
            super.initLocalNode(srvPort, addExtAddrAttr);

            locNode.hostNames().add("dummy");
        }
    }
}
