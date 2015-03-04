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

package org.apache.ignite.internal.client.impl;

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.consistenthash.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.client.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.affinity.consistenthash.CacheConsistentHashAffinityFunction.*;

/**
 * Client's partitioned affinity tests.
 */
public class ClientPartitionAffinitySelfTest extends GridCommonAbstractTest {
    /** Hash ID resolver. */
    private static final GridClientPartitionAffinity.HashIdResolver HASH_ID_RSLVR =
        new GridClientPartitionAffinity.HashIdResolver() {
            @Override public Object getHashId(GridClientNode node) {
                return node.nodeId();
            }
        };

    /**
     * Test predefined affinity - must be ported to other clients.
     */
    @SuppressWarnings("UnaryPlus")
    public void testPredefined() throws Exception {
        // Use Md5 hasher for this test.
        GridClientPartitionAffinity aff = new GridClientPartitionAffinity();

        getTestResources().inject(aff);

        aff.setHashIdResolver(HASH_ID_RSLVR);

        List<GridClientNode> nodes = new ArrayList<>();

        nodes.add(createNode("000ea4cd-f449-4dcb-869a-5317c63bd619", 50));
        nodes.add(createNode("010ea4cd-f449-4dcb-869a-5317c63bd62a", 60));
        nodes.add(createNode("0209ec54-ff53-4fdb-8239-5a3ac1fb31bd", 70));
        nodes.add(createNode("0309ec54-ff53-4fdb-8239-5a3ac1fb31ef", 80));
        nodes.add(createNode("040c9b94-02ae-45a6-9d5c-a066dbdf2636", 90));
        nodes.add(createNode("050c9b94-02ae-45a6-9d5c-a066dbdf2747", 100));
        nodes.add(createNode("0601f916-4357-4cfe-a7df-49d4721690bf", 110));
        nodes.add(createNode("0702f916-4357-4cfe-a7df-49d4721691c0", 120));

        Map<Object, Integer> data = new LinkedHashMap<>();

        data.put("", 4);
        data.put("asdf", 4);
        data.put("224ea4cd-f449-4dcb-869a-5317c63bd619", 5);
        data.put("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", 2);
        data.put("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", 2);
        data.put("d8f1f916-4357-4cfe-a7df-49d4721690bf", 7);
        data.put("c77ffeae-78a1-4ee6-a0fd-8d197a794412", 3);
        data.put("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564", 1);
        data.put("d67eb652-4e76-47fb-ad4e-cd902d9b868a", 7);

        data.put(0, 4);
        data.put(1, 7);
        data.put(12, 5);
        data.put(123, 6);
        data.put(1234, 4);
        data.put(12345, 6);
        data.put(123456, 6);
        data.put(1234567, 6);
        data.put(12345678, 0);
        data.put(123456789, 7);
        data.put(1234567890, 7);
        data.put(1234567890L, 7);
        data.put(12345678901L, 2);
        data.put(123456789012L, 1);
        data.put(1234567890123L, 0);
        data.put(12345678901234L, 1);
        data.put(123456789012345L, 6);
        data.put(1234567890123456L, 7);
        data.put(-23456789012345L, 4);
        data.put(-2345678901234L, 1);
        data.put(-234567890123L, 5);
        data.put(-23456789012L, 5);
        data.put(-2345678901L, 7);
        data.put(-234567890L, 4);
        data.put(-234567890, 7);
        data.put(-23456789, 7);
        data.put(-2345678, 0);
        data.put(-234567, 6);
        data.put(-23456, 6);
        data.put(-2345, 6);
        data.put(-234, 7);
        data.put(-23, 5);
        data.put(-2, 4);

        data.put(0x80000000, 4);
        data.put(0x7fffffff, 7);
        data.put(0x8000000000000000L, 4);
        data.put(0x7fffffffffffffffL, 4);

        data.put(+1.1, 3);
        data.put(-10.01, 4);
        data.put(+100.001, 4);
        data.put(-1000.0001, 4);
        data.put(+1.7976931348623157E+308, 6);
        data.put(-1.7976931348623157E+308, 6);
        data.put(+4.9E-324, 7);
        data.put(-4.9E-324, 7);

        boolean ok = true;

        for (Map.Entry<Object, Integer> entry : data.entrySet()) {
            UUID exp = nodes.get(entry.getValue()).nodeId();
            UUID act = aff.node(entry.getKey(), nodes).nodeId();

            if (exp.equals(act))
                continue;

            ok = false;

            info("Failed to validate affinity for key '" + entry.getKey() + "' [expected=" + exp +
                ", actual=" + act + ".");
        }

        if (ok)
            return;

        fail("Client partitioned affinity validation fails.");
    }

    /**
     * Test predefined affinity - must be ported to other clients.
     */
    @SuppressWarnings("UnaryPlus")
    public void testPredefinedHashIdResolver() throws Exception {
        // Use Md5 hasher for this test.
        GridClientPartitionAffinity aff = new GridClientPartitionAffinity();

        getTestResources().inject(aff);

        aff.setHashIdResolver(new GridClientPartitionAffinity.HashIdResolver() {
            @Override public Object getHashId(GridClientNode node) {
                return node.replicaCount();
            }
        });

        List<GridClientNode> nodes = new ArrayList<>();

        nodes.add(createNode("000ea4cd-f449-4dcb-869a-5317c63bd619", 50));
        nodes.add(createNode("010ea4cd-f449-4dcb-869a-5317c63bd62a", 60));
        nodes.add(createNode("0209ec54-ff53-4fdb-8239-5a3ac1fb31bd", 70));
        nodes.add(createNode("0309ec54-ff53-4fdb-8239-5a3ac1fb31ef", 80));
        nodes.add(createNode("040c9b94-02ae-45a6-9d5c-a066dbdf2636", 90));
        nodes.add(createNode("050c9b94-02ae-45a6-9d5c-a066dbdf2747", 100));
        nodes.add(createNode("0601f916-4357-4cfe-a7df-49d4721690bf", 110));
        nodes.add(createNode("0702f916-4357-4cfe-a7df-49d4721691c0", 120));

        Map<Object, Integer> data = new LinkedHashMap<>();

        data.put("", 4);
        data.put("asdf", 3);
        data.put("224ea4cd-f449-4dcb-869a-5317c63bd619", 5);
        data.put("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", 2);
        data.put("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", 2);
        data.put("d8f1f916-4357-4cfe-a7df-49d4721690bf", 4);
        data.put("c77ffeae-78a1-4ee6-a0fd-8d197a794412", 3);
        data.put("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564", 4);
        data.put("d67eb652-4e76-47fb-ad4e-cd902d9b868a", 2);

        data.put(0, 4);
        data.put(1, 1);
        data.put(12, 7);
        data.put(123, 1);
        data.put(1234, 6);
        data.put(12345, 2);
        data.put(123456, 5);
        data.put(1234567, 4);
        data.put(12345678, 6);
        data.put(123456789, 3);
        data.put(1234567890, 3);
        data.put(1234567890L, 3);
        data.put(12345678901L, 0);
        data.put(123456789012L, 1);
        data.put(1234567890123L, 3);
        data.put(12345678901234L, 5);
        data.put(123456789012345L, 5);
        data.put(1234567890123456L, 7);
        data.put(-23456789012345L, 6);
        data.put(-2345678901234L, 4);
        data.put(-234567890123L, 3);
        data.put(-23456789012L, 0);
        data.put(-2345678901L, 4);
        data.put(-234567890L, 5);
        data.put(-234567890, 3);
        data.put(-23456789, 3);
        data.put(-2345678, 6);
        data.put(-234567, 4);
        data.put(-23456, 5);
        data.put(-2345, 2);
        data.put(-234, 7);
        data.put(-23, 6);
        data.put(-2, 6);

        data.put(0x80000000, 7);
        data.put(0x7fffffff, 1);
        data.put(0x8000000000000000L, 7);
        data.put(0x7fffffffffffffffL, 7);

        data.put(+1.1, 2);
        data.put(-10.01, 0);
        data.put(+100.001, 2);
        data.put(-1000.0001, 0);
        data.put(+1.7976931348623157E+308, 6);
        data.put(-1.7976931348623157E+308, 1);
        data.put(+4.9E-324, 1);
        data.put(-4.9E-324, 1);

        boolean ok = true;

        for (Map.Entry<Object, Integer> entry : data.entrySet()) {
            UUID exp = nodes.get(entry.getValue()).nodeId();
            UUID act = aff.node(entry.getKey(), nodes).nodeId();

            if (exp.equals(act))
                continue;

            ok = false;

            info("Failed to validate affinity for key '" + entry.getKey() + "' [expected=" + exp +
                ", actual=" + act + ".");
        }

        if (ok)
            return;

        fail("Client partitioned affinity validation fails.");
    }

    /**
     * Create node with specified node id and replica count.
     *
     * @param nodeId Node id.
     * @param replicaCnt Node partitioned affinity replica count.
     * @return New node with specified node id and replica count.
     */
    private GridClientNode createNode(String nodeId, int replicaCnt) {
        return GridClientNodeImpl.builder()
            .nodeId(UUID.fromString(nodeId))
            .replicaCount(replicaCnt)
            .build();
    }

    /**
     * Validate client partitioned affinity and cache partitioned affinity produce the same result.
     *
     * @throws Exception On any exception.
     */
    public void testReplicas() throws Exception {
        // Emulate nodes in topology.
        Collection<GridClientNode> nodes = new ArrayList<>();
        Collection<ClusterNode> srvNodes = new ArrayList<>();

        // Define affinities to test.
        GridClientPartitionAffinity aff = new GridClientPartitionAffinity();

        getTestResources().inject(aff);

        aff.setHashIdResolver(HASH_ID_RSLVR);

        CacheConsistentHashAffinityFunction srvAff = new CacheConsistentHashAffinityFunction();

        getTestResources().inject(srvAff);

        srvAff.setHashIdResolver(new CacheAffinityNodeIdHashResolver());

        // Define keys to test affinity for.
        Collection<String> keys = new ArrayList<>(
            Arrays.asList("", "1", "12", "asdf", "Hadoop\u3092\u6bba\u3059"));

        for (int i = 0; i < 10; i++)
            keys.add(UUID.randomUUID().toString());

        // Test affinity behaviour on different topologies.
        for (int i = 0; i < 20; i++) {
            addNodes(1 + (int)Math.round(Math.random() * 50), nodes, srvNodes);

            for (String key : keys)
                assertSameAffinity(key, aff, srvAff, nodes, srvNodes);
        }
    }

    /**
     * Add {@code cnt} nodes into emulated topology.
     *
     * @param cnt Number of nodes to add into emulated topology.
     * @param nodes Client topology.
     * @param srvNodes Server topology.
     */
    private void addNodes(int cnt, Collection<GridClientNode> nodes, Collection<ClusterNode> srvNodes) {
        while (cnt-- > 0) {
            UUID nodeId = UUID.randomUUID();
            int replicaCnt = (int)Math.round(Math.random() * 500) + 1;

            nodes.add(GridClientNodeImpl.builder()
                .nodeId(nodeId)
                .replicaCount(replicaCnt)
                .build());

            ClusterNode srvNode = new TestRichNode(nodeId, replicaCnt);

            srvNodes.add(srvNode);
        }
    }

    /**
     * Compare server and client affinity for specified key in current topology.
     *
     * @param key Key to validate affinity for.
     * @param aff Client affinity.
     * @param srvAff Server affinity.
     * @param nodes Client topology.
     * @param srvNodes Server topology.
     */
    private void assertSameAffinity(Object key, GridClientDataAffinity aff, CacheAffinityFunction srvAff,
        Collection<? extends GridClientNode> nodes, Collection<ClusterNode> srvNodes) {
        GridClientNode node = aff.node(key, nodes);
        int part = srvAff.partition(key);

        CacheAffinityFunctionContext ctx = new GridCacheAffinityFunctionContextImpl(new ArrayList<>(srvNodes),
            null, null, 1, 0);

        ClusterNode srvNode = F.first(srvAff.assignPartitions(ctx).get(part));

        if (node == null)
            assertNull(srvNode);
        else {
            assertNotNull(srvNode);
            assertEquals(node.nodeId(), srvNode.id());
        }
    }

    /**
     * Rich node stub to use in emulated server topology.
     */
    private static class TestRichNode extends GridTestNode {
        /**
         * Node id.
         */
        private final UUID nodeId;

        /**
         * Partitioned affinity replicas count.
         */
        private final Integer replicaCnt;

        /**
         * Externalizable class requires public no-arg constructor.
         */
        @SuppressWarnings("UnusedDeclaration")
        public TestRichNode() {
            this(UUID.randomUUID(), DFLT_REPLICA_COUNT);
        }

        /**
         * Constructs rich node stub to use in emulated server topology.
         *
         * @param nodeId Node id.
         * @param replicaCnt Partitioned affinity replicas count.
         */
        private TestRichNode(UUID nodeId, int replicaCnt) {
            this.nodeId = nodeId;
            this.replicaCnt = replicaCnt;
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public <T> T attribute(String name) {
            if (DFLT_REPLICA_COUNT_ATTR_NAME.equals(name))
                return (T)replicaCnt;

            return super.attribute(name);
        }
    }
}
