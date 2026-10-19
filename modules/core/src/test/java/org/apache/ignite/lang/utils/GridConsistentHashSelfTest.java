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

package org.apache.ignite.lang.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.internal.util.GridConsistentHash;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Consistent hash test.
 */
@GridCommonTest(group = "Lang")
public class GridConsistentHashSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 20;

    /** */
    private static final int REPLICAS = 512;

    /**
     * Initializes hash.
     *
     * @param hash Hash to initialize.
     * @param replicas Replicas.
     * @param nodes Array of nodes.
     * @return Passed in hash for chaining.
     */
    private GridConsistentHash<UUID> initialize(GridConsistentHash<UUID> hash, int replicas, UUID... nodes) {
        assert hash != null;

        int before = hash.count();

        for (UUID node : nodes) {
            int cnt = hash.count();

            assert hash.addNode(node, replicas);

            assertEquals(cnt + 1, hash.count());
        }

        int after = hash.count();

        assert before + nodes.length == after : "Invalid number of nodes [hashed=" + hash.nodes() +
            ", added=" + Arrays.toString(nodes) + ']';

        return hash;
    }

    /**
     * Test hash codes collisions.
     *
     * @throws Exception In case of any exception.
     */
    @Test
    public void testCollisions() throws Exception {
        Map<Integer, Set<UUID>> map = new HashMap<>();

        // Different nodes, but collide hash codes.
        Collection<UUID> nodes = new LinkedHashSet<>();

        // Generate several nodes with collide hash codes.
        while (nodes.size() < 8) {
            UUID uuid = UUID.randomUUID();
            int hashCode = uuid.hashCode();

            Set<UUID> set = map.get(hashCode);

            if (set == null)
                map.put(hashCode, set = new LinkedHashSet<>());

            set.add(uuid);

            if (set.size() > 1)
                nodes.addAll(set);
        }

        map.clear(); // Clean up.

        GridConsistentHash<UUID> hash = new GridConsistentHash<>();

        hash.addNodes(nodes, REPLICAS);

        boolean fail = false;

        for (UUID exp : nodes) {
            UUID act = hash.node(0, Arrays.asList(exp));

            if (exp.equals(act))
                info("Validation succeed [exp=" + exp + ", act=" + act + ']');
            else {
                info("Validation failed  [exp=" + exp + ", act=" + act + ']');

                fail = true;
            }
        }

        if (fail)
            fail("Failed to resolve consistent hash node, when node's hash codes collide: " + nodes);
    }

    /**
     * Test restrictions from internal {@link TreeSet} usage.
     *
     * @throws Exception In case of any exception.
     */
    @Test
    public void testTreeSetRestrictions() throws Exception {
        // Constructs hash without explicit node's comparator.
        GridConsistentHash<Object> hash = new GridConsistentHash<>();

        try {
            // Add several objects with the same hash without neigther natural ordering nor comparator.
            hash.addNode(new Object() {
                @Override public int hashCode() {
                    return 0;
                }
            }, 1);
            hash.addNode(new Object() {
                @Override public int hashCode() {
                    return 0;
                }
            }, 1);

            fail("Expects failed due to internal TreeSet requires comparator or natural ordering.");
        }
        catch (ClassCastException e) {
            info("Expected fail due to internal TreeSet requires comparator or natural ordering: " + e.getMessage());
        }

        // Constructs hash with explicit node's comparator.
        hash = new GridConsistentHash<>(new Comparator<Object>() {
            @Override public int compare(Object o1, Object o2) {
                // Such comparator is invalid for production code, but acceptable for current test purposes.
                return System.identityHashCode(o1) - System.identityHashCode(o2);
            }
        }, null);

        // Add several objects with the same hash into consistent hash with explicit comparator.
        hash.addNode(new Object() {
            @Override public int hashCode() {
                return 0;
            }
        }, 1);
        hash.addNode(new Object() {
            @Override public int hashCode() {
                return 0;
            }
        }, 1);

        info("Expected pass due to internal TreeSet has explicit comparator.");
    }

    /**
     *
     */
    @Test
    public void testOneNode() {
        GridConsistentHash<UUID> hash = new GridConsistentHash<>();

        UUID nodeId = UUID.randomUUID();

        initialize(hash, REPLICAS, nodeId);

        Collection<UUID> id = hash.nodes("a", 2, F.asList(nodeId));

        assertFalse(F.isEmpty(id));
        assertEquals(1, id.size());
        assertEquals(nodeId, id.iterator().next());
    }

    /**
     *
     */
    @Test
    public void testHistory() {
        for (int i = NODES; i-- > 0; ) {
            GridConsistentHash<UUID> hash = new GridConsistentHash<>();

            UUID[] nodes = nodes(i + 1);

            initialize(hash, REPLICAS, nodes);

            Collection<UUID> selected = new HashSet<>();

            for (int j = i + 1; j-- > 0;) {
                String key = UUID.randomUUID().toString();

                selected.add(hash.node(key));

                hash.removeNode(nodes[j]);
            }

            info("Number of history nodes for topology [history=" + selected.size() +
                ", topology=" + (i + 1) + ", selected=" + selected + ']');
        }
    }

    /**
     * @param nodes Nodes.
     * @return Nodes.
     */
    private UUID[] nodes(int nodes) {
        UUID[] ids = new UUID[nodes];

        for (int i = 0; i < nodes; i++)
            ids[i] = UUID.randomUUID();

        return ids;
    }
}
