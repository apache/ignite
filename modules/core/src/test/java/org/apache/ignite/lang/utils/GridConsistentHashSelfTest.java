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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.GridConsistentHash;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Consistent hash test.
 */
@SuppressWarnings({"AssertWithSideEffects"})
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
     * @param hash Hash to clean.
     */
    private void clean(GridConsistentHash<UUID> hash) {
        if (hash != null) {
            int cnt = hash.count();

            assert hash.removeNode(hash.random());

            assertEquals(cnt - 1, hash.count());

//            info("Cleaning nodes", hash.nodes());

            hash.removeNodes(hash.nodes());

            hash.clear();

            assertEquals(0, hash.size());
            assertEquals("Invalid hash: " + hash.nodes(), 0, hash.count());

            assert hash.isEmpty();
        }
    }

    /**
     * Test hash codes collisions.
     *
     * @throws Exception In case of any exception.
     */
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
            else{
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
    public void testTreeSetRestrictions() throws Exception {
        // Constructs hash without explicit node's comparator.
        GridConsistentHash<Object> hash = new GridConsistentHash<>();

        try {
            // Add several objects with the same hash without neigther natural ordering nor comparator.
            hash.addNode(new Object() { public int hashCode() { return 0; } }, 1);
            hash.addNode(new Object() { public int hashCode() { return 0; } }, 1);

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
        hash.addNode(new Object() { public int hashCode() { return 0; } }, 1);
        hash.addNode(new Object() { public int hashCode() { return 0; } }, 1);

        info("Expected pass due to internal TreeSet has explicit comparator.");
    }

    /**
     *
     */
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
     * @param msg Message.
     * @param c Collection.
     */
    private void info(String msg, Collection c) {
        info(msg + " [size=" + c.size() + ", col=" + c + ']');
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

    /**
     * @param hash Hash.
     * @param replicas Replicas.
     * @param nodes Nodes.
     * @return Runnable.
     */
    private Runnable initializer(final GridConsistentHash<UUID> hash, final int replicas, final UUID[] nodes) {
        return new Runnable() {
            @Override public void run() {
                initialize(hash, replicas, nodes);
            }
        };
    }

    /**
     * @param hash Hash.
     * @param keys Keys.
     * @return Runnable.
     */
    private Runnable hasher(final GridConsistentHash<UUID> hash, final String[] keys) {
        return new Runnable() {
            @Override public void run() {
                for (String k : keys) {
                    assert hash.node(k) != null;
                }
            }
        };
    }

    /**
     * @param hash Hash.
     * @param cnts Counts.
     * @param mappings Mappings.
     * @param keys Keys.
     */
    private void hash(GridConsistentHash<UUID> hash, Map<UUID, AtomicInteger> cnts, Map<String, UUID> mappings,
        String[] keys) {
        for (String k : keys) {
            UUID id = hash.node(k);

            assert id != null;

            AtomicInteger i = cnts.get(id);

            if (i == null)
                cnts.put(id, i = new AtomicInteger());

            i.incrementAndGet();

            mappings.put(k, id);
        }
    }

    /**
     *
     * @param m1 Map 1.
     * @param m2 Map 2.
     * @param keys Keys.
     * @return Reassignment count.
     */
    private int compare(Map<String, UUID> m1, Map<String, UUID> m2, String[] keys) {
        int cnt = 0;

        // Check reassignment percentages.
        for (String key : keys) {
            UUID id1 = m1.get(key);
            UUID id2 = m2.get(key);

            assert id1 != null;
            assert id2 != null;

            if (!id1.equals(id2))
                cnt++;
        }

        return cnt;
    }

    /**
     * @param cnt Number of keys to create.
     * @return Array of keys.
     */
    private String[] keys(int cnt) {
        String[] keys = new String[cnt];

        for (int i = 0; i < cnt; i++)
            keys[i] = UUID.randomUUID().toString();

        return keys;
    }
}