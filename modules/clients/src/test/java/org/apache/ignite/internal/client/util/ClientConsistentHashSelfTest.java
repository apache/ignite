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

package org.apache.ignite.internal.client.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for consistent hash management class.
 */
public class ClientConsistentHashSelfTest extends GridCommonAbstractTest {
    /** Replicas count. */
    private static final int REPLICAS = 512;

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
        while (nodes.size() < 10) {
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

        GridClientConsistentHash<UUID> hash = new GridClientConsistentHash<>();

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
        GridClientConsistentHash<Object> hash = new GridClientConsistentHash<>();

        try {
            // Add several objects with the same hash without neither natural ordering nor comparator.
            hash.addNode(new Object() { public int hashCode() { return 0; } }, 1);
            hash.addNode(new Object() { public int hashCode() { return 0; } }, 1);

            fail("Expects failed due to internal TreeSet requires comparator or natural ordering.");
        }
        catch (ClassCastException e) {
            info("Expected fail due to internal TreeSet requires comparator or natural ordering: " + e.getMessage());
        }

        // Constructs hash with explicit node's comparator.
        hash = new GridClientConsistentHash<>(new Comparator<Object>() {
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
     * Validate generated hashes.<p>
     * Note! This test should be ported into all supported platforms.
     */
    public void testHashGeneraton() {
        // Validate strings.
        checkHash("", -1484017934);
        checkHash("1", -80388575);
        checkHash("a", -873690096);
        checkHash("Hadoop\u3092\u6bba\u3059", -695300527);
        checkHash("key1", -2067461682);

        // Validate primitives.
        checkHash(true, 1669973725);
        checkHash(false, -1900934144);

        checkHash(3, 386050343);
        checkHash(1000000000, -547312286);
        checkHash(0x7fffffff, 473949739);
        checkHash(0xffffffff, -1399925094);
        checkHash(0x7fffffffffffffffL, 201097861);
        checkHash(0xffffffffffffffffL, -1484017934);
        checkHash(1.4e-45f, 1262722378);
        checkHash(3.4028235e+38f, 1313755354);
        checkHash(4.9e-324, 1262722378);
        checkHash(1.7976931348623157e+308, -783615357);

        // Validate objects.
        checkHash(UUID.fromString("4d180911-21c9-48f2-a1e6-7bc1daf588a0"), -440525148);

        checkUUID("224ea4cd-f449-4dcb-869a-5317c63bd619", 806670090);
        checkUUID("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", -354375826);
        checkUUID("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", -1312538272);
        checkUUID("d8f1f916-4357-4cfe-a7df-49d4721690bf", -482944041);
        checkUUID("d67eb652-4e76-47fb-ad4e-cd902d9b868a", -449444069);
        checkUUID("c77ffeae-78a1-4ee6-a0fd-8d197a794412", -168980875);
        checkUUID("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564", -383915637);
    }

    /**
     * Test mapping to nodes.
     */
    @SuppressWarnings("UnaryPlus")
    public void testMappingToNodes() {
        String n1 = "node #1";
        String n2 = "node #2";
        String n3 = "node #3";
        String n4 = "node #4";

        List<String> nodes = Arrays.asList(n1, n2, n3, n4);

        GridClientConsistentHash<String> hash = new GridClientConsistentHash<>();

        for (String node : nodes)
            hash.addNode(node, 5);

        Map<Object, String> data = new LinkedHashMap<>();

        data.put("", n1);
        data.put("asdf", n3);
        data.put("224ea4cd-f449-4dcb-869a-5317c63bd619", n2);
        data.put("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", n4);
        data.put("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", n1);
        data.put("d8f1f916-4357-4cfe-a7df-49d4721690bf", n3);
        data.put("c77ffeae-78a1-4ee6-a0fd-8d197a794412", n4);
        data.put("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564", n4);
        data.put("d67eb652-4e76-47fb-ad4e-cd902d9b868a", n4);

        data.put(0, n1);
        data.put(1, n4);
        data.put(12, n3);
        data.put(123, n3);
        data.put(1234, n3);
        data.put(12345, n4);
        data.put(123456, n3);
        data.put(1234567, n4);
        data.put(12345678, n4);
        data.put(123456789, n4);
        data.put(1234567890, n3);
        data.put(1234567890L, n3);
        data.put(12345678901L, n4);
        data.put(123456789012L, n2);
        data.put(1234567890123L, n4);
        data.put(12345678901234L, n1);
        data.put(123456789012345L, n1);
        data.put(1234567890123456L, n3);
        data.put(-23456789012345L, n2);
        data.put(-2345678901234L, n1);
        data.put(-234567890123L, n4);
        data.put(-23456789012L, n3);
        data.put(-2345678901L, n3);
        data.put(-234567890L, n1);
        data.put(-234567890, n4);
        data.put(-23456789, n4);
        data.put(-2345678, n4);
        data.put(-234567, n4);
        data.put(-23456, n4);
        data.put(-2345, n1);
        data.put(-234, n4);
        data.put(-23, n3);
        data.put(-2, n4);

        data.put(0x80000000, n2);
        data.put(0x7fffffff, n4);
        data.put(0x8000000000000000L, n2);
        data.put(0x7fffffffffffffffL, n2);

        data.put(+1.1, n1);
        data.put(-10.01, n3);
        data.put(+100.001, n3);
        data.put(-1000.0001, n4);
        data.put(+1.7976931348623157E+308, n4);
        data.put(-1.7976931348623157E+308, n4);
        data.put(+4.9E-324, n4);
        data.put(-4.9E-324, n3);

        for (Map.Entry<Object, String> entry : data.entrySet())
            assertEquals("Validate key '" + entry.getKey() + "'.", entry.getValue(), hash.node(entry.getKey()));

        for (Map.Entry<Object, String> entry : data.entrySet())
            assertEquals("Validate key '" + entry.getKey() + "'.", entry.getValue(), hash.node(entry.getKey(), nodes));

        //
        // Change order nodes were added.
        //

        nodes = new ArrayList<>(nodes);

        Collections.reverse(nodes);

        // Reset consistent hash with new nodes order.
        hash = new GridClientConsistentHash<>();

        for (String node : nodes)
            hash.addNode(node, 5);

        for (Map.Entry<Object, String> entry : data.entrySet())
            assertEquals("Validate key '" + entry.getKey() + "'.", entry.getValue(), hash.node(entry.getKey()));

        for (Map.Entry<Object, String> entry : data.entrySet())
            assertEquals("Validate key '" + entry.getKey() + "'.", entry.getValue(), hash.node(entry.getKey(), nodes));
    }

    /**
     * Check unique id and generated hash code.
     *
     * @param uuid String presentation of unique id.
     * @param code Expected hash code.
     */
    private void checkUUID(String uuid, int code) {
        checkHash(UUID.fromString(uuid), code);
    }

    /**
     * Check hash generation for the specified object.
     *
     * @param o Object to verify hash generation for.
     * @param code Expected hash code.
     */
    private void checkHash(Object o, int code) {
        int i = GridClientConsistentHash.hash(o);

        assertEquals("Check affinity for object: " + o, code, i);
    }
}