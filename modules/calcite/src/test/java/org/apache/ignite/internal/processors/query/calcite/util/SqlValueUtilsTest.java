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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Equality, hash consistency and distribution for SQL keys. */
public class SqlValueUtilsTest {
    /** Arrays and list representations of the same nested ROW must compare and hash alike. */
    @Test
    public void testNestedRows() {
        Object left = List.of(new Object[] {1, "x", null}, new Object[] {2, new byte[] {3, 4}});
        Object right = Arrays.asList(Arrays.asList(1, "x", null), Arrays.asList(2, new byte[] {3, 4}));

        assertSameValue(left, right);
        assertFalse(SqlValueUtils.deepEquals(left, Collections.singletonList(new Object[] {1, "x", null})));
        assertFalse(SqlValueUtils.deepEquals(new Object[] {1, "x"}, new Object[] {1, "y"}));
        assertFalse(SqlValueUtils.deepEquals(new Object[] {1, "x"}, new Object[] {"x", 1}));
        assertFalse(SqlValueUtils.deepEquals(new Object[] {1}, 1));
        assertFalse(SqlValueUtils.deepEquals(left, null));
        assertSameValue(null, null);
    }

    /** MAP equality ignores entry order and compares both keys and values by contents. */
    @Test
    public void testMapsWithRows() {
        Map<Object, Object> left = new LinkedHashMap<>();
        Map<Object, Object> right = new LinkedHashMap<>();

        left.put(new Object[] {1}, new Object[] {"a", null});
        left.put(new Object[] {2}, new Object[] {"b", 3});
        right.put(new Object[] {2}, new Object[] {"b", 3});
        right.put(new Object[] {1}, new Object[] {"a", null});

        assertSameValue(left, right);

        right.clear();
        right.put(new Object[] {1}, new Object[] {"a", null});
        right.put(new Object[] {1}, new Object[] {"a", null});

        assertFalse(SqlValueUtils.deepEquals(left, right));
    }

    /** Scalars and collections without ROWs use the same equality and hashing methods. */
    @Test
    public void testSimpleValues() {
        assertSameValue("key", new String("key"));
        assertSameValue(42, 42);
        assertSameValue(Arrays.asList(1, null, 2), Arrays.asList(1, null, 2));

        Map<String, Integer> left = new LinkedHashMap<>();
        Map<String, Integer> right = new LinkedHashMap<>();

        left.put("a", 1);
        left.put("b", null);
        right.put("b", null);
        right.put("a", 1);

        assertSameValue(left, right);
        assertFalse(SqlValueUtils.deepEquals("key", "other"));
        assertFalse(SqlValueUtils.deepEquals(42, 43));
    }

    /** Equal independent keys must address the same hash bucket without changing join null rules. */
    @Test
    public void testGroupKeys() {
        Object[] left = {Collections.singletonList(new Object[] {1, "x", null})};
        Object[] right = {Collections.singletonList(new Object[] {1, "x", null})};
        GroupKey<Object[]> first = GroupKey.of(left, ArrayRowHandler.INSTANCE);
        GroupKey<Object[]> second = GroupKey.of(right, ArrayRowHandler.INSTANCE);
        Map<GroupKey<Object[]>, String> idx = new HashMap<>();

        idx.put(first, "found");

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertEquals("found", idx.get(second));
        assertNull(GroupKey.of(new Object[] {null}, ArrayRowHandler.INSTANCE, false));
        assertNull(GroupKey.of(new Object[] {null}, ArrayRowHandler.INSTANCE, ImmutableList.of(true)));
        assertNotNull(GroupKey.of(new Object[] {null}, ArrayRowHandler.INSTANCE, ImmutableList.of(false)));
        assertNotNull(GroupKey.of(left, ArrayRowHandler.INSTANCE, false));
    }

    /** Hash distribution uses content hashes; cache affinity still uses its configured service. */
    @Test
    @SuppressWarnings("unchecked")
    public void testDistribution() {
        ExecutionContext<Object[]> ctx = mock(ExecutionContext.class);

        when(ctx.rowHandler()).thenReturn(ArrayRowHandler.INSTANCE);

        List<List<UUID>> assignments = IntStream.range(0, 32)
            .mapToObj(i -> List.of(new UUID(0, i))).collect(Collectors.toList());
        ColocationGroup grp = ColocationGroup.forAssignments(assignments);
        IgniteDistribution distribution = IgniteDistributions.hash(List.of(0));
        AffinityService aff = cacheId -> val -> 7;
        Destination<Object[]> dest = distribution.destination(ctx, aff, grp);

        for (int i = 0; i < 32; i++) {
            Object[] left = {Collections.singletonList(new Object[] {i})};
            Object[] right = {Collections.singletonList(new Object[] {i})};

            assertEquals(dest.targets(left), dest.targets(right));
        }

        assertNotEquals(dest.targets(new Object[] {Collections.singletonList(new Object[] {0})}),
            dest.targets(new Object[] {Collections.singletonList(new Object[] {1})}));

        assertEquals(assignments.get(42 % 32), dest.targets(new Object[] {42}));
        assertEquals(dest.targets(new Object[] {Arrays.asList(1, null, 2)}),
            dest.targets(new Object[] {Arrays.asList(1, null, 2)}));
        assertEquals(dest.targets(new Object[] {"key"}), dest.targets(new Object[] {new String("key")}));

        Destination<Object[]> affDest = IgniteDistributions.affinity(0, 1, "cache").destination(ctx, aff, grp);

        assertEquals(assignments.get(7), affDest.targets(new Object[] {42}));
    }

    /** */
    private static void assertSameValue(Object left, Object right) {
        assertTrue(SqlValueUtils.deepEquals(left, right));
        assertTrue(SqlValueUtils.deepEquals(right, left));
        assertEquals(SqlValueUtils.deepHashCode(left), SqlValueUtils.deepHashCode(right));
    }
}
