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

package org.apache.ignite.internal.util.collection;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.util.collection.IntHashMap.INITIAL_CAPACITY;
import static org.apache.ignite.internal.util.collection.IntHashMap.MAXIMUM_CAPACITY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for the specific implementation of IntMap.
 */
public class IntHashMapTest extends AbstractBaseIntMapTest {
    /** {@inheritDoc} */
    @Override protected IntMap<String> instantiateMap() {
        return new IntHashMap<>();
    }

    /** */
    @Test
    public void removeBackShift() {
        IntHashMap<String> directPositionMap = new IntHashMap<String>() {
            @Override protected int index(int key) {
                return 14;
            }
        };

        directPositionMap.put(1, value(1));
        directPositionMap.put(2, value(2));
        directPositionMap.put(3, value(3));
        directPositionMap.put(4, value(4));
        directPositionMap.put(5, value(5));

        directPositionMap.remove(1);

        assertEquals(4, directPositionMap.size());
    }

    /** */
    @Test
    public void distance() {
        IntHashMap<String> directPositionMap = new IntHashMap<String>() {
            @Override protected int index(int key) {
                return 14;
            }
        };

        directPositionMap.put(1, value(1));
        directPositionMap.put(2, value(2));
        directPositionMap.put(3, value(3));
        directPositionMap.put(4, value(4));
        directPositionMap.put(5, value(5));
        directPositionMap.put(6, value(6));
        directPositionMap.put(7, value(7));
        directPositionMap.put(8, value(8));
        directPositionMap.put(9, value(9));

        assertEquals(0, directPositionMap.distance(14, 1));
        assertEquals(1, directPositionMap.distance(15, 1));
        assertEquals(2, directPositionMap.distance(0, 1));
        assertEquals(3, directPositionMap.distance(1, 1));
        assertEquals(4, directPositionMap.distance(2, 1));
        assertEquals(5, directPositionMap.distance(3, 1));
        assertEquals(15, directPositionMap.distance(13, 1));
    }

    /**
     *
     */
    @Test
    public void shouldAllocateMapWithInitialCapacity() {
        assertEquals(INITIAL_CAPACITY, realCapacityForInitialSize(1));
        assertEquals(16, realCapacityForInitialSize(9));
        assertEquals(128, realCapacityForInitialSize(99));
        assertEquals(256, realCapacityForInitialSize(155));
    }

    /**
     *
     */
    @Test
    public void shouldReturnsRequiredTableSizeForCustomCapacity() {
        assertEquals(INITIAL_CAPACITY, IntHashMap.tableSize(1));
        assertEquals(MAXIMUM_CAPACITY, IntHashMap.tableSize(Integer.MAX_VALUE));
    }

    /**
     * Checking the correctness of {@link IntMap#computeIfAbsent}.
     */
    @Test
    public void testComputeIfAbsent() {
        IntHashMap<Object> map0 = new IntHashMap<>();

        assertThrows(
            null,
            () -> map0.computeIfAbsent(0, null),
            NullPointerException.class,
            null
        );

        Map<Integer, Object> map1 = new HashMap<>();

        assertEquals(map1.computeIfAbsent(0, i -> i + " 0"), map0.computeIfAbsent(0, i -> i + " 0"));
        assertEquals(map1.computeIfAbsent(0, i -> i + " 1"), map0.computeIfAbsent(0, i -> i + " 1"));

        assertEquals(map1.computeIfAbsent(1, i -> i + " 0"), map0.computeIfAbsent(1, i -> i + " 0"));
        assertEquals(map1.computeIfAbsent(1, i -> i + " 1"), map0.computeIfAbsent(1, i -> i + " 1"));

        assertEquals("0 0", map0.get(0));
        assertEquals("1 0", map0.get(1));
    }

    /**
     * Tests the copy constructor.
     */
    @Test
    public void testCopyConstructor() {
        IntMap<String> expected = new IntHashMap<>();

        IntStream.range(0, 10).forEach(i -> expected.put(i, String.valueOf(i)));

        IntMap<Object> actual = new IntHashMap<>(expected);

        Object[] expectedKeys = Arrays.stream(expected.keys()).boxed().toArray();
        Object[] actualKeys = Arrays.stream(actual.keys()).boxed().toArray();

        assertThat(actualKeys, arrayContainingInAnyOrder(expectedKeys));

        expected.forEach((key, expectedVal) -> assertThat(actual.get(key), is((Object) expectedVal)));
    }

    /**
     * @param initSize Initial size.
     */
    private static int realCapacityForInitialSize(int initSize) {
        return ((Object[]) U.field(new IntHashMap<String>(initSize), "entries")).length;
    }
}
