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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.util.collection.IntHashMap.INITIAL_CAPACITY;
import static org.apache.ignite.internal.util.collection.IntHashMap.MAXIMUM_CAPACITY;
import static org.junit.Assert.assertEquals;

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
        HashMap<Integer, Integer> bijection = new HashMap<>();
        bijection.put(1, 14);
        bijection.put(2, 14);
        bijection.put(3, 14);
        bijection.put(4, 14);
        bijection.put(5, 14);

        IntMap<String> directPositionMap = bijectionHashFunctionMap(bijection);

        directPositionMap.put(1, value(1));
        directPositionMap.put(2, value(2));
        directPositionMap.put(3, value(3));
        directPositionMap.put(4, value(4));
        directPositionMap.put(5, value(5));

        directPositionMap.remove(1);

        Assert.assertEquals(4, directPositionMap.size());
    }

    /** */
    @Test
    public void distance() {
        HashMap<Integer, Integer> bijection = new HashMap<>();
        bijection.put(1, 14);
        bijection.put(2, 14);
        bijection.put(3, 14);
        bijection.put(4, 14);
        bijection.put(5, 14);
        bijection.put(6, 14);
        bijection.put(7, 14);
        bijection.put(8, 14);
        bijection.put(9, 14);

        IntHashMap<String> directPositionMap = (IntHashMap<String>)bijectionHashFunctionMap(bijection);

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
     * @param initSize Initial size.
     */
    private int realCapacityForInitialSize(int initSize) {
        return ((Object[]) U.field(new IntHashMap<String>(initSize), "entries")).length;
    }

    /**
     * @param bijection Bijection.
     */
    private IntMap<String> bijectionHashFunctionMap(Map<Integer, Integer> bijection) {
        return new IntHashMap<String>() {
            @Override protected int index(int key) {
                return bijection.get(key);
            }
        };
    }
}
