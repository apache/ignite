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

package org.apache.ignite.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid utils tests.
 */
@GridCommonTest(group = "Utils")
public class GridPartitionMapSelfTest extends GridCommonAbstractTest {
    /** */
    public void testPartitionStateMap() {
        GridPartitionStateMap map = initMap(new GridPartitionStateMap());

        Set<Map.Entry<Integer, GridDhtPartitionState>> entries = map.entrySet();

        assertEquals(10, map.size());

        for (Map.Entry<Integer, GridDhtPartitionState> entry : entries)
            entry.setValue(GridDhtPartitionState.OWNING);

        assertEquals(10, map.size());

        for (GridDhtPartitionState state : map.values())
            assertEquals(GridDhtPartitionState.OWNING, state);

        Set<Map.Entry<Integer, GridDhtPartitionState>> tmp = new HashSet<>();

        for (Map.Entry<Integer, GridDhtPartitionState> entry : entries) {
            tmp.add(entry);

            entry.setValue(GridDhtPartitionState.LOST);
        }

        for (Map.Entry<Integer, GridDhtPartitionState> entry : tmp)
            entry.setValue(GridDhtPartitionState.LOST);

        for (GridDhtPartitionState state : map.values())
            assertEquals(GridDhtPartitionState.LOST, state);

        assertFalse(map.containsKey(10));

        assertNull(map.remove(10));

        assertEquals(10, map.size());

        assertEquals(GridDhtPartitionState.LOST, map.put(9, GridDhtPartitionState.EVICTED));

        assertEquals(10, map.size());

        assertEquals(GridDhtPartitionState.EVICTED, map.put(9, GridDhtPartitionState.EVICTED));

        assertEquals(10, map.size());

        map.remove(5);

        assertEquals(9, map.size());
        assertEquals(9, map.keySet().size());
        assertEquals(9, map.values().size());

        map.clear();

        assertEquals(0, map.size());
        assertEquals(0, map.keySet().size());
        assertEquals(0, map.values().size());
    }

    /** */
    public void testEqualsAndHashCode() {
        GridPartitionStateMap map1 = initMap(new GridPartitionStateMap());

        GridPartitionStateMap map2 = initMap(new GridPartitionStateMap());

        assertEquals(map1, map2);

        assertEquals(map1.hashCode(), map2.hashCode());

        assertFalse(map1.equals(new HashMap()));

        assertFalse(map1.equals(null));
    }

    /**
     *
     */
    public void testCopy() {
        GridPartitionStateMap map1 = initMap(new GridPartitionStateMap());

        GridPartitionStateMap cp1 = new GridPartitionStateMap(map1, false);

        assertEquals(map1, cp1);

        GridPartitionStateMap map2 = new GridPartitionStateMap();
        map2.put(0, GridDhtPartitionState.MOVING);
        map2.put(1, GridDhtPartitionState.RENTING);
        map2.put(2, GridDhtPartitionState.LOST);
        map2.put(3, GridDhtPartitionState.OWNING);
        map2.put(5, GridDhtPartitionState.MOVING);
        map2.put(6, GridDhtPartitionState.RENTING);
        map2.put(7, GridDhtPartitionState.LOST);
        map2.put(8, GridDhtPartitionState.OWNING);

        GridPartitionStateMap cp2 = new GridPartitionStateMap(map1, true);

        assertEquals(map2, cp2);
    }

    /**
     *
     */
    public void testCopyNoActive() {
        GridPartitionStateMap map2 = new GridPartitionStateMap();

        map2.put(100, GridDhtPartitionState.EVICTED);
        map2.put(101, GridDhtPartitionState.EVICTED);
        map2.put(102, GridDhtPartitionState.EVICTED);
        map2.put(103, GridDhtPartitionState.OWNING);

        GridPartitionStateMap cp2 = new GridPartitionStateMap(map2, true);

        assertEquals(1, cp2.size());
    }

    /**
     * Tests that entries from {@link Iterator#next()} remain unaltered.
     */
    public void testIteratorNext() {
        GridPartitionStateMap map = new GridPartitionStateMap();

        initMap(map);

        Iterator<Map.Entry<Integer, GridDhtPartitionState>> iter = map.entrySet().iterator();

        for (int i = 0; i < map.size() + 1; i++)
            assertTrue(iter.hasNext());

        Map.Entry<Integer, GridDhtPartitionState> entry1 = iter.next();

        for (int i = 0; i < map.size() + 1; i++)
            assertTrue(iter.hasNext());

        Map.Entry<Integer, GridDhtPartitionState> entry2 = iter.next();

        iter.remove();

        assertNotNull(entry1.getValue());
        assertNotNull(entry2.getValue());

        assertEquals(Integer.valueOf(0), entry1.getKey());
        assertEquals(Integer.valueOf(1), entry2.getKey());

        assertEquals(GridDhtPartitionState.MOVING, entry1.getValue());
        assertEquals(GridDhtPartitionState.RENTING, entry2.getValue());
    }

    /**
     * Tests {@link GridDhtPartitionState} compatibility with {@link TreeMap} on random operations.
     */
    public void testOnRandomOperations() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Map<Integer, GridDhtPartitionState> treeMap = new TreeMap<>();
        Map<Integer, GridDhtPartitionState> gridMap = new GridPartitionStateMap();

        int statesNum = GridDhtPartitionState.values().length;

        for (int i = 0; i < 10000; i++) {
            Integer part = rnd.nextInt(65536);

            GridDhtPartitionState state = GridDhtPartitionState.fromOrdinal(rnd.nextInt(statesNum));

            int rndOperation = rnd.nextInt(9);

            if (rndOperation <= 5) {
                treeMap.put(part, state);
                gridMap.put(part, state);
            }
            else if (rndOperation == 6) {
                treeMap.remove(part);
                gridMap.remove(part);
            }
            else if (!treeMap.isEmpty()) {
                int n = rnd.nextInt(0, treeMap.size());

                Iterator<Map.Entry<Integer, GridDhtPartitionState>> iter1 = treeMap.entrySet().iterator();
                Iterator<Map.Entry<Integer, GridDhtPartitionState>> iter2 = gridMap.entrySet().iterator();

                Map.Entry<Integer, GridDhtPartitionState> entry1 = iter1.next();
                Map.Entry<Integer, GridDhtPartitionState> entry2 = iter2.next();

                for (int j = 1; j <= n; j++) {
                    entry1 = iter1.next();
                    entry2 = iter2.next();

                    assertEquals(entry1.getValue(), entry2.getValue());
                }

                if (rndOperation == 7) {
                    entry1.setValue(state);
                    entry2.setValue(state);
                }
                else {
                    iter1.remove();
                    iter2.remove();
                }
            }

            assertEquals(treeMap.size(), gridMap.size());
        }

        assertEquals(treeMap, gridMap);
    }

    /** */
    private GridPartitionStateMap initMap(GridPartitionStateMap map) {
        map.put(0, GridDhtPartitionState.MOVING);
        map.put(1, GridDhtPartitionState.RENTING);
        map.put(2, GridDhtPartitionState.LOST);
        map.put(3, GridDhtPartitionState.OWNING);
        map.put(4, GridDhtPartitionState.EVICTED);
        map.put(5, GridDhtPartitionState.MOVING);
        map.put(6, GridDhtPartitionState.RENTING);
        map.put(7, GridDhtPartitionState.LOST);
        map.put(8, GridDhtPartitionState.OWNING);
        map.put(9, GridDhtPartitionState.EVICTED);

        return map;
    }
}
