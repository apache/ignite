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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.internal.processors.fs.GridGgfsFileAffinityRange.*;

/**
 * File map self test.
 */
public class GridGgfsFileMapSelfTest extends GridGgfsCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testRanges() throws Exception {
        GridGgfsFileMap map = new GridGgfsFileMap();

        IgniteUuid[] affKeys = new IgniteUuid[20];

        for (int i = 0; i < affKeys.length; i++)
            affKeys[i] = IgniteUuid.randomUuid();

        int numOfRanges = 0;

        do {
            for (int i = 0; i < 2 * numOfRanges + 1; i++) {
                long off1 = i * 10;
                long off2 = i * 10 + 5;
                long off3 = i * 10 + 8;

                IgniteUuid affKey = i % 2 == 0 ? null : affKeys[i / 2];

                assertEquals("For i: " + i, affKey, map.affinityKey(off1, false));
                assertEquals("For i: " + i, affKey, map.affinityKey(off2, false));
                assertEquals("For i: " + i, affKey, map.affinityKey(off3, false));
            }

            map.addRange(new GridGgfsFileAffinityRange(10 + 20 * numOfRanges, 19 + 20 * numOfRanges,
                affKeys[numOfRanges]));

            numOfRanges++;
        } while (numOfRanges < 20);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddUpdateAdd() throws Exception {
        GridGgfsFileMap map = new GridGgfsFileMap();

        IgniteUuid affKey = IgniteUuid.randomUuid();

        map.addRange(new GridGgfsFileAffinityRange(0, 9, affKey));

        map.updateRangeStatus(new GridGgfsFileAffinityRange(0, 9, affKey), RANGE_STATUS_MOVING);

        map.addRange(new GridGgfsFileAffinityRange(10, 19, affKey));

        List<GridGgfsFileAffinityRange> ranges = map.ranges();

        assertEquals(2, ranges.size());

        assertEquals(RANGE_STATUS_MOVING, ranges.get(0).status());
        assertTrue(ranges.get(0).regionEqual(new GridGgfsFileAffinityRange(0, 9, affKey)));

        assertEquals(RANGE_STATUS_INITIAL, ranges.get(1).status());
        assertTrue(ranges.get(1).regionEqual(new GridGgfsFileAffinityRange(10, 19, affKey)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRangeUpdate1() throws Exception {
        GridGgfsFileMap map = new GridGgfsFileMap();

        IgniteUuid affKey = IgniteUuid.randomUuid();

        for (int i = 0; i < 4; i++)
            map.addRange(new GridGgfsFileAffinityRange(i * 20 + 10, i * 20 + 19, affKey));

        // Middle, first, last.
        map.updateRangeStatus(new GridGgfsFileAffinityRange(30, 39, affKey), RANGE_STATUS_MOVING);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(10, 19, affKey), RANGE_STATUS_MOVING);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(70, 79, affKey), RANGE_STATUS_MOVING);

        List<GridGgfsFileAffinityRange> ranges = map.ranges();

        assertEquals(RANGE_STATUS_MOVING, ranges.get(0).status());
        assertEquals(RANGE_STATUS_MOVING, ranges.get(1).status());
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(2).status());
        assertEquals(RANGE_STATUS_MOVING, ranges.get(3).status());

        // Middle, first, last.
        map.updateRangeStatus(new GridGgfsFileAffinityRange(30, 39, affKey), RANGE_STATUS_MOVED);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(10, 19, affKey), RANGE_STATUS_MOVED);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(70, 79, affKey), RANGE_STATUS_MOVED);

        ranges = map.ranges();

        assertEquals(RANGE_STATUS_MOVED, ranges.get(0).status());
        assertEquals(RANGE_STATUS_MOVED, ranges.get(1).status());
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(2).status());
        assertEquals(RANGE_STATUS_MOVED, ranges.get(3).status());

        // Middle, first, last.
        map.deleteRange(new GridGgfsFileAffinityRange(30, 39, affKey));
        map.deleteRange(new GridGgfsFileAffinityRange(10, 19, affKey));
        map.deleteRange(new GridGgfsFileAffinityRange(70, 79, affKey));

        ranges = map.ranges();

        assertEquals(1, ranges.size());
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(0).status());
        assertTrue(ranges.get(0).regionEqual(new GridGgfsFileAffinityRange(50, 59, affKey)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRangeUpdate2() throws Exception {
        GridGgfsFileMap map = new GridGgfsFileMap();

        IgniteUuid affKey = IgniteUuid.randomUuid();

        for (int i = 0; i < 4; i++)
            map.addRange(new GridGgfsFileAffinityRange(i * 20 + 10, i * 20 + 19, affKey));

        // Middle, first, last.
        map.updateRangeStatus(new GridGgfsFileAffinityRange(30, 35, affKey), RANGE_STATUS_MOVING);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(10, 15, affKey), RANGE_STATUS_MOVING);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(70, 75, affKey), RANGE_STATUS_MOVING);

        List<GridGgfsFileAffinityRange> ranges = map.ranges();

        assertEquals(7, ranges.size());

        int idx = 0;
        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(10, 15, affKey)));
        assertEquals(RANGE_STATUS_MOVING, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(16, 19, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(30, 35, affKey)));
        assertEquals(RANGE_STATUS_MOVING, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(36, 39, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(50, 59, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(70, 75, affKey)));
        assertEquals(RANGE_STATUS_MOVING, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(76, 79, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());

        // Middle, first, last.
        map.updateRangeStatus(new GridGgfsFileAffinityRange(30, 35, affKey), RANGE_STATUS_MOVED);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(10, 15, affKey), RANGE_STATUS_MOVED);
        map.updateRangeStatus(new GridGgfsFileAffinityRange(70, 75, affKey), RANGE_STATUS_MOVED);

        idx = 0;
        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(10, 15, affKey)));
        assertEquals(RANGE_STATUS_MOVED, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(16, 19, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(30, 35, affKey)));
        assertEquals(RANGE_STATUS_MOVED, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(36, 39, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(50, 59, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(70, 75, affKey)));
        assertEquals(RANGE_STATUS_MOVED, ranges.get(idx).status());
        idx++;

        assertTrue(ranges.get(idx).regionEqual(new GridGgfsFileAffinityRange(76, 79, affKey)));
        assertEquals(RANGE_STATUS_INITIAL, ranges.get(idx).status());

        // Middle, first, last.
        map.deleteRange(new GridGgfsFileAffinityRange(30, 35, affKey));
        map.deleteRange(new GridGgfsFileAffinityRange(10, 15, affKey));
        map.deleteRange(new GridGgfsFileAffinityRange(70, 75, affKey));

        ranges = map.ranges();

        assertEquals(4, ranges.size());

        assertEquals(RANGE_STATUS_INITIAL, ranges.get(0).status());
        assertTrue(ranges.get(0).regionEqual(new GridGgfsFileAffinityRange(16, 19, affKey)));

        assertEquals(RANGE_STATUS_INITIAL, ranges.get(1).status());
        assertTrue(ranges.get(1).regionEqual(new GridGgfsFileAffinityRange(36, 39, affKey)));

        assertEquals(RANGE_STATUS_INITIAL, ranges.get(2).status());
        assertTrue(ranges.get(2).regionEqual(new GridGgfsFileAffinityRange(50, 59, affKey)));

        assertEquals(RANGE_STATUS_INITIAL, ranges.get(3).status());
        assertTrue(ranges.get(3).regionEqual(new GridGgfsFileAffinityRange(76, 79, affKey)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidRangeUpdates() throws Exception {
        final GridGgfsFileMap map = new GridGgfsFileMap();

        final IgniteUuid affKey1 = IgniteUuid.randomUuid();
        final IgniteUuid affKey2 = IgniteUuid.randomUuid();

        map.addRange(new GridGgfsFileAffinityRange(10, 19, affKey1));
        map.addRange(new GridGgfsFileAffinityRange(30, 39, affKey1));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                map.updateRangeStatus(new GridGgfsFileAffinityRange(0, 5, affKey1), RANGE_STATUS_MOVING);

                return null;
            }
        }, GridGgfsInvalidRangeException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                map.updateRangeStatus(new GridGgfsFileAffinityRange(15, 19, affKey1), RANGE_STATUS_MOVING);

                return null;
            }
        }, GridGgfsInvalidRangeException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                map.updateRangeStatus(new GridGgfsFileAffinityRange(10, 19, affKey2), RANGE_STATUS_MOVING);

                return null;
            }
        }, AssertionError.class, null);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                map.updateRangeStatus(new GridGgfsFileAffinityRange(10, 22, affKey1), RANGE_STATUS_MOVING);

                return null;
            }
        }, AssertionError.class, null);

        assertEquals(2, map.ranges().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRangeSplit() throws Exception {
        IgniteUuid affKey = IgniteUuid.randomUuid();

        GridGgfsFileAffinityRange range = new GridGgfsFileAffinityRange(0, 9999, affKey);

        Collection<GridGgfsFileAffinityRange> split = range.split(10000);

        assertEquals(1, split.size());
        assertTrue(range.regionEqual(F.first(split)));

        split = range.split(5000);

        assertEquals(2, split.size());

        Iterator<GridGgfsFileAffinityRange> it = split.iterator();

        GridGgfsFileAffinityRange part = it.next();

        assertTrue(part.regionEqual(new GridGgfsFileAffinityRange(0, 4999, affKey)));

        part = it.next();

        assertTrue(part.regionEqual(new GridGgfsFileAffinityRange(5000, 9999, affKey)));

        split = range.split(3000);

        assertEquals(4, split.size());

        it = split.iterator();

        part = it.next();

        assertTrue(part.regionEqual(new GridGgfsFileAffinityRange(0, 2999, affKey)));

        part = it.next();

        assertTrue(part.regionEqual(new GridGgfsFileAffinityRange(3000, 5999, affKey)));

        part = it.next();

        assertTrue(part.regionEqual(new GridGgfsFileAffinityRange(6000, 8999, affKey)));

        part = it.next();

        assertTrue(part.regionEqual(new GridGgfsFileAffinityRange(9000, 9999, affKey)));
    }
}
