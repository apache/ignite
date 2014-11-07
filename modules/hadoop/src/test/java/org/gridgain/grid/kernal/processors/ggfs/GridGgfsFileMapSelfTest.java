/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.ggfs.GridGgfsFileAffinityRange.*;

/**
 * File map self test.
 */
public class GridGgfsFileMapSelfTest extends GridGgfsCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testRanges() throws Exception {
        GridGgfsFileMap map = new GridGgfsFileMap();

        GridUuid[] affKeys = new GridUuid[20];

        for (int i = 0; i < affKeys.length; i++)
            affKeys[i] = GridUuid.randomUuid();

        int numOfRanges = 0;

        do {
            for (int i = 0; i < 2 * numOfRanges + 1; i++) {
                long off1 = i * 10;
                long off2 = i * 10 + 5;
                long off3 = i * 10 + 8;

                GridUuid affKey = i % 2 == 0 ? null : affKeys[i / 2];

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

        GridUuid affKey = GridUuid.randomUuid();

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

        GridUuid affKey = GridUuid.randomUuid();

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

        GridUuid affKey = GridUuid.randomUuid();

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

        final GridUuid affKey1 = GridUuid.randomUuid();
        final GridUuid affKey2 = GridUuid.randomUuid();

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
        GridUuid affKey = GridUuid.randomUuid();

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
