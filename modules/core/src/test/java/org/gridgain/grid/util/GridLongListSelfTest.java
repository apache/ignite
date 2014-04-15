/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import junit.framework.*;

import static org.gridgain.grid.util.GridLongList.*;

/**
 *
 */
public class GridLongListSelfTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public void testCopyWithout() throws Exception {
        assertCopy(
            new GridLongList(new long[] {}),
            new GridLongList(new long[] {}));

        assertCopy(
            new GridLongList(new long[] {}),
            new GridLongList(new long[] {1}));

        assertCopy(
            new GridLongList(new long[] {1}),
            new GridLongList(new long[] {}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3}),
            new GridLongList(new long[] {4, 5, 6}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3}),
            new GridLongList(new long[] {1, 2, 3}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3, 4, 5, 1}),
            new GridLongList(new long[] {1, 1}));

        assertCopy(
            new GridLongList(new long[] {1, 1, 1, 2, 3, 4, 5, 1, 1, 1}),
            new GridLongList(new long[] {1, 1}));

        assertCopy(
            new GridLongList(new long[] {1, 2, 3}),
            new GridLongList(new long[] {1, 1, 2, 2, 3, 3}));
    }

    /**
     *
     */
    public void testTruncate() {
        GridLongList list = asList(1, 2, 3, 4, 5, 6, 7, 8);

        list.truncate(4, true);

        assertEquals(asList(1, 2, 3, 4), list);

        list.truncate(2, false);

        assertEquals(asList(3, 4), list);

        list = new GridLongList();

        list.truncate(0, false);
        list.truncate(0, true);

        assertEquals(new GridLongList(), list);
    }

    /**
     * Assert {@link GridLongList#copyWithout(GridLongList)} on given lists.
     *
     * @param lst Source lists.
     * @param rmv Exclude list.
     */
    private void assertCopy(GridLongList lst, GridLongList rmv) {
        GridLongList res = lst.copyWithout(rmv);

        for (int i = 0; i < lst.size(); i++) {
            long v = lst.get(i);

            if (rmv.contains(v))
                assertFalse(res.contains(v));
            else
                assertTrue(res.contains(v));
        }
    }

    /**
     *
     */
    public void testRemove() {
        GridLongList list = asList(1,2,3,4,5,6);

        assertEquals(2, list.removeValue(0, 3));
        assertEquals(asList(1,2,4,5,6), list);

        assertEquals(-1, list.removeValue(1, 1));
        assertEquals(-1, list.removeValue(0, 3));

        assertEquals(4, list.removeValue(0, 6));
        assertEquals(asList(1,2,4,5), list);

        assertEquals(2, list.removeIndex(1));
        assertEquals(asList(1,4,5), list);

        assertEquals(1, list.removeIndex(0));
        assertEquals(asList(4,5), list);
    }

    /**
     *
     */
    public void testSort() {
        assertEquals(new GridLongList(), new GridLongList().sort());
        assertEquals(asList(1), asList(1).sort());
        assertEquals(asList(1, 2), asList(2, 1).sort());
        assertEquals(asList(1, 2, 3), asList(2, 1, 3).sort());

        GridLongList list = new GridLongList();

        list.add(4);
        list.add(3);
        list.add(5);
        list.add(1);

        assertEquals(asList(1, 3, 4, 5), list.sort());

        list.add(0);

        assertEquals(asList(1, 3, 4, 5, 0), list);
        assertEquals(asList(0, 1, 3, 4, 5), list.sort());
    }
}
