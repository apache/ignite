/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.util.*;

import static org.jdk8.backport.ConcurrentLinkedDeque8.*;

/**
 * Tests for {@link ConcurrentLinkedDeque8}.
 *
 * TODO: add more tests http://gridgain.jira.com/browse/GG-2153
 */
public class GridConcurrentLinkedDequeSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testPoll() throws Exception {
        ConcurrentLinkedDeque8<Integer> deque = new ConcurrentLinkedDeque8<>();

        deque.add(1);
        deque.add(2);
        deque.add(3);

        assert !deque.isEmpty();

        checkSize(deque, 3);

        // Poll 1.
        assertEquals(Integer.valueOf(1), deque.poll());

        assert !deque.isEmpty();

        checkSize(deque, 2);

        // Poll 2.
        assertEquals(Integer.valueOf(2), deque.poll());

        assert !deque.isEmpty();

        checkSize(deque, 1);

        // Poll 3.
        assertEquals(Integer.valueOf(3), deque.poll());

        assert deque.isEmpty();

        checkSize(deque, 0);
    }

    /**
     *
     */
    public void testUnlink() {
        ConcurrentLinkedDeque8<Integer> deque = new ConcurrentLinkedDeque8<>();

        Node<Integer> n1 = deque.addx(1);
        Node<Integer> n2 = deque.addx(2);
        Node<Integer> n3 = deque.addx(3);
        Node<Integer> n4 = deque.addx(4);
        Node<Integer> n5 = deque.addx(5);

        deque.unlinkx(n2);

        checkSize(deque, 4);

        // Double unlinkx() call.
        deque.unlinkx(n2);

        checkSize(deque, 4);

        Iterator<Integer> iter = deque.iterator();

        boolean hasNext = iter.hasNext();

        assert hasNext;

        Integer next = iter.next();

        assert next == 1;

        iter.remove();

        // Iterator should have set item to null.
        assert n1.item() == null;

        checkSize(deque, 3);

        // Double unlinkx() call.
        deque.unlinkx(n1);

        checkSize(deque, 3);

        deque.unlinkx(n3);
        deque.unlinkx(n4);
        deque.unlinkx(n5);

        checkSize(deque, 0);
    }

    /**
     *
     */
    public void testEmptyDeque() {
        ConcurrentLinkedDeque8<Integer> deque = new ConcurrentLinkedDeque8<>();

        assert deque.poll() == null;
        assert deque.pollFirst() == null;
        assert deque.pollLast() == null;

        assert deque.peek() == null;
        assert deque.peekFirst() == null;
        assert deque.peekLast() == null;

        checkSize(deque, 0);
    }

    /**
     * @param q Deque.
     * @param expSize Expected size.
     */
    @SuppressWarnings({"ForLoopReplaceableByForEach"})
    private <T> void checkSize(ConcurrentLinkedDeque8<T> q, int expSize) {
        int actualSize = 0;

        for (Iterator<T> iter = q.iterator(); iter.hasNext();) {
            iter.next();

            actualSize++;
        }

        assertEquals(expSize, actualSize);

        actualSize = 0;

        for (Iterator<T> iter = q.iterator(); iter.hasNext();) {
            iter.next();

            actualSize++;
        }

        assertEquals(expSize, actualSize);

        assertEquals(expSize, q.size());

        assertEquals(expSize, q.sizex());

        if (expSize > 0) {
            assert !q.isEmpty();

            assert !q.isEmptyx();
        }
        else {
            assert q.isEmpty();

            assert q.isEmptyx();
        }
    }

    /**
     *
     */
    public void testUnlinkWithIterator() {
        ConcurrentLinkedDeque8<Integer> q = new ConcurrentLinkedDeque8<>();

        q.add(1);
        Node<Integer> n2 = q.addx(2);
        Node<Integer> n3 = q.addx(3);
        Node<Integer> n4 = q.addx(4);
        Node<Integer> n5 = q.addx(5);
        q.add(6);

        Iterator<Integer> it = q.iterator();

        assertTrue(it.hasNext());
        assertEquals(1, it.next().intValue());

        assertTrue(it.hasNext());
        assertEquals(2, it.next().intValue());

        assertTrue(it.hasNext());
        assertEquals(3, it.next().intValue());

        q.unlinkx(n2);
        q.unlinkx(n3);
        q.unlinkx(n4);
        q.unlinkx(n5);

        assertTrue(it.hasNext());
        assertEquals(4, it.next().intValue());

        assertTrue(it.hasNext());
        assertEquals(6, it.next().intValue());
    }

    /**
     *
     */
    public void testUnlinkLastWithIterator() {
        ConcurrentLinkedDeque8<Integer> q = new ConcurrentLinkedDeque8<>();

        q.add(1);
        q.addx(2);
        Node<Integer> n3 = q.addx(3);

        Iterator<Integer> it = q.iterator();

        assertTrue(it.hasNext());
        assertEquals(1, it.next().intValue());

        q.unlinkx(n3);

        assertTrue(it.hasNext());
        assertEquals(2, it.next().intValue());

        assertFalse(it.hasNext());
    }
}
