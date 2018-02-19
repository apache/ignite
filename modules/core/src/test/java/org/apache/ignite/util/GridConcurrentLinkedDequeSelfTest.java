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

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.deque.LongSizeCountingDeque;


/**
 * Tests for {@link LongSizeCountingDeque}.
 */
public class GridConcurrentLinkedDequeSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testPoll() throws Exception {
        Deque<Integer> deque = new LongSizeCountingDeque<>(new ConcurrentLinkedDeque<>());

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
    public void testEmptyDeque() {
        Deque<Integer> deque = new LongSizeCountingDeque<>(new ConcurrentLinkedDeque<>());

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
    private <T> void checkSize(Deque<T> q, int expSize) {
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

        assertEquals(expSize, q.size());

        if (expSize > 0)
            assert !q.isEmpty();
        else
            assert q.isEmpty();
    }
}