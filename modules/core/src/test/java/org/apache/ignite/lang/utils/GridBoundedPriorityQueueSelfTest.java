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

package org.apache.ignite.lang.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.ignite.internal.util.GridBoundedPriorityQueue;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Test for {@link GridBoundedPriorityQueue}.
 */
@GridCommonTest(group = "Lang")
public class GridBoundedPriorityQueueSelfTest extends GridCommonAbstractTest {
    /** Queue items comparator. */
    private static final Comparator<Integer> CMP = new Comparator<Integer>() {
        @Override public int compare(Integer o1, Integer o2) {
            return Integer.compare(o1, o2);
        }
    };

    /** Queue items reverse order comparator. */
    private static final Comparator<Integer> CMP_REVERSE = Collections.reverseOrder(CMP);

    /**
     * Test eviction in bounded priority queue.
     */
    @Test
    public void testEviction() {
        GridBoundedPriorityQueue<Integer> queue = new GridBoundedPriorityQueue<>(3, CMP);

        assertTrue(queue.offer(2));
        assertTrue(queue.offer(1));
        assertTrue(queue.offer(3));
        assertTrue(queue.offer(4));

        assertEquals(3, queue.size());

        assertFalse(queue.offer(0)); // Item with lower priority should not be put into queue.

        assertEquals(3, queue.size());

        assertEquals(Integer.valueOf(2), queue.poll());
        assertEquals(Integer.valueOf(3), queue.poll());
        assertEquals(Integer.valueOf(4), queue.poll());

        assertNull(queue.poll());
        assertTrue(queue.isEmpty());

        // Test queue with reverse comparator.
        GridBoundedPriorityQueue<Integer> queueReverse = new GridBoundedPriorityQueue<>(3, CMP_REVERSE);

        assertTrue(queueReverse.offer(2));
        assertTrue(queueReverse.offer(1));
        assertTrue(queueReverse.offer(3));

        assertFalse(queueReverse.offer(4)); // Item with lower priority should not be put into queue.

        assertEquals(Integer.valueOf(3), queueReverse.poll());
        assertEquals(Integer.valueOf(2), queueReverse.poll());
        assertEquals(Integer.valueOf(1), queueReverse.poll());

        assertNull(queueReverse.poll());
        assertTrue(queueReverse.isEmpty());

        // Test put random 100 items into GridBoundedPriorityQueue(10) and check result.
        queue = new GridBoundedPriorityQueue<>(10, CMP);
        queueReverse = new GridBoundedPriorityQueue<>(10, CMP_REVERSE);

        Random rnd = new Random();

        List<Integer> items = new ArrayList<>(100);

        for (int i = 0; i < 100; i++) {
            Integer item = rnd.nextInt(100);

            items.add(item);
            queue.offer(item);
            queueReverse.offer(item);
        }

        Collections.sort(items);

        for (int i = 0; i < 10; i++) {
            assertEquals(items.get(90 + i), queue.poll());
            assertEquals(items.get(9 - i), queueReverse.poll());
        }

        assertNull(queue.poll());
        assertTrue(queue.isEmpty());

        assertNull(queueReverse.poll());
        assertTrue(queueReverse.isEmpty());
    }
}
