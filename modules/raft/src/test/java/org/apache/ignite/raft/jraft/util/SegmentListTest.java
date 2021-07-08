/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SegmentListTest {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentListTest.class);

    private SegmentList<Integer> list;

    @BeforeEach
    public void setup() {
        this.list = new SegmentList<>(true);
    }

    @Test
    public void testAddGet() {
        assertTrue(this.list.isEmpty());
        fillList();
        assertFilledList();
        System.out.println(this.list);
    }

    private void assertFilledList() {
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, (int) this.list.get(i));
        }
        assertEquals(1000, this.list.size());
        assertFalse(this.list.isEmpty());
        assertEquals(1000 / SegmentList.SEGMENT_SIZE + 1, this.list.segmentSize());
    }

    private void fillList() {
        int originSize = this.list.size();
        for (int i = 0; i < 1000; i++) {
            this.list.add(i);
            assertEquals(originSize + i + 1, this.list.size());
        }
    }

    @Test
    public void testAddAllGet() {
        List<Integer> tmpList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            tmpList.add(i);
        }

        this.list.addAll(tmpList);
        assertFilledList();

        this.list.removeFromFirstWhen(x -> x < 100);
        assertEquals(900, this.list.size());

        this.list.addAll(tmpList);
        assertEquals(1900, this.list.size());

        for (int i = 0; i < 1900; i++) {
            if (i < 900) {
                assertEquals(i + 100, (int) this.list.get(i));
            }
            else {
                assertEquals(i - 900, (int) this.list.get(i));
            }
        }

    }

    @Test
    public void testRemoveFromFirst() {
        fillList();

        int len = SegmentList.SEGMENT_SIZE - 1;
        this.list.removeFromFirst(len);

        assertEquals(1000 - len, this.list.size());

        for (int i = 0; i < 1000 - len; i++) {
            assertEquals(i + len, (int) this.list.get(i));
        }

        this.list.removeFromFirst(100);
        assertEquals(1000 - len - 100, this.list.size());

        for (int i = 0; i < 1000 - len - 100; i++) {
            assertEquals(i + len + 100, (int) this.list.get(i));
        }

        this.list.removeFromFirst(1000 - len - 100);
        assertTrue(this.list.isEmpty());
        assertEquals(0, this.list.segmentSize());
        assertNull(this.list.peekFirst());
        assertNull(this.list.peekLast());
    }

    @Test
    public void testRemoveFromFirstWhen() {
        fillList();
        this.list.removeFromFirstWhen(x -> x < 200);
        assertEquals(800, this.list.size());
        assertEquals(200, (int) this.list.get(0));

        for (int i = 0; i < 800; i++) {
            assertEquals(200 + i, (int) this.list.get(i));
        }

        this.list.removeFromFirstWhen(x -> x < 500);
        assertEquals(500, this.list.size());
        for (int i = 0; i < 500; i++) {
            assertEquals(500 + i, (int) this.list.get(i));
        }

        this.list.removeFromFirstWhen(x -> x < 1000);
        assertTrue(this.list.isEmpty());
        assertEquals(0, this.list.segmentSize());

        fillList();
        assertFilledList();
    }

    @Test
    public void testRemoveFromLastWhen() {
        fillList();

        // remove elements is greater or equal to 150.
        this.list.removeFromLastWhen(x -> x >= 150);
        assertEquals(150, this.list.size());
        assertFalse(this.list.isEmpty());
        for (int i = 0; i < 150; i++) {
            assertEquals(i, (int) this.list.get(i));
        }
        try {
            this.list.get(151);
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            // No-op.
        }
        assertEquals(150 / SegmentList.SEGMENT_SIZE + 1, this.list.segmentSize());

        // remove  elements is greater or equal to 32.
        this.list.removeFromLastWhen(x -> x >= 32);
        assertEquals(32, this.list.size());
        assertFalse(this.list.isEmpty());
        for (int i = 0; i < 32; i++) {
            assertEquals(i, (int) this.list.get(i));
        }
        try {
            this.list.get(32);
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            // No-op.
        }
        assertEquals(1, this.list.segmentSize());

        // Add elements again.
        fillList();
        assertEquals(1032, this.list.size());
        for (int i = 0; i < 1032; i++) {
            if (i < 32) {
                assertEquals(i, (int) this.list.get(i));
            }
            else {
                assertEquals(i - 32, (int) this.list.get(i));
            }
        }
    }

    @Test
    public void testAddPeek() {
        for (int i = 0; i < 1000; i++) {
            this.list.add(i);
            assertEquals(i, (int) this.list.peekLast());
            assertEquals(0, (int) this.list.peekFirst());
        }
    }

    @Test
    public void simpleBenchmark() {
        int warmupRepeats = 10_0000;
        int repeats = 100_0000;

        double arrayDequeOps;
        double segListOps;
        // test ArrayDequeue
        {
            ArrayDeque<Integer> deque = new ArrayDeque<>();
            System.gc();
            // wramup
            benchArrayDequeue(warmupRepeats, deque);
            deque.clear();
            System.gc();
            long startNs = System.nanoTime();
            benchArrayDequeue(repeats, deque);
            long costMs = (System.nanoTime() - startNs) / repeats;
            arrayDequeOps = repeats * 3.0 / costMs * 1000;
            LOG.info("ArrayDeque, cost:" + costMs + ", ops: " + arrayDequeOps);
        }
        // test SegmentList
        {
            System.gc();
            // warmup
            benchSegmentList(warmupRepeats);

            this.list.clear();
            System.gc();

            long startNs = System.nanoTime();
            benchSegmentList(repeats);
            long costMs = (System.nanoTime() - startNs) / repeats;
            segListOps = repeats * 3.0 / costMs * 1000;
            System.out.println("SegmentList, cost:" + costMs + ", ops: " + segListOps);
            this.list.clear();
        }

        System.out.println("Improvement:" + Math.round((segListOps - arrayDequeOps) / arrayDequeOps * 100) + "%");

    }

    private void benchArrayDequeue(final int repeats, final ArrayDeque<Integer> deque) {
        int start = 0;
        for (int i = 0; i < repeats; i++) {
            List<Integer> tmpList = genData(start);
            deque.addAll(tmpList);
            //            for (Integer o : tmpList) {
            //                deque.add(o);
            //            }
            int removePos = start + ThreadLocalRandom.current().nextInt(tmpList.size());

            deque.get(removePos - start);

            int index = 0;
            for (int j = 0; j < deque.size(); j++) {
                if (deque.get(j) > removePos) {
                    index = j;
                    break;
                }
            }

            if (index > 0) {
                deque.removeRange(0, index);
            }

            start += tmpList.size();
        }
    }

    private List<Integer> genData(final int start) {
        int n = ThreadLocalRandom.current().nextInt(500) + 10;
        List<Integer> tmpList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tmpList.add(i + start);
        }
        return tmpList;
    }

    private void benchSegmentList(final int repeats) {
        int start = 0;

        for (int i = 0; i < repeats; i++) {
            List<Integer> tmpList = genData(start);
            this.list.addAll(tmpList);
            //            for(Integer o: tmpList) {
            //                list.add(o);
            //            }
            int removePos = start + ThreadLocalRandom.current().nextInt(tmpList.size());
            this.list.get(removePos - start);
            this.list.removeFromFirstWhen(x -> x <= removePos);
            start += tmpList.size();
        }
    }
}
