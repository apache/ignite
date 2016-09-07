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

package org.apache.ignite.internal.util.offheap.unsafe;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Striped LRU test.
 */
@SuppressWarnings("FieldCanBeLocal")
public class GridUnsafeStripedLruSefTest extends GridCommonAbstractTest {
    /** Number of stripes. */
    private short stripes = 1;

    /** Memory size. */
    private long memSize = 1024 * 1024;

    /** */
    private GridUnsafeMemory mem;

    /** */
    private GridUnsafeLru lru;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stripes = 1;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (lru != null)
            lru.destruct();

        lru = null;
        mem = null;
    }

    /**
     *
     */
    private void init() {
        mem = new GridUnsafeMemory(memSize);

        lru = new GridUnsafeLru(stripes, mem);
    }

    /**
     *
     */
    public void testOffer1() {
        checkOffer(1000);
    }

    /**
     *
     */
    public void testOffer2() {
        stripes = 11;

        checkOffer(1000);
    }

    /**
     * @param cnt Count.
     */
    private void checkOffer(int cnt) {
        init();

        for (int i = 0; i < cnt; i++)
            lru.offer(0, i, i);

        assertEquals(cnt, lru.size());

        info("Finished check offer for stripes count: " + stripes);
    }

    /**
     *
     */
    public void testRemove1() {
        checkRemove(1000);
    }

    /**
     *
     */
    public void testRemove2() {
        stripes = 35;

        checkRemove(1000);
    }

    /**
     * @param cnt Count.
     */
    private void checkRemove(int cnt) {
        init();

        Collection<Long> set = new HashSet<>(cnt);

        for (int i = 0; i < cnt; i++)
            assertTrue(set.add(lru.offer(0, i, i)));

        assertEquals(cnt, lru.size());

        for (long addr : set)
            lru.remove(addr);

        assertEquals(0, lru.size());
    }

    /**
     *
     */
    public void testPoll1() {
        checkPoll(1000);
    }

    /**
     *
     */
    public void testPoll2() {
        stripes = 20;

        checkPoll(1000);
    }

    /**
     * @param cnt Count.
     */
    private void checkPoll(int cnt) {
        init();

        int step = 10;

        assert cnt % step == 0;

        Collection<Long> set = new HashSet<>(step);

        for (int i = 0; i < cnt; i++)
            lru.offer(0, i, i);

        assertEquals(cnt, lru.size());

        for (int i = 0; i < cnt; i += step) {
            for (int j = 0; j < step; j++) {
                long qAddr = lru.prePoll();

                assertTrue(qAddr != 0);
                assertTrue(set.add(qAddr));
            }

            for (long qAddr : set)
                lru.poll(qAddr);

            set.clear();
        }

        assertEquals(0, lru.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLruMultithreaded() throws Exception {
        checkLruMultithreaded(1000000);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkLruMultithreaded(final int cnt) throws Exception {
        init();

        final AtomicInteger idGen = new AtomicInteger();

        multithreaded(new Runnable() {
            @Override public void run() {
                int id = idGen.getAndIncrement();

                int step = 10;

                assert cnt % step == 0;

                int start = id * cnt;
                int end = start + cnt;

                Collection<Long> set = new HashSet<>(step);

                for (int i = start; i < end; i++)
                    lru.offer(0, i, i);

                for (int i = start; i < end; i += step) {
                    for (int j = 0; j < step; j++) {
                        long qAddr = lru.prePoll();

                        assertTrue(qAddr != 0);
                        assertTrue(set.add(qAddr));
                    }

                    for (long qAddr : set)
                        lru.poll(qAddr);

                    set.clear();
                }
            }
        }, 10);

        assertEquals(0, lru.size());
    }
}