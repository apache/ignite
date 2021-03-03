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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.apache.ignite.internal.processors.cache.persistence.pagemem.SegmentedLruPageList.NULL_IDX;

/**
 * Test Segmented-LRU list implementation.
 */
public class SegmentedLruPageListTest extends GridCommonAbstractTest {
    /** Max pages count. */
    private static final int MAX_PAGES_CNT = 20;

    /** Memory provider. */
    private static DirectMemoryProvider provider;

    /** Memory region. */
    private static DirectMemoryRegion region;

    /** LRU list. */
    SegmentedLruPageList lru;

    /** Test watcher. */
    @Rule public TestRule testWatcher = new TestWatcher() {
        @Override protected void failed(Throwable e, Description description) {
            dump();
        }
    };

    /** */
    @BeforeClass
    public static void setUp() {
        provider = new UnsafeMemoryProvider(log);
        provider.initialize(new long[] {SegmentedLruPageList.requiredMemory(MAX_PAGES_CNT)});

        region = provider.nextRegion();
    }

    /** */
    @AfterClass
    public static void tearDown() {
        provider.shutdown(true);
    }

    /** */
    @Test
    public void testAdd() {
        // Check start with probationary page.
        lru = new SegmentedLruPageList(MAX_PAGES_CNT, region.address());

        addToTail(0, false);
        assertProbationarySegment(0);
        assertProtectedSegment();

        addToTail(1, true);
        assertProbationarySegment(0);
        assertProtectedSegment(1);

        addToTail(2, false);
        assertProbationarySegment(0, 2);
        assertProtectedSegment(1);

        addToTail(3, true);
        assertProbationarySegment(0, 2);
        assertProtectedSegment(1, 3);

        // Check start with protected page.
        lru = new SegmentedLruPageList(MAX_PAGES_CNT, region.address());

        addToTail(0, true);
        assertProbationarySegment();
        assertProtectedSegment(0);

        addToTail(1, false);
        assertProbationarySegment(1);
        assertProtectedSegment(0);

        addToTail(2, true);
        assertProbationarySegment(1);
        assertProtectedSegment(0, 2);

        addToTail(3, false);
        assertProbationarySegment(1, 3);
        assertProtectedSegment(0, 2);
    }

    /** */
    @Test
    public void testRemove() {
        lru = new SegmentedLruPageList(MAX_PAGES_CNT, region.address());

        addToTail(0, false);
        addToTail(1, true);
        addToTail(2, false);
        addToTail(3, true);
        addToTail(4, false);
        addToTail(5, true);

        remove(0); // Head.
        assertProbationarySegment(2, 4);
        assertProtectedSegment(1, 3, 5);

        remove(5); // Tail.
        assertProbationarySegment(2, 4);
        assertProtectedSegment(1, 3);

        remove(1); // Protected segment head.
        assertProbationarySegment(2, 4);
        assertProtectedSegment(3);

        remove(4); // Probationary segment tail.
        assertProbationarySegment(2);
        assertProtectedSegment(3);

        remove(2); // Last probationary segment item.
        assertProbationarySegment();
        assertProtectedSegment(3);

        remove(3); // Last potected segment and LRU item.
        assertProbationarySegment();
        assertProtectedSegment();

        addToTail(2, false);
        addToTail(3, true);

        remove(3); // Last protected segment item.
        assertProbationarySegment(2);
        assertProtectedSegment();

        remove(2); // Last probationary segment and LRU item.
        assertProbationarySegment();
        assertProtectedSegment();
    }

    /** */
    @Test
    public void testPoll() {
        lru = new SegmentedLruPageList(MAX_PAGES_CNT, region.address());

        assertEquals(NULL_IDX, poll());

        addToTail(0, false);
        addToTail(1, true);
        addToTail(2, false);
        addToTail(3, true);

        assertEquals(0, poll());
        assertEquals(2, poll());
        assertEquals(1, poll());
        assertEquals(3, poll());
        assertEquals(NULL_IDX, poll());
    }

    /** */
    @Test
    public void testMoveToTail() {
        lru = new SegmentedLruPageList(MAX_PAGES_CNT, region.address());

        addToTail(0, false);
        addToTail(1, true);
        addToTail(2, false);
        addToTail(3, true);

        moveToTail(3);
        assertProbationarySegment(0, 2);
        assertProtectedSegment(1, 3);

        moveToTail(2);
        assertProbationarySegment(0);
        assertProtectedSegment(1, 3, 2);

        moveToTail(1);
        assertProbationarySegment(0);
        assertProtectedSegment(3, 2, 1);

        moveToTail(0);
        assertProbationarySegment();
        assertProtectedSegment(3, 2, 1, 0);
    }

    /** */
    @Test
    public void testProtectedToProbationaryMigration() {
        lru = new SegmentedLruPageList(6, region.address());

        assertEquals(3, lru.protectedPagesLimit());

        addToTail(0, true);
        addToTail(1, true);
        addToTail(2, true);

        assertProbationarySegment();
        assertProtectedSegment(0, 1, 2);

        addToTail(3, true);
        assertProbationarySegment(0);
        assertProtectedSegment(1, 2, 3);

        addToTail(4, true);
        assertProbationarySegment(0, 1);
        assertProtectedSegment(2, 3, 4);
    }

    /** */
    private void addToTail(int pageIdx, boolean protectedPage) {
        lru.addToTail(pageIdx, protectedPage);

        checkInvariants();
    }

    /** */
    private void remove(int pageIdx) {
        lru.remove(pageIdx);

        checkInvariants();
    }

    /** */
    private int poll() {
        int idx = lru.poll();

        checkInvariants();

        return idx;
    }

    /** */
    private void moveToTail(int pageIdx) {
        lru.moveToTail(pageIdx);

        checkInvariants();
    }

    /** */
    private void assertProbationarySegment(int... pageIdxs) {
        assertTrue((lru.probTailIdx() != NULL_IDX) ^ F.isEmpty(pageIdxs));

        int curIdx = lru.headIdx();

        for (int pageIdx : pageIdxs) {
            assertEquals(pageIdx, curIdx);

            curIdx = (curIdx == lru.probTailIdx()) ? NULL_IDX : lru.next(curIdx);
        }

        if (!F.isEmpty(pageIdxs))
            assertTrue(curIdx == NULL_IDX);
    }

    /** */
    private void assertProtectedSegment(int... pageIdxs) {
        int curIdx = lru.headIdx();

        if (lru.probTailIdx() != NULL_IDX)
            curIdx = lru.next(lru.probTailIdx());

        assertTrue((curIdx != NULL_IDX) ^ F.isEmpty(pageIdxs));

        for (int pageIdx : pageIdxs) {
            assertEquals(pageIdx, curIdx);

            curIdx = lru.next(curIdx);
        }

        assertEquals(pageIdxs.length, lru.protectedPagesCount());
    }

    /**
     * Check LRU list invariants.
     */
    private void checkInvariants() {
        int limit = MAX_PAGES_CNT + 1;

        int curIdx = lru.headIdx();
        int protectedCnt = 0;
        boolean protectedPage = (lru.probTailIdx() == NULL_IDX);

        if (lru.headIdx() == NULL_IDX || lru.tailIdx() == NULL_IDX) {
            assertEquals(NULL_IDX, lru.headIdx());
            assertEquals(NULL_IDX, lru.tailIdx());
            assertEquals(NULL_IDX, lru.probTailIdx());
            assertEquals(0, lru.protectedPagesCount());
        }

        while (curIdx != NULL_IDX && limit-- > 0) {
            int prev = lru.prev(curIdx);
            int next = lru.next(curIdx);

            if (prev == NULL_IDX)
                assertEquals(lru.headIdx(), curIdx);
            else
                assertEquals(curIdx, lru.next(prev));

            if (next == NULL_IDX)
                assertEquals(lru.tailIdx(), curIdx);
            else
                assertEquals(curIdx, lru.prev(next));

            assertEquals(protectedPage, lru.protectedPage(curIdx));

            if (protectedPage)
                protectedCnt++;

            if (curIdx == lru.probTailIdx())
                protectedPage = true;

            curIdx = next;
        }

        assertTrue(limit > 0);

        assertEquals(protectedCnt, lru.protectedPagesCount());
        assertTrue(protectedCnt <= lru.protectedPagesLimit());
    }

    /**
     * Dump LRU list content.
     */
    private void dump() {
        int limit = MAX_PAGES_CNT;

        log.info(String.format("LRU list dump [headPtr=%d, probTailPtr=%d, tailPtr=%d, protectedCnt=%d]",
            lru.headIdx(), lru.probTailIdx(), lru.tailIdx(), lru.protectedPagesCount()));

        int curIdx = lru.headIdx();

        while (curIdx != NULL_IDX && limit-- > 0) {
            log.info(String.format("    Page %d [prev=%d, next=%d, protected=%b]%s", curIdx,
                lru.prev(curIdx), lru.next(curIdx),
                lru.protectedPage(curIdx), curIdx == lru.probTailIdx() ? " <- probationary list tail" : ""));

            curIdx = lru.next(curIdx);
        }

        if (limit == 0)
            log.info("...");
    }
}
