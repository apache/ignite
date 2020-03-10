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

package org.apache.ignite.internal.processors.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL;
import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.rnd;
import static org.apache.ignite.internal.processors.database.BPlusTreeSelfTest.TestTree.threadId;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.NOOP;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.PUT;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.REMOVE;

/**
 */
@WithSystemProperty(key = IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL, value = "20000")
public class BPlusTreeSelfTest extends GridCommonAbstractTest {
    /** */
    private static final short LONG_INNER_IO = 30000;

    /** */
    private static final short LONG_LEAF_IO = 30001;

    /** */
    protected static final int PAGE_SIZE = 256;

    /** */
    protected static final long MB = 1024 * 1024;

    /** */
    protected static final int CPUS = Runtime.getRuntime().availableProcessors();

    /** */
    private static final int CACHE_ID = 100500;

    /** */
    protected static int MAX_PER_PAGE = 0;

    /** */
    protected static int CNT = 10;

    /** */
    private static int PUT_INC = 1;

    /** */
    private static int RMV_INC = 1;

    /** Forces printing lock/unlock events on the test tree */
    private static boolean PRINT_LOCKS = false;

    /** */
    protected PageMemory pageMem;

    /** */
    private ReuseList reuseList;

    /** */
    private static final Collection<Long> rmvdIds = new GridConcurrentHashSet<>();

    /** Stop. */
    private final AtomicBoolean stop = new AtomicBoolean();

    /** Future. */
    private volatile GridCompoundFuture<?, ?> asyncRunFut;

    /** Tracking of locks holding. */
    private PageLockTrackerManager lockTrackerManager;

    /**
     * Check that we do not keep any locks at the moment.
     */
    protected void assertNoLocks() {
        assertTrue(TestPageLockListener.checkNoLocks());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stop.set(false);

        long seed = System.nanoTime();

        X.println("Test seed: " + seed + "L; // ");

        rnd = new Random(seed);

        pageMem = createPageMemory();

        reuseList = createReuseList(CACHE_ID, pageMem, 0, true);

        lockTrackerManager = new PageLockTrackerManager(log, "testTreeManager");

        lockTrackerManager.start();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param rootId Root page ID.
     * @param initNew Init new flag.
     * @return Reuse list.
     * @throws IgniteCheckedException If failed.
     */
    protected ReuseList createReuseList(int cacheId, PageMemory pageMem, long rootId, boolean initNew)
        throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        rnd = null;

        try {
            if (asyncRunFut != null && !asyncRunFut.isDone()) {
                stop.set(true);

                try {
                    asyncRunFut.cancel();
                    asyncRunFut.get(60000);
                }
                catch (Throwable ex) {
                    //Ignore
                }
            }

            if (reuseList != null) {
                long size = reuseList.recycledPagesCount();

                assertTrue("Reuse size: " + size, size < 7000);
            }

            for (int i = 0; i < 10; i++) {
                if (acquiredPages() != 0) {
                    System.out.println("!!!");
                    U.sleep(10);
                }
            }

            assertEquals(0, acquiredPages());
        }
        finally {
            if (pageMem != null)
                pageMem.stop(true);

            if (lockTrackerManager != null)
                lockTrackerManager.stop();

            MAX_PER_PAGE = 0;
            PUT_INC = 1;
            RMV_INC = -1;
            CNT = 10;
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testFind() throws IgniteCheckedException {
        TestTree tree = createTestTree(true);
        TreeMap<Long, Long> map = new TreeMap<>();

        long size = CNT * CNT;

        for (long i = 1; i <= size; i++) {
            tree.put(i);
            map.put(i, i);
        }

        checkCursor(tree.find(null, null), map.values().iterator());
        checkCursor(tree.find(10L, 70L), map.subMap(10L, true, 70L, true).values().iterator());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRetries() throws IgniteCheckedException {
        TestTree tree = createTestTree(true);

        tree.numRetries = 1;

        long size = CNT * CNT;

        try {
            for (long i = 1; i <= size; i++)
                tree.put(i);

            fail();
        }
        catch (IgniteCheckedException ignored) {
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIsEmpty() throws Exception {
        TestTree tree = createTestTree(true);

        assertTrue(tree.isEmpty());

        for (long i = 1; i <= 500; i++) {
            tree.put(i);

            assertFalse(tree.isEmpty());
        }

        for (long i = 1; i <= 500; i++) {
            assertFalse(tree.isEmpty());

            tree.remove(i);
        }

        assertTrue(tree.isEmpty());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testFindWithClosure() throws IgniteCheckedException {
        TestTree tree = createTestTree(true);
        TreeMap<Long, Long> map = new TreeMap<>();

        long size = CNT * CNT;

        for (long i = 1; i <= size; i++) {
            tree.put(i);
            map.put(i, i);
        }

        checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(Collections.<Long>emptySet()), null),
            Collections.<Long>emptyList().iterator());

        checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(map.keySet()), null),
            map.values().iterator());

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 100; i++) {
            Long val = rnd.nextLong(size) + 1;

            checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(Collections.singleton(val)), null),
                Collections.singleton(val).iterator());
        }

        for (int i = 0; i < 200; i++) {
            long vals = rnd.nextLong(size) + 1;

            TreeSet<Long> exp = new TreeSet<>();

            for (long k = 0; k < vals; k++)
                exp.add(rnd.nextLong(size) + 1);

            checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(exp), null), exp.iterator());

            checkCursor(tree.find(0L, null, new TestTreeFindFilteredClosure(exp), null), exp.iterator());

            checkCursor(tree.find(0L, size, new TestTreeFindFilteredClosure(exp), null), exp.iterator());

            checkCursor(tree.find(null, size, new TestTreeFindFilteredClosure(exp), null), exp.iterator());
        }
    }

    /**
     * @param cursor cursor to check.
     * @param iterator iterator with expected result.
     * @throws IgniteCheckedException If failed
     */
    private void checkCursor(GridCursor<Long> cursor, Iterator<Long> iterator) throws IgniteCheckedException {
        while (cursor.next()) {
            assertTrue(iterator.hasNext());

            assertEquals(iterator.next(), cursor.get());
        }

        assertFalse(iterator.hasNext());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_mm_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_mm_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_pm_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_pm_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_pp_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_pp_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_mp_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_1_20_mp_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    // ------- 2 - 40
    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_mm_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_mm_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_pm_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_pm_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_pp_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_pp_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_mp_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_2_40_mp_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    // ------- 3 - 60
    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_mm_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_mm_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_pm_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_pm_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_pp_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_pp_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_mp_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testPutRemove_3_60_mp_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestPutRemove(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = createTestTree(canGetRow);

        long cnt = CNT;

        for (long x = PUT_INC > 0 ? 0 : cnt - 1; x >= 0 && x < cnt; x += PUT_INC) {
            assertNull(tree.findOne(x));

            tree.put(x);

            assertNoLocks();

            assertEquals(x, tree.findOne(x).longValue());
            checkIterate(tree, x, x, x, true);

            assertNoLocks();

            tree.validateTree();

            assertNoLocks();
        }

        X.println(tree.printTree());

        assertNoLocks();

        assertNull(tree.findOne(-1L));

        for (long x = 0; x < cnt; x++) {
            assertEquals(x, tree.findOne(x).longValue());
            checkIterate(tree, x, x, x, true);
        }

        assertNoLocks();

        assertNull(tree.findOne(cnt));
        checkIterate(tree, cnt, cnt, null, false);

        for (long x = RMV_INC > 0 ? 0 : cnt - 1; x >= 0 && x < cnt; x += RMV_INC) {
            X.println(" -- " + x);

            assertEquals(Long.valueOf(x), tree.remove(x));

            assertNoLocks();

            X.println(tree.printTree());

            assertNoLocks();

            assertNull(tree.findOne(x));
            checkIterate(tree, x, x, null, false);

            assertNoLocks();

            tree.validateTree();

            assertNoLocks();
        }

        assertFalse(tree.find(null, null).next());
        assertEquals(0, tree.size());
        assertEquals(0, tree.rootLevel());

        assertNoLocks();
    }

    /**
     * @param tree Tree.
     * @param lower Lower bound.
     * @param upper Upper bound.
     * @param exp Value to find.
     * @param expFound {@code True} if value should be found.
     * @throws IgniteCheckedException If failed.
     */
    private void checkIterate(TestTree tree, long lower, long upper, Long exp, boolean expFound)
        throws IgniteCheckedException {
        TestTreeRowClosure c = new TestTreeRowClosure(exp);

        tree.iterate(lower, upper, c);

        assertEquals(expFound, c.found);
    }

    /**
     * @param tree Tree.
     * @param lower Lower bound.
     * @param upper Upper bound.
     * @param c Closure.
     * @param expFound {@code True} if value should be found.
     * @throws IgniteCheckedException If failed.
     */
    private void checkIterateC(TestTree tree, long lower, long upper, TestTreeRowClosure c, boolean expFound)
        throws IgniteCheckedException {
        c.found = false;

        tree.iterate(lower, upper, c);

        assertEquals(expFound, c.found);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRandomInvoke_1_30_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomInvoke(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRandomInvoke_1_30_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomInvoke(false);
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestRandomInvoke(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = createTestTree(canGetRow);

        Map<Long,Long> map = new HashMap<>();

        int loops = reuseList == null ? 20_000 : 60_000;

        for (int i = 0 ; i < loops; i++) {
            final Long x = (long)BPlusTree.randomInt(CNT);
            final int rnd = BPlusTree.randomInt(11);

            if (i % 10_000 == 0) {
//                X.println(tree.printTree());
                X.println(" --> " + i + "  ++> " + x);
            }

            // Update map.
            if (!map.containsKey(x)) {
                if (rnd % 2 == 0) {
                    map.put(x, x);

//                    X.println("put0: " + x);
                }
                else {
//                    X.println("noop0: " + x);
                }
            }
            else {
                if (rnd % 2 == 0) {
//                    X.println("put1: " + x);
                }
                else if (rnd % 3 == 0) {
                    map.remove(x);

//                    X.println("rmv1: " + x);
                }
                else {
//                    X.println("noop1: " + x);
                }
            }

            // Consistently update tree.
            tree.invoke(x, null, new IgniteTree.InvokeClosure<Long>() {

                IgniteTree.OperationType op;

                Long newRow;

                @Override public void call(@Nullable Long row) throws IgniteCheckedException {
                    if (row == null) {
                        if (rnd % 2 == 0) {
                            op = PUT;
                            newRow = x;
                        }
                        else {
                            op = NOOP;
                            newRow = null;
                        }
                    }
                    else {
                        assertEquals(x, row);

                        if (rnd % 2 == 0) {
                            op = PUT;
                            newRow = x; // We can not replace x with y here, because keys must be equal.
                        }
                        else if (rnd % 3 == 0) {
                            op = REMOVE;
                            newRow = null;
                        }
                        else {
                            op = NOOP;
                            newRow = null;
                        }
                    }
                }

                @Override public Long newRow() {
                    return newRow;
                }

                @Override public IgniteTree.OperationType operationType() {
                    return op;
                }
            });

            assertNoLocks();

//            X.println(tree.printTree());

            tree.validateTree();

            if (i % 100 == 0)
                assertEqualContents(tree, map);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRandomPutRemove_1_30_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRandomPutRemove_1_30_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemove(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassiveRemove3_false() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassiveRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassiveRemove3_true() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassiveRemove(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassiveRemove2_false() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassiveRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassiveRemove2_true() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassiveRemove(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassiveRemove1_false() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassiveRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassiveRemove1_true() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassiveRemove(true);
    }

    /**
     * @param canGetRow Can get row in inner page.
     * @throws Exception If failed.
     */
    private void doTestMassiveRemove(final boolean canGetRow) throws Exception {
        final int threads = 64;
        final int keys = 3000;

        final AtomicLongArray rmvd = new AtomicLongArray(keys);

        final TestTree tree = createTestTree(canGetRow);

        // Put keys in reverse order to have a better balance in the tree (lower height).
        for (long i = keys - 1; i >= 0; i--) {
            tree.put(i);
//            X.println(tree.printTree());
        }

        assertEquals(keys, tree.size());

        tree.validateTree();

        info("Remove...");

        try {
            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = new GridRandom();

                    for(;;) {
                        int idx = 0;
                        boolean found = false;

                        for (int i = 0, shift = rnd.nextInt(keys); i < keys; i++) {
                            idx = (i + shift) % keys;

                            if (rmvd.get(idx) == 0 && rmvd.compareAndSet(idx, 0, 1)) {
                                found = true;

                                break;
                            }
                        }

                        if (!found)
                            break;

                        assertEquals(Long.valueOf(idx), tree.remove((long)idx));

                        if (canGetRow)
                            rmvdIds.add((long)idx);
                    }

                    return null;
                }
            }, threads, "remove");

            assertEquals(0, tree.size());

            tree.validateTree();
        }
        finally {
            rmvdIds.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassivePut1_true() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassivePut(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassivePut1_false() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassivePut(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassivePut2_true() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassivePut(true);
    }

    @Test
    public void testMassivePut2_false() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassivePut(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMassivePut3_true() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassivePut(true);
    }

    @Test
    public void testMassivePut3_false() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassivePut(false);
    }

    /**
     * @param canGetRow Can get row in inner page.
     * @throws Exception If failed.
     */
    private void doTestMassivePut(final boolean canGetRow) throws Exception {
        final int threads = 16;
        final int keys = 26; // We may fail to insert more on small pages size because of tree height.

        final TestTree tree = createTestTree(canGetRow);

        info("Put...");

        final AtomicLongArray k = new AtomicLongArray(keys);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new GridRandom();

                for (;;) {
                    int idx = 0;
                    boolean found = false;

                    for (int i = 0, shift = rnd.nextInt(keys); i < keys; i++) {
                        idx = (i + shift) % keys;

                        if (k.get(idx) == 0 && k.compareAndSet(idx, 0, 1)) {
                            found = true;

                            break;
                        }
                    }

                    if (!found)
                        break;

                    assertNull(tree.put((long)idx));

                    assertNoLocks();
                }

                return null;
            }
        }, threads, "put");

        assertEquals(keys, tree.size());

        tree.validateTree();

        GridCursor<Long> c = tree.find(null, null);

        long x = 0;

        while (c.next())
            assertEquals(Long.valueOf(x++), c.get());

        assertEquals(keys, x);

        assertNoLocks();
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestRandomPutRemove(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = createTestTree(canGetRow);

        Map<Long,Long> map = new HashMap<>();

        int loops = reuseList == null ? 100_000 : 300_000;

        for (int i = 0 ; i < loops; i++) {
            Long x = (long)BPlusTree.randomInt(CNT);

            boolean put = BPlusTree.randomInt(2) == 0;

            if (i % 10_000 == 0) {
//                X.println(tree.printTree());
                X.println(" --> " + (put ? "put " : "rmv ") + i + "  " + x);
            }

            if (put)
                assertEquals(map.put(x, x), tree.put(x));
            else {
                if (map.remove(x) != null)
                    assertEquals(x, tree.remove(x));

                assertNull(tree.remove(x));
            }

            assertNoLocks();

//            X.println(tree.printTree());
            tree.validateTree();

            if (i % 100 == 0)
                assertEqualContents(tree, map);
        }
    }

    /**
     * @param tree Tree.
     * @param map Map.
     * @throws IgniteCheckedException If failed.
     */
    private void assertEqualContents(IgniteTree<Long, Long> tree, Map<Long,Long> map) throws IgniteCheckedException {
        GridCursor<Long> cursor = tree.find(null, null);

        while (cursor.next()) {
            Long x = cursor.get();

            assert x != null;

            assertEquals(map.get(x), x);

            assertNoLocks();
        }

        assertEquals(map.size(), tree.size());

        assertNoLocks();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testEmptyCursors() throws IgniteCheckedException {
        MAX_PER_PAGE = 5;

        TestTree tree = createTestTree(true);

        assertFalse(tree.find(null, null).next());
        assertFalse(tree.find(0L, 1L).next());

        tree.put(1L);
        tree.put(2L);
        tree.put(3L);

        assertEquals(3, size(tree.find(null, null)));

        assertFalse(tree.find(4L, null).next());
        assertFalse(tree.find(null, 0L).next());

        assertNoLocks();
    }

    private void doTestCursor(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = createTestTree(canGetRow);

        for (long i = 15; i >= 0; i--)
            tree.put(i);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testCursorConcurrentMerge() throws IgniteCheckedException {
        MAX_PER_PAGE = 5;

//        X.println(" " + pageMem.pageSize());

        TestTree tree = createTestTree(true);

        TreeMap<Long,Long> map = new TreeMap<>();

        for (int i = 0; i < 20_000 + rnd.nextInt(2 * MAX_PER_PAGE); i++) {
            Long row = (long)rnd.nextInt(40_000);

//            X.println(" <-- " + row);

            assertEquals(map.put(row, row), tree.put(row));
            assertEquals(row, tree.findOne(row));

            assertNoLocks();
        }

        final int off = rnd.nextInt(5 * MAX_PER_PAGE);

        Long upperBound = 30_000L + rnd.nextInt(2 * MAX_PER_PAGE);

        GridCursor<Long> c = tree.find(null, upperBound);
        Iterator<Long> i = map.headMap(upperBound, true).keySet().iterator();

        Long last = null;

        for (int j = 0; j < off; j++) {
            assertTrue(c.next());

//            X.println(" <-> " + c.get());

            assertEquals(i.next(), c.get());

            last = c.get();

            assertNoLocks();
        }

        if (last != null) {
//            X.println(" >-< " + last + " " + upperBound);

            c = tree.find(last, upperBound);

            assertTrue(c.next());
            assertEquals(last, c.get());

            assertNoLocks();
        }

        while (c.next()) {
//            X.println(" --> " + c.get());

            assertNotNull(c.get());
            assertEquals(i.next(), c.get());
            assertEquals(c.get(), tree.remove(c.get()));

            i.remove();

            assertNoLocks();
        }

        assertEquals(map.size(), size(tree.find(null, null)));

        assertNoLocks();
    }

    /**
     * Verifies that {@link BPlusTree#size} and {@link BPlusTree#size} methods behave correctly
     * on single-threaded addition and removal of elements in random order.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSizeForPutRmvSequential() throws IgniteCheckedException {
        MAX_PER_PAGE = 5;

        boolean DEBUG_PRINT = false;

        int itemCnt = (int) Math.pow(MAX_PER_PAGE, 5) + rnd.nextInt(MAX_PER_PAGE * MAX_PER_PAGE);

        Long[] items = new Long[itemCnt];
        for (int i = 0; i < itemCnt; ++i)
            items[i] = (long) i;

        TestTree testTree = createTestTree(true);
        TreeMap<Long,Long> goldenMap = new TreeMap<>();

        assertEquals(0, testTree.size());
        assertEquals(0, goldenMap.size());

        final Predicate<Long> rowMatcher = new Predicate<Long>() {
            @Override public boolean test(Long row) {
                return row % 7 == 0;
            }
        };

        final BPlusTree.TreeRowClosure<Long, Long> rowClosure = new BPlusTree.TreeRowClosure<Long, Long>() {
            @Override public boolean apply(BPlusTree<Long, Long> tree, BPlusIO<Long> io, long pageAddr, int idx)
                throws IgniteCheckedException {
                return rowMatcher.test(io.getLookupRow(tree, pageAddr, idx));
            }
        };

        int correctMatchingRows = 0;

        Collections.shuffle(Arrays.asList(items), rnd);

        for (Long row : items) {
            if (DEBUG_PRINT) {
                X.println(" --> put(" + row + ")");
                X.print(testTree.printTree());
            }

            assertEquals(goldenMap.put(row, row), testTree.put(row));
            assertEquals(row, testTree.findOne(row));

            if (rowMatcher.test(row))
                ++correctMatchingRows;

            assertEquals(correctMatchingRows, testTree.size(rowClosure));

            long correctSize = goldenMap.size();

            assertEquals(correctSize, testTree.size());
            assertEquals(correctSize, size(testTree.find(null, null)));

            assertNoLocks();
        }

        Collections.shuffle(Arrays.asList(items), rnd);

        for (Long row : items) {
            if (DEBUG_PRINT) {
                X.println(" --> rmv(" + row + ")");
                X.print(testTree.printTree());
            }

            assertEquals(row, goldenMap.remove(row));
            assertEquals(row, testTree.remove(row));
            assertNull(testTree.findOne(row));

            if (rowMatcher.test(row))
                --correctMatchingRows;

            assertEquals(correctMatchingRows, testTree.size(rowClosure));

            long correctSize = goldenMap.size();

            assertEquals(correctSize, testTree.size());
            assertEquals(correctSize, size(testTree.find(null, null)));

            assertNoLocks();
        }
    }

    /**
     * Verifies that {@link BPlusTree#size()} method behaves correctly when run concurrently with
     * {@link BPlusTree#put}, {@link BPlusTree#remove} methods. Please see details in
     * {@link #doTestSizeForRandomPutRmvMultithreaded}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSizeForRandomPutRmvMultithreaded_5_4() throws Exception {
        MAX_PER_PAGE = 5;
        CNT = 10_000;

        doTestSizeForRandomPutRmvMultithreaded(4);
    }

    @Test
    public void testSizeForRandomPutRmvMultithreaded_3_256() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 10_000;

        doTestSizeForRandomPutRmvMultithreaded(256);
    }

    /**
     * Verifies that {@link BPlusTree#size()} method behaves correctly when run between series of
     * concurrent {@link BPlusTree#put}, {@link BPlusTree#remove} methods.
     *
     * @param rmvPutSlidingWindowSize Sliding window size (distance between items being deleted and added).
     * @throws Exception If failed.
     */
    private void doTestSizeForRandomPutRmvMultithreaded(final int rmvPutSlidingWindowSize) throws Exception {
        final TestTree tree = createTestTree(false);

        final boolean DEBUG_PRINT = false;

        final AtomicLong curRmvKey = new AtomicLong(0);
        final AtomicLong curPutKey = new AtomicLong(rmvPutSlidingWindowSize);

        for (long i = curRmvKey.get(); i < curPutKey.get(); ++i)
            assertNull(tree.put(i));

        final int putRmvThreadCnt = Math.min(Runtime.getRuntime().availableProcessors(), rmvPutSlidingWindowSize);

        final int loopCnt = CNT / putRmvThreadCnt;

        final CyclicBarrier putRmvOpBarrier = new CyclicBarrier(putRmvThreadCnt);
        final CyclicBarrier sizeOpBarrier = new CyclicBarrier(putRmvThreadCnt);

        IgniteInternalFuture<?> putRmvFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {

                for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                    putRmvOpBarrier.await();

                    Long putVal = curPutKey.getAndIncrement();

                    if (DEBUG_PRINT || (i & 0x7ff) == 0)
                        X.println(" --> put(" + putVal + ")");

                    assertNull(tree.put(putVal));

                    assertNoLocks();

                    Long rmvVal = curRmvKey.getAndIncrement();

                    if (DEBUG_PRINT || (i & 0x7ff) == 0)
                        X.println(" --> rmv(" + rmvVal + ")");

                    assertEquals(rmvVal, tree.remove(rmvVal));
                    assertNull(tree.remove(rmvVal));

                    assertNoLocks();

                    if (stop.get())
                        break;

                    sizeOpBarrier.await();

                    long correctSize = curPutKey.get() - curRmvKey.get();

                    if (DEBUG_PRINT || (i & 0x7ff) == 0)
                        X.println("====> correctSize=" + correctSize);

                    assertEquals(correctSize, size(tree.find(null, null)));
                    assertEquals(correctSize, tree.size());
                }

                return null;
            }
        }, putRmvThreadCnt, "put-remove-size");

        IgniteInternalFuture<?> lockPrintingFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    Thread.sleep(5000);

                    X.println(TestTree.printLocks());
                }

                return null;
            }
        }, 1, "printLocks");

        asyncRunFut = new GridCompoundFuture<>();

        asyncRunFut.add((IgniteInternalFuture) putRmvFut);
        asyncRunFut.add((IgniteInternalFuture) lockPrintingFut);

        asyncRunFut.markInitialized();

        try {
            putRmvFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            stop.set(true);
            putRmvOpBarrier.reset();
            sizeOpBarrier.reset();

            asyncRunFut.get();
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * Verifies that concurrent running of {@link BPlusTree#put} + {@link BPlusTree#remove} sequence
     * and {@link BPlusTree#size} methods results in correct calculation of tree size.
     *
     * @see #doTestSizeForRandomPutRmvMultithreadedAsync doTestSizeForRandomPutRmvMultithreadedAsync() for details.
     */
    @Test
    public void testSizeForRandomPutRmvMultithreadedAsync_16() throws Exception {
        doTestSizeForRandomPutRmvMultithreadedAsync(16);
    }

    /**
     * Verifies that concurrent running of {@link BPlusTree#put} + {@link BPlusTree#remove} sequence
     * and {@link BPlusTree#size} methods results in correct calculation of tree size.
     *
     * @see #doTestSizeForRandomPutRmvMultithreadedAsync doTestSizeForRandomPutRmvMultithreadedAsync() for details.
     */
    @Test
    public void testSizeForRandomPutRmvMultithreadedAsync_3() throws Exception {
        doTestSizeForRandomPutRmvMultithreadedAsync(3);
    }

    /**
     * Verifies that concurrent running of {@link BPlusTree#put} + {@link BPlusTree#remove} sequence
     * and {@link BPlusTree#size} methods results in correct calculation of tree size.
     *
     * Since in the presence of concurrent modifications the size may differ from the actual one, the test maintains
     * sliding window of records in the tree, uses a barrier between concurrent runs to limit runaway delta in
     * the calculated size, and checks that the measured size lies within certain bounds.
     *
     * NB: This test has to be changed with the integration of IGNITE-3478.
     *
     */
    public void doTestSizeForRandomPutRmvMultithreadedAsync(final int rmvPutSlidingWindowSize) throws Exception {
        MAX_PER_PAGE = 5;

        final boolean DEBUG_PRINT = false;

        final TestTree tree = createTestTree(false);

        final AtomicLong curRmvKey = new AtomicLong(0);
        final AtomicLong curPutKey = new AtomicLong(rmvPutSlidingWindowSize);

        for (long i = curRmvKey.get(); i < curPutKey.get(); ++i)
            assertNull(tree.put(i));

        final int putRmvThreadCnt = Math.min(Runtime.getRuntime().availableProcessors(), rmvPutSlidingWindowSize);
        final int sizeThreadCnt = putRmvThreadCnt;

        final CyclicBarrier putRmvOpBarrier = new CyclicBarrier(putRmvThreadCnt + sizeThreadCnt, new Runnable() {
            @Override public void run() {
                if (DEBUG_PRINT) {
                    try {
                        X.println("===BARRIER=== size=" + tree.size()
                            + "; contents=[" + tree.findFirst() + ".." + tree.findLast() + "]"
                            + "; rmvVal=" + curRmvKey.get() + "; putVal=" + curPutKey.get());

                        X.println(tree.printTree());
                    }
                    catch (IgniteCheckedException e) {
                        // ignore
                    }
                }
            }
        });

        final int loopCnt = 500;

        IgniteInternalFuture<?> putRmvFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                    int order;
                    try {
                        order = putRmvOpBarrier.await();
                    } catch (BrokenBarrierException e) {
                        break;
                    }

                    Long putVal = curPutKey.getAndIncrement();

                    if (DEBUG_PRINT || (i & 0x3ff) == 0)
                        X.println(order + ": --> put(" + putVal + ")");

                    assertNull(tree.put(putVal));

                    Long rmvVal = curRmvKey.getAndIncrement();

                    if (DEBUG_PRINT || (i & 0x3ff) == 0)
                        X.println(order + ": --> rmv(" + rmvVal + ")");

                    assertEquals(rmvVal, tree.remove(rmvVal));
                    assertNull(tree.findOne(rmvVal));
                }

                return null;
            }
        }, putRmvThreadCnt, "put-remove");

        IgniteInternalFuture<?> sizeFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {

                final List<Long> treeContents = new ArrayList<>(rmvPutSlidingWindowSize * 2);

                final BPlusTree.TreeRowClosure<Long, Long> rowDumper = new BPlusTree.TreeRowClosure<Long, Long>() {
                    @Override public boolean apply(BPlusTree<Long, Long> tree, BPlusIO<Long> io, long pageAddr, int idx)
                        throws IgniteCheckedException {

                        treeContents.add(io.getLookupRow(tree, pageAddr, idx));
                        return true;
                    }
                };

                for (long iter = 0; !stop.get(); ++iter) {
                    int order = 0;

                    try {
                        order = putRmvOpBarrier.await();
                    } catch (BrokenBarrierException e) {
                        break;
                    }

                    long correctSize = curPutKey.get() - curRmvKey.get();

                    treeContents.clear();
                    long treeSize = tree.size(rowDumper);

                    long minBound = correctSize - putRmvThreadCnt;
                    long maxBound = correctSize + putRmvThreadCnt;

                    if (DEBUG_PRINT || (iter & 0x3ff) == 0)
                      X.println(order + ": size=" + treeSize + "; bounds=[" + minBound + ".." + maxBound
                            + "]; contents=" + treeContents);

                    if (treeSize < minBound || treeSize > maxBound) {
                        fail("Tree size is not in bounds ["  + minBound + ".." + maxBound + "]: " + treeSize
                            + "; Tree contents: " + treeContents);
                    }
                }

                return null;
            }
        }, sizeThreadCnt, "size");

        IgniteInternalFuture<?> lockPrintingFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    Thread.sleep(5000);

                    X.println(TestTree.printLocks());
                }

                return null;
            }
        }, 1, "printLocks");

        asyncRunFut = new GridCompoundFuture<>();

        asyncRunFut.add((IgniteInternalFuture) putRmvFut);
        asyncRunFut.add((IgniteInternalFuture) sizeFut);
        asyncRunFut.add((IgniteInternalFuture) lockPrintingFut);

        asyncRunFut.markInitialized();

        try {
            putRmvFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            stop.set(true);
            putRmvOpBarrier.reset();

            asyncRunFut.get();
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * The test forces {@link BPlusTree#size} method to run into a livelock: during single run
     * the method is picking up new pages which are concurrently added to the tree until the new pages are not added
     * anymore. Test verifies that despite livelock condition a size from a valid range is returned.
     *
     * NB: This test has to be changed with the integration of IGNITE-3478.
     *
     * @throws Exception if test failed
     */
    @Test
    public void testPutSizeLivelock() throws Exception {
        MAX_PER_PAGE = 5;
        CNT = 800;

        final int SLIDING_WINDOW_SIZE = 16;
        final boolean DEBUG_PRINT = false;

        final TestTree tree = createTestTree(false);

        final AtomicLong curRmvKey = new AtomicLong(0);
        final AtomicLong curPutKey = new AtomicLong(SLIDING_WINDOW_SIZE);

        for (long i = curRmvKey.get(); i < curPutKey.get(); ++i)
            assertNull(tree.put(i));

        final int hwThreads = Runtime.getRuntime().availableProcessors();
        final int putRmvThreadCnt = Math.max(1, hwThreads / 2);
        final int sizeThreadCnt = hwThreads - putRmvThreadCnt;

        final CyclicBarrier putRmvOpBarrier = new CyclicBarrier(putRmvThreadCnt, new Runnable() {
            @Override public void run() {
                if (DEBUG_PRINT) {
                    try {
                        X.println("===BARRIER=== size=" + tree.size()
                            + " [" + tree.findFirst() + ".." + tree.findLast() + "]");
                    }
                    catch (IgniteCheckedException e) {
                        // ignore
                    }
                }
            }
        });

        final int loopCnt = CNT / hwThreads;

        IgniteInternalFuture<?> putRmvFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                    int order;
                    try {
                        order = putRmvOpBarrier.await();
                    } catch (BrokenBarrierException e) {
                        // barrier reset() has been called: terminate
                        break;
                    }

                    Long putVal = curPutKey.getAndIncrement();

                    if ((i & 0xff) == 0)
                        X.println(order + ": --> put(" + putVal + ")");

                    assertNull(tree.put(putVal));

                    Long rmvVal = curRmvKey.getAndIncrement();

                    if ((i & 0xff) == 0)
                        X.println(order + ": --> rmv(" + rmvVal + ")");

                    assertEquals(rmvVal, tree.remove(rmvVal));
                    assertNull(tree.findOne(rmvVal));
                }

                return null;
            }
        }, putRmvThreadCnt, "put-remove");

        IgniteInternalFuture<?> sizeFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {

                final List<Long> treeContents = new ArrayList<>(SLIDING_WINDOW_SIZE * 2);

                final BPlusTree.TreeRowClosure<Long, Long> rowDumper = new BPlusTree.TreeRowClosure<Long, Long>() {
                    @Override public boolean apply(BPlusTree<Long, Long> tree, BPlusIO<Long> io, long pageAddr, int idx)
                        throws IgniteCheckedException {

                        treeContents.add(io.getLookupRow(tree, pageAddr, idx));

                        final long endMs = System.currentTimeMillis() + 10;
                        final long endPutKey = curPutKey.get() + MAX_PER_PAGE;

                        while (System.currentTimeMillis() < endMs && curPutKey.get() < endPutKey)
                            Thread.yield();

                        return true;
                    }
                };

                while (!stop.get()) {
                    treeContents.clear();

                    long treeSize = tree.size(rowDumper);
                    long curPutVal = curPutKey.get();

                    X.println(" ======> size=" + treeSize + "; last-put-value=" + curPutVal);

                    if (treeSize < SLIDING_WINDOW_SIZE || treeSize > curPutVal)
                        fail("Tree size is not in bounds [" + SLIDING_WINDOW_SIZE + ".." + curPutVal + "]:"
                            + treeSize + "; contents=" + treeContents);
                }

                return null;
            }
        }, sizeThreadCnt, "size");

        asyncRunFut = new GridCompoundFuture<>();

        asyncRunFut.add((IgniteInternalFuture) putRmvFut);
        asyncRunFut.add((IgniteInternalFuture) sizeFut);

        asyncRunFut.markInitialized();

        try {
            putRmvFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            stop.set(true);
            putRmvOpBarrier.reset();

            asyncRunFut.get();
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * Verifies that in case for threads concurrently calling put and remove
     * on a tree with 1-3 pages, the size() method performs correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutRmvSizeSinglePageContention() throws Exception {
        MAX_PER_PAGE = 10;
        CNT = 20_000;
        final boolean DEBUG_PRINT = false;
        final int SLIDING_WINDOWS_SIZE = MAX_PER_PAGE * 2;

        final TestTree tree = createTestTree(false);

        final AtomicLong curPutKey = new AtomicLong(0);
        final BlockingQueue<Long> rowsToRemove = new ArrayBlockingQueue<>(MAX_PER_PAGE / 2);

        final int hwThreadCnt = Runtime.getRuntime().availableProcessors();
        final int putThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int rmvThreadCnt = Math.max(1, hwThreadCnt / 2 - putThreadCnt);
        final int sizeThreadCnt = Math.max(1, hwThreadCnt - putThreadCnt - rmvThreadCnt);

        final AtomicInteger sizeInvokeCnt = new AtomicInteger(0);

        final int loopCnt = CNT;

        IgniteInternalFuture<?> sizeFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int iter = 0;
                while (!stop.get()) {
                    long size = tree.size();

                    if (DEBUG_PRINT || (++iter & 0xffff) == 0)
                        X.println(" --> size() = " + size);

                    sizeInvokeCnt.incrementAndGet();
                }

                return null;
            }
        }, sizeThreadCnt, "size");

        // Let the size threads ignite
        while (sizeInvokeCnt.get() < sizeThreadCnt * 2)
            Thread.yield();

        IgniteInternalFuture<?> rmvFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int iter = 0;
                while(!stop.get()) {
                    Long rmvVal = rowsToRemove.poll(200, TimeUnit.MILLISECONDS);
                    if (rmvVal != null)
                        assertEquals(rmvVal, tree.remove(rmvVal));

                    if (DEBUG_PRINT || (++iter & 0x3ff) == 0)
                        X.println(" --> rmv(" + rmvVal + ")");
                }

                return null;
            }
        }, rmvThreadCnt, "rmv");

        IgniteInternalFuture<?> putFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                    Long putVal = curPutKey.getAndIncrement();
                    assertNull(tree.put(putVal));

                    while (rowsToRemove.size() > SLIDING_WINDOWS_SIZE && !stop.get())
                        Thread.yield();

                    rowsToRemove.put(putVal);

                    if (DEBUG_PRINT || (i & 0x3ff) == 0)
                        X.println(" --> put(" + putVal + ")");
                }

                return null;
            }
        }, putThreadCnt, "put");

        IgniteInternalFuture<?> treePrintFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    Thread.sleep(1000);

                    X.println(TestTree.printLocks());
                    X.println(tree.printTree());
                }

                return null;
            }
        }, 1, "printTree");

        asyncRunFut = new GridCompoundFuture<>();

        asyncRunFut.add((IgniteInternalFuture) sizeFut);
        asyncRunFut.add((IgniteInternalFuture) rmvFut);
        asyncRunFut.add((IgniteInternalFuture) putFut);
        asyncRunFut.add((IgniteInternalFuture) treePrintFut);

        asyncRunFut.markInitialized();

        try {
            putFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            stop.set(true);

            asyncRunFut.get();
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * The test verifies that {@link BPlusTree#put}, {@link BPlusTree#remove}, {@link BPlusTree#find}, and
     * {@link BPlusTree#size} run concurrently, perform correctly and report correct values.
     *
     * A sliding window of numbers is maintainted in the tests.
     *
     * NB: This test has to be changed with the integration of IGNITE-3478.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutRmvFindSizeMultithreaded() throws Exception {
        MAX_PER_PAGE = 5;
        CNT = 60_000;

        final int SLIDING_WINDOW_SIZE = 100;

        final TestTree tree = createTestTree(false);

        final AtomicLong curPutKey = new AtomicLong(0);
        final BlockingQueue<Long> rowsToRemove = new ArrayBlockingQueue<>(SLIDING_WINDOW_SIZE);

        final int hwThreadCnt = Runtime.getRuntime().availableProcessors();
        final int putThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int rmvThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int findThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int sizeThreadCnt = Math.max(1, hwThreadCnt - putThreadCnt - rmvThreadCnt - findThreadCnt);

        final AtomicInteger sizeInvokeCnt = new AtomicInteger(0);

        final int loopCnt = CNT;

        IgniteInternalFuture<?> sizeFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int iter = 0;
                while (!stop.get()) {
                    long size = tree.size();

                    if ((++iter & 0x3ff) == 0)
                        X.println(" --> size() = " + size);

                    sizeInvokeCnt.incrementAndGet();
                }

                return null;
            }
        }, sizeThreadCnt, "size");

        // Let the size threads start
        while (sizeInvokeCnt.get() < sizeThreadCnt * 2)
            Thread.yield();

        IgniteInternalFuture<?> rmvFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int iter = 0;
                while(!stop.get()) {
                    Long rmvVal = rowsToRemove.poll(200, TimeUnit.MILLISECONDS);
                    if (rmvVal != null)
                        assertEquals(rmvVal, tree.remove(rmvVal));

                    if ((++iter & 0x3ff) == 0)
                        X.println(" --> rmv(" + rmvVal + ")");
                }

                return null;
            }
        }, rmvThreadCnt, "rmv");

        IgniteInternalFuture<?> findFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int iter = 0;
                while(!stop.get()) {
                    Long findVal = curPutKey.get()
                        + SLIDING_WINDOW_SIZE / 2
                        - rnd.nextInt(SLIDING_WINDOW_SIZE * 2);

                    tree.findOne(findVal);

                    if ((++iter & 0x3ff) == 0)
                        X.println(" --> fnd(" + findVal + ")");
                }

                return null;
            }
        }, findThreadCnt, "find");

        IgniteInternalFuture<?> putFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                    Long putVal = curPutKey.getAndIncrement();
                    assertNull(tree.put(putVal));

                    while (rowsToRemove.size() > SLIDING_WINDOW_SIZE) {
                        if (stop.get())
                            return null;

                        Thread.yield();
                    }

                    rowsToRemove.put(putVal);

                    if ((i & 0x3ff) == 0)
                        X.println(" --> put(" + putVal + ")");
                }

                return null;
            }
        }, putThreadCnt, "put");

        IgniteInternalFuture<?> lockPrintingFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    Thread.sleep(1000);

                    X.println(TestTree.printLocks());
                }

                return null;
            }
        }, 1, "printLocks");

        asyncRunFut = new GridCompoundFuture<>();

        asyncRunFut.add((IgniteInternalFuture) sizeFut);
        asyncRunFut.add((IgniteInternalFuture) rmvFut);
        asyncRunFut.add((IgniteInternalFuture) findFut);
        asyncRunFut.add((IgniteInternalFuture) putFut);
        asyncRunFut.add((IgniteInternalFuture) lockPrintingFut);

        asyncRunFut.markInitialized();

        try {
            putFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            stop.set(true);

            asyncRunFut.get();
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestRandomPutRemoveMultithreaded_1_30_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemoveMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestRandomPutRemoveMultithreaded_1_30_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemoveMultithreaded(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestRandomPutRemoveMultithreaded_2_50_0() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 50;

        doTestRandomPutRemoveMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestRandomPutRemoveMultithreaded_2_50_1() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 50;

        doTestRandomPutRemoveMultithreaded(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestRandomPutRemoveMultithreaded_3_70_0() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 70;

        doTestRandomPutRemoveMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestRandomPutRemoveMultithreaded_3_70_1() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 70;

        doTestRandomPutRemoveMultithreaded(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testFindFirstAndLast() throws IgniteCheckedException {
        MAX_PER_PAGE = 5;

        TestTree tree = createTestTree(true);

        Long first = tree.findFirst();
        assertNull(first);

        Long last = tree.findLast();
        assertNull(last);

        for (long idx = 1L; idx <= 10L; ++idx)
            tree.put(idx);

        first = tree.findFirst();
        assertEquals((Long)1L, first);

        last = tree.findLast();
        assertEquals((Long)10L, last);

        assertNoLocks();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIterate() throws Exception {
        MAX_PER_PAGE = 5;

        TestTree tree = createTestTree(true);

        checkIterate(tree, 0L, 100L, null, false);

        for (long idx = 1L; idx <= 10L; ++idx)
            tree.put(idx);

        for (long idx = 1L; idx <= 10L; ++idx)
            checkIterate(tree, idx, 100L, idx, true);

        checkIterate(tree, 0L, 100L, 1L, true);

        for (long idx = 1L; idx <= 10L; ++idx)
            checkIterate(tree, idx, 100L, 10L, true);

        checkIterate(tree, 0L, 100L, 100L, false);

        for (long idx = 1L; idx <= 10L; ++idx)
            checkIterate(tree, 0L, 100L, idx, true);

        for (long idx = 0L; idx <= 10L; ++idx)
            checkIterate(tree, idx, 11L, -1L, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIterateConcurrentPutRemove() throws Exception {
        iterateConcurrentPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIterateConcurrentPutRemove_1() throws Exception {
        MAX_PER_PAGE = 1;

        iterateConcurrentPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIterateConcurrentPutRemove_2() throws Exception {
        MAX_PER_PAGE = 2;

        iterateConcurrentPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIteratePutRemove_10() throws Exception {
        MAX_PER_PAGE = 10;

        iterateConcurrentPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    private void iterateConcurrentPutRemove() throws Exception {
        final TestTree tree = createTestTree(true);

        // Single key per page is a degenerate case: it is very hard to merge pages in a tree because
        // to merge we need to remove a split key from a parent page and add it to a back page, but this
        // is impossible if we already have a key in a back page, thus we will have lots of empty routing pages.
        // This way the tree grows faster than shrinks and gets out of height limit of 26 (for this page size) quickly.
        // Since the tree height can not be larger than the key count for this case, we can use 26 as a safe number.
        final int KEYS = MAX_PER_PAGE == 1 ? 26 : GridTestUtils.SF.applyLB(10_000, 2_500);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            for (long idx = 0L; idx < KEYS; ++idx)
                tree.put(idx);

            final Long findKey;

            if (MAX_PER_PAGE > 0) {
                switch (i) {
                    case 0:
                        findKey = 1L;

                        break;

                    case 1:
                        findKey = (long)MAX_PER_PAGE;

                        break;

                    case 2:
                        findKey = (long)MAX_PER_PAGE - 1;

                        break;

                    case 3:
                        findKey = (long)MAX_PER_PAGE + 1;

                        break;

                    case 4:
                        findKey = (long)(KEYS / MAX_PER_PAGE / 2) * MAX_PER_PAGE;

                        break;

                    case 5:
                        findKey = (long)(KEYS / MAX_PER_PAGE / 2) * MAX_PER_PAGE - 1;

                        break;

                    case 6:
                        findKey = (long)(KEYS / MAX_PER_PAGE / 2) * MAX_PER_PAGE + 1;

                        break;

                    case 7:
                        findKey = (long)KEYS - 1;

                        break;

                    default:
                        findKey = rnd.nextLong(KEYS);
                }
            }
            else
                findKey = rnd.nextLong(KEYS);

            info("Iteration [iter=" + i + ", key=" + findKey + ']');

            assertEquals(findKey, tree.findOne(findKey));
            checkIterate(tree, findKey, findKey, findKey, true);

            IgniteInternalFuture getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    TestTreeRowClosure p = new TestTreeRowClosure(findKey);

                    TestTreeRowClosure falseP = new TestTreeRowClosure(-1L);

                    int cnt = 0;

                    while (!stop.get()) {
                        int shift = MAX_PER_PAGE > 0 ? rnd.nextInt(MAX_PER_PAGE * 2) : rnd.nextInt(100);

                        checkIterateC(tree, findKey, findKey, p, true);

                        checkIterateC(tree, findKey - shift, findKey, p, true);

                        checkIterateC(tree, findKey - shift, findKey + shift, p, true);

                        checkIterateC(tree, findKey, findKey + shift, p, true);

                        checkIterateC(tree, -100L, KEYS + 100L, falseP, false);

                        cnt++;
                    }

                    info("Done, read count: " + cnt);

                    return null;
                }
            }, 10, "find");

            asyncRunFut = new GridCompoundFuture<>();

            asyncRunFut.add(getFut);

            asyncRunFut.markInitialized();

            try {
                U.sleep(100);

                for (int j = 0; j < 20; j++) {
                    for (long idx = 0L; idx < KEYS / 2; ++idx) {
                        long toRmv = rnd.nextLong(KEYS);

                        if (toRmv != findKey)
                            tree.remove(toRmv);
                    }

                    for (long idx = 0L; idx < KEYS / 2; ++idx) {
                        long put = rnd.nextLong(KEYS);

                        tree.put(put);
                    }
                }
            }
            finally {
                stop.set(true);
            }

            asyncRunFut.get();

            stop.set(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentGrowDegenerateTreeAndConcurrentRemove() throws Exception {
        //calculate tree size when split happens
        final TestTree t = createTestTree(true);
        long i = 0;

        for (; ; i++) {
            t.put(i);

            if (t.rootLevel() > 0)  //split happened
                break;
        }

        final long treeStartSize = i;

        final AtomicReference<Throwable> failed = new AtomicReference<>();

        for (int k = 0; k < 100; k++) {
            final TestTree tree = createTestTree(true);

            final AtomicBoolean start = new AtomicBoolean();

            final AtomicInteger ready = new AtomicInteger();

            Thread first = new Thread(new Runnable() {
                @Override public void run() {
                    ready.incrementAndGet();

                    while (!start.get()); //waiting without blocking

                    try {
                        tree.remove(treeStartSize / 2L);
                    }
                    catch (Throwable th) {
                        failed.set(th);
                    }
                }
            });

            Thread second = new Thread(new Runnable() {
                @Override public void run() {
                    ready.incrementAndGet();

                    while (!start.get()); //waiting without blocking

                    try {
                        tree.put(treeStartSize + 1);
                    }
                    catch (Throwable th) {
                        failed.set(th);
                    }
                }
            });

            for (int j = 0; j < treeStartSize; j++)
                tree.put((long)j);

            first.start();
            second.start();

            while (ready.get() != 2);

            start.set(true);

            first.join();
            second.join();

            assertNull(failed.get());
        }
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws Exception If failed.
     */
    private void doTestRandomPutRemoveMultithreaded(boolean canGetRow) throws Exception {
        final TestTree tree = createTestTree(canGetRow);

        final Map<Long,Long> map = new ConcurrentHashMap<>();

        final int loops = reuseList == null ? 20_000 : 60_000;

        final GridStripedLock lock = new GridStripedLock(256);

        final String[] ops = {"put", "rmv", "inv_put", "inv_rmv"};

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < loops && !stop.get(); i++) {
                    final Long x = (long)DataStructure.randomInt(CNT);
                    final int op = DataStructure.randomInt(4);

                    if (i % 10000 == 0)
                        X.println(" --> " + ops[op] + "_" + i + "  " + x);

                    Lock l = lock.getLock(x.longValue());

                    l.lock();

                    try {
                        if (op == 0) { // Put.
                            assertEquals(map.put(x, x), tree.put(x));

                            assertNoLocks();
                        }
                        else if (op == 1) { // Remove.
                            if (map.remove(x) != null) {
                                assertEquals(x, tree.remove(x));

                                assertNoLocks();
                            }

                            assertNull(tree.remove(x));

                            assertNoLocks();
                        }
                        else if (op == 2) {
                            tree.invoke(x, null, new IgniteTree.InvokeClosure<Long>() {
                                IgniteTree.OperationType opType;

                                @Override public void call(@Nullable Long row) {
                                    opType = PUT;

                                    if (row != null)
                                        assertEquals(x, row);
                                }

                                @Override public Long newRow() {
                                    return x;
                                }

                                @Override public IgniteTree.OperationType operationType() {
                                    return opType;
                                }
                            });

                            map.put(x,x);
                        }
                        else if (op == 3) {
                            tree.invoke(x, null, new IgniteTree.InvokeClosure<Long>() {
                                IgniteTree.OperationType opType;

                                @Override public void call(@Nullable Long row) {
                                    if (row != null) {
                                        assertEquals(x, row);
                                        opType = REMOVE;
                                    }
                                    else
                                        opType = NOOP;
                                }

                                @Override public Long newRow() {
                                    return null;
                                }

                                @Override public IgniteTree.OperationType operationType() {
                                    return opType;
                                }
                            });

                            map.remove(x);
                        }
                        else
                            fail();
                    }
                    finally {
                        l.unlock();
                    }
                }

                return null;
            }
        }, Runtime.getRuntime().availableProcessors(), "put-remove");

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    Thread.sleep(5000);

                    X.println(TestTree.printLocks());
                }

                return null;
            }
        }, 1, "printLocks");

        IgniteInternalFuture<?> fut3 = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    int low = DataStructure.randomInt(CNT);
                    int high = low + DataStructure.randomInt(CNT - low);

                    GridCursor<Long> c = tree.find((long)low, (long)high);

                    Long last = null;

                    while (c.next()) {
                        // Correct bounds.
                        assertTrue(low + " <= " + c.get() + " <= " + high, c.get() >= low);
                        assertTrue(low + " <= " + c.get() + " <= " + high, c.get() <= high);

                        if (last != null) // No duplicates.
                            assertTrue(low + " <= " + last + " < " + c.get() + " <= " + high, c.get() > last);

                        last = c.get();
                    }

                    TestTreeFindFirstClosure cl = new TestTreeFindFirstClosure();

                    tree.iterate((long)low, (long)high, cl);

                    last = cl.val;

                    if (last != null) {
                        assertTrue(low + " <= " + last + " <= " + high, last >= low);
                        assertTrue(low + " <= " + last + " <= " + high, last <= high);
                    }
                }

                return null;
            }
        }, 4, "find");

        asyncRunFut = new GridCompoundFuture<>();

        asyncRunFut.add((IgniteInternalFuture)fut);
        asyncRunFut.add((IgniteInternalFuture)fut2);
        asyncRunFut.add((IgniteInternalFuture)fut3);

        asyncRunFut.markInitialized();

        try {
            fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            stop.set(true);

            asyncRunFut.get();
        }

        GridCursor<Long> cursor = tree.find(null, null);

        while (cursor.next()) {
            Long x = cursor.get();

            assert x != null;

            assertEquals(map.get(x), x);
        }

        info("size: " + map.size());

        assertEquals(map.size(), tree.size());

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * @param c Cursor.
     * @return Number of elements.
     * @throws IgniteCheckedException If failed.
     */
    private static int size(GridCursor<?> c) throws IgniteCheckedException {
        int cnt = 0;

        while(c.next())
            cnt++;

        return cnt;
    }

    /**
     * @param pageId Page ID.
     * @param pageAddr Page address.
     */
    public static void checkPageId(long pageId, long pageAddr) {
        long actual = PageIO.getPageId(pageAddr);

        // Page ID must be 0L for newly allocated page, for reused page effective ID must remain the same.
        if (actual != 0L && pageId != actual)
            throw new IllegalStateException("Page ID: " + U.hexLong(actual));
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @return Test tree instance.
     * @throws IgniteCheckedException If failed.
     */
    protected TestTree createTestTree(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = new TestTree(
            reuseList, canGetRow, CACHE_ID, pageMem, allocateMetaPage().pageId(), lockTrackerManager);

        assertEquals(0, tree.size());
        assertEquals(0, tree.rootLevel());

        return tree;
    }

    /**
     * @return Allocated meta page ID.
     * @throws IgniteCheckedException If failed.
     */
    private FullPageId allocateMetaPage() throws IgniteCheckedException {
        return new FullPageId(pageMem.allocatePage(CACHE_ID, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX), CACHE_ID);
    }

    /**
     * Test tree.
     */
    protected static class TestTree extends BPlusTree<Long, Long> {
        /** Number of retries. */
        private int numRetries = super.getLockRetries();

        /**
         * @param reuseList Reuse list.
         * @param canGetRow Can get row from inner page.
         * @param cacheId Cache ID.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @throws IgniteCheckedException If failed.
         */
        public TestTree(
            ReuseList reuseList,
            boolean canGetRow,
            int cacheId,
            PageMemory pageMem,
            long metaPageId,
            PageLockTrackerManager lockTrackerManager
        ) throws IgniteCheckedException {
            super(
                "test",
                cacheId,
                null,
                pageMem,
                null,
                new AtomicLong(),
                metaPageId,
                reuseList,
                new IOVersions<>(new LongInnerIO(canGetRow)),
                new IOVersions<>(new LongLeafIO()),
                new FailureProcessor(new GridTestKernalContext(log)) {
                    @Override public boolean process(FailureContext failureCtx) {
                        lockTrackerManager.dumpLocksToLog();

                        return true;
                    }
                },
                new TestPageLockListener(lockTrackerManager.createPageLockTracker("testTree"))
            );

            PageIO.registerTest(latestInnerIO(), latestLeafIO());

            initTree(true);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<Long> io, long pageAddr, int idx, Long n2)
            throws IgniteCheckedException {
            Long n1 = io.getLookupRow(this, pageAddr, idx);

            return Long.compare(n1, n2);
        }

        /** {@inheritDoc} */
        @Override public Long getRow(BPlusIO<Long> io, long pageAddr, int idx, Object ignore)
            throws IgniteCheckedException {
            assert io.canGetRow() : io;

            return io.getLookupRow(this, pageAddr, idx);
        }

        /**
         * @return Thread ID.
         */
        static Object threadId() {
            return Thread.currentThread().getId(); //.getName();
        }

        /**
         * @param b String builder.
         * @param locks Locks.
         * @param beforeLock Before lock.
         */
        private static void printLocks(SB b, ConcurrentMap<Object, Map<Long, Long>> locks, Map<Object, Long> beforeLock) {
            for (Map.Entry<Object,Map<Long,Long>> entry : locks.entrySet()) {
                Object thId = entry.getKey();

                Long z = beforeLock.get(thId);

                Set<Map.Entry<Long,Long>> xx = entry.getValue().entrySet();

                if (z == null && xx.isEmpty())
                    continue;

                b.a(" ## " + thId);

                if (z != null)
                    b.a("   --> ").appendHex(z).a("  (").appendHex(effectivePageId(z)).a(')');

                b.a('\n');

                for (Map.Entry<Long,Long> x : xx)
                    b.a(" -  ").appendHex(x.getValue()).a("  (").appendHex(x.getKey()).a(")\n");

                b.a('\n');
            }
        }

        /**
         * @return List of locks as a String.
         */
        static String printLocks() {
            SB b = new SB();

            b.a("\n--------read---------\n");

            printLocks(b, TestPageLockListener.readLocks, TestPageLockListener.beforeReadLock);

            b.a("\n-+------write---------\n");

            printLocks(b, TestPageLockListener.writeLocks, TestPageLockListener.beforeWriteLock);

            return b.toString();
        }

        /** {@inheritDoc} */
        @Override
        protected int getLockRetries() {
            return numRetries;
        }
    }

    /**
     * TODO refactor to use integer in inner page
     * Long inner.
     */
    private static final class LongInnerIO extends BPlusInnerIO<Long> {
        /**
         */
        protected LongInnerIO(boolean canGetRow) {
            super(LONG_INNER_IO, 1, canGetRow, 8);
        }

        /** {@inheritDoc} */
        @Override public int getMaxCount(long buf, int pageSize) {
            if (MAX_PER_PAGE != 0)
                return MAX_PER_PAGE;

            return super.getMaxCount(buf, pageSize);
        }

        /** {@inheritDoc} */
        @Override public void store(long dst, int dstIdx, BPlusIO<Long> srcIo, long src, int srcIdx)
            throws IgniteCheckedException {
            Long row = srcIo.getLookupRow(null, src, srcIdx);

            store(dst, dstIdx, row, null, false);
        }

        /**
         * @param row Row.
         */
        private void checkNotRemoved(Long row) {
            if (rmvdIds.contains(row))
                fail("Removed row: " + row);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, Long row) {
            checkNotRemoved(row);

            PageUtils.putLong(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public Long getLookupRow(BPlusTree<Long,?> tree, long pageAddr, int idx) {
            Long row = PageUtils.getLong(pageAddr, offset(idx));

            checkNotRemoved(row);

            return row;
        }
    }

    /**
     * @return Page memory.
     */
    protected PageMemory createPageMemory() throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log,
            new UnsafeMemoryProvider(log),
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
            true);

        pageMem.start();

        return pageMem;
    }

    /**
     * @return Number of acquired pages.
     */
    protected long acquiredPages() {
        return ((PageMemoryNoStoreImpl)pageMem).acquiredPages();
    }

    /**
     * Long leaf.
     */
    private static final class LongLeafIO extends BPlusLeafIO<Long> {
        /**
         */
        LongLeafIO() {
            super(LONG_LEAF_IO, 1, 8);
        }

        /** {@inheritDoc} */
        @Override public int getMaxCount(long pageAddr, int pageSize) {
            if (MAX_PER_PAGE != 0)
                return MAX_PER_PAGE;

            return super.getMaxCount(pageAddr, pageSize);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, Long row) {
            PageUtils.putLong(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dst, int dstIdx, BPlusIO<Long> srcIo, long src, int srcIdx) {
            assert srcIo == this;

            PageUtils.putLong(dst, offset(dstIdx), PageUtils.getLong(src, offset(srcIdx)));
        }

        /** {@inheritDoc} */
        @Override public Long getLookupRow(BPlusTree<Long,?> tree, long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx));
        }
    }

    /**
     *
     */
    static class TestTreeRowClosure implements BPlusTree.TreeRowClosure<Long, Long> {
        /** */
        private final Long expVal;

        /** */
        private boolean found;

        /**
         * @param expVal Value to find or {@code null} to find first.
         */
        TestTreeRowClosure(Long expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BPlusTree<Long, Long> tree, BPlusIO<Long> io, long pageAddr, int idx)
            throws IgniteCheckedException {
            assert !found;

            found = expVal == null || io.getLookupRow(tree, pageAddr, idx).equals(expVal);

            return !found;
        }
    }

    /**
     *
     */
    static class TestTreeFindFirstClosure implements BPlusTree.TreeRowClosure<Long, Long> {
        /** */
        private Long val;


        /** {@inheritDoc} */
        @Override public boolean apply(BPlusTree<Long, Long> tree, BPlusIO<Long> io, long pageAddr, int idx)
            throws IgniteCheckedException {
            assert val == null;

            val = io.getLookupRow(tree, pageAddr, idx);

            return false;
        }
    }

    /**
     *
     */
    static class TestTreeFindFilteredClosure implements BPlusTree.TreeRowClosure<Long, Long> {
        /** */
        private final Set<Long> vals;

        /**
         * @param vals Values to allow in filter.
         */
        TestTreeFindFilteredClosure(Set<Long> vals) {
            this.vals = vals;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BPlusTree<Long, Long> tree, BPlusIO<Long> io, long pageAddr, int idx)
            throws IgniteCheckedException {
            Long val = io.getLookupRow(tree, pageAddr, idx);

            return vals.contains(val);
        }
    }

    private static class TestPageLockListener implements PageLockListener {
        /** */
        static ConcurrentMap<Object, Long> beforeReadLock = new ConcurrentHashMap<>();

        /** */
        static ConcurrentMap<Object, Long> beforeWriteLock = new ConcurrentHashMap<>();

        /** */
        static ConcurrentMap<Object, Map<Long, Long>> readLocks = new ConcurrentHashMap<>();

        /** */
        static ConcurrentMap<Object, Map<Long, Long>> writeLocks = new ConcurrentHashMap<>();

        /** */
        private final PageLockListener delegate;

        /**
         * @param delegate Real implementation of page lock listener.
         */
        private TestPageLockListener(
            PageLockListener delegate) {

            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
            delegate.onBeforeReadLock(cacheId, pageId, page);

            if (PRINT_LOCKS)
                X.println("  onBeforeReadLock: " + U.hexLong(pageId));

            assertNull(beforeReadLock.put(threadId(), pageId));
        }

        /** {@inheritDoc} */
        @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onReadLock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS)
                X.println("  onReadLock: " + U.hexLong(pageId));

            if (pageAddr != 0L) {
                long actual = PageIO.getPageId(pageAddr);

                checkPageId(pageId, pageAddr);

                assertNull(locks(true).put(pageId, actual));
            }

            assertEquals(Long.valueOf(pageId), beforeReadLock.remove(threadId()));
        }

        /** {@inheritDoc} */
        @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onReadUnlock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS)
                X.println("  onReadUnlock: " + U.hexLong(pageId));

            checkPageId(pageId, pageAddr);

            long actual = PageIO.getPageId(pageAddr);

            assertEquals(Long.valueOf(actual), locks(true).remove(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
            delegate.onBeforeWriteLock(cacheId, pageId, page);

            if (PRINT_LOCKS)
                X.println("  onBeforeWriteLock: " + U.hexLong(pageId));

            assertNull(beforeWriteLock.put(threadId(), pageId));
        }

        /** {@inheritDoc} */
        @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onWriteLock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS)
                X.println("  onWriteLock: " + U.hexLong(pageId));

            if (pageAddr != 0L) {
                checkPageId(pageId, pageAddr);

                long actual = PageIO.getPageId(pageAddr);

                if (actual == 0L)
                    actual = pageId; // It is a newly allocated page.

                assertNull(locks(false).put(pageId, actual));
            }

            assertEquals(Long.valueOf(pageId), beforeWriteLock.remove(threadId()));
        }

        /** {@inheritDoc} */
        @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onWriteUnlock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS)
                X.println("  onWriteUnlock: " + U.hexLong(pageId));

            assertEquals(effectivePageId(pageId), effectivePageId(PageIO.getPageId(pageAddr)));

            assertEquals(Long.valueOf(pageId), locks(false).remove(pageId));
        }

        /**
         * @param read Read or write locks.
         * @return Locks map.
         */
        private static Map<Long, Long> locks(boolean read) {
            ConcurrentMap<Object, Map<Long, Long>> m = read ? readLocks : writeLocks;

            Object thId = threadId();

            Map<Long, Long> locks = m.get(thId);

            if (locks == null) {
                locks = new ConcurrentLinkedHashMap<>();

                if (m.putIfAbsent(thId, locks) != null)
                    locks = m.get(thId);
            }

            return locks;
        }

        /**
         * @return {@code true} If current thread does not keep any locks.
         */
        static boolean checkNoLocks() {
            return locks(true).isEmpty() && locks(false).isEmpty();
        }
    }
}
