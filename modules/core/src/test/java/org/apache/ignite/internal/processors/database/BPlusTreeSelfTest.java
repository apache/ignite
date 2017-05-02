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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.MemoryMetricsImpl;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.rnd;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.NOOP;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.PUT;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.REMOVE;

/**
 */
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

    /** */
    protected PageMemory pageMem;

    /** */
    private ReuseList reuseList;

    /** */
    private static final Collection<Long> rmvdIds = new GridConcurrentHashSet<>();


//    /** {@inheritDoc} */
//    @Override protected long getTestTimeout() {
//        return 25 * 60 * 1000;
//    }

    /**
     * Check that we do not keep any locks at the moment.
     */
    protected void assertNoLocks() {
        assertTrue(TestTree.checkNoLocks());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        long seed = System.nanoTime();

        X.println("Test seed: " + seed + "L; // ");

        rnd = new Random(seed);

        pageMem = createPageMemory();

        reuseList = createReuseList(CACHE_ID, pageMem, 0, true);
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
            pageMem.stop();

            MAX_PER_PAGE = 0;
            PUT_INC = 1;
            RMV_INC = -1;
            CNT = 10;
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
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
    public void _testBenchInvoke() throws IgniteCheckedException {
        MAX_PER_PAGE = 10;

        TestTree tree = createTestTree(true);

        long start = System.nanoTime();

        for (int i = 0; i < 10_000_000; i++) {
            final long key = BPlusTree.randomInt(1000);

//            tree.findOne(key); // 39
//            tree.putx(key); // 22

            tree.invoke(key, null, new IgniteTree.InvokeClosure<Long>() { // 25
                @Override public void call(@Nullable Long row) throws IgniteCheckedException {
                    // No-op.
                }

                @Override public Long newRow() {
                    return key;
                }

                @Override public IgniteTree.OperationType operationType() {
                    return PUT;
                }
            });
        }

        X.println("   __ time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
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

            assertNoLocks();

            tree.validateTree();

            assertNoLocks();
        }

        X.println(tree.printTree());

        assertNoLocks();

        assertNull(tree.findOne(-1L));

        for (long x = 0; x < cnt; x++)
            assertEquals(x, tree.findOne(x).longValue());

        assertNoLocks();

        assertNull(tree.findOne(cnt));

        for (long x = RMV_INC > 0 ? 0 : cnt - 1; x >= 0 && x < cnt; x += RMV_INC) {
            X.println(" -- " + x);

            assertEquals(Long.valueOf(x), tree.remove(x));

            assertNoLocks();

            X.println(tree.printTree());

            assertNoLocks();

            assertNull(tree.findOne(x));

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
     * @throws IgniteCheckedException If failed.
     */
    public void testRandomInvoke_1_30_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomInvoke(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
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
    public void testRandomPutRemove_1_30_0() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemove(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testRandomPutRemove_1_30_1() throws IgniteCheckedException {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemove(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassiveRemove3_false() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassiveRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassiveRemove3_true() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassiveRemove(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassiveRemove2_false() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassiveRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassiveRemove2_true() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassiveRemove(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassiveRemove1_false() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassiveRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testMassivePut1_true() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassivePut(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassivePut1_false() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassivePut(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassivePut2_true() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassivePut(true);
    }

    public void testMassivePut2_false() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassivePut(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMassivePut3_true() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassivePut(true);
    }

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
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_1_30_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemoveMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_1_30_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemoveMultithreaded(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_2_50_0() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 50;

        doTestRandomPutRemoveMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_2_50_1() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 50;

        doTestRandomPutRemoveMultithreaded(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_3_70_0() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 70;

        doTestRandomPutRemoveMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_3_70_1() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 70;

        doTestRandomPutRemoveMultithreaded(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
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
     * @param canGetRow Can get row from inner page.
     * @throws Exception If failed.
     */
    private void doTestRandomPutRemoveMultithreaded(boolean canGetRow) throws Exception {
        final TestTree tree = createTestTree(canGetRow);

        final Map<Long,Long> map = new ConcurrentHashMap8<>();

        final int loops = reuseList == null ? 20_000 : 60_000;

        final GridStripedLock lock = new GridStripedLock(256);

        final String[] ops = {"put", "rmv", "inv_put", "inv_rmv"};

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < loops; i++) {
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

        final AtomicBoolean stop = new AtomicBoolean();

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
                }

                return null;
            }
        }, 4, "find");

        try {
            fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            stop.set(true);

            fut2.get();
            fut3.get();
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
        TestTree tree = new TestTree(reuseList, canGetRow, CACHE_ID, pageMem, allocateMetaPage().pageId());

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
        /** */
        private static ConcurrentMap<Object, Long> beforeReadLock = new ConcurrentHashMap8<>();

        /** */
        private static ConcurrentMap<Object, Long> beforeWriteLock = new ConcurrentHashMap8<>();

        /** */
        private static ConcurrentMap<Object, Map<Long, Long>> readLocks = new ConcurrentHashMap8<>();

        /** */
        private static ConcurrentMap<Object, Map<Long, Long>> writeLocks = new ConcurrentHashMap8<>();

        /**
         * @param reuseList Reuse list.
         * @param canGetRow Can get row from inner page.
         * @param cacheId Cache ID.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @throws IgniteCheckedException If failed.
         */
        public TestTree(ReuseList reuseList, boolean canGetRow, int cacheId, PageMemory pageMem, long metaPageId)
            throws IgniteCheckedException {
            super("test", cacheId, pageMem, null, new AtomicLong(), metaPageId, reuseList,
                new IOVersions<>(new LongInnerIO(canGetRow)), new IOVersions<>(new LongLeafIO()));

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
        @Override protected Long getRow(BPlusIO<Long> io, long pageAddr, int idx, Object ignore)
            throws IgniteCheckedException {
            assert io.canGetRow() : io;

            return io.getLookupRow(this, pageAddr, idx);
        }

        /**
         * @return Thread ID.
         */
        private static Object threadId() {
            return Thread.currentThread().getId(); //.getName();
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

        /** {@inheritDoc} */
        @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
//            X.println("  onBeforeReadLock: " + U.hexLong(page.id()));
//
//            U.dumpStack();

            assertNull(beforeReadLock.put(threadId(), pageId));
        }

        /** {@inheritDoc} */
        @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
//            X.println("  onReadLock: " + U.hexLong(page.id()));

            if (pageAddr != 0L) {
                long actual = PageIO.getPageId(pageAddr);

                checkPageId(pageId, pageAddr);

                assertNull(locks(true).put(pageId, actual));
            }

            assertEquals(Long.valueOf(pageId), beforeReadLock.remove(threadId()));
        }

        /** {@inheritDoc} */
        @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
//            X.println("  onReadUnlock: " + U.hexLong(page.id()));

            checkPageId(pageId, pageAddr);

            long actual = PageIO.getPageId(pageAddr);

            assertEquals(Long.valueOf(actual), locks(true).remove(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
//            X.println("  onBeforeWriteLock: " + U.hexLong(page.id()));

            assertNull(beforeWriteLock.put(threadId(), pageId));
        }

        /** {@inheritDoc} */
        @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
//            X.println("  onWriteLock: " + U.hexLong(page.id()));
//
//            U.dumpStack();

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
//            X.println("  onWriteUnlock: " + U.hexLong(page.id()));

            assertEquals(effectivePageId(pageId), effectivePageId(PageIO.getPageId(pageAddr)));

            assertEquals(Long.valueOf(pageId), locks(false).remove(pageId));
        }

        /**
         * @return {@code true} If current thread does not keep any locks.
         */
        static boolean checkNoLocks() {
            return locks(true).isEmpty() && locks(false).isEmpty();
        }

        /**
         * @param b String builder.
         * @param locks Locks.
         * @param beforeLock Before lock.
         */
        private static void printLocks(SB b, ConcurrentMap<Object, Map<Long, Long>> locks, Map<Object, Long> beforeLock) {
            for (Map.Entry<Object,Map<Long,Long>> entry : locks.entrySet()) {
                Object thId = entry.getKey();

                b.a(" ## " + thId);

                Long z = beforeLock.get(thId);

                if (z != null)
                    b.a("   --> ").appendHex(z).a("  (").appendHex(effectivePageId(z)).a(')');

                b.a('\n');

                for (Map.Entry<Long,Long> x : entry.getValue().entrySet())
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

            printLocks(b, readLocks, beforeReadLock);

            b.a("\n-+------write---------\n");

            printLocks(b, writeLocks, beforeWriteLock);

            return b.toString();
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
        MemoryPolicyConfiguration plcCfg = new MemoryPolicyConfiguration().setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log,
            new UnsafeMemoryProvider(log),
            null,
            PAGE_SIZE,
            plcCfg,
            new MemoryMetricsImpl(plcCfg), true);

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
}
