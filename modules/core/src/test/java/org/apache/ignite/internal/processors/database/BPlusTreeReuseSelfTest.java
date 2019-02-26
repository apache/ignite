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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.IgniteTree;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.processors.cache.persistence.DataStructure.randomInt;
import static org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest.Operation.INVOKE;
import static org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest.Operation.INVOKE_ALL;
import static org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest.Operation.REMOVE_ALL;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.NOOP;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.PUT;
import static org.apache.ignite.internal.util.IgniteTree.OperationType.REMOVE;

/**
 * Test with reuse list.
 */
public class BPlusTreeReuseSelfTest extends BPlusTreeSelfTest {
    /** {@inheritDoc} */
    @Override protected ReuseList createReuseList(int cacheId, PageMemory pageMem, long rootId, boolean initNew)
        throws IgniteCheckedException {
        return new TestReuseList(cacheId, "test", pageMem, null, rootId, initNew);
    }

    /** {@inheritDoc} */
    @Override protected void assertNoLocks() {
        super.assertNoLocks();

        assertTrue(TestReuseList.checkNoLocks());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllConcurrent_1_false() throws Exception {
        CNT = 26;
        MAX_PER_PAGE = 1;

        doTestInvokeAllConcurrent(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllConcurrent_1_true() throws Exception {
        CNT = 26;
        MAX_PER_PAGE = 1;

        doTestInvokeAllConcurrent(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllConcurrent_2_false() throws Exception {
        CNT = 300;
        MAX_PER_PAGE = 2;

        doTestInvokeAllConcurrent(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllConcurrent_2_true() throws Exception {
        CNT = 300;
        MAX_PER_PAGE = 2;

        doTestInvokeAllConcurrent(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void doTestInvokeAllConcurrent(boolean canGetRow) throws Exception {
        GridStripedLock lock = new GridStripedLock(CNT);

        TestTree tree = createTestTree(canGetRow);
        Set<Long> set = Collections.newSetFromMap(new ConcurrentSkipListMap<>());

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync((Callable<?>)() -> {
            Random rnd = ThreadLocalRandom.current();

            while (!stop.get()) {
                int batchSize = 1 + rnd.nextInt(20);

                Set<Long> puts = new HashSet<>(batchSize);
                TreeSet<Long> all = new TreeSet<>();

                for (int j = 0; j < batchSize; j++) {
                    long x = rnd.nextInt(CNT);

                    if (rnd.nextBoolean())
                        puts.add(x);

                    all.add(x);
                }

                class UpdateAllClosure
                    implements IgniteTree.InvokeClosure<Long>, Function<Long,IgniteTree.InvokeClosure<Long>> {

                    Long lockedRow;
                    Long newRow;
                    IgniteTree.OperationType opType;

                    @Override public IgniteTree.InvokeClosure<Long> apply(Long searchRow) {
                        unlockRow();

                        opType = puts.contains(searchRow) ? PUT : REMOVE;
                        newRow = searchRow;

                        lockRow(searchRow);

                        if (opType == PUT)
                            set.add(searchRow);
                        else
                            set.remove(searchRow);

                        return this;
                    }

                    @Override public void call(@Nullable Long foundRow) throws IgniteCheckedException {
                        if (foundRow != null)
                            assertEquals(newRow, foundRow);

                        if ((foundRow == null && opType == REMOVE) || (foundRow != null && opType == PUT)) {
                            opType = NOOP;
                            newRow = null;
                        }
                    }

                    @Override public Long newRow() {
                        Long r = newRow;
                        newRow = null;
                        return Objects.requireNonNull(r);
                    }

                    @Override public IgniteTree.OperationType operationType() {
                        IgniteTree.OperationType t = opType;
                        opType = null;
                        return Objects.requireNonNull(t);
                    }

                    void lockRow(Long row) {
                        assertNull(lockedRow);
                        lock.lock(row.longValue());
                        lockedRow = row;
                    }

                    void unlockRow() {
                        if (lockedRow != null) {
                            lock.unlock(lockedRow.longValue());
                            lockedRow = null;
                        }
                    }
                }

                UpdateAllClosure c = new UpdateAllClosure();

                tree.invokeAll(all.iterator(), null, c);

                c.unlockRow(); // The last row must be manually unlocked here.
            }

            return null;
        }, 16);

        fut.listen((f) -> stop.set(true));

        long start = nanoTime();
        long timeout = SECONDS.toNanos(15);

        try {
            while (nanoTime() - start < timeout) {
                doSleep(100);

                lock.lockAll();

                try {
                    assertEqualContents(tree, set);
                }
                finally {
                    lock.unlockAll();
                }
            }
        }
        finally {
            stop.set(true);
        }

        fut.get(3000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAll_1_true() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 26;

        doTestInvokeAll(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAll_1_false() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 26;

        doTestInvokeAll(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAll_2_true() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 300;

        doTestInvokeAll(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAll_2_false() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 300;

        doTestInvokeAll(false);
    }

    /**
     * @param canGetRow Can get row.
     * @throws Exception If failed.
     */
    private void doTestInvokeAll(boolean canGetRow) throws Exception {
        TestTree tree = createTestTree(canGetRow);
        TreeSet<Long> set = new TreeSet<>();

        for (int i = 0; i < 10_000; i++) {
            long batchSize = 1 + randomInt(CNT);

            TreeSet<Long> puts = new TreeSet<>();
            TreeSet<Long> all = new TreeSet<>();

            for (int j = 0; j < batchSize; j++) {
                long x = randomInt(CNT);

                if (randomInt(2) == 0)
                    puts.add(x);

                all.add(x);
            }

            TreeSet<Long> rmvs = new TreeSet<>(all);
            rmvs.removeAll(puts);

            set.addAll(puts);
            set.removeAll(rmvs);

            class UpdateAllClosure
                implements IgniteTree.InvokeClosure<Long>, Function<Long, IgniteTree.InvokeClosure<Long>> {
                /** */
                Long newRow;

                /** */
                IgniteTree.OperationType opType;

                /** {@inheritDoc} */
                @Override public IgniteTree.InvokeClosure<Long> apply(Long searchRow) {
                    opType = puts.contains(searchRow) ? PUT : REMOVE;
                    newRow = searchRow;

                    return this;
                }

                /** {@inheritDoc} */
                @Override public void call(@Nullable Long foundRow) throws IgniteCheckedException {
                    if (foundRow != null)
                        assertEquals(newRow, foundRow);

                    if ((foundRow == null && opType == REMOVE) || (foundRow != null && opType == PUT)) {
                        opType = NOOP;
                        newRow = null;
                    }
                }

                /** {@inheritDoc} */
                @Override public Long newRow() {
                    Long r = newRow;
                    newRow = null;
                    return Objects.requireNonNull(r);
                }

                /** {@inheritDoc} */
                @Override public IgniteTree.OperationType operationType() {
                    IgniteTree.OperationType t = opType;
                    opType = null;
                    return Objects.requireNonNull(t);
                }
            }

            tree.invokeAll(all.iterator(), null, new UpdateAllClosure());

            tree.validateTree();

            assertEqualContents(tree, set);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllRemoveAll_1_true() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 26;

        doTestPutAllRemoveAll(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllRemoveAll_1_false() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 26;

        doTestPutAllRemoveAll(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllRemoveAll_2_true() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 300;

        doTestPutAllRemoveAll(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllRemoveAll_2_false() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 300;

        doTestPutAllRemoveAll(false);
    }

    /**
     * @param canGetRow Can get row.
     * @throws Exception If failed.
     */
    private void doTestPutAllRemoveAll(boolean canGetRow) throws Exception {
        TestTree tree = createTestTree(canGetRow);
        TreeSet<Long> set = new TreeSet<>();

        for (int i = 0; i < 10_000; i++) {
            long batchSize = 1 + randomInt(CNT);

            TreeSet<Long> puts = new TreeSet<>();
            TreeSet<Long> all = new TreeSet<>();

            for (int j = 0; j < batchSize; j++) {
                long x = randomInt(CNT);

                if (randomInt(2) == 0)
                    puts.add(x);

                all.add(x);
            }

            TreeSet<Long> rmvs = new TreeSet<>(all);
            rmvs.removeAll(puts);

            List<Long> putRes = new ArrayList<>();
            List<Boolean> putxRes = new ArrayList<>();

            for (Long x : puts) {
                boolean contains = set.contains(x);
                putRes.add(contains ? x : null);
                putxRes.add(contains);
            }

            List<Long> rmvRes = new ArrayList<>();
            List<Boolean> rmvxRes = new ArrayList<>();

            for (Long x : rmvs) {
                boolean contains = set.contains(x);
                rmvRes.add(contains ? x : null);
                rmvxRes.add(contains);
            }

            set.addAll(puts);
            set.removeAll(rmvs);

            if (!puts.isEmpty()) {
                if (randomInt(2) == 0)
                    assertEquals(putRes, tree.putAll(puts.iterator()));
                else
                    assertEquals(putxRes, tree.putAllx(puts.iterator()));
            }

            if (!rmvs.isEmpty()) {
                if (randomInt(2) == 0)
                    assertEquals(rmvRes, tree.removeAll(rmvs.iterator()));
                else
                    assertEquals(rmvxRes, tree.removeAllx(rmvs.iterator()));
            }

            assertEqualContents(tree, set);

            TreeSet<Long> gets = new TreeSet<>();

            for (int j = 0; j < batchSize; j++) {
                long x = randomInt(CNT);

                gets.add(x);
            }

            List<Long> foundRows = tree.findAll(gets.iterator(), null, null);

            int ix = 0;

            for (Long x : gets) {
                Long y = foundRows.get(ix++);

                if (set.contains(x))
                    assertEquals(x, y);
                else
                    assertNull(y);
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRemovePageLeaks_true() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(true, Operation.REMOVE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRemovePageLeaks_false() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(false, Operation.REMOVE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testInvokePageLeaks_true() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(true, INVOKE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testInvokePageLeaks_false() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(false, INVOKE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testInvokeAllPageLeaks_true() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(true, INVOKE_ALL);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testInvokeAllPageLeaks_false() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(false, INVOKE_ALL);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRemoveAllPageLeaks_true() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(true, REMOVE_ALL);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testRemoveAllPageLeaks_false() throws IgniteCheckedException {
        doTestInvokeAllPageLeaks(false, REMOVE_ALL);
    }

    /**
     * @param canGetRow Can get row.
     * @throws IgniteCheckedException If failed.
     */
    public void doTestInvokeAllPageLeaks(boolean canGetRow, Operation op) throws IgniteCheckedException {
        class RemoveAllClosure
            implements IgniteTree.InvokeClosure<Long>, Function<Long, IgniteTree.InvokeClosure<Long>> {
            @Override public IgniteTree.InvokeClosure<Long> apply(Long aLong) {
                return this;
            }

            @Override public void call(@Nullable Long row) throws IgniteCheckedException {
                // No-op.
            }

            @Override public Long newRow() {
                return null;
            }

            @Override public IgniteTree.OperationType operationType() {
                return REMOVE;
            }
        }

        long prevPages = 0;

        for (int i = 0; i < 10; i++) {
            TestTree tree = createTestTree(canGetRow);

            assertTrue(tree.isEmpty());

            int rowsCnt = 3000;

            for (long x = 0; x < rowsCnt; x++)
                tree.put(x);

            assertEquals(rowsCnt, tree.size());

            long pages = pageMem.loadedPages();

            info("Pages: " + pages);

            if (i < 2)
                prevPages = pages;
            else
                assertEquals(prevPages, pages);

            List<Long> rows = new ArrayList<>(rowsCnt);

            tree.find(null, null).forEach(rows::add);

            switch (op) {
                case REMOVE:
                    for (Long row : rows)
                        tree.removex(row);
                    break;

                case INVOKE:
                    for (Long row : rows)
                        tree.invoke(row, null, new RemoveAllClosure());
                    break;

                case INVOKE_ALL:
                    tree.invokeAll(rows.iterator(), null, new RemoveAllClosure());
                    break;

                case REMOVE_ALL:
                    tree.removeAll(rows.iterator());
                    break;

                default:
                    fail();
            }

            assertTrue(tree.isEmpty());

            tree.destroy();
        }
    }

    enum Operation {
        REMOVE, REMOVE_ALL, INVOKE, INVOKE_ALL
    }

    /**
     *
     */
    private static class TestReuseList extends ReuseListImpl {
        /** */
        private static ThreadLocal<Set<Long>> readLocks = new ThreadLocal<Set<Long>>() {
            @Override protected Set<Long> initialValue() {
                return new HashSet<>();
            }
        };

        /** */
        private static ThreadLocal<Set<Long>> writeLocks = new ThreadLocal<Set<Long>>() {
            @Override protected Set<Long> initialValue() {
                return new HashSet<>();
            }
        };

        /**
         * @param cacheId    Cache ID.
         * @param name       Name (for debug purpose).
         * @param pageMem    Page memory.
         * @param wal        Write ahead log manager.
         * @param metaPageId Metadata page ID.
         * @param initNew    {@code True} if new metadata should be initialized.
         * @throws IgniteCheckedException If failed.
         */
        public TestReuseList(
            int cacheId,
            String name,
            PageMemory pageMem,
            IgniteWriteAheadLogManager wal,
            long metaPageId,
            boolean initNew
        ) throws IgniteCheckedException {
            super(cacheId, name, pageMem, wal, metaPageId, initNew);
        }

        /** {@inheritDoc} */
        @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().remove(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
            if (pageAddr == 0L)
                return; // Failed to lock.

            checkPageId(pageId, pageAddr);

            assertTrue(writeLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
            assertEquals(effectivePageId(pageId), effectivePageId(PageIO.getPageId(pageAddr)));

            assertTrue(writeLocks.get().remove(pageId));
        }

        static boolean checkNoLocks() {
            return readLocks.get().isEmpty() && writeLocks.get().isEmpty();
        }
    }
}