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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.util.IgniteTree;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.processors.cache.persistence.DataStructure.randomInt;
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
    public void testInvokeAll_true() throws Exception {
        doTestInvokeAll(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAll_false() throws Exception {
        doTestInvokeAll(false);
    }

    /**
     * @param canGetRow Can get row.
     * @throws Exception If failed.
     */
    private void doTestInvokeAll(boolean canGetRow) throws Exception {
        CNT = 1000;

        TestTree tree = createTestTree(canGetRow);
        TreeSet<Long> set = new TreeSet<>();

        for (int i = 0; i < 20_000; i++) {
            long batchSize = 2 + randomInt(100);

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
                    newRow = opType == PUT ? searchRow : null;

                    return this;
                }

                /** {@inheritDoc} */
                @Override public void call(@Nullable Long foundRow) throws IgniteCheckedException {
                    if ((foundRow == null && opType == REMOVE) || (foundRow != null && opType == PUT)) {
                        opType = NOOP;
                        newRow = null;
                    }
                }

                /** {@inheritDoc} */
                @Override public Long newRow() {
                    return Objects.requireNonNull(newRow);
                }

                /** {@inheritDoc} */
                @Override public IgniteTree.OperationType operationType() {
                    return Objects.requireNonNull(opType);
                }
            }

            tree.invokeAll(all.iterator(), null, new UpdateAllClosure());

            assertEqualContents(tree, set);
        }
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