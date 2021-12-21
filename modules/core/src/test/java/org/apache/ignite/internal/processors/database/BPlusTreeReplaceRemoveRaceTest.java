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

import java.io.Externalizable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test is based on {@link BPlusTreeSelfTest} and has a partial copy of its code.
 */
public class BPlusTreeReplaceRemoveRaceTest extends GridCommonAbstractTest {
    /** */
    private static final short PAIR_INNER_IO = 30000;

    /** */
    private static final short PAIR_LEAF_IO = 30001;

    /** */
    protected static final int PAGE_SIZE = 512;

    /** */
    private static final int CACHE_ID = 100500;

    /** */
    protected PageMemory pageMem;

    /** Tracking of locks holding. */
    private PageLockTrackerManager lockTrackerManager;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        pageMem = createPageMemory();

        lockTrackerManager = new PageLockTrackerManager(log, "testTreeManager");

        lockTrackerManager.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (pageMem != null)
            pageMem.stop(true);

        if (lockTrackerManager != null)
            lockTrackerManager.stop();
    }

    /**
     * @return Page memory.
     */
    protected PageMemory createPageMemory() {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log,
            new UnsafeMemoryProvider(log),
            null,
            PAGE_SIZE,
            plcCfg,
            new DataRegionMetricsImpl(plcCfg, new GridTestKernalContext(log())),
            true);

        pageMem.start();

        return pageMem;
    }

    /**
     * @return Allocated meta page ID.
     * @throws IgniteCheckedException If failed.
     */
    private FullPageId allocateMetaPage() throws IgniteCheckedException {
        return new FullPageId(pageMem.allocatePage(CACHE_ID, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX), CACHE_ID);
    }

    /**
     * Short for {@code T2<Integer, Integer>}.
     */
    protected static class Pair extends T2<Integer, Integer> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param key Key.
         * @param val Value.
         */
        public Pair(Integer key, Integer val) {
            super(key, val);
        }

        /**
         * No-arg constructor for {@link Externalizable}.
         */
        public Pair() {
        }
    }

    /**
     * Test tree, maps {@link Integer} to {@code Integer}.
     */
    protected static class TestPairTree extends BPlusTree<Pair, Pair> {
        /**
         * Constructor.
         *
         * @param cacheId Cache ID.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @throws IgniteCheckedException If failed.
         */
        public TestPairTree(
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
                null,
                new IOVersions<>(new TestPairInnerIO()),
                new IOVersions<>(new TestPairLeafIO()),
                PageIdAllocator.FLAG_IDX,
                new FailureProcessor(new GridTestKernalContext(log)) {
                    /** {@inheritdoc} */
                    @Override public boolean process(FailureContext failureCtx) {
                        return true;
                    }
                },
                lockTrackerManager
            );

            PageIO.registerTest(latestInnerIO(), latestLeafIO());

            initTree(true);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<Pair> io, long pageAddr, int idx, Pair n2)
            throws IgniteCheckedException {
            Pair n1 = io.getLookupRow(this, pageAddr, idx);

            return Integer.compare(n1.getKey(), n2.getKey());
        }

        /** {@inheritDoc} */
        @Override public Pair getRow(BPlusIO<Pair> io, long pageAddr, int idx, Object ignore)
            throws IgniteCheckedException {
            return io.getLookupRow(this, pageAddr, idx);
        }
    }

    /** */
    private static final class TestPairInnerIO extends BPlusInnerIO<Pair> {
        /** */
        TestPairInnerIO() {
            super(PAIR_INNER_IO, 1, true, 8);
        }

        /** {@inheritDoc} */
        @Override public int getMaxCount(long buf, int pageSize) {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public void store(long dst, int dstIdx, BPlusIO<Pair> srcIo, long src, int srcIdx)
            throws IgniteCheckedException {
            Pair row = srcIo.getLookupRow(null, src, srcIdx);

            store(dst, dstIdx, row, null, false);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, Pair row) {
            PageUtils.putInt(pageAddr, off, row.getKey());
            PageUtils.putInt(pageAddr, off + 4, row.getValue());
        }

        /** {@inheritDoc} */
        @Override public Pair getLookupRow(BPlusTree<Pair, ?> tree, long pageAddr, int idx) {
            int key = PageUtils.getInt(pageAddr, offset(idx));
            int val = PageUtils.getInt(pageAddr, offset(idx) + 4);

            return new Pair(key, val);
        }
    }

    /** */
    private static final class TestPairLeafIO extends BPlusLeafIO<Pair> {
        /** */
        TestPairLeafIO() {
            super(PAIR_LEAF_IO, 1, 8);
        }

        /** {@inheritDoc} */
        @Override public int getMaxCount(long pageAddr, int pageSize) {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, Pair row) {
            PageUtils.putInt(pageAddr, off, row.getKey());
            PageUtils.putInt(pageAddr, off + 4, row.getValue());
        }

        /** {@inheritDoc} */
        @Override public void store(long dst, int dstIdx, BPlusIO<Pair> srcIo, long src, int srcIdx) throws IgniteCheckedException {
            Pair row = srcIo.getLookupRow(null, src, srcIdx);

            store(dst, dstIdx, row, null, false);
        }

        /** {@inheritDoc} */
        @Override public Pair getLookupRow(BPlusTree<Pair, ?> tree, long pageAddr, int idx) {
            int key = PageUtils.getInt(pageAddr, offset(idx));
            int val = PageUtils.getInt(pageAddr, offset(idx) + 4);

            return new Pair(key, val);
        }
    }

    /**
     * Tests a very specific scenario for concurrent replace and remove that used to corrupt the tree before the fix.
     * <p/>
     *
     * Consider the following tree, represented by a {@code tree} variable in test:<br>
     * <pre><code>
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:0 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     * </code></pre>
     *
     * Individual replace {@code 4:0} to {@code 4:8} would take two steps and look like this:
     * <pre><code>
     * // Inner node goes first.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // Leaf node goes last.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:8 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     * </code></pre>
     *
     * Note that inbetween these two updates tree is fully unlocked and available for modifications. So, if one tries
     * to remove {@code 5:0} during the replacement, following modifications would happen:
     * <pre><code>
     * // Inner node update from replacement goes first, as before.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // Removal of 5:0 starts from the leaf.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   []        [ 6:0 ]   [ 7:0 ]
     *
     * // Merge of empty branch is now required, 4:8 is removed from inner node.
     *                                [ 5:0 ]
     *                            /              \
     *                 [ 2:0 ]                        [ 6:0 ]
     *               /         \                     /       \
     * [ 1:0 | 2:0 ]             [ 3:0 | 4:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // Inner replace is happening in the root. To do that, closest left value is retrieved from the leaf, it's 4:0.
     *                                [ 4:0 ]
     *                            /              \
     *                 [ 2:0 ]                        [ 6:0 ]
     *               /         \                     /       \
     * [ 1:0 | 2:0 ]             [ 3:0 | 4:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // At this point removal is complete. Last replacement step will do the following.
     *                               [ 4:0 ]
     *                            /              \
     *                 [ 2:0 ]                        [ 6:0 ]
     *               /         \                     /       \
     * [ 1:0 | 2:0 ]             [ 3:0 | 4:8 ]   [ 6:0 ]   [ 7:0 ]
     * </code></pre>
     *
     * It is clear that root has an invalid value {@code 4:0}, hence the tree should be considered corrupted.
     * This is the exact situation that test is trying to check.
     * <p/>
     *
     * Several iterations are required for this, given that there's no guaranteed way to force a tree to perform page
     * modifications in the desired order. Typically, less than {@code 10} attempts have been required to get a
     * corrupted tree. Value {@code 100} is arbitrary and has been chosen to be big enough for test to fail in case of
     * regression, but not too big so that test won't run for too long.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutRemove() throws Exception {
        for (int i = 0; i < 100; i++) {
            TestPairTree tree = prepareBPlusTree();

            // Exact tree from the description is constructed at this point.
            CyclicBarrier barrier = new CyclicBarrier(2);

            // This is the replace operation.
            IgniteInternalFuture<?> putFut = runAsync(() -> {
                barrier.await();

                tree.putx(new Pair(4, 999));
            });

            // This is the remove operation.
            IgniteInternalFuture<?> remFut = runAsync(() -> {
                barrier.await();

                tree.removex(new Pair(5, -1));
            });

            // Wait for both operations.
            try {
                putFut.get(1, TimeUnit.SECONDS);
            }
            finally {
                remFut.get(1, TimeUnit.SECONDS);
            }

            // Just in case.
            tree.validateTree();

            // Find a value associated with 4. It'll be right in the root page.
            Pair pair = tree.findOne(new Pair(4, -1));

            // Assert that it is valid.
            assertEquals(999, pair.getValue().intValue());
        }
    }

    /**
     * Checks that there will be no corrupted B+tree during concurrent update and deletion
     * of the same key that is contained in the inner and leaf nodes of the B+tree.
     *
     * NOTE: Test logic is the same as of {@link #testConcurrentPutRemove},
     * the only difference is that it operates (puts and removes) on a single key.
     * 
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutRemoveSameRow() throws Exception {
        for (int i = 0; i < 100; i++) {
            TestPairTree tree = prepareBPlusTree();

            // Exact tree from the description is constructed at this point.
            CyclicBarrier barrier = new CyclicBarrier(2);

            // This is the replace operation.
            IgniteInternalFuture<?> putFut = runAsync(() -> {
                barrier.await();

                tree.putx(new Pair(5, 999));
            });

            // This is the remove operation.
            IgniteInternalFuture<?> remFut = runAsync(() -> {
                barrier.await();

                tree.removex(new Pair(5, 0));
            });

            // Wait for both operations.
            try {
                putFut.get(1, TimeUnit.SECONDS);
            }
            finally {
                remFut.get(1, TimeUnit.SECONDS);
            }

            // Just in case.
            tree.validateTree();
        }
    }

    /**
     * Creates and fills a tree:
     * <pre><code>
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:0 ]                  [ 6:0 ]
     *               /       |       \              /      |
     * [ 1:0 | 2:0 ]->[ 3:0 | 4:0 ]->[ 5:0 ]->[ 6:0 ]->[ 7:0 ]
     * </code></pre>
     *
     * @return New B+tree.
     * @throws Exception If failed.
     */
    private TestPairTree prepareBPlusTree() throws Exception {
        TestPairTree tree = new TestPairTree(
            CACHE_ID,
            pageMem,
            allocateMetaPage().pageId(),
            lockTrackerManager
        );

        tree.putx(new Pair(1, 0));
        tree.putx(new Pair(2, 0));
        tree.putx(new Pair(4, 0));
        tree.putx(new Pair(6, 0));
        tree.putx(new Pair(7, 0));

        // Split root.
        tree.putx(new Pair(5, 0));

        // Split its left subtree.
        tree.putx(new Pair(3, 0));

        return tree;
    }
}
