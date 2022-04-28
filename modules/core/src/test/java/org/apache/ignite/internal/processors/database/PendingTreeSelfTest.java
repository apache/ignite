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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cache.tree.CacheIdAwarePendingEntryInnerIO;
import org.apache.ignite.internal.processors.cache.tree.CacheIdAwarePendingEntryLeafIO;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntryInnerIO;
import org.apache.ignite.internal.processors.cache.tree.PendingEntryLeafIO;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.MB;

/** */
public class PendingTreeSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_ID1 = 1;

    /** */
    private static final int CACHE_ID2 = 2;

    /** */
    private static final int CACHE_ID3 = 3;

    /** */
    private static final int LINK_ID = 1;

    /** */
    private static final int PAGE_SIZE = 512;

    /** */
    protected PageMemory pageMem;

    /** */
    private ReuseList reuseList;

    /** Tracking of locks holding. */
    private PageLockTrackerManager lockTrackerManager;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        pageMem = createPageMemory();

        reuseList = null; //createReuseList(CACHE_ID, pageMem, 0, true);

        lockTrackerManager = new PageLockTrackerManager(log, "testTreeManager") {
            @Override public PageLockListener createPageLockTracker(String name) {
                return new BPlusTreeSelfTest.TestPageLockListener(super.createPageLockTracker(name));
            }
        };

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
     * @return Allocated meta page ID.
     * @throws IgniteCheckedException If failed.
     */
    private FullPageId allocateMetaPage() throws IgniteCheckedException {
        return new FullPageId(pageMem.allocatePage(CACHE_ID2, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX), CACHE_ID2);
    }

    /** */
    @Test
    public void testPutRemoveRange() throws Throwable {
        TestPendingEntriesTree tree = createTree(true);
        AtomicReference<Throwable> stopped = new AtomicReference<>();

        int startIdx = 1;
        int cnt = 10_000;
        long endTime = U.currentTimeMillis() + 5_000;
        AtomicLong cntr = new AtomicLong();

        IgniteInternalFuture<Long> putFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                while (stopped.get() == null && U.currentTimeMillis() < endTime) {
                    int key = (int)(cntr.getAndIncrement() % cnt);

                    tree.putx(row(startIdx + key));
                }
            }
            catch (Throwable t) {
                t.printStackTrace();

                stopped.compareAndSet(null, t);
            }
        }, 4, "put");

        IgniteInternalFuture<Long> rmvFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                while (stopped.get() == null && U.currentTimeMillis() < endTime) {
                    int idx = startIdx + ThreadLocalRandom.current().nextInt(cnt);
                    int amount = 1 + ThreadLocalRandom.current().nextInt(5);

                    tree.removeRange(row(idx), row(idx + 10), amount);
                }
            }
            catch (Throwable t) {
                t.printStackTrace();

                stopped.compareAndSet(null, t);
            }
        }, 2, "remove");

        putFut.get(getTestTimeout());
        rmvFut.get(getTestTimeout());

        if (stopped.get() != null) {
            System.err.println(tree.printTree());

            throw stopped.get();
        }
    }

    /** */
    @Test
    public void testRemoveRange() throws IgniteCheckedException {
        TestPendingEntriesTree tree = createTree(true);

        assertEquals(0, tree.size());
        assertEquals(0, tree.rootLevel());

        for (int i = 0; i < 1000; i++) {
            tree.put(new PendingRow(CACHE_ID1, i + 1, LINK_ID));
            tree.put(row(i + 1));
            tree.put(new PendingRow(CACHE_ID3, i + 1, LINK_ID));
        }

        T2<Integer, Integer> rmvRange = new T2<>(17, 30);

        for (int i = rmvRange.get1(); i <= rmvRange.get2(); i++)
            tree.remove(row(i));

        int end = 50;

        List<PendingRow> res = tree.removeRange(row(rmvRange.get1()), row(end), 0);

        res.sort(Comparator.comparingLong(r -> r.expireTime));

        assertEquals(end - rmvRange.get2(), res.size());

        PendingRow startRow = res.get(0);
        PendingRow endRow = res.get(res.size() - 1);

        assertEquals(rmvRange.get2() + 1, startRow.expireTime);
        assertEquals(end, endRow.expireTime);

        res = tree.removeRange(row(rmvRange.get1()), row(end), 0);
        assertTrue(res.isEmpty());

        res = tree.removeRange(row(rmvRange.get1()), row(end + 1), 0);
        assertEquals(1, res.size());

        res = tree.removeRange(row(1001), row(10010), 0);

        assertNotNull(res);
        assertTrue(res.isEmpty());

        res = tree.removeRange(row(1000), row(10010), 0);
        assertNotNull(res);
        assertFalse(res.isEmpty());
        assertNotNull(res.get(0));
        assertEquals(1000, res.get(0).expireTime);

        res = tree.removeRange(row(100), row(200), 5);
        assertEquals(5, res.size());

        res = tree.removeRange(row(100), row(200), 95);
        assertEquals(95, res.size());

        tree.validateTree();
    }

    /**
     * @param expTime Expire time.
     * @return Row.
     */
    private PendingRow row(long expTime) {
        return new PendingRow(CACHE_ID2, expTime, LINK_ID);
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
     * @return Tree.
     */
    private TestPendingEntriesTree createTree(boolean sharedGrp) throws IgniteCheckedException {
        return new TestPendingEntriesTree(sharedGrp, CACHE_ID2, pageMem, allocateMetaPage().pageId(), reuseList, lockTrackerManager);
    }

    /** */
    static class TestPendingEntriesTree extends PendingEntriesTree {
        /** */
        public TestPendingEntriesTree(boolean shared, int cacheId, PageMemory pageMem, long metaPageId,
            ReuseList reuseList, PageLockTrackerManager lockTrackerManager) throws IgniteCheckedException {
            super(
                "test",
                cacheId,
                null,
                pageMem,
                null,
                new AtomicLong(),
                metaPageId,
                reuseList,
                shared ? CacheIdAwarePendingEntryInnerIO.VERSIONS : PendingEntryInnerIO.VERSIONS,
                shared ? CacheIdAwarePendingEntryLeafIO.VERSIONS : PendingEntryLeafIO.VERSIONS,
                PageIdAllocator.FLAG_IDX,
                new FailureProcessor(new GridTestKernalContext(log)) {
                    @Override public boolean process(FailureContext failureCtx) {
                        lockTrackerManager.dumpLocksToLog();

                        return true;
                    }
                },
                lockTrackerManager,
                null,
                true
            );

            PageIO.registerTest(latestInnerIO(), latestLeafIO());

            initTree(true);
        }

        /** {@inheritDoc} */
        @Override public PendingRow getRow(BPlusIO<PendingRow> io, long addr, int idx, Object f) throws IgniteCheckedException {
            return io.getLookupRow(this, addr, idx);
        }
    }
}
