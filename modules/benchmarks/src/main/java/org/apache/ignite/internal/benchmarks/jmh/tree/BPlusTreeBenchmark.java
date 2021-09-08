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

package org.apache.ignite.internal.benchmarks.jmh.tree;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.logger.java.JavaLogger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
@State(Scope.Benchmark)
public class BPlusTreeBenchmark extends JmhAbstractBenchmark {
    /** */
    private static final short LONG_INNER_IO = 30000;

    /** */
    private static final short LONG_LEAF_IO = 30001;

    /** */
    private static final int PAGE_SIZE = 256;

    /** */
    private static final long MB = 1024 * 1024;

    /** */
    static int MAX_PER_PAGE = 0;

    /** */
    private static final int CACHE_ID = 100500;

    /** */
    private static final int KEYS = 1_000_000;

    /** */
    private TestTree tree;

    /** */
    private PageMemory pageMem;

    /**
     * Fake reuse list.
     */
    private static class FakeReuseList implements ReuseList {
        /** */
        private final ConcurrentLinkedDeque<Long> deque = new ConcurrentLinkedDeque<>();

        /** {@inheritDoc} */
        @Override public void addForRecycle(ReuseBag bag) {
            long pageId;

            while ((pageId = bag.pollFreePage()) != 0L)
                deque.addFirst(pageId);
        }

        /** {@inheritDoc} */
        @Override public long takeRecycledPage() {
            Long pageId = deque.pollFirst();

            return pageId == null ? 0L : pageId;
        }

        /** {@inheritDoc} */
        @Override public long initRecycledPage(long pageId, byte flag, PageIO initIO) {
            return pageId;
        }

        /** {@inheritDoc} */
        @Override public long recycledPagesCount() {
            return deque.size();
        }
    }

    /**
     * @return Allocated meta page ID.
     * @throws IgniteCheckedException If failed.
     */
    private FullPageId allocateMetaPage() throws IgniteCheckedException {
        return new FullPageId(pageMem.allocatePage(CACHE_ID, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX), CACHE_ID);
    }

    /**
     * @throws Exception If failed.
     */
    @Setup
    public void setup() throws Exception {
        pageMem = createPageMemory();

        tree = new TestTree(new FakeReuseList(), CACHE_ID, pageMem, allocateMetaPage().pageId());

        for (long l = 0; l < KEYS; l++)
            tree.put(l);
    }

    /**
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
        tree.destroy();

        pageMem.stop(true);
    }

    /**
     * @throws Exception If failed.
     * @return Value.
     */
    @Benchmark
    public Long get() throws Exception {
        Long key = ThreadLocalRandom.current().nextLong(KEYS);

        return tree.findOne(key);
    }

    /**
     * @throws Exception If failed.
     * @return Value.
     */
    @Benchmark
    public Long put() throws Exception {
        Long key = ThreadLocalRandom.current().nextLong(KEYS);

        return tree.put(key);
    }

    /**
     * Test tree.
     */
    protected static class TestTree extends BPlusTree<Long, Long> {
        /**
         * @param reuseList Reuse list.
         * @param cacheId Cache ID.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @throws IgniteCheckedException If failed.
         */
        TestTree(ReuseList reuseList, int cacheId, PageMemory pageMem, long metaPageId)
            throws IgniteCheckedException {
            super(
                "test",
                cacheId,
                null,
                pageMem,
                null,
                new AtomicLong(),
                metaPageId,
                reuseList,
                new IOVersions<>(new LongInnerIO()),
                new IOVersions<>(new LongLeafIO()),
                PageIdAllocator.FLAG_IDX,
                null,
                mockPageLockTrackerManager()
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

        /** */
        private static PageLockTrackerManager mockPageLockTrackerManager() {
            PageLockTrackerManager manager = mock(PageLockTrackerManager.class);

            when(manager.createPageLockTracker(anyString())).thenReturn(PageLockTrackerManager.NOOP_LSNR);

            return manager;
        }
    }

    /**
     * @return Page memory.
     */
    private PageMemory createPageMemory() {
        DataRegionConfiguration dataRegionConfiguration = new DataRegionConfiguration().setMaxSize(1024 * MB);

        DataRegionMetricsImpl dataRegionMetrics = mock(DataRegionMetricsImpl.class);
        PageMetrics pageMetrics = mock(PageMetrics.class);
        LongAdderMetric noOpMetric = new LongAdderMetric("foobar", null);

        when(dataRegionMetrics.cacheGrpPageMetrics(anyInt())).thenReturn(pageMetrics);

        when(pageMetrics.totalPages()).thenReturn(noOpMetric);
        when(pageMetrics.indexPages()).thenReturn(noOpMetric);

        PageMemory pageMem = new PageMemoryNoStoreImpl(
            new JavaLogger(),
            new UnsafeMemoryProvider(new JavaLogger()),
            null,
            PAGE_SIZE,
            dataRegionConfiguration,
            dataRegionMetrics,
            false);

        pageMem.start();

        return pageMem;
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run(8);
    }

    /**
     * Run benchmark.
     *
     * @param threads Amount of threads.
     * @throws Exception If failed.
     */
    private static void run(int threads) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(threads)
            .warmupIterations(10)
            .measurementIterations(10)
            .benchmarks(BPlusTreeBenchmark.class.getSimpleName())
            .jvmArguments("-Xms4g", "-Xmx4g")
            .run();
    }

    /**
     * Long inner.
     */
    private static final class LongInnerIO extends BPlusInnerIO<Long> {
        /**
         */
        LongInnerIO() {
            super(LONG_INNER_IO, 1, true, 8);
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

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, Long row) {
            PageUtils.putLong(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public Long getLookupRow(BPlusTree<Long, ?> tree, long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx));
        }
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
        @Override public Long getLookupRow(BPlusTree<Long, ?> tree, long pageAddr, int idx) {
            return PageUtils.getLong(pageAddr, offset(idx));
        }
    }
}
