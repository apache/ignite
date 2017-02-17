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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.IndexKeeper;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexHelper;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueString;
import org.jsr166.ConcurrentHashMap8;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 *
 */
@State(Scope.Benchmark)
public class H2TreeBenchmark extends JmhAbstractBenchmark {

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final int INLINE_SIZE = 32;

    /** */
    private static final long MB = 1024 * 1024;

    /** */
    private static final int CPUS = Runtime.getRuntime().availableProcessors();

    /** */
    private static final int CACHE_ID = 100500;

    /** */
    private static final int KEYS = 100_000;

    /** */
    private PageMemory pageMem;

    /** */
    private H2Tree tree;

    /** */
    private ConcurrentHashMap8<Long, GridH2Row> rows = new ConcurrentHashMap8<>(KEYS);

    /** */
    private List<InlineIndexHelper> idxs;

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run(2);
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
            .benchmarks(H2TreeBenchmark.class.getSimpleName())
            .jvmArguments("-Xms4g", "-Xmx4g")
            .run();
    }

    /**
     * @throws Exception If failed.
     */
    @Setup
    public void setup() throws Exception {

        pageMem = createPageMemory();
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        setupTable();

        IndexKeeper.setContext(new IndexKeeper.PageContext(idxs));

        for (long i = 1; i < KEYS; i++) {
            GridH2Row row = makeRow(i, "aaa");
            tree.put(row);
            rows.put(i, row);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
        pageMem.stop();

    }

    /** */
    @Benchmark
    public void put() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long key = rnd.nextLong(1, KEYS);

//        GridH2Row row = makeRow(key, UUID.randomUUID().toString());
        GridH2Row row = makeRow(key, "bb");
        try {
            IndexKeeper.setContext(new IndexKeeper.PageContext(idxs));
            rows.put(key, row);
            tree.put(row);
        }
        finally {
            IndexKeeper.clearContext();
        }
    }

    /** */
    private void setupTable() throws Exception {
        FullPageId metaPage = allocateMetaPage(pageMem);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        idxs = Arrays.asList(new InlineIndexHelper(Value.LONG, 0, 0), new InlineIndexHelper(Value.STRING, 1, 0));

        IndexColumn col1 = new IndexColumn();
        col1.columnName = "c1";
        col1.sortType = 0;
        col1.column = new Column("c1", Value.LONG);

        IndexColumn col2 = new IndexColumn();
        col2.columnName = "c2";
        col2.sortType = 0;
        col2.column = new Column("c2", Value.STRING);

        final IndexColumn[] cols = new IndexColumn[] {col1, col2};

        tree = new H2Tree("name", new MockReuseList(), 1, pageMem, new NoOpWALManager(), new AtomicLong(), new MockRowFactory(), metaPage.pageId(), true, cols, idxs, INLINE_SIZE) {
            @Override public int compareValues(Value v1, Value v2, int order) {
                if (v1 == v2)
                    return 0;

                int comp = v1.compareTypeSafe(v2, CompareMode.getInstance(CompareMode.DEFAULT, 0));

                if ((order & SortOrder.DESCENDING) != 0)
                    comp = -comp;

                return comp;
            }
        };
    }

    /** */
    public final GridH2Row makeRow(long v1, String v2) {
        GridH2Row row = GridH2RowFactory.create(
            ValueLong.get(v1),
            ValueString.get(v2));

        row.link(v1);
        return row;
    }

    /**
     * @return Page memory.
     * @throws Exception If failed.
     */
    private PageMemory createPageMemory() throws Exception {
        long[] sizes = new long[CPUS];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / CPUS;

        PageMemory pageMem = new PageMemoryNoStoreImpl(new JavaLogger(),
            new UnsafeMemoryProvider(sizes),
            null,
            PAGE_SIZE,
            false);

        pageMem.start();

        return pageMem;
    }

    /**
     * @return Allocated meta page ID.
     * @throws IgniteCheckedException If failed.
     */
    private static FullPageId allocateMetaPage(PageMemory pageMem) throws IgniteCheckedException {
        return new FullPageId(pageMem.allocatePage(CACHE_ID, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX), CACHE_ID);
    }

    /** */
    private class MockRowFactory extends H2RowFactory {

        /** */
        public MockRowFactory() {
            super(null, null);
        }

        /** {@inheritDoc} */
        @Override public GridH2Row getRow(long link) throws IgniteCheckedException {
            // heavy operation.
            U.sleep(1);
            return rows.get(link);
        }
    }

    /**
     * Mock reuse list.
     */
    private static class MockReuseList implements ReuseList {
        /** */
        private final ConcurrentLinkedDeque<Long> deque = new ConcurrentLinkedDeque<>();

        /** {@inheritDoc} */
        @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
            long pageId;

            while ((pageId = bag.pollFreePage()) != 0L)
                deque.addFirst(pageId);
        }

        /** {@inheritDoc} */
        @Override public long takeRecycledPage() throws IgniteCheckedException {
            Long pageId = deque.pollFirst();

            return pageId == null ? 0L : pageId;
        }

        /** {@inheritDoc} */
        @Override public long recycledPagesCount() throws IgniteCheckedException {
            return deque.size();
        }
    }

}
