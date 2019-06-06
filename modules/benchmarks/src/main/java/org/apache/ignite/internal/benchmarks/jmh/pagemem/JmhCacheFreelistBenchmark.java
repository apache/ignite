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

package org.apache.ignite.internal.benchmarks.jmh.pagemem;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.evict.NoOpPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.stat.IoStatisticsHolder;
import org.apache.ignite.internal.stat.IoStatisticsHolderNoOp;
import org.apache.ignite.logger.java.JavaLogger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Performance comparision between FreeList.insertRow(..) and FreeList.insertRows(..).
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xms1g", "-server", "-XX:+AggressiveOpts", "-XX:MaxMetaspaceSize=256m", "-ea"})
@OutputTimeUnit(MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 10, time = 200, timeUnit = MILLISECONDS)
@Measurement(iterations = 11, time = 200, timeUnit = MILLISECONDS)
public class JmhCacheFreelistBenchmark {
    /** */
    private static final long MEMORY_REGION_SIZE = 10 * 1024 * 1024 * 1024L; // 10 GB

    /** */
    private static final int PAGE_SIZE = 4096;

    /** */
    private static final int ROWS_COUNT = 200;

    /** */
    public enum DATA_ROW_SIZE {
        /** */
        r4_64(4, 64),

        /** */
        r100_300(100, 300),

        /** */
        r300_700(300, 700),

        /** */
        r700_1200(700, 1200),

        /** */
        r1200_3000(1_200, 3_000),

        /** */
        r1000_8000(1_000, 8_000),

        /** Large objects only. */
        r4000_16000(4_000, 16_000),

        /** Mixed objects, mostly large objects. */
        r100_32000(100, 32_000);

        /** */
        private final int min;

        /** */
        private final int max;

        /** */
        DATA_ROW_SIZE(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }

    /**
     * Check {@link FreeList#insertDataRow(Storable, IoStatisticsHolder)} performance.
     */
    @Benchmark
    public void insertRow(FreeListProvider provider, Data rows) throws IgniteCheckedException {
        for (CacheDataRow row : rows)
            provider.freeList.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE);
    }

    /**
     * Check {@link FreeList#insertDataRows(Collection, IoStatisticsHolder)} performance.
     */
    @Benchmark
    public void insertRows(FreeListProvider provider, Data rows) throws IgniteCheckedException {
        provider.freeList.insertDataRows(rows, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** */
    @State(Scope.Thread)
    public static class Data extends AbstractCollection<CacheDataRow> {
        /** */
        @Param
        private DATA_ROW_SIZE range;

        /** */
        private Collection<CacheDataRow> rows = new ArrayList<>(ROWS_COUNT);

        /** */
        @Setup(Level.Invocation)
        public void prepare() {
            Random rnd = ThreadLocalRandom.current();

            int randomRange = range.max - range.min;

            for (int i = 0; i < ROWS_COUNT; i++) {
                int keySize = (range.min + rnd.nextInt(randomRange)) / 2;
                int valSize = (range.min + rnd.nextInt(randomRange)) / 2;

                CacheDataRow row = new TestDataRow(keySize, valSize);

                rows.add(row);
            }
        }

        /** */
        @TearDown(Level.Invocation)
        public void cleanup() {
            rows.clear();
        }

        /** {@inheritDoc} */
        @Override public Iterator<CacheDataRow> iterator() {
            return rows.iterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return rows.size();
        }
    }

    /** */
    @State(Scope.Thread)
    public static class FreeListProvider {
        /** */
        private final DataRegionConfiguration plcCfg =
            new DataRegionConfiguration().setInitialSize(MEMORY_REGION_SIZE).setMaxSize(MEMORY_REGION_SIZE);

        /** */
        private final JavaLogger log = new JavaLogger();

        /** */
        private PageMemory pageMem;

        /** */
        private FreeList<CacheDataRow> freeList;

        /** */
        @Setup(Level.Trial)
        public void setup() throws IgniteCheckedException {
            pageMem = createPageMemory(log, PAGE_SIZE, plcCfg);

            freeList = createFreeList(pageMem, plcCfg);
        }

        /** */
        @TearDown(Level.Trial)
        public void tearDown() {
            pageMem.stop(true);
        }

        /**
         * @return Page memory.
         */
        protected PageMemory createPageMemory(IgniteLogger log, int pageSize, DataRegionConfiguration plcCfg) {
            PageMemory pageMem = new PageMemoryNoStoreImpl(log,
                new UnsafeMemoryProvider(log),
                null,
                pageSize,
                plcCfg,
                new DataRegionMetricsImpl(plcCfg),
                true);

            pageMem.start();

            return pageMem;
        }

        /**
         * @param pageMem Page memory.
         * @return Free list.
         * @throws IgniteCheckedException If failed.
         */
        private FreeList<CacheDataRow> createFreeList(
            PageMemory pageMem,
            DataRegionConfiguration plcCfg
        ) throws IgniteCheckedException {
            long metaPageId = pageMem.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);

            DataRegionMetricsImpl regionMetrics = new DataRegionMetricsImpl(plcCfg);

            DataRegion dataRegion = new DataRegion(pageMem, plcCfg, regionMetrics, new NoOpPageEvictionTracker());

            return new CacheFreeListImpl(1, "freelist", regionMetrics, dataRegion, null,
                null, metaPageId, true);
        }
    }

    /** */
    private static class TestDataRow extends CacheDataRowAdapter {
        /** */
        private long link;

        /**
         * @param keySize Key size.
         * @param valSize Value size.
         */
        private TestDataRow(int keySize, int valSize) {
            super(
                new KeyCacheObjectImpl(0, new byte[keySize], 0),
                new CacheObjectImpl(0, new byte[valSize]),
                new GridCacheVersion(keySize, valSize, 1),
                0
            );
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return link;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            this.link = link;
        }
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
            .include(JmhCacheFreelistBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
