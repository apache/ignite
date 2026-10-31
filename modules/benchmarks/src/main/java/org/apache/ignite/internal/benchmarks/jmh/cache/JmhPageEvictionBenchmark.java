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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the overhead of page eviction on the write paths of an in-memory data region with enabled page eviction.
 * <ul>
 *     <li>{@link #putBelowThreshold} — puts within a bounded key range that keeps the region below the eviction
 *     threshold: the admission fast path, no eviction.</li>
 *     <li>{@link #putAtThreshold}, {@link #putAllAtThreshold}, {@link #txPutAtThreshold} — new rows inserted into a
 *     region pre-filled up to the eviction threshold: steady-state eviction on the atomic single put, atomic batch and
 *     transactional commit paths.</li>
 * </ul>
 * The benchmark uses the public API only, so the same class can be run on another branch (e.g. {@code master}) with the
 * same parameters to compare the results.
 */
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
public class JmhPageEvictionBenchmark {
    /** Atomic cache name. */
    private static final String ATOMIC_CACHE = "atomic";

    /** Transactional cache name. */
    private static final String TX_CACHE = "tx";

    /** Value size (bytes): several rows per data page. */
    private static final int VALUE_SIZE = 1024;

    /** Data region size (bytes). */
    private static final long REGION_SIZE = 256 * 1024L * 1024L;

    /** Bounded key range of {@link #putBelowThreshold}: about 15% of the region, far below the eviction threshold. */
    private static final int BELOW_THRESHOLD_KEYS = 32_000;

    /** Pre-filled entries of the at-threshold scenarios: more than the region holds, so eviction has already started. */
    private static final int PRE_FILL_ENTRIES = 400_000;

    /** Batch size of {@link #putAllAtThreshold}. */
    private static final int BATCH_SIZE = 100;

    /** Value (reused to avoid allocation noise in the hot path). */
    private static final byte[] VAL = new byte[VALUE_SIZE];

    /** Node with an empty data region. */
    @State(Scope.Benchmark)
    public static class BelowThreshold {
        /** Atomic cache. */
        private IgniteCache<Integer, Object> cache;

        /** Key source. */
        private final AtomicInteger keyGen = new AtomicInteger();

        /** */
        @Setup
        public void setup() {
            cache = startNode().cache(ATOMIC_CACHE);
        }

        /** */
        @TearDown
        public void tearDown() {
            Ignition.stopAll(true);
        }
    }

    /** Node with a data region pre-filled up to the eviction threshold. */
    @State(Scope.Benchmark)
    public static class AtThreshold {
        /** Atomic cache. */
        private IgniteCache<Integer, Object> atomicCache;

        /** Transactional cache. */
        private IgniteCache<Integer, Object> txCache;

        /** Key source: every key is new, so every put inserts a new row. */
        private final AtomicInteger keyGen = new AtomicInteger(PRE_FILL_ENTRIES);

        /** */
        @Setup
        public void setup() {
            Ignite ignite = startNode();

            atomicCache = ignite.cache(ATOMIC_CACHE);
            txCache = ignite.cache(TX_CACHE);

            try (IgniteDataStreamer<Integer, Object> ldr = ignite.dataStreamer(ATOMIC_CACHE)) {
                for (int i = 0; i < PRE_FILL_ENTRIES; i++)
                    ldr.addData(i, VAL);
            }
        }

        /** */
        @TearDown
        public void tearDown() {
            Ignition.stopAll(true);
        }
    }

    /**
     * Atomic put below the eviction threshold (updates of a bounded key range).
     *
     * @param state State.
     */
    @Benchmark
    public void putBelowThreshold(BelowThreshold state) {
        state.cache.put(state.keyGen.incrementAndGet() % BELOW_THRESHOLD_KEYS, VAL);
    }

    /**
     * Atomic put of a new row into a region at the eviction threshold.
     *
     * @param state State.
     */
    @Benchmark
    public void putAtThreshold(AtThreshold state) {
        state.atomicCache.put(state.keyGen.incrementAndGet(), VAL);
    }

    /**
     * Atomic putAll of new rows into a region at the eviction threshold.
     *
     * @param state State.
     */
    @Benchmark
    public void putAllAtThreshold(AtThreshold state) {
        int base = state.keyGen.getAndAdd(BATCH_SIZE);

        Map<Integer, Object> batch = new TreeMap<>();

        for (int i = 0; i < BATCH_SIZE; i++)
            batch.put(base + i, VAL);

        state.atomicCache.putAll(batch);
    }

    /**
     * Transactional put (implicit transaction) of a new row into a region at the eviction threshold.
     *
     * @param state State.
     */
    @Benchmark
    public void txPutAtThreshold(AtThreshold state) {
        state.txCache.put(state.keyGen.incrementAndGet(), VAL);
    }

    /** @return Started node with an atomic and a transactional cache in an in-memory region with page eviction. */
    private static Ignite startNode() {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setInitialSize(REGION_SIZE)
                .setMaxSize(REGION_SIZE)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU));

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setIgniteInstanceName("page-eviction-benchmark")
            .setLocalHost("127.0.0.1")
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(
                new CacheConfiguration<Integer, Object>(ATOMIC_CACHE),
                new CacheConfiguration<Integer, Object>(TX_CACHE).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return Ignition.start(cfg);
    }

    /**
     * Runs benchmark.
     *
     * @param args Ignored.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .benchmarks(JmhPageEvictionBenchmark.class.getSimpleName())
            .run();
    }
}
