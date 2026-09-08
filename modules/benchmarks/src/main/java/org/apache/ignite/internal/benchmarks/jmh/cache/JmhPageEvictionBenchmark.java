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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
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

/**
 * Measures the impact of size-aware page eviction on an in-memory (non-persistent) data region.
 * <p>
 * Two benchmark methods:
 * <ul>
 *   <li>{@link #putSmall()} - puts of small values (well below the empty-pages pool, so the size-aware reserve
 *       in {@code RowStore.addRow} hits its fast path). This is the hot path whose per-operation cost the patch
 *       adds on every put, and is the primary A/B metric for detecting a performance regression between the
 *       unpatched baseline and this branch.</li>
 *   <li>{@link #putLarge()} - puts of large values (larger than the empty-pages pool) against a region that has
 *       been pre-filled to near capacity, so that each large put must actually run the size-aware eviction loop.
 *       This exercises the new eviction behavior; on an unpatched build such a put fails with an out-of-memory
 *       error, so this benchmark only runs meaningfully on the patched build.</li>
 * </ul>
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
public class JmhPageEvictionBenchmark {
    /** Default cache name. */
    private static final String CACHE_NAME = "default";

    /** Empty pages pool size (kept low so that the LARGE scenario reliably exceeds it). */
    private static final int POOL_SIZE = 100;

    /** Small value size (bytes): a single data page, far below the empty-pages pool. */
    private static final int SMALL_VALUE_SIZE = 1024;

    /** Large value size (bytes): larger than the empty-pages pool in page terms. */
    private static final int LARGE_VALUE_SIZE = 2 * 1024 * 1024;

    /** Number of pre-fill small entries for the LARGE scenario (fills the region close to capacity). */
    private static final int PRE_FILL_ENTRIES = 48_000;

    /** Benchmark scenario: selects the value size and the pre-fill strategy. */
    @Param({"SMALL", "LARGE"})
    private String scenario;

    /** Ignite cache. */
    private IgniteCache<Integer, Object> cache;

    /** Pre-allocated small value (reused to avoid allocation noise in the hot path). */
    private final byte[] smallVal = new byte[SMALL_VALUE_SIZE];

    /** Pre-allocated large value (reused to avoid allocation noise in the hot path). */
    private final byte[] largeVal = new byte[LARGE_VALUE_SIZE];

    /** Monotonic key source for overwrite-style puts. */
    private final AtomicInteger keyGen = new AtomicInteger();

    /** Checked once whether the region currently evicts (to warn about the LARGE scenario on an unpatched build). */
    private volatile String evictionMode;

    /** {@inheritDoc} */
    public JmhPageEvictionBenchmark() {
        keyGen.set(0);
    }

    /** Put of a small value (hot path, size-aware reserve takes its fast path). */
    @Benchmark
    public void putSmall() {
        int key = keyGen.incrementAndGet();

        cache.put(key, smallVal);
    }

    /** Put of a large value against a nearly-full region (runs the size-aware eviction loop). */
    @Benchmark
    public void putLarge() {
        int key = keyGen.incrementAndGet();

        cache.put(key, largeVal);
    }

    /** Starts Ignite with an in-memory, eviction-enabled data region and pre-fills it for the LARGE scenario. */
    @Setup(Level.Trial)
    public void setup() {
        DataPageEvictionMode evictionMode = DataPageEvictionMode.RANDOM_LRU;

        this.evictionMode = evictionMode.name();

        long regionSize = 256 * 1024L * 1024L;

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(regionSize)
                .setEmptyPagesPoolSize(POOL_SIZE)
                .setPageEvictionMode(evictionMode));

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setIgniteInstanceName("test")
            .setLocalHost("127.0.0.1")
            .setDataStorageConfiguration(dsCfg);

        Ignite ignite = Ignition.start(cfg);

        cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Object>(CACHE_NAME).setBackups(0));

        // Pre-fill the region with small entries for the LARGE scenario so that a large put cannot fit into the
        // remaining headroom and must actually evict.
        if ("LARGE".equalsIgnoreCase(scenario)) {
            try (IgniteDataStreamer<Integer, Object> ldr = ignite.dataStreamer(CACHE_NAME)) {
                ldr.perNodeBufferSize(1024);

                for (int i = 0; i < PRE_FILL_ENTRIES; i++)
                    ldr.addData(i, smallVal);
            }
        }
    }

    /** @return Test data. */
    @Override public String toString() {
        return "JmhPageEvictionBenchmark[scenario=" + scenario + ", evictionMode=" + evictionMode + ']';
    }

    /** Stops all Ignite instances started by this benchmark. */
    @TearDown
    public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * Runs this benchmark over both {@code SMALL} and {@code LARGE} scenarios (configured by {@code @Param}).
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
