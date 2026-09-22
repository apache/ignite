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
 * Measures the impact of size-aware page eviction on an in-memory data region.
 * <p>
 * {@link #putSmall()} — small values within a bounded key range that keeps the region below the eviction threshold
 * (size-aware reserve fast path).
 * <p>
 * {@link #putLarge()} — large values against a pre-filled region near capacity (size-aware eviction loop).
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

    /** Small value size (bytes): a single data page, far below the empty-pages pool. */
    private static final int SMALL_VALUE_SIZE = 1024;

    /** Large value size (bytes): larger than the empty-pages pool in page terms. */
    private static final int LARGE_VALUE_SIZE = 2 * 1024 * 1024;

    /** Pre-fill entries for the LARGE scenario; exceeds region capacity to pin it at the eviction threshold. */
    private static final int PRE_FILL_ENTRIES = 400_000;

    /** Bounded key range for {@link #putSmall()} to keep the region below the eviction threshold. */
    private static final int SMALL_KEY_RANGE = 32_000;

    /** Benchmark scenario: selects the value size and the pre-fill strategy. */
    @Param({"SMALL", "LARGE"})
    private String scenario;

    /** Ignite cache. */
    private IgniteCache<Integer, Object> cache;

    /** Pre-allocated small value (reused to avoid allocation noise in the hot path). */
    private final byte[] smallVal = new byte[SMALL_VALUE_SIZE];

    /** Pre-allocated large value (reused to avoid allocation noise in the hot path). */
    private final byte[] largeVal = new byte[LARGE_VALUE_SIZE];

    /** Monotonic key source. */
    private final AtomicInteger keyGen = new AtomicInteger();

    /** Small value put within a bounded key range (fast path, no eviction). */
    @Benchmark
    public void putSmall() {
        int key = keyGen.incrementAndGet() % SMALL_KEY_RANGE;

        cache.put(key, smallVal);
    }

    /** Large value put against a nearly-full region (size-aware eviction loop). Single-threaded to avoid OOM. */
    @Benchmark
    @Threads(1)
    public void putLarge() {
        int key = keyGen.incrementAndGet();

        cache.put(key, largeVal);
    }

    /** */
    @Setup(Level.Trial)
    public void setup() {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(256 * 1024L * 1024L)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU));

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setIgniteInstanceName("test")
            .setLocalHost("127.0.0.1")
            .setDataStorageConfiguration(dsCfg);

        Ignite ignite = Ignition.start(cfg);

        cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Object>(CACHE_NAME).setBackups(0));

        if ("LARGE".equalsIgnoreCase(scenario)) {
            try (IgniteDataStreamer<Integer, Object> ldr = ignite.dataStreamer(CACHE_NAME)) {
                ldr.perNodeBufferSize(1024);

                for (int i = 0; i < PRE_FILL_ENTRIES; i++)
                    ldr.addData(i, smallVal);
            }

            keyGen.set(PRE_FILL_ENTRIES);
        }
    }

    /** */
    @TearDown
    public void tearDown() {
        Ignition.stopAll(true);
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
