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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.TimeValue;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Cache put and get through a client node with two servers, one backup and {@code PRIMARY_SYNC}, for allocation runs
 * with {@link JmhAllocationProfiler}. Keys and values are created up front, so the benchmark thread allocates only what
 * Ignite does.
 */
@SuppressWarnings("unchecked")
public class JmhCacheAllocationBenchmark extends JmhCacheAbstractBenchmark {
    /** Atomicity mode. */
    @Param({"ATOMIC", "TRANSACTIONAL"})
    private CacheAtomicityMode atomicity;

    /** Keys. */
    private Integer[] keys;

    /** Values. */
    private IntValue[] vals;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        keys = new Integer[CNT];
        vals = new IntValue[CNT];

        try (IgniteDataStreamer<Integer, IntValue> streamer = node.dataStreamer(cache.getName())) {
            for (int i = 0; i < CNT; i++) {
                keys[i] = i;
                vals[i] = new IntValue(i);

                streamer.addData(keys[i], vals[i]);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected int dataNodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientMode() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration()
            .setAtomicityMode(atomicity)
            .setBackups(1)
            .setWriteSynchronizationMode(PRIMARY_SYNC);
    }

    /** Replaces the value of a random preloaded key with the same value: an update, not an insert. */
    @Benchmark
    public void put() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        cache.put(keys[i], vals[i]);
    }

    /** @return Value of a random key. */
    @Benchmark
    public Object get() {
        return cache.get(keys[ThreadLocalRandom.current().nextInt(CNT)]);
    }

    /**
     * Runs the benchmarks with the allocation profiler and escape analysis off, so that runs repeat, and writes the
     * results to {@code JmhCacheAllocationBenchmark.json}.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Options opts = JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(1)
            .warmupIterations(5)
            .warmupTime(TimeValue.seconds(3))
            .measurementIterations(5)
            .measurementTime(TimeValue.seconds(3))
            .benchmarks(JmhCacheAllocationBenchmark.class.getSimpleName())
            .profilers(JmhAllocationProfiler.class)
            .jvmArguments("-Xms2g", "-Xmx2g", "-XX:-DoEscapeAnalysis")
            .optionsBuilder()
            .resultFormat(ResultFormatType.JSON)
            .result(JmhCacheAllocationBenchmark.class.getSimpleName() + ".json")
            .build();

        new Runner(opts).run();
    }
}
