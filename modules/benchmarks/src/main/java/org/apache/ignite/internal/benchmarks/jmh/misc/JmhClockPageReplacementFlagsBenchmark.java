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

package org.apache.ignite.internal.benchmarks.jmh.misc;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.ClockPageReplacementFlags;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks {@link ClockPageReplacementFlags} class.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
public class JmhClockPageReplacementFlagsBenchmark {
    /** Pages count. */
    private static final int PAGES_CNT = 10_000;

    /** Random numbers generator. */
    private Random rnd;

    /** Direct memory provider. */
    DirectMemoryProvider provider;

    /** LRU list. */
    private ClockPageReplacementFlags flags;

    /**
     * Setup.
     */
    @Setup(Level.Iteration)
    public void setup() {
        rnd = new Random(0);

        provider = new UnsafeMemoryProvider(null);
        provider.initialize(new long[] {ClockPageReplacementFlags.requiredMemory(PAGES_CNT)});

        DirectMemoryRegion region = provider.nextRegion();

        flags = new ClockPageReplacementFlags(PAGES_CNT, region.address());
    }

    /**
     * Tear down.
     */
    @TearDown(Level.Iteration)
    public void tearDown() {
        provider.shutdown(true);
    }

    /**
     * Benchmark {@link ClockPageReplacementFlags#setFlag(int)} method.
     */
    @Benchmark
    public void setFlag() {
        int nextIdx = rnd.nextInt(PAGES_CNT - 100);

        for (int i = 0; i < 100; i++)
            flags.setFlag(nextIdx + i);
    }

    /**
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhClockPageReplacementFlagsBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
