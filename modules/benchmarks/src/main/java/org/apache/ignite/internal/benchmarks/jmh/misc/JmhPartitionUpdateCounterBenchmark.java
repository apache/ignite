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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterTrackingImpl;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks {@link PartitionUpdateCounterTrackingImpl} class.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 3)
public class JmhPartitionUpdateCounterBenchmark {
    /** Buffer size to store testing gaps. */
    private static final int GAPS_BUFFER_SIZE = 50;

    /** Max delta for next counter value. */
    private static final int COUNTER_MAX_DELTA = 50;

    /** Testing gaps buffer. */
    private final long[][] gapsBuf = new long[GAPS_BUFFER_SIZE][];

    /** Random numbers generator. */
    private Random rnd;

    /** Counter. */
    private final AtomicLong reservedCntr = new AtomicLong();

    /** Partition update counter. */
    private final PartitionUpdateCounterTrackingImpl partCntr = new PartitionUpdateCounterTrackingImpl(null);

    /**
     * Setup.
     */
    @Setup(Level.Iteration)
    public void setup() {
        rnd = new Random(0);

        reservedCntr.set(0);

        for (int i = 0; i < GAPS_BUFFER_SIZE; i++) {
            long cntrDelta = rnd.nextInt(COUNTER_MAX_DELTA);

            gapsBuf[i] = new long[] {reservedCntr.getAndAdd(cntrDelta), cntrDelta};
        }

        partCntr.reset();
    }

    /**
     * Update partition update counter with random gap.
     */
    @Benchmark
    public void updateWithGap() {
        int nextIdx = rnd.nextInt(GAPS_BUFFER_SIZE);

        partCntr.update(gapsBuf[nextIdx][0], gapsBuf[nextIdx][1]);

        gapsBuf[nextIdx][0] = reservedCntr.getAndAdd(gapsBuf[nextIdx][1]);
    }

    /**
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhPartitionUpdateCounterBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
