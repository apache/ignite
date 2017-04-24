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

import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test various increment strategies.
 */
@State(Scope.Benchmark)
public class JmhIncrementBenchmark extends JmhAbstractBenchmark {
    /** Batch size. */
    private static final int BATCH_SIZE = 1024;

    /** Thread-local value. */
    private static final ThreadLocal<Long> VAL = new ThreadLocal<Long>() {
        @Override protected Long initialValue() {
            return 0L;
        }
    };

    /** Atomic variable. */
    private final AtomicLong atomic = new AtomicLong();

    /**
     * Test standard increment.
     */
    @Benchmark
    public long increment() {
        return atomic.incrementAndGet();
    }

    /**
     * Test increment with thread-local batching.
     */
    @Benchmark
    public long threadLocalIncrement() {
        long val = VAL.get();

        if ((val & (BATCH_SIZE - 1)) == 0)
            val = atomic.addAndGet(BATCH_SIZE);

        VAL.set(++val);

        return val;
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
            .warmupIterations(30)
            .measurementIterations(30)
            .benchmarks(JmhIncrementBenchmark.class.getSimpleName())
            .jvmArguments("-Xms4g", "-Xmx4g")
            .run();
    }
}
