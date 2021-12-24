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

package org.apache.ignite.internal.benchmarks.jmh.algo;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.processors.cache.transactions.TxCounters;
import org.apache.ignite.internal.processors.cache.transactions.TxCounters2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/** */
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UnlockDiagnosticVMOptions"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class TxCountersBenchmark {
    /** */
    @State(Thread)
    public static class Context {
        /** */
        private static final int CACHE_CNT = 1;

        /** */
        private static final int PART_CNT = 1;

        /** */
        final int[] caches = new int[] {CACHE_CNT};

        /** */
        final int[] parts = new int[] {PART_CNT};

        /** */
        @Setup
        public void setup() {
            for (int i = 0; i < CACHE_CNT; i++)
                caches[i] = CU.cacheId("accounts-" + i);

            for (int i = 0; i < PART_CNT; i++)
                parts[i] = ThreadLocalRandom.current().nextInt(65535);
        }
    }

    /** */
    @Benchmark
    public TxCounters txCounters(Context ctx) {
        final TxCounters cntrs = new TxCounters();

        for (int i = 0; i < ctx.caches.length; i++)
            for (int j = 0; j < ctx.parts.length; j++)
                cntrs.incrementUpdateCounter(ctx.caches[i], ctx.parts[j]);

        return cntrs;
    }

    /** */
    @Benchmark
    public TxCounters2 txCounters2(Context ctx) {
        final TxCounters2 cntrs = new TxCounters2();

        for (int i = 0; i < ctx.caches.length; i++)
            for (int j = 0; j < ctx.parts.length; j++)
                cntrs.incrementUpdateCounter(ctx.caches[i], ctx.parts[j]);

        return cntrs;
    }
}
