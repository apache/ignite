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

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
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

/**
 *
 */
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UnlockDiagnosticVMOptions"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BenchmarkCRC {
    /** */
    static final int SIZE = 1024;

    /** */
    static final int BUF_LEN = 4096;

    /** */
    @State(Thread)
    public static class Context {
        /** */
        final int[] results = new int[SIZE];

        /** */
        final ByteBuffer bb = ByteBuffer.allocate(BUF_LEN);

        /** */
        @Setup
        public void setup() {
            new Random().ints(BUF_LEN, Byte.MIN_VALUE, Byte.MAX_VALUE).forEach(k -> bb.put((byte) k));
        }
    }

    /** */
    @Benchmark
    public int[] pureJavaCrc32(Context context) {
        for (int i = 0; i < SIZE; i++) {
            context.bb.rewind();

            context.results[i] = PureJavaCrc32.calcCrc32(context.bb, BUF_LEN);
        }

        return context.results;
    }

    /** */
    @Benchmark
    public int[] crc32(Context context) {
        for (int i = 0; i < SIZE; i++) {
            context.bb.rewind();

            context.results[i] = FastCrc.calcCrc(context.bb, BUF_LEN);
        }

        return context.results;
    }
}


