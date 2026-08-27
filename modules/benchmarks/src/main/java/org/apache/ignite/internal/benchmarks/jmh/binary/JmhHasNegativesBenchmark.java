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

package org.apache.ignite.internal.benchmarks.jmh.binary;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.util.GridUnsafe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Compares implementations of a "byte array has a negative byte" scan: a branch-free 8-byte stride loop,
 * the same loop with an early exit, and the intrinsified {@code java.lang.StringCoding#hasNegatives}.
 */
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Warmup(iterations = 3, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
public class JmhHasNegativesBenchmark {
    /** Mask to test 8 bytes for a set sign bit at once. */
    private static final long NEGATIVE_BYTES_MSK = 0x8080808080808080L;

    /** Handle of the intrinsified {@code java.lang.StringCoding#hasNegatives}. */
    private static final MethodHandle HAS_NEGATIVES;

    static {
        try {
            Method mtd = Class.forName("java.lang.StringCoding")
                .getDeclaredMethod("hasNegatives", byte[].class, int.class, int.class);

            mtd.setAccessible(true);

            HAS_NEGATIVES = MethodHandles.lookup().unreflect(mtd);
        }
        catch (Throwable e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** */
    @Param({"8", "64", "512", "4096"})
    private int len;

    /** */
    @Param({"clean", "dirtyStart", "dirtyEnd"})
    private String data;

    /** */
    private byte[] arr;

    /** */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .benchmarks(JmhHasNegativesBenchmark.class.getName())
            .run();
    }

    /** */
    @Setup
    public void setup() {
        arr = new byte[len];

        Arrays.fill(arr, (byte)'a');

        if ("dirtyStart".equals(data))
            arr[0] = -1;
        else if ("dirtyEnd".equals(data))
            arr[len - 1] = -1;
    }

    /** */
    @Benchmark
    public boolean branchFree() {
        long acc = 0;

        int i = 0;

        for (int lim = arr.length - Long.BYTES; i <= lim; i += Long.BYTES)
            acc |= GridUnsafe.getLong(arr, GridUnsafe.BYTE_ARR_OFF + i);

        int tail = 0;

        for (; i < arr.length; i++)
            tail |= arr[i];

        return (acc & NEGATIVE_BYTES_MSK) != 0 || tail < 0;
    }

    /** */
    @Benchmark
    public boolean earlyExit() {
        int i = 0;

        for (int lim = arr.length - Long.BYTES; i <= lim; i += Long.BYTES) {
            if ((GridUnsafe.getLong(arr, GridUnsafe.BYTE_ARR_OFF + i) & NEGATIVE_BYTES_MSK) != 0)
                return true;
        }

        for (; i < arr.length; i++) {
            if (arr[i] < 0)
                return true;
        }

        return false;
    }

    /** */
    @Benchmark
    public boolean intrinsic() throws Throwable {
        return (boolean)HAS_NEGATIVES.invokeExact(arr, 0, arr.length);
    }
}
