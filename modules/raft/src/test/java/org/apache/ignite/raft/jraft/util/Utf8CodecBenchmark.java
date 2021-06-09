/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 *
 */
@State(Scope.Benchmark)
public class Utf8CodecBenchmark {
    /**
     * Benchmark                                Mode  Cnt      Score       Error   Units
     * Utf8CodecBenchmark.defaultToUtf8Bytes   thrpt    3  13744.773 ±  2188.618  ops/ms
     * Utf8CodecBenchmark.defaultToUtf8String  thrpt    3  18136.042 ± 10964.592  ops/ms
     * Utf8CodecBenchmark.unsafeToUtf8Bytes    thrpt    3  21743.863 ±   228.019  ops/ms
     * Utf8CodecBenchmark.unsafeToUtf8String   thrpt    3  20670.839 ±  9921.726  ops/ms
     */

    private String str;
    private byte[] bytes;

    @Setup
    public void setup() {
        str = UUID.randomUUID().toString();
        bytes = Utils.getBytes(str);
    }

    @SuppressWarnings("all")
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void defaultToUtf8Bytes() {
        Utils.getBytes(str);
    }

//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void unsafeToUtf8Bytes() {
//        BytesUtil.writeUtf8(str);
//    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void defaultToUtf8String() {
        new String(bytes, StandardCharsets.UTF_8);
    }

//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    public void unsafeToUtf8String() {
//        BytesUtil.readUtf8(bytes);
//    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(Utf8CodecBenchmark.class.getSimpleName()) //
            .warmupIterations(3) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }
}
