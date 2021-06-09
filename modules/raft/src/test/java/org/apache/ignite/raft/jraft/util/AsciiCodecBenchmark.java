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

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 *
 */
@SuppressWarnings({"all"})
@State(Scope.Benchmark)
public class AsciiCodecBenchmark {

    /*
        Benchmark                              Mode  Cnt  Score   Error   Units
        AsciiCodecBenchmark.fastpathDecode    thrpt    3  0.087 ± 0.024  ops/ns
        AsciiCodecBenchmark.fastpathEncode    thrpt    3  0.093 ± 0.047  ops/ns
        AsciiCodecBenchmark.normalpathDecode  thrpt    3  0.020 ± 0.006  ops/ns
        AsciiCodecBenchmark.normalpathEncode  thrpt    3  0.017 ± 0.032  ops/ns
     */

    private static final String PEER_STR = "127.0.0.1:18090:1";
    private static final byte[] PEER_BYTES = PEER_STR.getBytes();

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void normalpathEncode() {
        PEER_STR.getBytes();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void normalpathDecode() {
        new String(PEER_BYTES);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void fastpathEncode() {
        // fast ptah
        AsciiStringUtil.unsafeEncode(PEER_STR);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void fastpathDecode() {
        // fast ptah
        AsciiStringUtil.unsafeDecode(PEER_BYTES);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(AsciiCodecBenchmark.class.getSimpleName()) //
            .warmupIterations(3) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }
}
