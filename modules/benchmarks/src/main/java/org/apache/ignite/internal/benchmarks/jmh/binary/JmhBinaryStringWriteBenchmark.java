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

import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.binary.StringWriter;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_BINARY_STRING_ZERO_COPY;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Compares zero-copy string serialization with the legacy serialization.
 * @see org.apache.ignite.IgniteCommonsSystemProperties#IGNITE_BINARY_STRING_ZERO_COPY
 */
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Warmup(iterations = 5, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = SECONDS)
public class JmhBinaryStringWriteBenchmark {
    /** */
    @Param({"true", "false"})
    private boolean zeroCopy;

    /** */
    @Param({"8", "64", "512", "4096"})
    private int len;

    /** */
    @Param({"ascii", "latin1", "cyrillic", "mixed"})
    private String content;

    /** */
    private BinaryOutputStream out;

    /** */
    private String str;

    /** */
    public static void main(String[] args) throws Exception {
        OptionsBuilder builder = JmhIdeBenchmarkRunner.create()
            .forks(1)
            .benchmarks(JmhBinaryStringWriteBenchmark.class.getName())
            .profilers(GCProfiler.class)
            .optionsBuilder();

        new Runner(builder.build()).run();
    }

    /** */
    @Setup
    public void setup() {
        // Must be set before the first use of StringWriter in this JVM.
        System.setProperty(IGNITE_BINARY_STRING_ZERO_COPY, String.valueOf(zeroCopy));

        StringBuilder sb = new StringBuilder(len);

        for (int i = 0; sb.length() < len; i++) {
            switch (content) {
                case "ascii":
                    sb.append((char)('a' + i % 26));

                    break;

                case "latin1":
                    // Every 8th char is a Latin-1 char with the sign bit set.
                    sb.append(i % 8 == 7 ? (char)(0xC0 + i % 0x20) : (char)('a' + i % 26));

                    break;

                case "cyrillic":
                    sb.append((char)('\u0410' + i % 32));

                    break;

                case "mixed":
                    // ASCII, Latin-1, 2-byte, 3-byte chars and a surrogate pair.
                    switch (i % 5) {
                        case 0:
                            sb.append((char)('a' + i % 26));

                            break;

                        case 1:
                            sb.append('\u00e9');

                            break;

                        case 2:
                            sb.append('\u0416');

                            break;

                        case 3:
                            sb.append('\u20ac');

                            break;

                        default:
                            sb.append("\ud83d\ude00");
                    }

                    break;

                default:
                    throw new IllegalArgumentException("Unknown content type: " + content);
            }
        }

        str = sb.toString();

        out = BinaryStreams.outputStream(4 * len + 64);
    }

    /** */
    @TearDown
    public void tearDown() {
        out.close();
    }

    /** */
    @Benchmark
    public void writeString(Blackhole bh) {
        out.position(0);

        StringWriter.write(str, out);

        bh.consume(out.position());
    }
}
