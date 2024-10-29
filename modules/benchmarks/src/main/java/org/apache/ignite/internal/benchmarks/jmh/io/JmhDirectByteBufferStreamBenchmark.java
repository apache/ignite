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

package org.apache.ignite.internal.benchmarks.jmh.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.direct.stream.v3.DirectByteBufferStreamImplV3;
import org.apache.ignite.internal.direct.stream.v4.DirectByteBufferStreamImplV4;
import org.apache.ignite.lang.IgniteUuid;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark different DirectByteBufferStream versions.
 */
@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
public class JmhDirectByteBufferStreamBenchmark {
    /** */
    private static final int BATCH_SIZE = 100;

    /** */
    private static final int UNIQUE_UUID_CNT = 10_000;

    /** */
    @Param({"3", "4"})
    private String ver;

    /** */
    private DirectByteBufferStream stream;

    /** */
    private final UUID[] uuids = new UUID[UNIQUE_UUID_CNT];

    /** */
    private final IgniteUuid[] igniteUuids = new IgniteUuid[UNIQUE_UUID_CNT];

    /** */
    private final ByteBuffer buf = ByteBuffer.allocateDirect(BATCH_SIZE * 30).order(ByteOrder.nativeOrder());

    /** */
    @Setup(Level.Trial)
    public void setup() {
        switch (ver) {
            case "3" :
                stream = new DirectByteBufferStreamImplV3(null);
                break;

            case "4" :
                stream = new DirectByteBufferStreamImplV4(null);
                break;

            default:
                throw new AssertionError("Unexpected version " + ver);
        }

        stream.setBuffer(buf);

        for (int i = 0; i < UNIQUE_UUID_CNT; i++) {
            uuids[i] = UUID.randomUUID();
            igniteUuids[i] = IgniteUuid.randomUuid();
        }
    }

    /** */
    @Benchmark
    public void writeReadUuid() {
        int startIdx = ThreadLocalRandom.current().nextInt(UNIQUE_UUID_CNT - BATCH_SIZE);
        buf.clear();

        for (int i = startIdx; i < startIdx + BATCH_SIZE; i++) {
            stream.writeUuid(uuids[i]);

            if (!stream.lastFinished())
                throw new AssertionError("Buffer overflow");
        }

        buf.flip();

        for (int i = startIdx; i < startIdx + BATCH_SIZE; i++) {
            if (!uuids[i].equals(stream.readUuid()))
                throw new AssertionError("Unexpected value");
        }
    }

    /** */
    @Benchmark
    public void writeReadIgniteUuid() {
        int startIdx = ThreadLocalRandom.current().nextInt(UNIQUE_UUID_CNT - BATCH_SIZE);
        buf.clear();

        for (int i = startIdx; i < startIdx + BATCH_SIZE; i++) {
            stream.writeIgniteUuid(igniteUuids[i]);

            if (!stream.lastFinished())
                throw new AssertionError("Buffer overflow");
        }

        buf.flip();

        for (int i = startIdx; i < startIdx + BATCH_SIZE; i++) {
            if (!igniteUuids[i].equals(stream.readIgniteUuid()))
                throw new AssertionError("Unexpected value");
        }
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhDirectByteBufferStreamBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
