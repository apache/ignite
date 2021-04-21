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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.Tracing;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks {@link MTC} class.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 3, time = 3)
public class JmhTracingContextBenchmark {
    /** Tracing. */
    Tracing tracing = new NoopTracing();

    /**
     * Setup.
     */
    @Setup(Level.Iteration)
    public void setup() {
        MTC.supportInitial(tracing.create(SpanType.TX));
    }

    /** */
    @Benchmark
    public void traceSurroundings(Blackhole bh) {
        for (int i = 0; i < 100; i++) {
            try (MTC.TraceSurroundings ts = MTC.support(tracing.create(SpanType.TX_CLOSE, MTC.span()))) {
                bh.consume(0);
            }
        }
    }

    /** */
    @Benchmark
    public void spanThreadLocal() {
        for (int i = 0; i < 100; i++) {
            Span span = tracing.create(SpanType.TX);
            MTC.supportInitial(span);
            MTC.span().addTag("isolation", () -> "isolation");
            MTC.span().addTag("concurrency", () -> "concurrency");
            MTC.span().addTag("timeout", () -> "timeout");
            MTC.span().addTag("label", () -> "label");
        }
    }

    /** */
    @Benchmark
    public void spanCached() {
        for (int i = 0; i < 100; i++) {
            Span span = tracing.create(SpanType.TX);
            MTC.supportInitial(span);
            span.addTag("isolation", () -> "isolation");
            span.addTag("concurrency", () -> "concurrency");
            span.addTag("timeout", () -> "timeout");
            span.addTag("label", () -> "label");
        }
    }

    /**
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhTracingContextBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
