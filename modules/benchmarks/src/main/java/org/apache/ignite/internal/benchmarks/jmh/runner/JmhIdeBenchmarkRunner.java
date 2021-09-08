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

package org.apache.ignite.internal.benchmarks.jmh.runner;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH IDE benchmark runner configuration.
 */
public class JmhIdeBenchmarkRunner {
    /** Benchmark modes. */
    private Mode[] benchmarkModes = new Mode[] { Mode.Throughput };

    /** Amount of forks */
    private int forks = 1;

    /** Warmup iterations. */
    private int warmupIterations = 10;

    /** Measurement operations. */
    private int measurementIterations = 10;

    /** Output time unit. */
    private TimeUnit outputTimeUnit = TimeUnit.SECONDS;

    /** Benchmarks to run. */
    private Object[] benchmarks;

    /** JVM arguments. */
    private String[] jvmArgs;

    /** Output. */
    private String output;

    /** Amount of threads. */
    private int threads;

    /** Profilers. */
    private Class[] profilers;

    /**
     * Create new runner.
     *
     * @return New runner.
     */
    public static JmhIdeBenchmarkRunner create() {
        return new JmhIdeBenchmarkRunner();
    }

    /**
     * Constructor.
     */
    private JmhIdeBenchmarkRunner() {
        // No-op.
    }

    /**
     * @param benchmarkModes Benchmark modes.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner benchmarkModes(Mode... benchmarkModes) {
        this.benchmarkModes = benchmarkModes;

        return this;
    }

    /**
     * @param forks Forks.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner forks(int forks) {
        this.forks = forks;

        return this;
    }

    /**
     * @param warmupIterations Warmup iterations.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner warmupIterations(int warmupIterations) {
        this.warmupIterations = warmupIterations;

        return this;
    }

    /**
     * @param measurementIterations Measurement iterations.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner measurementIterations(int measurementIterations) {
        this.measurementIterations = measurementIterations;

        return this;
    }

    /**
     * @param outputTimeUnit Output time unit.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner outputTimeUnit(TimeUnit outputTimeUnit) {
        this.outputTimeUnit = outputTimeUnit;

        return this;
    }

    /**
     * @param benchmarks Benchmarks.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner benchmarks(Object... benchmarks) {
        this.benchmarks = benchmarks;

        return this;
    }

    /**
     * @param output Output file.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner output(String output) {
        this.output = output;

        return this;
    }

    /**
     * @param jvmArgs JVM arguments.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner jvmArguments(String... jvmArgs) {
        this.jvmArgs = jvmArgs;

        return this;
    }

    /**
     * @param threads Threads.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner threads(int threads) {
        this.threads = threads;

        return this;
    }

    /**
     * @param profilers Profilers.
     * @return This instance.
     */
    public JmhIdeBenchmarkRunner profilers(Class... profilers) {
        this.profilers = profilers;

        return this;
    }

    /**
     * Get prepared options builder.
     *
     * @return Options builder.
     */
    public OptionsBuilder optionsBuilder() {
        OptionsBuilder builder = new OptionsBuilder();

        builder.forks(forks);
        builder.warmupIterations(warmupIterations);
        builder.measurementIterations(measurementIterations);
        builder.timeUnit(outputTimeUnit);
        builder.threads(threads);

        if (benchmarkModes != null) {
            for (Mode benchmarkMode : benchmarkModes)
                builder.getBenchModes().add(benchmarkMode);
        }

        if (benchmarks != null) {
            for (Object benchmark : benchmarks) {
                if (benchmark instanceof Class)
                    builder.include(((Class)benchmark).getSimpleName());
                else
                    builder.include(benchmark.toString());
            }
        }

        if (jvmArgs != null)
            builder.jvmArgs(jvmArgs);

        if (output != null)
            builder.output(output);

        if (profilers != null) {
            for (Class profiler : profilers)
                builder.addProfiler(profiler);
        }

        return builder;
    }

    /**
     * Run benchmarks.
     *
     * @throws Exception If failed.
     */
    public void run() throws Exception {
        new Runner(optionsBuilder().build()).run();
    }

    /**
     * Create property.
     *
     * @param name Name.
     * @param val Value.
     * @return Result.
     */
    public static String createProperty(String name, Object val) {
        return "-D" + name + "=" + val;
    }
}
