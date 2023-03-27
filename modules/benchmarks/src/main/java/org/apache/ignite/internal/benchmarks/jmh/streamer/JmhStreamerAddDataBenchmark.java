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

package org.apache.ignite.internal.benchmarks.jmh.streamer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * DataStreamerImpl.addData(Collection) vs DataStreamerImpl.addData(Key, Value).
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xms1g", "-Xmx3g", "-server", "-XX:+AggressiveOpts", "-XX:MaxMetaspaceSize=256m"})
@Measurement(iterations = 11)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 21)
public class JmhStreamerAddDataBenchmark {
    /** Data amount. */
    private static final int DATA_AMOUNT = 512;

    /** Ignite client instance. */
    private static final String IGNITE_CLIENT_INSTANCE_NAME = "client";

    /** Ignite client cache name. */
    private static final String IGNITE_CLIENT_CACHE_NAME = "clientCache";

    /**
     * Create Ignite configuration.
     *
     * @return Ignite configuration.
     */
    private IgniteConfiguration getConfiguration(String cfgName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new NullLogger());

        cfg.setIgniteInstanceName(cfgName);

        if (isClient) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration(defaultCacheConfiguration(IGNITE_CLIENT_CACHE_NAME));
        }
        else
            cfg.setCacheConfiguration(defaultCacheConfiguration("server"));

        return cfg;
    }

    /**
     * Create cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration defaultCacheConfiguration(String cacheName) {
        CacheConfiguration cfg = new CacheConfiguration(cacheName);

        cfg.setWriteSynchronizationMode(writeSynchronizationMode());

        return cfg;
    }

    /**
     * Stop all grids after tests.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * Start 2 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() {
        int cnt = gridCnt();

        for (int i = 0; i < cnt; i++)
            Ignition.start(getConfiguration("srv" + i, false));

        Ignition.start(getConfiguration(IGNITE_CLIENT_INSTANCE_NAME, true));
    }

    /**
     * Prepare and clean collection with streaming data.
     */
    @State(Scope.Thread)
    public static class StreamingData {
        /**
         * Collection that will be streamed from client.
         */
        Collection<AbstractMap.SimpleEntry<Integer, Integer>> streamingCol = new ArrayList<>(DATA_AMOUNT);

        /**
         * Prepare collection.
         */
        @Setup(Level.Iteration)
        public void prepareCollection() {
            for (int i = 0; i < DATA_AMOUNT; i++)
                streamingCol.add(new HashMap.SimpleEntry<>(ThreadLocalRandom.current().nextInt(),
                    ThreadLocalRandom.current().nextInt()));
        }

        /**
         * Clean collection after each test.
         */
        @TearDown(Level.Iteration)
        public void cleanCollection() {
            streamingCol.clear();
        }
    }

    /**
     * Create streamer on client cache.
     */
    @State(Scope.Benchmark)
    public static class DataStreamer {
        /** Data loader. */
        final IgniteDataStreamer<Integer, Integer> dataLdr =
            Ignition.ignite(IGNITE_CLIENT_INSTANCE_NAME).dataStreamer(IGNITE_CLIENT_CACHE_NAME);
    }

    /**
     * Test addData(Collection).
     *
     * @param data Data that will be streamed.
     * @param streamer Data loader.
     */
    @Benchmark
    public void addDataCollection(StreamingData data, DataStreamer streamer) {
        streamer.dataLdr.addData(data.streamingCol);
    }

    /**
     * Test addData(Key, Value).
     *
     * @param data Data that will be streamed.
     * @param streamer Data loader.
     */
    @Benchmark
    public void addDataKeyValue(StreamingData data, DataStreamer streamer) {
        for (Map.Entry<Integer, Integer> entry : data.streamingCol)
            streamer.dataLdr.addData(entry.getKey(), entry.getValue());
    }

    /**
     * @return Synchronization mode.
     */
    @NotNull protected CacheWriteSynchronizationMode writeSynchronizationMode() {
        return FULL_SYNC;
    }

    /**
     * @return Node amount.
     */
    protected int gridCnt() {
        return 3;
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
            .include(JmhStreamerAddDataBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
