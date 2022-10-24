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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
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

/**
 * For research of the streamer throughput with different settings and the receivers.
 */
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@Threads(1)
@Measurement(iterations = 7)
@Warmup(iterations = 3)
public class JmhStreamerReceiverBenchmark {
    /** */
    private static final long ENTRIES_TO_LOAD = 500_000;

    /** */
    private static final int AVERAGE_RECORD_LEN = 500;

    /** */
    private static final boolean LOAD_FROM_CLIENT = true;

    /** */
    private static final boolean PERSISTENT = true;

    /** */
    private static final int CHECKPOINT_FREQUENCY = 3000;

    /** Enables or disables final checkpoint into the measurement. */
    private static final boolean INCLUDE_CHECKPOINT = false;

    /** Some fixed minimal + doubled average record size. */
    private static final long REGION_SIZE = 1024L * 1024L * 1024L;

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private static final int VALUES_BANK_SIZE = 2000;

    /** */
    private final Random rnd = new Random();

    /** */
    private final List<Ignite> nodes = new ArrayList<>();

    /** */
    private Ignite ldrNode;

    /** */
    private Object[] values;

    /** */
    private Params runParams;

    /**
     * Create Ignite configuration.
     *
     * @return Ignite configuration.
     */
    private IgniteConfiguration configuration(String instName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(instName);

        cfg.setWorkDirectory(workDirectory(instName));

        cfg.setGridLogger(new NullLogger());

        if (isClient)
            cfg.setClientMode(true);
        else {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration();

            DataRegionConfiguration regCfg = new DataRegionConfiguration();

            regCfg.setPersistenceEnabled(PERSISTENT);

            if (PERSISTENT) {
                //Reduce affection of side I/O.
                dsCfg.setCheckpointFrequency(CHECKPOINT_FREQUENCY);

                dsCfg.setWalMode(runParams.walMode);

                regCfg.setMaxSize(REGION_SIZE);
                regCfg.setInitialSize(REGION_SIZE);
            }

            dsCfg.setDefaultDataRegionConfiguration(regCfg);

            cfg.setDataStorageConfiguration(dsCfg);

            cfg.setFailureHandler(new StopNodeFailureHandler());
        }

        return cfg;
    }

    /** */
    private CacheConfiguration<?, ?> cacheCfg(String cacheName) {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(runParams.servers - 1);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(runParams.cacheWriteMode);

        return ccfg;
    }

    /** */
    private String workDirectory(String instName) {
        return System.getProperty("java.io.tmpdir") + File.separator + "ignite" + File.separator + instName;
    }

    /**
     * Tear down routine.
     *
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
        Ignition.stopAll(true);

        nodes.clear();
    }

    /** */
    @Setup(Level.Trial)
    public void setup(Params params) throws ExecutionException, InterruptedException {
        this.runParams = params;

        for (int s = 0; s < params.servers; ++s) {
            IgniteConfiguration cfg = configuration("srv" + s, false);

            IgniteUtils.delete(new File(cfg.getWorkDirectory()));

            nodes.add(Ignition.start(cfg));
        }

        nodes.get(0).cluster().state(ClusterState.ACTIVE);

        if (LOAD_FROM_CLIENT)
            nodes.add(ldrNode = Ignition.start(configuration("client", true)));
        else
            ldrNode = nodes.get(rnd.nextInt(params.servers));

        nodes.get(0).createCache(cacheCfg(CACHE_NAME));

        values = new Object[VALUES_BANK_SIZE];
    }

    /** */
    @Setup(Level.Iteration)
    public void prepareIteration() {
        nodes.get(0).cache(CACHE_NAME).clear();

        assert nodes.get(0).cache(CACHE_NAME).size() == 0;

        int minLen = Math.max(1, (int)(AVERAGE_RECORD_LEN * 0.9f));
        int maxLen = Math.max(1, (int)(AVERAGE_RECORD_LEN * 1.1f));

        for (int v = 0; v < values.length; v++) {
            int valLen = minLen + (maxLen > minLen ? rnd.nextInt(maxLen - minLen) : 0);

            StringBuilder sb = new StringBuilder();

            for (int ch = 0; ch < valLen; ++ch)
                sb.append((char)((int)'a' + rnd.nextInt(20)));

            values[v] = sb.toString();
        }
    }

    /**
     * Test with batched receiver.
     */
    @Benchmark
    public void benchIndividual() throws Exception {
        runLoad(DataStreamerCacheUpdaters.individual());
    }

    /**
     * Test with default receiver.
     */
    @Benchmark
    public void benchDefaultIsolated() throws Exception {
        runLoad(null);
    }

    /**
     * Test with batched receiver.
     */
    @Benchmark
    public void benchBatched() throws Exception {
        runLoad(DataStreamerCacheUpdaters.batched());
    }
    
    /** Launches test with all available params. */
    private void runLoad(@Nullable StreamReceiver<Long, Object> receiver) throws Exception {
        AtomicLong keySupplier = new AtomicLong();

        try (IgniteDataStreamer<Long, Object> streamer = ldrNode.dataStreamer(CACHE_NAME)) {
            if (receiver != null)
                streamer.receiver(receiver);

            assert runParams.dsBatchSize != 0;

            if (runParams.dsBatchSize > 0)
                streamer.perNodeBufferSize(runParams.dsBatchSize);

            assert runParams.maxDsOps != 0;

            if (runParams.maxDsOps > 0)
                streamer.perNodeParallelOperations(runParams.maxDsOps);

            long key;

            while ((key = keySupplier.getAndIncrement()) < ENTRIES_TO_LOAD)
                streamer.addData(key, value(key));
        }

        if (PERSISTENT && INCLUDE_CHECKPOINT) {
            CompletableFuture.allOf(nodes.stream().filter(n -> !n.configuration().isClientMode())
                .map(n -> CompletableFuture.runAsync(new Runnable() {
                    @Override public void run() {
                        try {
                            ((IgniteEx)n).context().cache().context().database().waitForCheckpoint("forced");
                        }
                        catch (IgniteCheckedException e) {
                            n.log().error("Unable to wait for checkpoint.", e);
                        }
                    }
                })).toArray(CompletableFuture[]::new)).get();
        }

        assert nodes.get(0).cache(CACHE_NAME).size() == ENTRIES_TO_LOAD;
    }

    /** Extracts a value. */
    private Object value(long key) {
        return values[(int)(key % values.length)];
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
            .include(JmhStreamerReceiverBenchmark.class.getSimpleName())
            .forks(1)
            .jvmArgs("-Xms1g", "-Xmx1g", "-server", "-XX:+AlwaysPreTouch")
            .build();

        new Runner(options).run();
    }

    @State(Scope.Benchmark)
    public static class Params {
        @Param({"1", "2"})
        private int servers;

        @Param({"4", "8", "16"})
        private int maxDsOps;

        @Param({"256", "512", "1024"})
        private int dsBatchSize;

        @Param({"FSYNC", "LOG_ONLY", "NONE"})
        private WALMode walMode;

        @Param({"PRIMARY_SYNC", "FULL_SYNC"})
        private CacheWriteSynchronizationMode cacheWriteMode;
    }
}
