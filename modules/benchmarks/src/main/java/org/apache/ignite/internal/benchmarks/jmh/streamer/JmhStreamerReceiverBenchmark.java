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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
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
 * For research of the streamer settings and the receivers.
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
    private static final int RECORD_LEN_DELTA = AVERAGE_RECORD_LEN / 10;

    /** */
    private static final boolean LOAD_FROM_CLIENT = true;

    /** */
    private static final boolean PERSISTENT = true;

    /** */
    private static final int SERVERS = 1;

    /** Cache backups num. */
    private static final int BACKUPS = SERVERS - 1;

    /** */
    private static final int CHECKPOINT_FREQUENCY = 3000;

    /** Enables or disables final checkpoint into the measurement. */
    private static final boolean INCLUDE_CHECKPOINT = false;

    /** Some fixed minimal + doubled average record size. */
    private static final long REGION_SIZE = 100L * 1024L * 1024L + ENTRIES_TO_LOAD * AVERAGE_RECORD_LEN * 2;

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private static final int VALUES_BANK_SIZE = 2000;

    /** */
    private final Random rnd = new Random();

    /** */
    private List<Ignite> nodes;

    /** */
    private Ignite ldrNode;

    /** */
    private Object[] values;

    /**
     * Create Ignite configuration.
     *
     * @return Ignite configuration.
     */
    private IgniteConfiguration configuration(String instName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(instName);

        cfg.setWorkDirectory(workDirectory(instName));

        if (isClient)
            cfg.setClientMode(true);
        else {
            cfg.setGridLogger(new NullLogger());

            DataStorageConfiguration dsCfg = new DataStorageConfiguration();

            DataRegionConfiguration regCfg = new DataRegionConfiguration();

            regCfg.setPersistenceEnabled(PERSISTENT);

            if (PERSISTENT) {
                //Reduce affection of side I/O.
                dsCfg.setCheckpointFrequency(CHECKPOINT_FREQUENCY);

                //dsCfg.setWalMode(WALMode.LOG_ONLY);

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
        ccfg.setBackups(BACKUPS);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

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
        Collections.reverse(nodes);

        CompletableFuture.allOf(nodes.stream().map(n -> CompletableFuture.runAsync(new Runnable() {
            @Override public void run() {
                String workDir = n.configuration().getWorkDirectory();

                try {
                    n.close();
                }
                catch (Exception ignore) {
                    // No-op
                }

                IgniteUtils.delete(new File(workDir));
            }
        })).toArray(CompletableFuture[]::new)).get();

        nodes.clear();
    }

    /**
     * Start 2 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() throws ExecutionException, InterruptedException {
        nodes = new CopyOnWriteArrayList<>();

        AtomicInteger idx = new AtomicInteger();

        CompletableFuture.allOf(Stream.generate(new Supplier<CompletableFuture<?>>() {
            @Override public CompletableFuture<?> get() {
                return CompletableFuture.runAsync(new Runnable() {
                    @Override public void run() {
                        IgniteConfiguration cfg = configuration("srv" + idx.getAndIncrement(), false);

                        IgniteUtils.delete(new File(cfg.getWorkDirectory()));

                        nodes.add(Ignition.start(cfg));
                    }
                });
            }
        }).limit(SERVERS).toArray((IntFunction<CompletableFuture<?>[]>)CompletableFuture[]::new)).get();

        nodes.get(0).cluster().state(ClusterState.ACTIVE);

        if (LOAD_FROM_CLIENT)
            nodes.add(ldrNode = Ignition.start(configuration("client", true)));
        else
            ldrNode = nodes.get(rnd.nextInt(SERVERS));

        nodes.get(0).createCache(cacheCfg(CACHE_NAME));

        values = new Object[VALUES_BANK_SIZE];
    }

    /** */
    @Setup(Level.Iteration)
    public void prepareIteration() {
        nodes.get(0).cache(CACHE_NAME).clear();

        assert nodes.get(0).cache(CACHE_NAME).size() == 0;

        int minLen = Math.max(1, AVERAGE_RECORD_LEN - RECORD_LEN_DELTA / 2);
        int maxLen = Math.max(1, AVERAGE_RECORD_LEN + RECORD_LEN_DELTA / 2);

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
    public void bchIndividual_512() throws Exception {
        runLoad(DataStreamerCacheUpdaters.individual(), 512);
    }

    /**
     * Test with default receiver.
     */
    @Benchmark
    public void bchDefaultIsolated_256() throws Exception {
        runLoad(null, 256);
    }

    /**
     * Test with default receiver.
     */
    @Benchmark
    public void bchDefaultIsolated_512() throws Exception {
        runLoad(null, 512);
    }

    /**
     * Test with individual receiver.
     */
    @Benchmark
    public void bchIndividual_256() throws Exception {
        runLoad(DataStreamerCacheUpdaters.individual(), 256);
    }

    /**
     * Test with batched receiver.
     */
    @Benchmark
    public void bchBatched_256() throws Exception {
        runLoad(DataStreamerCacheUpdaters.batched(), 256);
    }

    /**
     * Test with batched receiver.
     */
    @Benchmark
    public void bchBatched_512() throws Exception {
        runLoad(DataStreamerCacheUpdaters.batched(), 512);
    }
    
    /** Launches test with all available params. */
    private void runLoad(@Nullable StreamReceiver<Long, Object> receiver, int batchSize) throws Exception {

        AtomicLong keySupplier = new AtomicLong();

        try (IgniteDataStreamer<Long, Object> streamer = ldrNode.dataStreamer(CACHE_NAME)) {
            if (receiver != null)
                streamer.receiver(receiver);

            if (batchSize > 0)
                streamer.perNodeBufferSize(batchSize);

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
}
