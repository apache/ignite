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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * For research of the streamer throughput with different settings and the receivers.
 */
abstract class JmhAbstractStreamerReceiverBenchmark {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final long DFLT_DATA_AMOUNT_TO_LOAD = 100L * 1024L * 1024L;

    /** */
    private static final boolean LOAD_FROM_CLIENT = true;

    /** */
    private static final int CHECKPOINT_FREQUENCY = 3000;

    /** Enables or disables final checkpoint into the measurement. */
    private static final boolean INCLUDE_CHECKPOINT = false;

    /** */
    private static final long REGION_SIZE = 512L * 1024L * 1024L;

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private final Random rnd = new Random();

    /** */
    private final List<Ignite> nodes = new ArrayList<>();

    /** */
    private final boolean persistent;

    /** */
    protected final long dataAmountToLoad;

    /** Loader node. */
    private Ignite ldrNode;

    /** Values to laod bank. */
    private Object[] values;

    /** */
    private Params runParams;

    /** */
    protected JmhAbstractStreamerReceiverBenchmark(boolean persistent, long dataAmountToLoad) {
        this.persistent = persistent;
        this.dataAmountToLoad = dataAmountToLoad > 0 ? dataAmountToLoad : DFLT_DATA_AMOUNT_TO_LOAD;
    }

    /**
     * Create Ignite configuration.
     *
     * @return Ignite configuration.
     */
    protected IgniteConfiguration configuration(String instName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(instName);

        cfg.setWorkDirectory(workDirectory(instName));

        cfg.setGridLogger(new NullLogger());

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoverySpi);
        cfg.setLocalHost("127.0.0.1");

        if (isClient)
            cfg.setClientMode(true);
        else {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration();

            DataRegionConfiguration regCfg = new DataRegionConfiguration();

            regCfg.setPersistenceEnabled(persistent);

            if (persistent) {
                dsCfg.setCheckpointFrequency(CHECKPOINT_FREQUENCY);

                dsCfg.setWalMode(runParams.walMode());

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
    protected CacheConfiguration<?, ?> cacheCfg(String cacheName) {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(runParams.servers() - 1);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(runParams.cacheWriteMode());

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
    protected void setup(Params params) {
        this.runParams = params;

        for (int s = 0; s < params.servers(); ++s) {
            IgniteConfiguration cfg = configuration("srv" + s, false);

            IgniteUtils.delete(new File(cfg.getWorkDirectory()));

            nodes.add(Ignition.start(cfg));
        }

        nodes.get(0).cluster().state(ClusterState.ACTIVE);

        if (LOAD_FROM_CLIENT)
            nodes.add(ldrNode = Ignition.start(configuration("client", true)));
        else
            ldrNode = nodes.get(rnd.nextInt(params.servers()));

        nodes.get(0).createCache(cacheCfg(CACHE_NAME));

        values = new Object[2000];
    }

    /** */
    @Setup(Level.Iteration)
    public void prepareIteration() {
        nodes.get(0).cache(CACHE_NAME).clear();

        assert nodes.get(0).cache(CACHE_NAME).size() == 0;

        assert runParams.avgDataSize() != 0;

        int minLen = Math.max(1, (int)(runParams.avgDataSize() * 0.9f));
        int maxLen = Math.max(1, (int)(runParams.avgDataSize() * 1.1f));

        for (int v = 0; v < values.length; v++) {
            int valLen = minLen + (maxLen > minLen ? rnd.nextInt(maxLen - minLen) : 0);

            StringBuilder sb = new StringBuilder();

            for (int ch = 0; ch < valLen; ++ch)
                sb.append((char)((int)'a' + rnd.nextInt(20)));

            values[v] = sb.toString();
        }
    }

    /**
     * Test with batched receiver. When 'allowOverwrite' is 'true'.
     */
    @Benchmark
    public void benchIndividual() throws Exception {
        runLoad(DataStreamerCacheUpdaters.individual());
    }

    /**
     * Test with default (isolated) receiver. When 'allowOverwrite' is 'false'.
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
        long loadCnt = dataAmountToLoad / runParams.avgDataSize();

        try (IgniteDataStreamer<Long, Object> streamer = ldrNode.dataStreamer(CACHE_NAME)) {
            if (receiver != null)
                streamer.receiver(receiver);

            assert runParams.dsBatchSize() != 0;

            if (runParams.dsBatchSize() > 0)
                streamer.perNodeBufferSize(runParams.dsBatchSize());

            assert runParams.maxDsOps() != 0;

            if (runParams.maxDsOps() > 0)
                streamer.perNodeParallelOperations(runParams.maxDsOps());

            for (long i = 0; i < loadCnt; ++i)
                streamer.addData(i, value(i));
        }

        if (persistent && INCLUDE_CHECKPOINT) {
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

        assert nodes.get(0).cache(CACHE_NAME).size() == loadCnt;
    }

    /** Extracts a value. */
    private Object value(long key) {
        return values[(int)(key % values.length)];
    }

    /** */
    protected static Options options(Class<? extends JmhAbstractStreamerReceiverBenchmark> bchClazz) {
        return new OptionsBuilder()
            .include(bchClazz.getSimpleName())
            .forks(1)
            .jvmArgs("-Xms4g", "-Xmx4g", "-server", "-XX:+AlwaysPreTouch")
            .build();
    }

    /** */
    protected interface Params {
        /** */
        default int servers() {
            return 2;
        }

        /** */
        default WALMode walMode(){ return null; }

        /** */
        default CacheWriteSynchronizationMode cacheWriteMode() {
            return null;
        }

        /** */
        int avgDataSize();

        /** */
        default int dsBatchSize() {
            return 512;
        }

        /** */
        default int maxDsOps() {
            return -1;
        }
    }
}
