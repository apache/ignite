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
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponse;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerResponse;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
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
 * Compares streamer receivers.
 */
abstract class JmhAbstractStreamerReceiverBenchmark {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final boolean LOAD_FROM_CLIENT = true;

    /** */
    private static final int CHECKPOINT_FREQUENCY = 3000;

    /** Enables or disables final checkpoint into the measurement. */
    private static final boolean INCLUDE_CHECKPOINT = false;

    /** */
    private static final long REGION_SIZE = 512L * 1024L * 1024L;

    /** */
    private static final boolean DELAY_ONLY_IO_RESPONSES = true;

    /** */
    private static final int DEFAULT_BATCH_SIZE = 512;

    /** */
    private static final int DEFAULT_AVG_DATA_SIZE = 777;

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
    protected int avgDataSize;

    /** */
    private Params runParams;

    /** */
    protected JmhAbstractStreamerReceiverBenchmark(boolean persistent, long dataAmountToLoad) {
        this.persistent = persistent;
        this.dataAmountToLoad = dataAmountToLoad;
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

        if (DELAY_ONLY_IO_RESPONSES) {
            cfg.setCommunicationSpi(new NetDelaySimulationCommunicationSpi(runParams.sendMsgDelayMs(), msg -> {
                if (msg instanceof GridIoMessage) {
                    msg = ((GridIoMessage)msg).message();

                    return msg instanceof DataStreamerResponse || msg instanceof GridDhtAtomicUpdateResponse ||
                        msg instanceof GridDhtAtomicDeferredUpdateResponse;
                }

                return false;
            }));
        }
        else
            cfg.setCommunicationSpi(new NetDelaySimulationCommunicationSpi(runParams.sendMsgDelayMs(), null));

        return cfg;
    }

    /** */
    protected CacheConfiguration<?, ?> cacheCfg(String cacheName) {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(Math.min(runParams.serversCnt() - 1, 2));
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

        for (int s = 0; s < params.serversCnt(); ++s) {
            IgniteConfiguration cfg = configuration("srv" + s, false);

            IgniteUtils.delete(new File(cfg.getWorkDirectory()));

            nodes.add(Ignition.start(cfg));
        }

        nodes.get(0).cluster().state(ClusterState.ACTIVE);

        if (LOAD_FROM_CLIENT)
            nodes.add(ldrNode = Ignition.start(configuration("client", true)));
        else
            ldrNode = nodes.get(rnd.nextInt(params.serversCnt()));

        nodes.get(0).createCache(cacheCfg(CACHE_NAME));

        values = new Object[2000];
    }

    /** */
    @Setup(Level.Iteration)
    public void prepareIteration() {
        nodes.get(0).cache(CACHE_NAME).clear();

        assert nodes.get(0).cache(CACHE_NAME).size() == 0;

        avgDataSize = runParams.avgDataSize() > 0 ? runParams.avgDataSize() : DEFAULT_AVG_DATA_SIZE;

        int minLen = Math.max(1, (int)(avgDataSize * 0.9f));
        int maxLen = Math.max(1, (int)(avgDataSize * 1.1f));

        for (int v = 0; v < values.length; v++) {
            int valLen = minLen + (maxLen > minLen ? rnd.nextInt(maxLen - minLen) : 0);

            StringBuilder sb = new StringBuilder();

            for (int ch = 0; ch < valLen; ++ch)
                sb.append((char)((int)'a' + rnd.nextInt(20)));

            values[v] = sb.toString();
        }
    }

    /**
     * Test with individual receiver. When 'allowOverwrite' is 'true'.
     * This receiver is much slower any batched one.
     * Add @Benchmark to enable the test.
     */
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
        long loadCnt = dataAmountToLoad / avgDataSize;

        try (IgniteDataStreamer<Long, Object> streamer = ldrNode.dataStreamer(CACHE_NAME)) {
            if (receiver != null)
                streamer.receiver(receiver);

            streamer.perNodeBufferSize(runParams.dsBatchSize() > 0 ? runParams.dsBatchSize() : DEFAULT_BATCH_SIZE);

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
            .jvmArgs("-Xms2g", "-Xmx2g", "-server", "-XX:+AlwaysPreTouch")
            .build();
    }

    /** */
    protected interface Params {
        /** */
        default int serversCnt() {
            return 2;
        }

        /** */
        default WALMode walMode() {
            return null;
        }

        /** */
        default CacheWriteSynchronizationMode cacheWriteMode() {
            return null;
        }

        /**
         * Average data size. Default is {@link #DEFAULT_AVG_DATA_SIZE}.
         */
        default int avgDataSize() {
            return -1;
        }

        /**
         * Batch size per node. Default is {@link #DEFAULT_BATCH_SIZE}.
         */
        default int dsBatchSize() {
            return -1;
        }

        /**
         * Maximal unresponded batches per node. If negative, ignored.
         */
        default int maxDsOps() {
            return -1;
        }

        /**
         * Network delay simulation in mills on message sending. If not positive, ignored.
         */
        default int sendMsgDelayMs() {
            return 3;
        }
    }

    /** */
    private static class NetDelaySimulationCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final int delay;

        /** */
        private final Function<Message, Boolean> msgFilter;

        /** */
        private NetDelaySimulationCommunicationSpi(int delay, @Nullable Function<Message, Boolean> msgFilter) {
            this.delay = delay;
            this.msgFilter = msgFilter;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            doDelay(msg);

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            doDelay(msg);

            super.sendMessage(node, msg, ackC);
        }

        /** */
        private void doDelay(Message msg) {
            if (delay > 0 && (msgFilter == null || msgFilter.apply(msg))) {
                try {
                    U.sleep(delay);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }
            }
        }
    }
}
