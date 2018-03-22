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

package org.apache.ignite.internal.benchmarks.jmh.tcp;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.util.nio.compression.CompressionEngine;
import org.apache.ignite.messaging.MessagingListenActor;
import org.apache.ignite.spi.communication.DeflaterFactory;
import org.apache.ignite.spi.communication.LZ4Factory;
import org.apache.ignite.spi.communication.ZstdFactory;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Tests for SPI compression and SSL.
 */
@Warmup(iterations = 15)
@Measurement(iterations = 10)
@Fork(5)
@Threads(1)
public class GridTcpCommunicationSpiBenchmark extends JmhCacheAbstractBenchmark {
    /** */
    static Ignite snd = null;

    /** */
    static Ignite receiver = null;

    /** */
    @Param({"No", "LZ4", "ZSTD", "Deflater"})
    private Type compressionType = Type.No;

    /** */
    @Param({"false", "true"})
    boolean isSsl = false;

    /** {@inheritDoc} */
    @Override protected final Factory<CompressionEngine> compressionEngineFactory() {
        switch (compressionType) {
            case LZ4:
                return new LZ4Factory();
            case ZSTD:
                return new ZstdFactory();
            case Deflater:
                return new DeflaterFactory();
            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected final boolean isSsl() {
        return isSsl;
    }

    /** Benchmark states with zero size message. */
    @State(Scope.Benchmark)
    public static class IoSendReceiveBaselineState extends IoState {
        /** */
        public IoSendReceiveBaselineState() {
            super();

            msg = new byte[0];

            receiver.message().localListen(null, new MessagingListenActor<byte[]>() {
                @Override protected void receive(UUID nodeId, byte[] rcvMsg) {
                    latch.get().countDown();
                }
            });
        }
    }

    /**
     * Send and receive the least message without statistic.
     */
    @Benchmark
    public final void sendAndReceiveBaseline(IoSendReceiveBaselineState state) {
        state.latch.set(new CountDownLatch(1));

        state.messaging.send(null, state.msg);

        try {
            state.latch.get().await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send and receive the least message.
     */
    @Benchmark
    public final void sendAndReceiveStatBaseline(IoSendReceiveBaselineState state, EventCounters counters) {
        sendAndReceive(state, counters);
    }

    /** Base class for all benchmark states. */
    static class IoState {
        /** */
        final IgniteMessaging messaging;

        /** */
        Object msg = null;

        /** */
        final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

        /** */
        IoState() {
            messaging = snd.message(snd.cluster().forNodeId(receiver.cluster().localNode().id()));
        }
    }

    /** Benchmark states with sized message with unregular data. */
    @State(Scope.Benchmark)
    public static class IoSendReceiveSizeState extends IoState {
        /** */
        @Param({"1000", "5000", "30000", "60000", "100000", "1000000", "5000000"})
        int size = 1000;

        /** */
        public IoSendReceiveSizeState() {
            super();

            receiver.message().localListen(null, new MessagingListenActor<byte[]>() {
                @Override protected void receive(UUID nodeId, byte[] rcvMsg) {
                    latch.get().countDown();
                }
            });
        }

        /** */
        @Setup(Level.Trial)
        public final void setup() {
            byte[] bytes = new byte[size];

            new Random(31415L).nextBytes(bytes);

            msg = bytes;
        }
    }

    /**
     * Base method for benchmarks.
     *
     * @param state State.
     * @param counters Counters.
     */
    private static void sendAndReceive(IoState state, EventCounters counters) {
        long b = snd.cluster().localNode().metrics().getSentBytesCount();

        state.latch.set(new CountDownLatch(1));

        state.messaging.send(null, state.msg);

        try {
            state.latch.get().await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        counters.sentBytes += (snd.cluster().localNode().metrics().getSentBytesCount() - b);

        counters.sentMessages++;
    }

    /**
     * Send and receive message with random data.
     */
    @Benchmark
    public final void sendAndReceiveSize(IoSendReceiveSizeState state, EventCounters counters) {
        sendAndReceive(state, counters);
    }

    /** Counters. */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class EventCounters {
        /** This field would be counted as metric */
        public int sentBytes = 0;

        /** This field would be counted as metric */
        public int sentMessages = 0;
    }

    /** Benchmark states with sized message with regular data. */
    @State(Scope.Benchmark)
    public static class IoSendReceiveSizeGoodState extends IoState {
        /** */
        @Param({"1000", "5000", "30000", "60000", "100000", "1000000", "5000000"})
        int size = 1000;

        /** */
        public IoSendReceiveSizeGoodState() {
            super();

            receiver.message().localListen(null, new MessagingListenActor<byte[]>() {
                @Override protected void receive(UUID nodeId, byte[] rcvMsg) {
                    latch.get().countDown();
                }
            });
        }

        /** */
        @Setup(Level.Trial)
        public final void setup() {
            msg = RegularDataUtils.generateRegularData(size, 10);
        }
    }

    /**
     * Send and receive message with regular data.
     */
    @Benchmark
    public final void sendAndReceiveSizeGood(IoSendReceiveSizeGoodState state, EventCounters counters) {
        sendAndReceive(state, counters);
    }

    /**
     * Create locks and put values in the cache.
     */
    @Setup(Level.Trial)
    public final void setup1() {
        snd = node;

        receiver = Ignition.start(configuration("node" + 147));
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String... args) throws Exception {
        // Use in linux for emulate slow network:
        // sudo tc qdisc add dev lo root tbf rate 1Gbit burst 100kb latency 5ms
        // And remove it: sudo tc qdisc del dev lo root
        // Check your network: sudo tc -s qdisc ls dev lo

        String simpleClsName = GridTcpCommunicationSpiBenchmark.class.getSimpleName();
        boolean client = false;
        CacheAtomicityMode atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        CacheWriteSynchronizationMode writeSyncMode = CacheWriteSynchronizationMode.FULL_SYNC;

        Options opt = new OptionsBuilder()
            .include(simpleClsName)
            .timeUnit(TimeUnit.MICROSECONDS)
            .mode(Mode.AverageTime)
            .jvmArgs(
                "-Xms1g",
                "-Xmx1g",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, atomicityMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, writeSyncMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 1),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client)).build();

        Collection<RunResult> results = new Runner(opt).run();

        System.out.println("-----------------------------------");

        EnumMap<Type, Result> baseline = new EnumMap<>(Type.class);

        EnumMap<Type, Result> baselineStat = new EnumMap<>(Type.class);

        TreeMap<Integer, EnumMap<Type, Result>> sizedResults = new TreeMap<>();

        TreeMap<Integer, EnumMap<Type, Result>> sizedNiceResults = new TreeMap<>();

        for (RunResult result : results) {
            Type type = Type.getType(result.getParams().getParam("compressionType"),
                result.getParams().getParam("isSsl"));

            double val = result.getPrimaryResult().getScore();
            double error = result.getPrimaryResult().getScoreError();

            if ("sendAndReceiveBaseline".equals(result.getPrimaryResult().getLabel())) {
                baseline.put(type, new Result(val, error, 0));

                continue;
            }

            double avgSize = result.getSecondaryResults().get("sentBytes").getScore() /
                result.getSecondaryResults().get("sentMessages").getScore();

            switch (result.getPrimaryResult().getLabel()) {
                case "sendAndReceiveStatBaseline":
                    baselineStat.put(type, new Result(val, error, avgSize));

                    break;
                case "sendAndReceiveSize": {
                    int size = Integer.valueOf(result.getParams().getParam("size"));

                    EnumMap<Type, Result> map = sizedResults.computeIfAbsent(size, k -> new EnumMap<>(Type.class));

                    map.put(type, new Result(val, error, avgSize));

                    break;
                }
                case "sendAndReceiveSizeGood": {
                    int size = Integer.valueOf(result.getParams().getParam("size"));

                    EnumMap<Type, Result> map = sizedNiceResults.computeIfAbsent(size, k -> new EnumMap<>(Type.class));

                    map.put(type, new Result(val, error, avgSize));

                    break;
                }
            }
        }

        System.out.println("Latency:");
        System.out.println("||Type||Time||NetFootprint||");
        for (Entry<Type, Result> entry : baseline.entrySet()) {
            System.out.println("|" + entry.getKey() + "|" +
                entry.getValue().val + "±" + entry.getValue().error + "|" +
                baselineStat.get(entry.getKey()).avgSize + "|");
        }

        System.out.println("Throughput worst case");
        printThroughput(sizedResults, baselineStat, baseline);

        System.out.println("Throughput real case");
        printThroughput(sizedNiceResults, baselineStat, baseline);
    }

    /** */
    private static double sqrtSum(double... xs) {
        double sum = 0.0;

        for (double x : xs)
            sum += (x * x);

        return Math.sqrt(sum);
    }

    /** */
    private static void printThroughput(Map<Integer,
        EnumMap<Type, Result>> results,
        EnumMap<Type, Result> baselineStat,
        EnumMap<Type, Result> baseline) {

        DecimalFormat df = new DecimalFormat("#0.00");

        for (Entry<Integer, EnumMap<Type, Result>> entry : results.entrySet()) {
            int size = entry.getKey();

            System.out.println(
                "||Type||MsgSize||MsgPerSecond||RealMbitsPerSecond||EffectiveMbitsPerSecond||CompressionRatio||"
            );
            for (Entry<Type, Result> resultEntry : entry.getValue().entrySet()) {
                double statTime = baselineStat.get(resultEntry.getKey()).val - baseline.get(resultEntry.getKey()).val;

                double baselineAvgSize = Math.round(baselineStat.get(resultEntry.getKey()).avgSize);
                double val = resultEntry.getValue().val - statTime;

                double error = val * sqrtSum(resultEntry.getValue().error / resultEntry.getValue().val,
                    baselineStat.get(resultEntry.getKey()).error / baselineStat.get(resultEntry.getKey()).val,
                    baseline.get(resultEntry.getKey()).error / baseline.get(resultEntry.getKey()).val);

                double avgSize = Math.round(resultEntry.getValue().avgSize);

                System.out.println("|" + resultEntry.getKey() + "|" + size + "|" +
                    df.format(1.0E6 / val) + " ∈ [" +
                    df.format(1.0E6 / (val + error)) + " : " + df.format(1.0E6 / (val - error)) +
                    "]" + "|" +
                    df.format(avgSize / val * 8) + " ∈ ["
                    + df.format(avgSize / (val + error) * 8) + " : " +
                    df.format(avgSize / (val - error) * 8) +
                    "]" + "|" +
                    df.format(size / val * 8) + " ∈ [" +
                    df.format(size / (val + error) * 8) + " : " +
                    df.format(size / (val - error) * 8) +
                    "]" + "|" +
                    df.format(size / (avgSize - baselineAvgSize)) + "|");
            }
        }
    }

    /**
     * Type of connection.
     *
     * Need public for JMH.
     * */
    public enum Type {
        /** */
        No,
        /** */
        LZ4,
        /** */
        ZSTD,
        /** */
        Deflater,
        /** */
        Ssl,
        /** */
        SslLZ4,
        /** */
        SslZSTD,
        /** */
        SslDeflater;

        /** */
        private static Type getType(String comp0, String ssl0) {
            Type comp = Type.valueOf(comp0);

            if (Boolean.valueOf(ssl0)) {
                switch (comp) {
                    case No:
                        comp = Ssl;
                        break;
                    case LZ4:
                        comp = SslLZ4;
                        break;
                    case ZSTD:
                        comp = SslZSTD;
                        break;
                    case Deflater:
                        comp = SslDeflater;
                        break;
                }
            }

            return comp;
        }
    }

    /** */
    private static final class Result {
        /** */
        private final double val;

        /** */
        private final double error;

        /** */
        private final double avgSize;

        /** */
        private Result(double val, double error, double avgSize) {
            this.val = val;
            this.error = error;
            this.avgSize = avgSize;
        }
    }
}
