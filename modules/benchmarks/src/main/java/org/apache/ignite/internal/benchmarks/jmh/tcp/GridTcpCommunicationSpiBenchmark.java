package org.apache.ignite.internal.benchmarks.jmh.tcp;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.messaging.MessagingListenActor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 *
 */
@Warmup(iterations = 20)
@Measurement(iterations = 20)
@Fork(1)
public class GridTcpCommunicationSpiBenchmark extends JmhCacheAbstractBenchmark {
    /** Number threads per node. */
    static final int THREADS_PER_NODE = 1;

    /** */
    static Ignite from;

    /** */
    static Ignite to;

    /** */
    @State(Scope.Thread)
    public static class IoSendReceiveBaselineState {
        final IgniteMessaging messaging;

        final Object msg;

        final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

        /** */
        public IoSendReceiveBaselineState() {
            messaging = from.message(from.cluster().forNodeId(to.cluster().localNode().id()));

            msg = new Object();
            //msg = String.valueOf(System.nanoTime());

            to.message().localListen(null, new MessagingListenActor<Object>() {
                @Override protected void receive(UUID nodeId, Object rcvMsg) throws Throwable {
                    latch.get().countDown();
                }
            });
        }
    }

    /**
     * Test IgniteCache.lock() with fixed key and no-op inside.
     */
    @Benchmark
    public void sendAndReceiveBaseline(final IoSendReceiveBaselineState state) {
        state.latch.set(new CountDownLatch(1));

        state.messaging.send(null, state.msg);
        try {
            state.latch.get().await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /** */
    @State(Scope.Benchmark)
    public static class IoSendReceiveSizeState {
        final IgniteMessaging messaging;

        byte[] msg;

        final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

        @Param({"1000", "10000", "100000" ,"1000000"})
        int size = 1024*1024;

        /** */
        public IoSendReceiveSizeState() {
            messaging = from.message(from.cluster().forNodeId(to.cluster().localNode().id()));

            to.message().localListen(null, new MessagingListenActor<byte[]>() {
                @Override protected void receive(UUID nodeId, byte[] rcvMsg) throws Throwable {
                    latch.get().countDown();
                }
            });
        }

        @Setup(Level.Trial)
        public void setup() {
            msg = new byte[size];
            Random random = new Random();
            random.nextBytes(msg);
        }
    }

    /**
     * Test IgniteCache.lock() with fixed key and no-op inside.
     */
    @Benchmark
    public void sendAndReceiveSize(final IoSendReceiveSizeState state) {
        state.latch.set(new CountDownLatch(1));

        state.messaging.send(null, state.msg);
        try {
            state.latch.get().await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /** */
    @State(Scope.Benchmark)
    public static class IoSendReceiveSizeNiceState {
        final IgniteMessaging messaging;

        byte[] msg;

        final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

        @Param({"1000", "10000", "100000" ,"1000000"})
        int size = 1024*1024;

        /** */
        public IoSendReceiveSizeNiceState() {
            messaging = from.message(from.cluster().forNodeId(to.cluster().localNode().id()));

            to.message().localListen(null, new MessagingListenActor<byte[]>() {
                @Override protected void receive(UUID nodeId, byte[] rcvMsg) throws Throwable {
                    latch.get().countDown();
                }
            });
        }

        @Setup(Level.Trial)
        public void setup() {
            msg = new byte[size];
        }
    }

    /**
     * Test IgniteCache.lock() with fixed key and no-op inside.
     */
    @Benchmark
    public void sendAndReceiveSizeNice(final IoSendReceiveSizeNiceState state) {
        state.latch.set(new CountDownLatch(1));

        state.messaging.send(null, state.msg);
        try {
            state.latch.get().await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Param({"false", "true"})
    boolean isCompress = false;

    @Override protected boolean isCompress(){
        return isCompress;
    }


    /**
     * Create locks and put values in the cache.
     */
    @Setup(Level.Trial)
    public void setup1() throws Exception {
        //super.setup();

        from = node;

        to = Ignition.start(configuration("node" + 147));
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final String simpleClsName = GridTcpCommunicationSpiBenchmark.class.getSimpleName();
        final int threads = THREADS_PER_NODE;
        final boolean client = false;
        final CacheAtomicityMode atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        final CacheWriteSynchronizationMode writeSyncMode = CacheWriteSynchronizationMode.FULL_SYNC;

        final String output = simpleClsName +
            "-" + threads + "-threads" +
            "-" + (client ? "client" : "data") +
            "-" + atomicityMode +
            "-" + writeSyncMode;

        final Options opt1 = new OptionsBuilder()
            .threads(threads)
            .include(simpleClsName)
            //.output(output + ".jmh.log")
            .timeUnit(TimeUnit.MICROSECONDS)
            .mode(Mode.AverageTime)
            .jvmArgs(
                "-Xms1g",
                "-Xmx1g",
                //"-XX:+UnlockCommercialFeatures",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, atomicityMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, writeSyncMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 1),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client)).build();

        Collection<RunResult> results1 = new Runner(opt1).run();

        System.out.println("-----------------------------------");

        System.out.println(results1.size());

        EnumMap<Type, Result> baseline = new EnumMap<Type, Result>(Type.class);

        TreeMap<Integer, EnumMap<Type, Result>> sizedResults = new TreeMap<>();

        TreeMap<Integer, EnumMap<Type, Result>> sizedNiceResults = new TreeMap<>();

        for (RunResult result : results1) {
            System.out.println(result.getBenchmarkResults().size());
            for (BenchmarkResult benchmarkResult : result.getBenchmarkResults()) {
                Type type = Type.getType(benchmarkResult.getParams().getParam("isCompress"),
                    benchmarkResult.getParams().getParam("isSsl"));

                double value = benchmarkResult.getPrimaryResult().getScore();
                double error = benchmarkResult.getPrimaryResult().getScoreError();

                switch (benchmarkResult.getPrimaryResult().getLabel()) {
                    case "sendAndReceiveBaseline" :
                        baseline.put(type, new Result(value, error));
                        break;
                    case "sendAndReceiveSize" : {
                        int size = Integer.valueOf(benchmarkResult.getParams().getParam("size"));

                        EnumMap<Type, Result> map = sizedResults.get(size);
                        if (map == null) {
                            map = new EnumMap<Type, Result>(Type.class);
                            sizedResults.put(size, map);
                        }

                        map.put(type, new Result(value, error));
                        break;
                    }
                    case "sendAndReceiveSizeNice" : {
                        int size = Integer.valueOf(benchmarkResult.getParams().getParam("size"));

                        EnumMap<Type, Result> map = sizedNiceResults.get(size);
                        if (map == null) {
                            map = new EnumMap<Type, Result>(Type.class);
                            sizedNiceResults.put(size, map);
                        }

                        map.put(type, new Result(value, error));
                        break;
                    }
                }
            }
        }

        DecimalFormat df = new DecimalFormat("#0.00");

        for (Map.Entry<Integer, EnumMap<Type, Result>> entry : sizedResults.entrySet()) {
            int size = entry.getKey();

            for (Map.Entry<Type, Result> resultEntry : entry.getValue().entrySet()) {
                double baselineValue = baseline.get(resultEntry.getKey()).value;
                double baselineError = baseline.get(resultEntry.getKey()).error;

                double value = resultEntry.getValue().value;
                double error = resultEntry.getValue().error;

                double minTime = (value-error)-(baselineValue+baselineError);
                double maxTime = (value+error)-(baselineValue-baselineError);
                if (minTime < 0)
                    minTime = 0;
                double mean = value-baselineValue;
                if (mean < 0)
                    mean = 0;

                System.out.println("Throughput for "+ size +" "+ resultEntry.getKey() +" : " + df.format(size/mean/1000*8) +
                    " ∈ ["+df.format(size/maxTime/1000*8)+" : "+df.format(size/minTime/1000*8)+"] Gb/s");
            }
        }

        for (Map.Entry<Integer, EnumMap<Type, Result>> entry : sizedNiceResults.entrySet()) {
            int size = entry.getKey();

            for (Map.Entry<Type, Result> resultEntry : entry.getValue().entrySet()) {
                double baselineValue = baseline.get(resultEntry.getKey()).value;
                double baselineError = baseline.get(resultEntry.getKey()).error;

                double value = resultEntry.getValue().value;
                double error = resultEntry.getValue().error;

                double minTime = (value-error)-(baselineValue+baselineError);
                double maxTime = (value+error)-(baselineValue-baselineError);
                if (minTime < 0)
                    minTime = 0;
                double mean = value-baselineValue;
                if (mean < 0)
                    mean = 0;

                System.out.println("Throughput nice for "+ size +" "+ resultEntry.getKey() +" : " + df.format(size/mean/1000*8) +
                    " ∈ ["+df.format(size/maxTime/1000*8)+" : "+df.format(size/minTime/1000*8)+"] Gb/s");
            }
        }
    }

    static enum Type {
        No,
        Comp,
        Ssl,
        CompSsl;

        static Type getType(String comp0, String ssl0) {
            boolean comp = Boolean.valueOf(comp0);
            boolean ssl = Boolean.valueOf(ssl0);

            if (comp && ssl)
                return CompSsl;
            if (comp && !ssl)
                return Comp;
            if (!comp && ssl)
                return Ssl;
            if (!comp && !ssl)
                return No;

            return No;
        }
    }

    static class Result {
        final double value;
        final double error;

        Result(double value, double error) {
            this.value = value;
            this.error = error;
        }
    }
}
