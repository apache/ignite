package org.apache.ignite.internal.benchmarks.jmh.preloader;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.profile.GCProfiler;

public class JmhPreloaderCacheBenchmark extends JmhPreloaderAbstractBenchmark {

    protected static final String PROP_REBALANCE = "ignite.jmh.preloader.rebalance";

    protected final AtomicBoolean stop = new AtomicBoolean();
    protected Thread demanderThread;
    protected Thread supplierThread;

    protected boolean rebalance;

    @Override public void setup() throws Exception {
        super.setup();

        rebalance = booleanProperty(PROP_REBALANCE);

        stop.set(false);

        if (rebalance) {
            demanderThread = new Thread(new Runnable() {
                @Override public void run() {
                    while (!stop.get()) {
                        try {
                            demanderPreloader.handleSupplyMessage(0, supplierNode.localNode().id(), generateSupplyMessage());
                        }
                        catch (Exception ex) {
                            ex.printStackTrace();

                            throw new RuntimeException(ex);
                        }
                    }
                }
            }, "demander-loader");

            supplierThread = new Thread(new Runnable() {
                @Override public void run() {
                    while (!stop.get()) {
                        try {
                            supplierPreloader.handleDemandMessage(0, supplierNode.localNode().id(), generateDemandMessage());
                        }
                        catch (Exception ex) {
                            ex.printStackTrace();

                            throw new RuntimeException(ex);
                        }
                    }
                }
            }, "supplier-loader");

            demanderThread.start();
//            supplierThread.start();

            U.sleep(2000);
        }
    }

    @Override public void tearDown() throws Exception {
        stop.set(true);

        if (rebalance) {
            demanderThread.join();
//        supplierThread.join();
        }

        super.tearDown();
    }

    @Benchmark
    public void supplierPut() {
        int k = ThreadLocalRandom.current().nextInt(100_000);

        supplierCache.put(k, k);
    }

    @Benchmark
    public void demanderPut() {
        int k = ThreadLocalRandom.current().nextInt(100_000);

        demanderCache.put(k, k);
    }

    public static void main(String[] args) throws Exception {
        run("supplierPut", 1, 10000, true);
        run("supplierPut", 2, 10000, true);
        run("supplierPut", 4, 10000, true);
        run("supplierPut", 8, 10000, true);

        run("supplierPut", 1, 10000, false);
        run("supplierPut", 2, 10000, false);
        run("supplierPut", 4, 10000, false);
        run("supplierPut", 8, 10000, false);

//        run("demanderPut", 1, 10000, true);
//        run("demanderPut", 2, 10000, true);
//
//        run("demanderPut", 1, 10000, false);
//        run("demanderPut", 2, 10000, false);

//        run("demanderPut", 1, 10000);

//        JmhPreloaderCacheBenchmark benchmark = new JmhPreloaderCacheBenchmark();
//
//        benchmark.setup();
//
//        for (int i = 0; i < 1000; i++)
//            benchmark.supplierPut();
//
//        benchmark.tearDown();
    }

    /**
     * Run benchmark.
     *
     * @param benchmark Benchmark to run.
     * @param threads Amount of threads.
     * @throws Exception If failed.
     */
    private static void run(String benchmark, int threads, int entriesPerMessage, boolean rebalance) throws Exception {
        String simpleClsName = JmhPreloaderCacheBenchmark.class.getSimpleName();

        String output = simpleClsName + "-" + benchmark +
            "-" + threads + "-threads" +
            "-" + entriesPerMessage + "-epm" +
            "-" + rebalance + "-rebalance";

        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(threads)
            .warmupIterations(10)
            .measurementIterations(60)
            .benchmarks(simpleClsName + "." + benchmark)
            .output(output + ".jmh.log")
            .profilers(GCProfiler.class)
            .jvmArguments(
                "-Xms4g",
                "-Xmx4g",
                "-XX:+UnlockCommercialFeatures",
                "-XX:+FlightRecorder",
                "-XX:StartFlightRecording=delay=30s,dumponexit=true,filename=" + output + ".jfr",
                JmhIdeBenchmarkRunner.createProperty(PROP_ENTRIES_PER_MESSAGE, entriesPerMessage),
                JmhIdeBenchmarkRunner.createProperty(PROP_REBALANCE, rebalance)
            )
            .run();
    }

}
