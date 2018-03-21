package org.apache.ignite.internal.benchmarks.jmh.preloader;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.profile.GCProfiler;

public class JmhPreloaderCacheBenchmark extends JmhPreloaderAbstractBenchmark {

    protected final AtomicBoolean stop = new AtomicBoolean();
    protected Thread supplierThread;
    protected Thread demanderThread;

    @Override public void setup() throws Exception {
        super.setup();

        stop.set(false);

//        supplierThread = new Thread(new Runnable() {
//            @Override public void run() {
//                while (!stop.get()) {
//                    try {
//                        demanderPreloader.handleSupplyMessage(0, supplierNode.localNode().id(), generateSupplyMessage());
//                    }
//                    catch (Exception ex) {
//                        ex.printStackTrace();
//
//                        throw new RuntimeException(ex);
//                    }
//                }
//            }
//        }, "demander-loader");
//
//        demanderThread = new Thread(new Runnable() {
//            @Override public void run() {
//                while (!stop.get()) {
//                    try {
//                        supplierPreloader.handleDemandMessage(0, supplierNode.localNode().id(), generateDemandMessage());
//                    }
//                    catch (Exception ex) {
//                        ex.printStackTrace();
//
//                        throw new RuntimeException(ex);
//                    }
//                }
//            }
//        }, "supplier-loader");
    }

    @Override public void tearDown() throws Exception {
        stop.set(true);

//        supplierThread.join();
//        demanderThread.join();

        super.tearDown();
    }

    @Benchmark
    public void supplierPut() {
        int k = ThreadLocalRandom.current().nextInt();

        supplierCache.put(k, k);
    }

    @Benchmark
    public void demanderPut() {
        int k = ThreadLocalRandom.current().nextInt();

        demanderCache.put(k, k);
    }

    public static void main(String[] args) throws Exception {
        run("demanderPut", 1, 10000);
        run("supplierPut", 1, 10000);
    }

    /**
     * Run benchmark.
     *
     * @param benchmark Benchmark to run.
     * @param threads Amount of threads.
     * @throws Exception If failed.
     */
    private static void run(String benchmark, int threads, int entriesPerMessage) throws Exception {
        String simpleClsName = JmhPreloaderCacheBenchmark.class.getSimpleName();

        String output = simpleClsName + "-" + benchmark +
            "-" + threads + "-threads" +
            "-" + entriesPerMessage + "-epm";

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
                JmhIdeBenchmarkRunner.createProperty(PROP_ENTRIES_PER_MESSAGE, entriesPerMessage)
            )
            .run();
    }

}
