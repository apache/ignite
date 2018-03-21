package org.apache.ignite.internal.benchmarks.jmh.preloader;

import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.profile.GCProfiler;

public class JmhPreloaderMessageHandlingAbstractBenchmark extends JmhPreloaderAbstractBenchmark {

    @Benchmark
    public void handleSupplyMessage() throws Exception {
        try {
            demanderPreloader.handleSupplyMessage(0, supplierNode.localNode().id(), generateSupplyMessage());
        }
        catch (Throwable ex) {
            ex.printStackTrace();

            throw ex;
        }
    }

    @Benchmark
    public void handleDemandMessage() throws Exception {
        try {
            supplierPreloader.handleDemandMessage(0, demanderNode.localNode().id(), generateDemandMessage());
        }
        catch (Throwable ex) {
            ex.printStackTrace();

            throw ex;
        }
    }

    public static void main(String[] args) throws Exception {
        run("handleSupplyMessage", 4, 10000);
        run("handleDemandMessage", 4, 10000);

        //        JmhCachePreloaderBenchmark benchmark = new JmhCachePreloaderBenchmark();
        //
        //        benchmark.setup();
        //
        //        benchmark.handleSupplyMessage();
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
    private static void run(String benchmark, int threads, int entriesPerMessage) throws Exception {
        String simpleClsName = JmhPreloaderMessageHandlingAbstractBenchmark.class.getSimpleName();

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
