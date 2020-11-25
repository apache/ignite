package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class JmhThinClientCacheBenchmark extends JmhThinClientAbstractBenchmark {
    /**
     * Cache put benchmark.
     */
    @Benchmark
    public void put() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        cache.put(key, PAYLOAD);
    }

    /**
     * Cache get benchmark.
     */
    @Benchmark
    public Object get() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        return cache.get(key);
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final String simpleClsName = JmhThinClientCacheBenchmark.class.getSimpleName();
        final int threads = 4;

        final String output = simpleClsName +
                "-" + threads + "-threads";

        final Options opt = new OptionsBuilder()
                .threads(threads)
                .include(simpleClsName)
                .output(output + ".jmh.log")
                .jvmArgs(
                        "-Xms1g",
                        "-Xmx1g",
                        "-XX:+UnlockCommercialFeatures",
                        JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 4)).build();

        new Runner(opt).run();
    }
}
