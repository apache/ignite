package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;

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
//        JmhThinClientCacheBenchmark b = new JmhThinClientCacheBenchmark();
//        b.setup();
//        b.put();
//        b.get();
//        b.tearDown();
        JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(4)
                .warmupIterations(30)
                .measurementIterations(30)
                .benchmarks(JmhThinClientCacheBenchmark.class.getSimpleName())
                .jvmArguments("-Xms4g", "-Xmx4g")
                .run();
    }
}
