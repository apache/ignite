package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Thin client cache benchmark.
 *
 * Benchmark                         Mode  Cnt      Score      Error  Units
 * JmhThinClientCacheBenchmark.get  thrpt   10  92501.557 ± 1380.384  ops/s
 * JmhThinClientCacheBenchmark.put  thrpt   10  82907.446 ± 7572.537  ops/s
 */
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
        JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(4)
                .benchmarks(JmhThinClientCacheBenchmark.class.getSimpleName())
                .jvmArguments("-Xms4g", "-Xmx4g")
                .run();
    }
}
