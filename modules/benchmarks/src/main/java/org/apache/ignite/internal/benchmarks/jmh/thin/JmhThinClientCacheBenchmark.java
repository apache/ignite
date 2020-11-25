package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.IgniteSystemProperties;
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
        // TODO: Benchmark hangs when GridNioServer.selectorCount > 1
        System.setProperty(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, "0");

        JmhThinClientCacheBenchmark b = new JmhThinClientCacheBenchmark();
        b.setup();
        b.put();
        b.get();

        ArrayList<ForkJoinTask> tasks = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            int threadIdx = i;
            ForkJoinTask<?> task = ForkJoinPool.commonPool().submit(() -> {
                for (int j = 0; j < 1000; j++) {
                    System.out.println(">> " + threadIdx + " - " + j);
                    b.get();
                }
            });

            tasks.add(task);
        }

        for (ForkJoinTask t : tasks) {
            t.join();
            System.out.println("JOINED");
        }

        b.tearDown();

//        JmhIdeBenchmarkRunner.create()
//                .forks(1)
//                .threads(4)
//                .warmupIterations(5)
//                .measurementIterations(10)
//                .benchmarks(JmhThinClientCacheBenchmark.class.getSimpleName())
//                .jvmArguments("-Xms4g", "-Xmx4g")
//                .run();
    }
}
