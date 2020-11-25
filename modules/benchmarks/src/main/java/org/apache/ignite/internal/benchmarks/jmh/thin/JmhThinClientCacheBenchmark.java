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
        // TODO: Benchmark hangs when GridNioServer.selectorCount > 1
        // This goes away with -DIGNITE_IO_BALANCE_PERIOD=0
        // TODO: File ticket for IGNITE_IO_BALANCE_PERIOD issue
        // TODO: Exception with multiple selectors - race condition?
//        class org.apache.ignite.binary.BinaryObjectException: Invalid flag value: 0
//        at org.apache.ignite.internal.binary.BinaryReaderExImpl.deserialize0(BinaryReaderExImpl.java:1964)
//        at org.apache.ignite.internal.binary.BinaryReaderExImpl.deserialize(BinaryReaderExImpl.java:1716)
//        at org.apache.ignite.internal.binary.GridBinaryMarshaller.deserialize(GridBinaryMarshaller.java:319)
//        at org.apache.ignite.internal.client.thin.ClientBinaryMarshaller.deserialize(ClientBinaryMarshaller.java:74)
//        at org.apache.ignite.internal.client.thin.ClientUtils.readObject(ClientUtils.java:579)
//        at org.apache.ignite.internal.client.thin.ClientUtils.readObject(ClientUtils.java:569)
//        at org.apache.ignite.internal.client.thin.TcpClientCache.readObject(TcpClientCache.java:783)
//        at org.apache.ignite.internal.client.thin.TcpClientCache.readObject(TcpClientCache.java:788)
//        at org.apache.ignite.internal.client.thin.TcpClientChannel.receive(TcpClientChannel.java:261)
//        at org.apache.ignite.internal.client.thin.TcpClientChannel.service(TcpClientChannel.java:187)
//        at org.apache.ignite.internal.client.thin.ReliableChannel.lambda$affinityService$5(ReliableChannel.java:321)
//        at org.apache.ignite.internal.client.thin.ReliableChannel.applyOnNodeChannelWithFallback(ReliableChannel.java:820)
//        at org.apache.ignite.internal.client.thin.ReliableChannel.affinityService(ReliableChannel.java:320)
//        at org.apache.ignite.internal.client.thin.TcpClientCache.cacheSingleKeyOperation(TcpClientCache.java:711)
//        at org.apache.ignite.internal.client.thin.TcpClientCache.get(TcpClientCache.java:111)
//        at org.apache.ignite.internal.benchmarks.jmh.thin.JmhThinClientCacheBenchmark.get(JmhThinClientCacheBenchmark.java:26)
//
//
//        JmhThinClientCacheBenchmark b = new JmhThinClientCacheBenchmark();
//        b.setup();
//        b.put();
//        b.get();
//
//        ArrayList<ForkJoinTask> tasks = new ArrayList<>();
//        for (int i =0; i < 4; i++) {
//            int finalI = i;
//            ForkJoinTask<?> task = ForkJoinPool.commonPool().submit(() -> {
//                for (int j = 0; j < 1000; j++) {
//                    System.out.println(">> " + finalI + " - " + j);
//                    b.get();
//                }
//            });
//
//            tasks.add(task);
//        }
//
//        for (ForkJoinTask t: tasks) {
//            t.join();
//            System.out.println("JOINED");
//        }
//
//        b.tearDown();

        JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(4)
                .warmupIterations(5)
                .measurementIterations(10)
                .benchmarks(JmhThinClientCacheBenchmark.class.getSimpleName())
                .jvmArguments("-Xms4g", "-Xmx4g")
                .run();
    }
}
