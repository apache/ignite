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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;


/**
 * Cache async listen benchmark.
 * Measures {@link IgniteFuture#listen(IgniteInClosure)} performance.
 *
 * Results on i7-9700K, Ubuntu 20.04.1, JDK 1.8.0_275:
 *
 * Without ForkJoinPool async continuation executor:
 * Benchmark                          Mode  Cnt      Score      Error  Units
 * JmhCacheAsyncListenBenchmark.get  thrpt   10  82052.664 ± 2891.182  ops/s
 * JmhCacheAsyncListenBenchmark.put  thrpt   10  77859.584 ± 2071.196  ops/s
 *
 * With ForkJoinPool async continuation executor:
 * Benchmark                          Mode  Cnt      Score      Error  Units
 * JmhCacheAsyncListenBenchmark.get  thrpt   10  76008.272 ± 1506.928  ops/s
 * JmhCacheAsyncListenBenchmark.put  thrpt   10  73393.986 ± 1336.420  ops/s
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class JmhCacheAsyncListenBenchmark extends JmhCacheAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        IgniteDataStreamer<Integer, IntValue> dataLdr = node.dataStreamer(cache.getName());

        for (int i = 0; i < CNT; i++)
            dataLdr.addData(i, new IntValue(i));

        dataLdr.close();

        System.out.println("Cache populated.");
    }

    /**
     * Test PUT operation.
     *
     * @throws Exception If failed.
     */
    @Benchmark
    public void put() throws Exception {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        blockingListen(cache.putAsync(key, new IntValue(key)));
    }

    /**
     * Test GET operation.
     *
     * @throws Exception If failed.
     */
    @Benchmark
    public void get() throws Exception {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        blockingListen(cache.getAsync(key));
    }

    /**
     * Blocks until future completion using {@link IgniteFuture#listen(IgniteInClosure)}.
     *
     * @param future Future
     */
    private static void blockingListen(IgniteFuture future) throws Exception {
        AtomicBoolean ab = new AtomicBoolean();

        future.listen(f -> {
            try {
                synchronized (ab) {
                    ab.set(true);
                    ab.notifyAll();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });

        synchronized (ab) {
            while (!ab.get()) {
                ab.wait(5000);
            }
        }
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run(CacheAtomicityMode.ATOMIC);
    }

    /**
     * Run benchmarks for atomic cache.
     *
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private static void run(CacheAtomicityMode atomicityMode) throws Exception {
        run(4, true, atomicityMode, CacheWriteSynchronizationMode.PRIMARY_SYNC);
    }

    /**
     * Run benchmark.
     *
     * @param threads Amount of threads.
     * @param client Client mode flag.
     * @param atomicityMode Atomicity mode.
     * @param writeSyncMode Write synchronization mode.
     * @throws Exception If failed.
     */
    private static void run(int threads, boolean client, CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode writeSyncMode) throws Exception {

        JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(threads)
                .benchmarks(JmhCacheAsyncListenBenchmark.class.getSimpleName())
                .jvmArguments(
                    "-Xms4g",
                    "-Xmx4g",
                    JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, atomicityMode),
                    JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, writeSyncMode),
                    JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 2),
                    JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client))
                .benchmarkModes(Mode.Throughput)
                .run();
    }
}
