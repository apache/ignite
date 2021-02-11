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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.profile.GCProfiler;

/**
 * Put benchmark.
 */
@SuppressWarnings("unchecked")
public class JmhCacheBenchmark extends JmhCacheAbstractBenchmark {
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

        cache.put(key, new IntValue(key));
    }

    /**
     * Test PUT operation.
     *
     * @throws Exception If failed.
     */
    @Benchmark
    public Object get() throws Exception {
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
        run("put", CacheAtomicityMode.ATOMIC);
        run("get", CacheAtomicityMode.ATOMIC);
        run("put", CacheAtomicityMode.TRANSACTIONAL);
        run("get", CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * Run benchmarks for atomic cache.
     *
     * @param benchmark Benchmark name.
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private static void run(String benchmark, CacheAtomicityMode atomicityMode) throws Exception {
        run(benchmark, 4, true, atomicityMode, CacheWriteSynchronizationMode.PRIMARY_SYNC);
        run(benchmark, 4, true, atomicityMode, CacheWriteSynchronizationMode.FULL_SYNC);
        run(benchmark, 4, false, atomicityMode, CacheWriteSynchronizationMode.PRIMARY_SYNC);
        run(benchmark, 4, false, atomicityMode, CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /**
     * Run benchmark.
     *
     * @param benchmark Benchmark to run.
     * @param threads Amount of threads.
     * @param client Client mode flag.
     * @param atomicityMode Atomicity mode.
     * @param writeSyncMode Write synchronization mode.
     * @throws Exception If failed.
     */
    private static void run(String benchmark, int threads, boolean client, CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode writeSyncMode) throws Exception {
        String simpleClsName = JmhCacheBenchmark.class.getSimpleName();

        String output = simpleClsName + "-" + benchmark +
            "-" + threads + "-threads" +
            "-" + (client ? "client" : "data") +
            "-" + atomicityMode +
            "-" + writeSyncMode;

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
                "-XX:StartFlightRecording=delay=30s,dumponexit=true,settings=alloc,filename=" + output + ".jfr",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, atomicityMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, writeSyncMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 2),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client))
            .run();
    }
}
