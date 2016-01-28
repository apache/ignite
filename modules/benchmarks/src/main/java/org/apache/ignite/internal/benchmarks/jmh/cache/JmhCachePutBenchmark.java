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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Put benchmark.
 */
@SuppressWarnings("unchecked")
public class JmhCachePutBenchmark extends JmhCacheAbstractBenchmark {
    /** Items count. */
    private static final int CNT = 100000;

    /**
     * Set up routine.
     *
     * @throws Exception If failed.
     */

    public void setup() throws Exception {
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
    @Threads(4)
    public void testPut() throws Exception {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        cache.put(key, new IntValue(key));
    }

    /**
     * Runner.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        runAtomic(CacheAtomicityMode.ATOMIC);
    }

    /**
     * Run benchmarks for atomic cache.
     *
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private static void runAtomic(CacheAtomicityMode atomicityMode) throws Exception {
        run(true, atomicityMode, CacheWriteSynchronizationMode.PRIMARY_SYNC);
        run(true, atomicityMode, CacheWriteSynchronizationMode.FULL_SYNC);
        run(false, atomicityMode, CacheWriteSynchronizationMode.PRIMARY_SYNC);
        run(false, atomicityMode, CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /**
     * Run benchmark.
     *
     * @param client Client mode flag.
     * @param writeSyncMode Write synchronization mode.
     * @throws Exception If failed.
     */
    private static void run(boolean client, CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode writeSyncMode) throws Exception {
        String output = "ignite-cache-put-" + (client ? "client" : "data") + "-" + atomicityMode + "-" + writeSyncMode;

        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .warmupIterations(10)
            .measurementIterations(60)
            .classes(JmhCachePutBenchmark.class)
            .output(output + ".jmh.log")
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
