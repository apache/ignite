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

package org.apache.ignite.internal.benchmarks.jmh.load;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.profile.GCProfiler;

/**
 * Put benchmark.
 */
@SuppressWarnings("unchecked")
public class JmhWALModesBenchmark extends JmhCacheAbstractBenchmark {
    /**
     * Set up routine.
     *
     * @throws Exception If failed.
     */
    public void setup() throws Exception {
        super.setup();
    }

    /**
     */
    @Benchmark
    public void load() throws Exception {
        System.out.println("Loading ... ");

        IntValue iv = new IntValue(0);

        IgniteDataStreamer<Integer, IntValue> dataLdr = node.dataStreamer(cache.getName());

        for (int i = 0; i < 50_000_000; i++)
            dataLdr.addData(i, iv);

        dataLdr.close();

        dataLdr.future().get();
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run("load", 1, true, WALMode.DEFAULT);
        run("load", 1, true, WALMode.LOG_ONLY);
        run("load", 1, true, WALMode.BACKGROUND);
        run("load", 1, true, WALMode.NONE);
    }

    /**
     * Run benchmark.
     *
     * @param benchmark Benchmark to run.
     * @param threads Amount of threads.
     * @param client Client mode flag.
     * @throws Exception If failed.
     */
    private static void run(
        String benchmark,
        int threads,
        boolean client,
        WALMode walMode) throws Exception {
        String simpleClsName = JmhWALModesBenchmark.class.getSimpleName();

        String output = simpleClsName + "-" + benchmark +
            "-" + threads + "-threads" +
            "-" + (client ? "client" : "data") +
            "-" + walMode;

        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(threads)
            .warmupIterations(0) // No warmup
            .measurementIterations(1) // Only one iteration
            .benchmarks(simpleClsName + "." + benchmark)
            .output(output + ".jmh.log")
            .profilers(GCProfiler.class)
            .jvmArguments(
                "-Xms7g",
                "-Xmx7g",
                "-XX:+UnlockCommercialFeatures",
                "-XX:+FlightRecorder",
                "-XX:StartFlightRecording=delay=30s,dumponexit=true,settings=alloc,filename=" + output + ".jfr",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, CacheAtomicityMode.ATOMIC),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, CacheWriteSynchronizationMode.FULL_SYNC),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 2),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client),
                JmhIdeBenchmarkRunner.createProperty(PROP_WAL_MODE, walMode))
            .run();
    }
}
