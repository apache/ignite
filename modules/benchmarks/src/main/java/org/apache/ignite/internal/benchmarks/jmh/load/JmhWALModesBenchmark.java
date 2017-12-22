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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.HeavyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;

/**
 * WAL modes benchmark.
 */
@SuppressWarnings("unchecked")
public class JmhWALModesBenchmark extends JmhCacheAbstractBenchmark {
    /** Size. */
    private static final int SIZE = 2_000_000;

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
    public void loadIndexed() throws Exception {
        IgniteCache c = node.getOrCreateCache(cacheCfg(true));

        load(c, false);
    }

    /**
     */
    @Benchmark
    public void loadAsIs() throws Exception {
        IgniteCache c = node.getOrCreateCache(cacheCfg(false));

        load(c, false);
    }

    /**
     */
    @Benchmark
    public void loadDisabledWal() throws Exception {
        IgniteCache c = node.getOrCreateCache(cacheCfg(false));

        load(c, true);
    }

    /**
     * @param c Closure.
     */
    private void load(IgniteCache c, boolean disableWal) {
        if (c.size() != 0)
            throw new RuntimeException("Cache is not empty!");

        System.out.println("Loading ... " + new Date().toString());

        IgniteDataStreamer<Integer, HeavyValue> dataLdr = node.dataStreamer(c.getName());

        if (disableWal)
            node.cluster().disableWal(c.getName());

        for (int i = 0; i < SIZE; i++) {
            if (i % 100_000 == 0)
                System.out.println("... " + i);

            dataLdr.addData(i, HeavyValue.generate());
        }

        dataLdr.close();

        dataLdr.future().get();

        if (c.size() != SIZE)
            throw new RuntimeException("Loading failed.");

        if(disableWal)
            node.cluster().enableWal(c.getName());

        System.out.println("Loaded ... " + new Date().toString());
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run("loadDisabledWal", 1, false, WALMode.DEFAULT);
        run("loadDisabledWal", 1, false, WALMode.NONE);
        run("loadAsIs", 1, false, WALMode.DEFAULT);
        run("loadAsIs", 1, false, WALMode.NONE);
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
            .measurementIterations(1) // Single shot
            .benchmarks(simpleClsName + "." + benchmark)
            .output(output + ".jmh.log")
            .benchmarkModes(Mode.SingleShotTime)
            .jvmArguments(
                "-Xms10g",
                "-Xmx10g",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, CacheAtomicityMode.ATOMIC),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, CacheWriteSynchronizationMode.FULL_SYNC),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 1),
                JmhIdeBenchmarkRunner.createProperty(PROP_WAL_MODE, walMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client))
            .run();
    }

    /**
     * @param indexed Indexed.
     */
    private static CacheConfiguration<Integer, HeavyValue> cacheCfg(boolean indexed) {
        CacheConfiguration cacheCfg = new CacheConfiguration<Object, HeavyValue>("wal-cache");

        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setBackups(0);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        if (indexed) {
            System.out.println("Indexes turned on.");

            QueryEntity entity = new QueryEntity(Integer.class.getName(), HeavyValue.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            for (int i = 1; i < 50; i++)
                fields.put("field" + i, i < 44 ? "java.lang.String" : "java.lang.Double");

            entity.setFields(fields);

            QueryIndex idx1 = new QueryIndex("field1");
            QueryIndex idx2 = new QueryIndex("field2");
            QueryIndex idx3 = new QueryIndex("field3");
            QueryIndex idx4 = new QueryIndex("field4");
            QueryIndex idx5 = new QueryIndex("field5");

            entity.setIndexes(Arrays.asList(
                idx1,
                idx2,
                idx3,
                idx4,
                idx5
            ));

            cacheCfg.setQueryEntities(Arrays.asList(entity));
        }

        return cacheCfg;
    }
}
