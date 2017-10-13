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
    public void indexedLoad() throws Exception {
        IgniteCache c = node.getOrCreateCache(cacheCfg(true));

        load(c);
    }

    /**
     */
    @Benchmark
    public void load() throws Exception {
        IgniteCache c = node.getOrCreateCache(cacheCfg(false));

        load(c);
    }

    /**
     * @param c Closure.
     */
    private void load(IgniteCache c) {
        if (c.size() != 0)
            throw new RuntimeException("Cache is not empty!");

        System.out.println("Loading ... " + new Date().toString());

        IgniteDataStreamer<Integer, HeavyValue> dataLdr = node.dataStreamer(c.getName());

        for (int i = 0; i < SIZE; i++) {
            if (i % 100_000 == 0)
                System.out.println("... " + i);

            dataLdr.addData(i, HeavyValue.generate());
        }

        dataLdr.close();

        dataLdr.future().get();

        if (c.size() != SIZE)
            throw new RuntimeException("Loading failed.");

        System.out.println("Loaded ... " + new Date().toString());
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run("load", 1, false, WALMode.DEFAULT);
        run("load", 1, false, WALMode.LOG_ONLY);
        run("load", 1, false, WALMode.BACKGROUND);
        run("load", 1, false, WALMode.NONE);

        run("indexedLoad", 1, false, WALMode.DEFAULT);
        run("indexedLoad", 1, false, WALMode.LOG_ONLY);
        run("indexedLoad", 1, false, WALMode.BACKGROUND);
        run("indexedLoad", 1, false, WALMode.NONE);
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

            fields.put("field1", "java.lang.String");
            fields.put("field2", "java.lang.String");
            fields.put("field3", "java.lang.String");
            fields.put("field4", "java.lang.String");
            fields.put("field5", "java.lang.String");
            fields.put("field6", "java.lang.String");
            fields.put("field7", "java.lang.String");
            fields.put("field8", "java.lang.String");
            fields.put("field9", "java.lang.String");
            fields.put("field10", "java.lang.String");
            fields.put("field11", "java.lang.String");
            fields.put("field12", "java.lang.String");
            fields.put("field13", "java.lang.String");
            fields.put("field14", "java.lang.String");
            fields.put("field15", "java.lang.String");
            fields.put("field16", "java.lang.String");
            fields.put("field17", "java.lang.String");
            fields.put("field18", "java.lang.String");
            fields.put("field19", "java.lang.String");
            fields.put("field20", "java.lang.String");
            fields.put("field21", "java.lang.String");
            fields.put("field22", "java.lang.String");
            fields.put("field23", "java.lang.String");
            fields.put("field24", "java.lang.String");
            fields.put("field25", "java.lang.String");
            fields.put("field26", "java.lang.String");
            fields.put("field27", "java.lang.String");
            fields.put("field28", "java.lang.String");
            fields.put("field29", "java.lang.String");
            fields.put("field30", "java.lang.String");
            fields.put("field31", "java.lang.String");
            fields.put("field32", "java.lang.String");
            fields.put("field33", "java.lang.String");
            fields.put("field34", "java.lang.String");
            fields.put("field35", "java.lang.String");
            fields.put("field36", "java.lang.String");
            fields.put("field37", "java.lang.String");
            fields.put("field38", "java.lang.String");
            fields.put("field39", "java.lang.String");
            fields.put("field40", "java.lang.String");
            fields.put("field41", "java.lang.String");
            fields.put("field42", "java.lang.String");
            fields.put("field43", "java.lang.String");
            fields.put("field44", "java.lang.Double");
            fields.put("field45", "java.lang.Double");
            fields.put("field46", "java.lang.Double");
            fields.put("field47", "java.lang.Double");
            fields.put("field48", "java.lang.Double");
            fields.put("field49", "java.lang.Double");

            entity.setFields(fields);

            QueryIndex businessdate = new QueryIndex("field1");
            QueryIndex risksubjectid = new QueryIndex("field2");
            QueryIndex seriesdate = new QueryIndex("field3");
            QueryIndex snapversion = new QueryIndex("field4");
            QueryIndex vartype = new QueryIndex("field5");

            entity.setIndexes(Arrays.asList(
                businessdate,
                risksubjectid,
                seriesdate,
                snapversion,
                vartype
            ));

            cacheCfg.setQueryEntities(Arrays.asList(entity));
        }

        return cacheCfg;
    }
}
