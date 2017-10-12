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
import org.openjdk.jmh.profile.GCProfiler;

/**
 * WAL modes benchmark.
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
    public void loadIndexed() throws Exception {
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

        int size = 2_000_000;

        for (int i = 0; i < size; i++) {
            if (i % 100_000 == 0)
                System.out.println("... " + i);

            dataLdr.addData(i, HeavyValue.generate());
        }

        dataLdr.close();

        dataLdr.future().get();

        if (c.size() != size)
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

        run("loadIndexed", 1, false, WALMode.DEFAULT);
        run("loadIndexed", 1, false, WALMode.LOG_ONLY);
        run("loadIndexed", 1, false, WALMode.BACKGROUND);
        run("loadIndexed", 1, false, WALMode.NONE);
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
                "-Xms10g",
                "-Xmx10g",
                "-XX:+UnlockCommercialFeatures",
                "-XX:+FlightRecorder",
                "-XX:StartFlightRecording=delay=30s,dumponexit=true,settings=alloc,filename=" + output + ".jfr",
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

            fields.put("ACCOUNTCODE", "java.lang.String");
            fields.put("ASSETTYPE", "java.lang.String");
            fields.put("ASSETUNIT", "java.lang.String");
            fields.put("ATLASFOLDERID", "java.lang.String");
            fields.put("ATLASINSTRUMENTSTRUCTUREPATH", "java.lang.String");
            fields.put("BOOKSOURCESYSTEM", "java.lang.String");
            fields.put("BOOKSOURCESYSTEMCODE", "java.lang.String");
            fields.put("BUSINESSDATE", "java.lang.String");
            fields.put("CUSIP", "java.lang.String");
            fields.put("DATASETFILTER", "java.lang.String");
            fields.put("DATASETLABEL", "java.lang.String");
            fields.put("ESMP", "java.lang.String");
            fields.put("FOAGGRCODE", "java.lang.String");
            fields.put("HOSTPRODID", "java.lang.String");
            fields.put("INSTRUMENTEXPIRYDATE", "java.lang.String");
            fields.put("INSTRUMENTMATURITYDATE", "java.lang.String");
            fields.put("INSTRUMENTTYPE", "java.lang.String");
            fields.put("ISIN", "java.lang.String");
            fields.put("PROXYINSTRUMENTID", "java.lang.String");
            fields.put("PROXYINSTRUMENTIDTYPE", "java.lang.String");
            fields.put("PROXYINSTRUMENTTYPE", "java.lang.String");
            fields.put("REGION", "java.lang.String");
            fields.put("RIC", "java.lang.String");
            fields.put("RISKFACTORNAME", "java.lang.String");
            fields.put("RISKPARENTINSTRUMENTID", "java.lang.String");
            fields.put("RISKPARENTINSTRUMENTIDTYPE", "java.lang.String");
            fields.put("RISKSOURCESYSTEM", "java.lang.String");
            fields.put("RISKSUBJECTCHORUSBOOKID", "java.lang.String");
            fields.put("RISKSUBJECTID", "java.lang.String");
            fields.put("RISKSUBJECTINSTRUMENTCOUNTERPARTYID", "java.lang.String");
            fields.put("RISKSUBJECTINSTRUMENTID", "java.lang.String");
            fields.put("RISKSUBJECTINSTRUMENTIDTYPE", "java.lang.String");
            fields.put("RISKSUBJECTSOURCE", "java.lang.String");
            fields.put("RISKSUBJECTTYPE", "java.lang.String");
            fields.put("SENSITIVITYTYPE", "java.lang.String");
            fields.put("SERIESDATE", "java.lang.String");
            fields.put("SERIESDAY", "java.lang.String");
            fields.put("SNAPVERSION", "java.lang.String");
            fields.put("SYS_AUDIT_TRACE", "java.lang.String");
            fields.put("UNDERLYINGSECURITYID", "java.lang.String");
            fields.put("UNDERLYINGSECURITYIDTYPE", "java.lang.String");
            fields.put("VALUATIONSOURCECONTEXTLABELNAME", "java.lang.String");
            fields.put("VARTYPE", "java.lang.String");
            fields.put("EODTOTALVALUE", "java.lang.Double");
            fields.put("QUANTITY", "java.lang.Double");
            fields.put("STRIKEVALUE", "java.lang.Double");
            fields.put("THEOPRICE", "java.lang.Double");
            fields.put("TOTALVALUE", "java.lang.Double");
            fields.put("VALUE", "java.lang.Double");

            entity.setFields(fields);

            QueryIndex businessdate = new QueryIndex("BUSINESSDATE");
            businessdate.setInlineSize(32);

            QueryIndex risksubjectid = new QueryIndex("RISKSUBJECTID");
            risksubjectid.setInlineSize(32);

            QueryIndex seriesdate = new QueryIndex("SERIESDATE");
            seriesdate.setInlineSize(32);

            QueryIndex snapversion = new QueryIndex("SNAPVERSION");
            snapversion.setInlineSize(32);

            QueryIndex vartype = new QueryIndex("VARTYPE");
            vartype.setInlineSize(32);

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
