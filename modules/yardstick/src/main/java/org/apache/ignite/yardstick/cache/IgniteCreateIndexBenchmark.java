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

package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.cache.model.Person8NotIndexed;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Benchmark for a dynamic creating indexes time measurement. In <code>setUp()</code> method cache populates with
 * unindexed {@link Person8NotIndexed} items. Number of preloaded items is taken from the <code>--preloadAmount</code>
 * argument value. <code>test()</code> method runs only once and only by the one thread (other threads are ignored). It
 * creates dynamic index over the cache with SQL statement and measures time of it's creation. Results will be available
 * in logs after the benchmark's end.
 */
public class IgniteCreateIndexBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Cache name. */
    private static final String CACHE_NAME = "Person8NotIndexed";

    /** Cache configuration */
    private CacheConfiguration<Integer, Object> personCacheCfg = getCfg();

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {

        BenchmarkUtils.println("IgniteCreateIndexBenchmark started creating index over the cache [size=" +
            cache().size() + ']');

        SqlFieldsQuery qry = new SqlFieldsQuery("CREATE INDEX idx_person8 ON " + CACHE_NAME + " (val2)");

        final long start = System.currentTimeMillis();

        cache().query(qry).getAll();

        final long stop = System.currentTimeMillis();

        BenchmarkUtils.println("IgniteCreateIndexBenchmark ==========================================");
        BenchmarkUtils.println("IgniteCreateIndexBenchmark created index in " + (stop - start) + " ms");
        BenchmarkUtils.println("IgniteCreateIndexBenchmark ==========================================");

        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().getOrCreateCache(personCacheCfg);
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        final int quantity = args.preloadAmount();

        if (quantity < 1)
            throw new IllegalArgumentException("Invalid number of entries: " + quantity);

        BenchmarkUtils.println("Preload entries [quantity=" + quantity + ']');

        final int testThreads = cfg.threads();

         if (testThreads != 1)
            throw new IllegalArgumentException("Invalid number of threads. IgniteCreateIndexBenchmark should be run" +
                " with a single thread. [threads=" + testThreads + ']');

        final int threads = Runtime.getRuntime().availableProcessors();

        BenchmarkUtils.println("Preloader thread pool size [threads=" + threads + ']');
        BenchmarkUtils.println("Local node: " + ignite().cluster().localNode());
        BenchmarkUtils.println("Cluster nodes: " + ignite().cluster().nodes());

        try (IgniteDataStreamer<Integer, Person8NotIndexed> streamer = ignite().dataStreamer(CACHE_NAME)) {
            ExecutorService executor = Executors.newFixedThreadPool(threads);

            Collection<Future<Void>> futs = new ArrayList<>();

            final LongAdder cntr = new LongAdder();

            final long start = System.currentTimeMillis();

            final int batchSize = 1000;

            final int[] ranges = getRanges(quantity, threads, batchSize);

            for (int i = 0; i < threads; i++) {
                final int finalI = i;

                futs.add(executor.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        if (ranges[finalI] < ranges[finalI + 1]) {
                            Map<Integer, Person8NotIndexed> batch = new HashMap<>();

                            for (int val = ranges[finalI]; val < ranges[finalI + 1]; val++) {
                                Person8NotIndexed person = createFromValue(val);

                                batch.put(val, person);

                                cntr.increment();
                            }

                            streamer.addData(batch);
                        }

                        return null;
                    }
                }));
            }

            for (Future<Void> fut : futs) {
                try {
                    fut.get();
                }
                catch (Exception e) {
                    BenchmarkUtils.println("Exception on node [id=" + ignite().cluster().localNode().id() +
                        ", exc=" + e + ']');
                }
            }

            streamer.flush();

            final long stop = System.currentTimeMillis();

            BenchmarkUtils.println("Node [id=" + ignite().cluster().localNode().id() +
                "] populated cache with [cntr=" + cntr.sum() + "]  entries in " + (stop - start) + "ms");
        }

        BenchmarkUtils.println("Total cache size after preloading [size=" + cache().size() + ']');
    }

    /**
     * Splits given integer range into approximately equal parts.
     *
     * @param range given range (from zero).
     * @param parts parts number on which range has to be split to.
     * @param batchSize minimum batch size for range parts.
     * @return array of range intervals.
     */
    private static int[] getRanges(int range, int parts, int batchSize) {
        assert range > 0 : "range > 0 ";
        assert parts > 0 : "parts > 0 ";
        assert batchSize > 0 : "batchSize > 0 ";

        int[] ranges = new int[parts + 1];

        ranges[0] = 0;

        int rangesSize = Math.max(range / parts, batchSize);

        for (int i = 1; i <= parts; i++) {
            int nextVal = ranges[i - 1] + rangesSize;

            ranges[i] = nextVal < range ? nextVal : range;
        }

        ranges[parts] = range;

        return ranges;
    }

    /**
     * Creates benchmark configuration
     *
     * @return benchmark configuration
     */
    // TODO: getConfiguration
    private CacheConfiguration<Integer, Object> getCfg() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person8NotIndexed.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("val1", Integer.class.getName());
        fields.put("val2", Integer.class.getName());
        fields.put("val3", Integer.class.getName());
        fields.put("val4", Integer.class.getName());
        fields.put("val5", Integer.class.getName());
        fields.put("val6", Integer.class.getName());
        fields.put("val7", Integer.class.getName());
        fields.put("val8", Integer.class.getName());

        entity.setFields(fields);
        entity.setKeyFieldName("val1");

        personCacheCfg = new CacheConfiguration<>(CACHE_NAME);

        personCacheCfg.setSqlSchema("PUBLIC").setQueryEntities(Collections.singleton(entity));

        return personCacheCfg;
    }

    /**
     * Returns initialized Person8 with duplicated val2-val8 fields.
     *
     * @param val primary key
     * @return initialized Person8
     */
    private static Person8NotIndexed createFromValue(int val) {
        Person8NotIndexed person = new Person8NotIndexed();

        person.setVal1(val);
        person.setVal2(val % 5);
        person.setVal3(val % 11);
        person.setVal4(val % 51);
        person.setVal5(val % 101);
        person.setVal6(val % 503);
        person.setVal7(val % 1001);
        person.setVal8(val % 5003);

        return person;
    }
}
