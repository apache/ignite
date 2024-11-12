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

package org.apache.ignite.internal.benchmarks.jmh.tree;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexPlainRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Index find benchmark.
 */
@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 10)
public class IndexFindBenchmark {
    /** Items count. */
    private static final int CNT = 1_000_000;

    /** Items in each range. */
    private static final int RANGE = 1;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Ignite. */
    private IgniteEx ignite;

    /** */
    InlineIndex idxId;

    /** */
    InlineIndex idxName;

    /** */
    InlineIndex idxSalary;

    /** */
    @Benchmark
    public void findOneIndex() {
        int key = ThreadLocalRandom.current().nextInt(CNT - RANGE);

        find(idxSalary, searchRowSalary(key), searchRowSalary(key + RANGE));
    }

    /** */
    @Benchmark
    public void findThreeIndexes() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        find(idxId, searchRowId(key), searchRowId(key + RANGE));
        find(idxName, searchRowName(key), searchRowName(key + RANGE));
        find(idxSalary, searchRowSalary(key), searchRowSalary(key + RANGE));
    }

    /** */
    private static void find(InlineIndex idx, IndexRow lower, IndexRow upper) {
        try {
            GridCursor<IndexRow> cur = idx.find(lower, upper, true, false, 0, null);

            int cnt = 0;

            while (cur.next())
                cnt++;

            assert cnt == RANGE;
        }
        catch (IgniteCheckedException e) {
            throw new AssertionError(e);
        }
    }

    /** */
    private static IndexRow searchRow(Object key, IndexKeyType type) {
        IndexKey[] keys = new IndexKey[] { IndexKeyFactory.wrap(key, type, null, null), null };
        return new IndexPlainRowImpl(keys, null);
    }

    /** */
    private static IndexRow searchRowId(int key) {
        return searchRow(key, IndexKeyType.INT);
    }

    /** */
    private static IndexRow searchRowName(int key) {
        return searchRow("name" + String.format("%07d", key), IndexKeyType.STRING);
    }

    /** */
    private static IndexRow searchRowSalary(int key) {
        return searchRow(key * 1_000d, IndexKeyType.DOUBLE);
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() {
        ignite = (IgniteEx)Ignition.start(new IgniteConfiguration().setIgniteInstanceName("test"));

        CacheConfiguration<Integer, Person> cfg = new CacheConfiguration<>(CACHE_NAME);
        cfg.setIndexedTypes(Integer.class, Person.class);

        ignite.getOrCreateCache(cfg);

        try (IgniteDataStreamer<Integer, Person> dataLdr = ignite.dataStreamer(CACHE_NAME)) {
            for (int i = 0; i < CNT; i++)
                dataLdr.addData(i, new Person(i, "name" + String.format("%07d", i), i * 1_000d));
        }

        for (InlineIndex idx : ignite.context().indexProcessor().treeIndexes(CACHE_NAME, true)) {
            if (idx.name().contains("_ID_"))
                idxId = idx;
            else if (idx.name().contains("_NAME_"))
                idxName = idx;
            else if (idx.name().contains("_SALARY_"))
                idxSalary = idx;
        }
    }

    /**
     * Stop Ignite instance.
     */
    @TearDown
    public void tearDown() {
        ignite.close();
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(IndexFindBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        private final int id;

        /** */
        @QuerySqlField(index = true)
        private final String name;

        /** */
        @QuerySqlField(index = true)
        private final double salary;

        /** */
        private Person(int id, String name, double salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }
    }
}
