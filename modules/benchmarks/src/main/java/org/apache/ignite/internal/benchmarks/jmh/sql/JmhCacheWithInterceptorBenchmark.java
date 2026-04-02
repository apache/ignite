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

package org.apache.ignite.internal.benchmarks.jmh.sql;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Benchmark cache with interceptor queries.
 */
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 10, time = 5)
@State(Scope.Benchmark)
public class JmhCacheWithInterceptorBenchmark extends JmhSqlAbstractBenchmark {
    /** Keep binary mode. */
    @Param({"true", "false"})
    protected boolean keepBinary;

    /** Target cache. */
    protected IgniteCache cache;

    /** Items count. */
    protected static final int CNT = 100;

    /**
     * Create Ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Configuration.
     */
    @Override protected IgniteConfiguration configuration(String igniteInstanceName) {
        IgniteConfiguration cfg = super.configuration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration()
        ));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        var cityCfg = new CacheConfiguration<Integer, Object>("city")
            .setAtomicityMode(TRANSACTIONAL)
            .setSqlSchema("PUBLIC")
            .setInterceptor(new CacheInterceptorAdapter<>())
            .setQueryEntities(List.of(new QueryEntity(Integer.class, City.class)
                .setTableName("CITY")
                .addQueryField("ID", Integer.class.getName(), null)
                .setKeyFieldName("ID")
            ));

        cache = client.getOrCreateCache(cityCfg);

        if (keepBinary)
            cache = cache.withKeepBinary();

        for (int i = 0; i < CNT; ++i)
            executeSql("INSERT INTO PUBLIC.CITY(id, name) VALUES (?, '')", i);
    }

    /** Test update operation. */
    @Benchmark
    public void update(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(CNT);
        int valSuffix = ThreadLocalRandom.current().nextInt();
        List<?> res = executeSql("UPDATE CITY SET name = ? WHERE ID = ?", "val" + valSuffix, key);

        bh.consume(res);
    }

    /** */
    @Override protected List<List<?>> executeSql(String sql, Object... args) {
        return cache.query(new SqlFieldsQuery(sql).setArgs(args)).getAll();
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhCacheWithInterceptorBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }

    /** */
    private static class City {
        /** */
        @QuerySqlField
        String name;

        /** */
        City(String name) {
            this.name = name;
        }
    }
}
