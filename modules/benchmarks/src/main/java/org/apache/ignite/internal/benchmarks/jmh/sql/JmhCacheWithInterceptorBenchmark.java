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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

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
    /** Query engine. */
    @Param({"CALCITE"})
    protected String engine;

    /** Keep binary mode. */
    @Param({"true", "false"})
    protected boolean keepBinary;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, Item> cacheConfiguration() {
        return super.cacheConfiguration().setInterceptor(new CacheInterceptorAdapter<>());
    }

    /** Test update operation. */
    @Benchmark
    public void update() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        executeSql("UPDATE CACHE.Item SET fld = fld + 1 WHERE fldIdx=?", key);
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
}
