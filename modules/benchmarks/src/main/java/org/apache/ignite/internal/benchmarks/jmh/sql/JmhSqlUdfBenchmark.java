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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark user defined functions in SQL queries.
 */
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
@State(Scope.Benchmark)
public class JmhSqlUdfBenchmark extends JmhSqlAbstractBenchmark {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, Item> cacheConfiguration() {
        return super.cacheConfiguration().setSqlFunctionClasses(FunctionsLibrary.class);
    }

    /** */
    public static class FunctionsLibrary {
        /** */
        @QuerySqlFunction
        public static int mul(int a, int b) {
            return a * b;
        }
    }

    /**
     * Query with user defined functions executed on initiator node.
     */
    @Benchmark
    public void queryFunctionsLocal() {
        List<?> res = executeSql("SELECT mul(mul(x, x), mul(x, x)) FROM system_range(1, ?)", KEYS_CNT);

        if (res.size() != KEYS_CNT)
            throw new AssertionError("Unexpected result size: " + res.size());
    }

    /**
     * Query with user defined functions executed on remote nodes.
     */
    @Benchmark
    public void queryFunctionsRemote() {
        List<?> res = executeSql("SELECT mul(mul(fld, fldIdx), mul(fldBatch, fldIdxBatch)) FROM Item");

        if (res.size() != KEYS_CNT)
            throw new AssertionError("Unexpected result size: " + res.size());
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhSqlUdfBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
