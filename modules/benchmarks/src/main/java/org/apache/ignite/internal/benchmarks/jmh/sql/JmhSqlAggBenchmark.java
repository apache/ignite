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
 * Benchmark aggregate SQL queries.
 */
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
@State(Scope.Benchmark)
public class JmhSqlAggBenchmark extends JmhSqlAbstractBenchmark {
    /**
     * Query with group by and aggregate.
     */
    @Benchmark
    public void queryGroupBy() {
        List<?> res = executeSql("SELECT fldBatch, AVG(fld) FROM Item GROUP BY fldBatch");

        if (res.size() != KEYS_CNT / BATCH_SIZE)
            throw new AssertionError("Unexpected result size: " + res.size());
    }

    /**
     * Query with indexed field group by and aggregate.
     */
    @Benchmark
    public void queryGroupByIndexed() {
        List<?> res = executeSql("SELECT fldIdxBatch, AVG(fld) FROM Item GROUP BY fldIdxBatch");

        if (res.size() != KEYS_CNT / BATCH_SIZE)
            throw new AssertionError("Unexpected result size: " + res.size());
    }

    /**
     * Query sum of indexed field.
     */
    @Benchmark
    public void querySumIndexed() {
        List<List<?>> res = executeSql("SELECT sum(fldIdx) FROM Item");

        Long expRes = ((long)KEYS_CNT) * (KEYS_CNT - 1) / 2;

        if (!expRes.equals(res.get(0).get(0)))
            throw new AssertionError("Unexpected result: " + res.get(0));
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhSqlAggBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
