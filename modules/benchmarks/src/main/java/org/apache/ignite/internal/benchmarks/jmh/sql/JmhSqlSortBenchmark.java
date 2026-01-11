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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark sort SQL queries.
 */
public class JmhSqlSortBenchmark extends JmhSqlAbstractBenchmark {
    /**
     * Query with sorting (full set).
     */
    @Benchmark
    public void queryOrderByFull() {
        List<?> res = executeSql("SELECT name, fld FROM Item ORDER BY fld DESC");

        if (res.size() != KEYS_CNT)
            throw new AssertionError("Unexpected result size: " + res.size());
    }

    /**
     * Query with sorting (batch).
     */
    @Benchmark
    public void queryOrderByBatch() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<?> res = executeSql("SELECT name, fld FROM Item WHERE fldIdxBatch=? ORDER BY fld DESC", key / BATCH_SIZE);

        if (res.size() != BATCH_SIZE)
            throw new AssertionError("Unexpected result size: " + res.size());
    }

    /**
     * Query with sorting (with limit).
     */
    @Benchmark
    public void queryOrderByWithLimit() {
        List<?> res = executeSql("SELECT name, fld FROM Item ORDER BY fld DESC LIMIT " + BATCH_SIZE);

        if (res.size() != BATCH_SIZE)
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
            .include(JmhSqlSortBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
