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
 * Benchmark correlated SQL queries.
 */
public class JmhSqlCorrelateBenchmark extends JmhSqlAbstractBenchmark {
    /**
     * Query with correlated subquery.
     */
    @Benchmark
    public void queryCorrelated() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<?> res = executeSql(
            "SELECT fld FROM Item i0 WHERE fldIdxBatch=? AND EXISTS " +
                "(SELECT 1 FROM Item i1 WHERE i0.fld = i1.fld + ? AND i0.fldBatch = i1.fldBatch)",
            key / BATCH_SIZE, BATCH_SIZE / 2);

        // Skip result check for H2 engine, because query can't be executed correctly on H2.
        if (!"H2".equals(engine) && res.size() != BATCH_SIZE / 2)
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
            .include(JmhSqlCorrelateBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
