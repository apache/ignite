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
import org.apache.ignite.IgniteDataStreamer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark DML queries.
 */
public class JmhSqlDmlBenchmark extends JmhSqlAbstractBenchmark {
    /**
     * Initiate new cache.
     */
    @Override public void setup() {
        super.setup();

        client.getOrCreateCache(cacheConfiguration().setName("CACHE2").setSqlSchema("CACHE2"));

        try (IgniteDataStreamer<Integer, Item> ds = client.dataStreamer("CACHE2")) {
            for (int i = 0; i < KEYS_CNT; i++)
                ds.addData(i, new Item(i));
        }
    }

    /**
     * INSERT/DELETE batched.
     */
    @Benchmark
    public void insertDeleteBatch() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<List<?>> res = executeSql("INSERT INTO CACHE2.Item (_key, name, fld, fldBatch, fldIdx, fldIdxBatch) " +
                "SELECT _key + ?, name, fld, fldBatch, fldIdx, fldIdxBatch + ? FROM CACHE.Item WHERE fldIdxBatch=?",
            KEYS_CNT, KEYS_CNT, key / BATCH_SIZE);

        checkUpdatedCount(res, BATCH_SIZE);

        res = executeSql("DELETE FROM CACHE2.Item WHERE fldIdxBatch=?",
            KEYS_CNT + key / BATCH_SIZE);

        checkUpdatedCount(res, BATCH_SIZE);
    }

    /**
     * INSERT/DELETE single key.
     */
    @Benchmark
    public void insertDeleteUniqueIndexed() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<List<?>> res = executeSql("INSERT INTO CACHE2.Item (_key, name, fld, fldBatch, fldIdx, fldIdxBatch) " +
                "VALUES (?, 'name', 0, 0, ?, 0)",
            KEYS_CNT + key, KEYS_CNT + key);

        checkUpdatedCount(res, 1);

        res = executeSql("DELETE FROM CACHE2.Item WHERE fldIdx=?", KEYS_CNT + key);

        checkUpdatedCount(res, 1);
    }

    /**
     * UPDATE batched.
     */
    @Benchmark
    public void updateBatch() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<List<?>> res = executeSql("UPDATE CACHE2.Item SET fld = fld + 1 WHERE fldIdxBatch=?", key / BATCH_SIZE);

        checkUpdatedCount(res, BATCH_SIZE);
    }

    /**
     * UPDATE single key.
     */
    @Benchmark
    public void updateUniqueIndexed() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<List<?>> res = executeSql("UPDATE CACHE2.Item SET fld = fld + 1 WHERE fldIdx=?", key);

        checkUpdatedCount(res, 1);
    }

    /** */
    private void checkUpdatedCount(List<List<?>> res, int expCnt) {
        if (res.size() != 1 || (Long)res.get(0).get(0) != expCnt) {
            throw new AssertionError("Unexpected " + (res.size() != 1 ? "result size: " + res.size()
                : "updated entries count: " + res.get(0).get(0)));
        }
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhSqlDmlBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
