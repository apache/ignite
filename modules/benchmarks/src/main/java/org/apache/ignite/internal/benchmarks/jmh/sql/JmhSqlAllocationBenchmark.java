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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAllocationAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.model.SqlValueType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

/** SQL queries and DML on one table through a node's cache API; queries and arguments are created in setup. */
@SuppressWarnings("unchecked")
public class JmhSqlAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Rows of a range select. */
    private static final int RANGE = 10;

    /** Range selects created in setup, each from a different first row. */
    private static final int RANGES = 1024;

    /** Query engine. */
    public enum Engine {
        /** H2, from {@code ignite-indexing}. */
        H2,

        /** Apache Calcite, from {@code ignite-calcite}. */
        CALCITE
    }

    /** Query engine. */
    @Param("H2")
    private Engine engine;

    /** Value class, which sets the table's columns. */
    @Param("POJO")
    private SqlValueType valType;

    /** Select of each key the setup writes. */
    private SqlFieldsQuery[] selects;

    /** Range selects. */
    private SqlFieldsQuery[] rangeSelects;

    /** Update of each key the setup writes, to the same value. */
    private SqlFieldsQuery[] updates;

    /** Insert of each key the setup doesn't write. */
    private SqlFieldsQuery[] inserts;

    /** Delete of each key of {@link #inserts}. */
    private SqlFieldsQuery[] deletes;

    /** Index of the next key in {@link #inserts}. */
    private int newKeyIdx;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        String table = valType.valueClass().getSimpleName();
        String selectSql = "SELECT * FROM " + table + " WHERE _key = ?";
        String updateSql = "UPDATE " + table + " SET id = ? WHERE _key = ?";
        String deleteSql = "DELETE FROM " + table + " WHERE _key = ?";
        String rangeSql = "SELECT * FROM " + table + " WHERE id BETWEEN ? AND ?";
        String insertSql = valType.insertSql();

        keys = new Integer[CNT];
        vals = new Object[CNT];

        selects = new SqlFieldsQuery[CNT];
        updates = new SqlFieldsQuery[CNT];
        inserts = new SqlFieldsQuery[CNT];
        deletes = new SqlFieldsQuery[CNT];

        for (int i = 0; i < CNT; i++) {
            keys[i] = i;
            vals[i] = valType.value(i);

            selects[i] = new SqlFieldsQuery(selectSql).setArgs(keys[i]);
            updates[i] = new SqlFieldsQuery(updateSql).setArgs(keys[i], keys[i]);

            Integer newKey = CNT + i;

            inserts[i] = new SqlFieldsQuery(insertSql).setArgs(valType.insertArgs(newKey));
            deletes[i] = new SqlFieldsQuery(deleteSql).setArgs(newKey);
        }

        rangeSelects = new SqlFieldsQuery[RANGES];

        for (int i = 0; i < RANGES; i++) {
            int first = i * ((CNT - RANGE) / RANGES);

            rangeSelects[i] = new SqlFieldsQuery(rangeSql).setArgs(keys[first], keys[first + RANGE - 1]);
        }

        populate(keys, vals);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration configuration(String igniteInstanceName) {
        return super.configuration(igniteInstanceName).setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(engine == Engine.CALCITE
                ? new CalciteQueryEngineConfiguration()
                : new IndexingQueryEngineConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration().setIndexedTypes(Integer.class, valType.valueClass());
    }

    /** @return Row of a key. */
    @Benchmark
    public List<List<?>> selectByKey() {
        return query(selects[ThreadLocalRandom.current().nextInt(CNT)]);
    }

    /** @return {@link #RANGE} rows by a range of the indexed field. */
    @Benchmark
    public List<List<?>> selectRange() {
        return query(rangeSelects[ThreadLocalRandom.current().nextInt(RANGES)]);
    }

    /**
     * Sets the indexed field of a key to the value it has.
     *
     * @return One row with the number of rows updated.
     */
    @Benchmark
    public List<List<?>> update() {
        return query(updates[ThreadLocalRandom.current().nextInt(CNT)]);
    }

    /** Inserts a row of a key the setup didn't write and deletes it, so the table keeps its size. */
    @Benchmark
    public void insertDelete() {
        int i = newKeyIdx++ % CNT;

        query(inserts[i]);
        query(deletes[i]);
    }

    /** @return All rows of the query. */
    private List<List<?>> query(SqlFieldsQuery qry) {
        return cache.query(qry).getAll();
    }
}
