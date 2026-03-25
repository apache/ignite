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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Integration test for user defined functions with tx aware.
 */
@WithSystemProperty(key = IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR, value = "true")
public class UserDefinedTxAwareFunctionsIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    @Parameterized.Parameter()
    public SqlTransactionMode sqlTxMode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "sqlTxMode={0}")
    public static Collection<?> parameters() {
        return List.of(SqlTransactionMode.ALL);
    }

    /** Check tx aware udf execution results. */
    @Test
    public void testTxAwareUserDefinedFunc() {
        assertTrue(nodeCount() > 1);
        int nodeCnt = nodeCount();

        client.getOrCreateCache(cacheConfig());

        List<List<Object>> refResults = new ArrayList<>();

        IgniteCache<Integer, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        refResults.add(List.of(0, Integer.toString(0)));
        // Insert outside tx.
        cache.query(new SqlFieldsQuery("INSERT INTO PUBLIC.CITY(id, name) VALUES (?, ?)").setArgs(0, 0)).getAll();

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            for (int i = 1; i < 2 * nodeCnt; ++i) {
                refResults.add(List.of(i, Integer.toString(i)));
                cache.query(new SqlFieldsQuery("INSERT INTO PUBLIC.CITY(id, name) VALUES (?, ?)").setArgs(i, i)).getAll();
            }

            // Simple select without udf.
            List<List<?>> selectResult = cache
                .query(new SqlFieldsQuery("SELECT id, name FROM PUBLIC.CITY ORDER BY id"))
                .getAll();

            assertThat(selectResult, equalTo(refResults));

            // Select with udf.
            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT customTableFunc() AS result")).getAll();

            assertThat(res.get(0).get(0), equalTo(refResults));

            // Select with nested udf.
            res = cache.query(new SqlFieldsQuery("SELECT customNestedTableFunc() AS result")).getAll();

            assertThat(res.get(0).get(0), equalTo(refResults));

            // Udf participate in dml.
            res = cache.query(new SqlFieldsQuery("INSERT INTO PUBLIC.CITY(id, name) VALUES (100, nameAsStr(1))")).getAll();

            res = cache.query(new SqlFieldsQuery("SELECT name FROM PUBLIC.CITY WHERE id = 100")).getAll();

            assertEquals("1", res.get(0).get(0));

            for (int i = 0; i < 2 * nodeCnt; ++i) {
                // A bit different case of udf.
                List<List<?>> res1 = cache.query(new SqlFieldsQuery("SELECT name(?) AS result").setArgs(i)).getAll();

                List<List<?>> res2 = (List<List<?>>)res1.get(0).get(0);

                assertThat(res2.get(0).get(0), equalTo(Integer.toString(i)));
            }

            tx.commit();
        }
    }

    /** */
    @Test
    public void testIsolationCorrectnessWithUdf() throws IgniteCheckedException {
        assertTrue(nodeCount() > 1);
        int nodeCnt = nodeCount();

        client.getOrCreateCache(cacheConfig());

        IgniteCache<Integer, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            for (int iter = 0; iter < 10; ++iter) {
                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    List<List<Object>> refResults = new ArrayList<>();

                    for (int i = 0; i < 2 * nodeCnt; ++i) {
                        refResults.add(List.of(i, Integer.toString(i)));
                        cache.query(new SqlFieldsQuery("INSERT INTO PUBLIC.CITY(id, name) VALUES (?, ?)").setArgs(i, i)).getAll();
                    }

                    List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT customNestedTableFunc() AS result")).getAll();

                    assertThat(res.get(0).get(0), equalTo(refResults));

                    tx.rollback();
                }
            }
        }, 10, "calcite-tx-with-udf");

        fut.get(20_000);
    }

    /** */
    private CacheConfiguration<Integer, Object> cacheConfig() {
        var entity = new QueryEntity()
            .setTableName("CITY")
            .setKeyType(Integer.class.getName())
            .setValueType(City.class.getName())
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .setKeyFieldName("id");

        return new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL)
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(InnerSqlFunctionsLibrary.class)
            .setQueryEntities(List.of(entity));
    }

    /** */
    public static class InnerSqlFunctionsLibrary {
        /** */
        @QuerySqlFunction
        public List<List<?>> customTableFunc() {
            Ignite ignite = Ignition.localIgnite();

            return ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT id, name FROM PUBLIC.CITY ORDER BY id"))
                .getAll();
        }

        /** */
        @QuerySqlFunction
        public List<List<?>> customNestedTableFunc() {
            Ignite ignite = Ignition.localIgnite();

            Object res = ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT customTableFuncInner() AS result"))
                .getAll().get(0).get(0);

            return (List<List<?>>)res;
        }

        /** */
        @QuerySqlFunction
        public List<List<?>> customTableFuncInner() {
            Ignite ignite = Ignition.localIgnite();

            return ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT id, name FROM PUBLIC.CITY ORDER BY id"))
                .getAll();
        }

        /** */
        @QuerySqlFunction
        public static List<List<?>> name(int id) {
            Ignite ignite = Ignition.localIgnite();

            return ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT name FROM PUBLIC.CITY WHERE id = ?").setArgs(id))
                .getAll();
        }

        /** */
        @QuerySqlFunction
        public static String nameAsStr(int id) {
            Ignite ignite = Ignition.localIgnite();

            List<List<?>> res = ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT name FROM PUBLIC.CITY WHERE id = ?").setArgs(id))
                .getAll();

            return (String)res.get(0).get(0);
        }
    }

    /** */
    private static class City {
        /** */
        @GridToStringInclude
        int id;

        /** */
        @GridToStringInclude
        String name;

        /** */
        City(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** */
        @Override public String toString() {
            return S.toString(City.class, this);
        }
    }
}
