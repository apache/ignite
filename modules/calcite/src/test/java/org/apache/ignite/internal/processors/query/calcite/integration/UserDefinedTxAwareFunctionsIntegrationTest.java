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
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Integration test for user defined functions with tx aware.
 */
@WithSystemProperty(key = IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR, value = "true")
public class UserDefinedTxAwareFunctionsIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int THREAD_NUM = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());
        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);
        cfg.setQueryThreadPoolSize(2 * THREAD_NUM + 1);

        return cfg;
    }

    /** Check tx aware UDF execution results. */
    @Test
    public void testTxAwareUserDefinedFunc() {
        assertTrue(nodeCount() > 1);
        int nodeCnt = nodeCount();

        List<List<Object>> refResults = new ArrayList<>();

        IgniteCache<Integer, Object> cache = client.getOrCreateCache(cacheConfig());

        refResults.add(List.of(0, Integer.toString(0)));
        // Insert outside tx.
        cache.query(new SqlFieldsQuery("INSERT INTO Employer(id, name) VALUES (?, ?)").setArgs(0, 0)).getAll();

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            for (int i = 1; i < 2 * nodeCnt; ++i) {
                refResults.add(List.of(i, Integer.toString(i)));
                cache.query(new SqlFieldsQuery("INSERT INTO Employer(id, name) VALUES (?, ?)").setArgs(i, i)).getAll();
            }

            // Simple select without UDF.
            List<List<?>> selectResult = cache
                .query(new SqlFieldsQuery("SELECT id, name FROM Employer ORDER BY id"))
                .getAll();

            assertThat(selectResult, equalTo(refResults));

            // Select with UDF.
            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT customTableFunc() AS result")).getAll();

            assertThat(res.get(0).get(0), equalTo(refResults));

            // Select with nested UDF.
            res = cache.query(new SqlFieldsQuery("SELECT customNestedTableFunc() AS result")).getAll();

            assertThat(res.get(0).get(0), equalTo(refResults));

            // UDF participate in DML.
            cache.query(new SqlFieldsQuery("INSERT INTO Employer(id, name) VALUES (100, nameAsStr(1))")).getAll();

            res = cache.query(new SqlFieldsQuery("SELECT name FROM Employer WHERE id = 100")).getAll();

            assertEquals("1", res.get(0).get(0));

            for (int i = 0; i < 2 * nodeCnt; ++i) {
                // A bit different case of UDF.
                List<List<?>> res1 = cache.query(new SqlFieldsQuery("SELECT nameTableFunc(?) AS result").setArgs(i)).getAll();

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

        IgniteCache<Integer, Object> cache = client.getOrCreateCache(cacheConfig());

        /* The pool size should be greater than the maximum number of concurrent queries initiated by UDFs. */
        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            for (int iter = 0; iter < 10; ++iter) {
                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    List<List<Object>> refResults = new ArrayList<>();

                    for (int i = 0; i < 2 * nodeCnt; ++i) {
                        refResults.add(List.of(i, Integer.toString(i)));
                        cache.query(new SqlFieldsQuery("INSERT INTO Employer(id, name) VALUES (?, ?)").setArgs(i, i)).getAll();
                    }

                    List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT customNestedTableFunc() AS result")).getAll();

                    assertThat(res.get(0).get(0), equalTo(refResults));

                    tx.rollback();
                }
            }
        }, THREAD_NUM, "calcite-tx-with-udf");

        fut.get(30_000);
    }

    /** */
    private CacheConfiguration<Integer, Object> cacheConfig() {
        return this.<Integer, Object>cacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class)
                .setTableName("Employer")
                .addQueryField("ID", Integer.class.getName(), null)
                .setKeyFieldName("ID")
            ))
            .setSqlFunctionClasses(InnerSqlFunctionsLibrary.class)
            .setAtomicityMode(TRANSACTIONAL);
    }

    /** */
    public static class InnerSqlFunctionsLibrary {
        /** */
        @QuerySqlFunction
        public List<List<?>> customTableFunc() {
            Ignite ignite = Ignition.localIgnite();

            return ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT id, name FROM Employer ORDER BY id"))
                .getAll();
        }

        /** */
        @QuerySqlFunction
        public List<List<?>> customNestedTableFunc() {
            Ignite ignite = Ignition.localIgnite();

            Object res = ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT customTableFunc() AS result"))
                .getAll().get(0).get(0);

            return (List<List<?>>)res;
        }

        /** */
        @QuerySqlFunction
        public static List<List<?>> nameTableFunc(int id) {
            Ignite ignite = Ignition.localIgnite();

            return ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT name FROM Employer WHERE id = ?").setArgs(id))
                .getAll();
        }

        /** */
        @QuerySqlFunction
        public static String nameAsStr(int id) {
            Ignite ignite = Ignition.localIgnite();

            List<List<?>> res = ignite.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT name FROM Employer WHERE id = ?").setArgs(id))
                .getAll();

            return (String)res.get(0).get(0);
        }
    }
}
