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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Integration tests for {@code SELECT ... FOR UPDATE} parsing in Calcite engine.
 */
public class SelectForUpdateParsingIntegrationTest extends GridCommonAbstractTest {
    /** Test cache name. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        QueryEntity entity = new QueryEntity()
            .setTableName("TEST")
            .setKeyType(Integer.class.getName())
            .setValueType(TestValue.class.getName())
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("val", Integer.class.getName(), null)
            .setKeyFieldName("id");

        CacheConfiguration<Integer, TestValue> cacheCfg = new CacheConfiguration<Integer, TestValue>(CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(1)
            .setQueryEntities(Collections.singletonList(entity));

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)))
            .setCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        sql("DELETE FROM TEST");
        sql("INSERT INTO TEST(id, val) VALUES (1, 10)");
        sql("INSERT INTO TEST(id, val) VALUES (2, 20)");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Ensures that execution result is identical for queries with and without {@code FOR UPDATE}.
     */
    @Test
    public void testSelectForUpdateClauseIsIgnoredOnExecution() {
        List<List<?>> withoutForUpdate = sql("SELECT id, val FROM TEST ORDER BY id");
        List<List<?>> withForUpdate = sql("SELECT id, val FROM TEST ORDER BY id FOR UPDATE");

        assertEquals(withoutForUpdate, withForUpdate);
    }

    /**
     * Ensures that {@code FOR UPDATE} combined with {@code GROUP BY} is rejected by validator.
     */
    @Test
    public void testSelectForUpdateWithGroupByIsRejected() {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> sql("SELECT id, COUNT(*) FROM TEST GROUP BY id FOR UPDATE"),
            SqlValidatorException.class,
            "FOR UPDATE with GROUP BY"
        );
    }

    /**
     * Executes SQL query.
     *
     * @param sqlText SQL text.
     * @param args Query arguments.
     * @return Query result.
     */
    private List<List<?>> sql(String sqlText, Object... args) {
        IgniteCache<?, ?> cache = grid(0).cache(CACHE_NAME);

        return cache.query(new SqlFieldsQuery(sqlText).setArgs(args)).getAll();
    }

    /**
     * Cache value type for test table.
     */
    @SuppressWarnings("unused")
    public static class TestValue {
        /** Identifier field. */
        private int id;

        /** Payload field. */
        private int val;

        /** Default constructor. */
        public TestValue() {
            // No-op.
        }

        /**
         * @param id Identifier field.
         * @param val Payload field.
         */
        public TestValue(int id, int val) {
            this.id = id;
            this.val = val;
        }
    }
}
