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

package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Checks add field with invalid data type to index.
 */
public class CreateIndexOnInvalidDataTypeTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Check case when index is created on the field with invalid data type.
     * Test steps:
     * - create cache with query entity describes a table;
     * - fill data (real data contains the fields that was not described by query entity);
     * - execute alter table (ADD COLUMN with invalid type for exists field);
     * - try to create index for the new field - exception must be throw;
     * - checks that index isn't created.
     */
    @Test
    public void testCreateIndexOnInvalidData() throws Exception {
        startGrid();

        grid().cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Value> c = grid().createCache(
            new CacheConfiguration<Integer, Value>()
                .setName("test")
                .setSqlSchema("PUBLIC")
                .setQueryEntities(
                    Collections.singleton(
                        new QueryEntity(Integer.class, Value.class)
                            .setTableName("TEST")
                    )
                )
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, 10))
        );

        for (int i = 0; i < KEY_CNT; ++i)
            c.put(i, new Value(i));

        sql("ALTER TABLE TEST ADD COLUMN (VAL_DATE DATE)");

        sql("CREATE INDEX TEST_VAL_INT_IDX ON TEST(VAL_INT)");

        GridTestUtils.assertThrowsAnyCause(log, () -> {
                sql("CREATE INDEX TEST_VAL_DATE_IDX ON TEST(VAL_DATE)");

                return null;
            },
            IgniteSQLException.class, "java.util.Date cannot be cast to java.sql.Date");

        // Wait for node stop if it is initiated by FailureHandler
        U.sleep(1000);

        List<List<?>> res = sql("SELECT val_int FROM TEST where val_int > -1").getAll();

        assertEquals(KEY_CNT, res.size());

        GridTestUtils.assertThrowsAnyCause(log, () -> {
                sql("DROP INDEX TEST_VAL_DATE_IDX");

                return null;
            },
            IgniteSQLException.class, "Index doesn't exist: TEST_VAL_DATE_IDX");
    }

    /**
     * Check case when row with invalid field is added.
     * Test steps:
     * - create table;
     * - create two index;
     * - try add entry - exception must be thrown;
     * - remove the index for field with invalid type;
     * - check that select query that uses the index for valid field is successful.
     */
    @Test
    public void testAddInvalidDataToIndex() throws Exception {
        startGrid();

        grid().cluster().state(ClusterState.ACTIVE);

        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL_INT INT, VAL_DATE DATE) " +
            "WITH \"CACHE_NAME=test,VALUE_TYPE=ValueType0\"");

        sql("CREATE INDEX TEST_0_VAL_DATE_IDX ON TEST(VAL_DATE)");
        sql("CREATE INDEX TEST_1_VAL_INT_IDX ON TEST(VAL_INT)");

        BinaryObjectBuilder bob = grid().binary().builder("ValueType0");

        bob.setField("VAL_INT", 10);
        bob.setField("VAL_DATE", new java.util.Date());

        assertThrowsWithCause(() -> grid().cache("test").put(0, bob.build()), IgniteSQLException.class);

        assertNull(grid().cache("test").get(0));

        sql("DROP INDEX TEST_0_VAL_DATE_IDX");

        // Check successful insert after index is dropped.
        grid().cache("test").put(1, bob.build());

        List<List<?>> res = sql("SELECT VAL_INT FROM TEST WHERE VAL_INT > 0").getAll();

        assertEquals(1, res.size());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid().context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setArgs(args), false);
    }

    /** */
    private static class Value {
        /** */
        @QuerySqlField
        int val_int;

        /** */
        Date val_date;

        /**
         * @param val Test value.
         */
        public Value(int val) {
            this.val_int = val;
            val_date = new Date(val);
        }
    }
}
