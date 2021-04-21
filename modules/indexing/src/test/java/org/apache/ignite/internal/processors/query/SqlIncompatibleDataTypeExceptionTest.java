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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for use _key, _val columns at the INSERT/MERGE/UPDATE statements.
 */
public class SqlIncompatibleDataTypeExceptionTest extends AbstractIndexingCommonTest {
    /** Old allow value. */
    private boolean oldAllowColumnsVal;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        oldAllowColumnsVal = GridTestUtils.getFieldValue(UpdatePlanBuilder.class, UpdatePlanBuilder.class,
            "ALLOW_KEY_VAL_UPDATES");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().destroyCaches(grid().cacheNames());

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", oldAllowColumnsVal);

        super.afterTest();
    }

    /**
     * Test use _key field at INSERT statement.
     */
    @Test
    public void testUseKeyField() {
        // Unwrapped simple key.
        execSql("CREATE TABLE test_0 (id integer primary key, val varchar)");
        execSql("INSERT INTO test_0 (_key, val) VALUES (?, ?)", 0, "0");
        execSql("MERGE INTO test_0 (_key, val) VALUES (?, ?)", 1, "1");

        // Composite key.
        execSql("CREATE TABLE test (id0 integer, id1 integer, val varchar, primary key (id0, id1))");

        final BinaryObjectBuilder bob = grid().binary().builder("key");
        bob.setField("id0", 0);
        bob.setField("id1", 1);

        GridTestUtils.assertThrows(log, () -> {
            execSql("INSERT INTO test (_key, val) VALUES (?, ?)", bob.build(), "asd");

            return null;
        }, IgniteSQLException.class, "Update of composite key column is not supported");

        GridTestUtils.assertThrows(log, () -> {
            execSql("MERGE INTO test (_key, val) VALUES (?, ?)", bob.build(), "asd");

            return null;
        }, IgniteSQLException.class, "Update of composite key column is not supported");
    }

    /**
     * Test use _val field at INSERT statement.
     */
    @Test
    public void testUseValField() {
        // Unwrapped simple value.
        execSql("CREATE TABLE test_0 (id integer primary key, val varchar) WITH \"WRAP_VALUE=0\"");
        execSql("INSERT INTO test_0 (id, _val) VALUES (?, ?)", 0, "0");
        execSql("MERGE INTO test_0 (id, _val) VALUES (?, ?)", 1, "1");
        execSql("UPDATE test_0 SET _val = ?", "upd");

        // Composite value.
        execSql("CREATE TABLE test (id integer primary key, val varchar)");

        final BinaryObjectBuilder bob = grid().binary().builder("val");
        bob.setField("val", "0");

        GridTestUtils.assertThrows(log, () -> {
            execSql("INSERT INTO test (id, _val) VALUES (?, ?)", 0, bob.build());

            return null;
        }, IgniteSQLException.class, "Update of composite value column is not supported");

        GridTestUtils.assertThrows(log, () -> {
            execSql("MERGE INTO test (id, _val) VALUES (?, ?)", 0, bob.build());

            return null;
        }, IgniteSQLException.class, "Update of composite value column is not supported");

        GridTestUtils.assertThrows(log, () -> {
            execSql("UPDATE test SET _val=?", bob.build());

            return null;
        }, IgniteSQLException.class, "Update of composite value column is not supported");
    }

    /**
     *
     */
    @Test
    public void testUseKeyField_Allow() {
        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        execSql("CREATE TABLE test (id0 integer, id1 integer, val varchar, primary key (id0, id1))");

        final BinaryObjectBuilder bob = grid().binary().builder("val");
        bob.setField("id0", 0);
        bob.setField("id1", 0);

        // Invalid usage, but allowed for backward compatibility
        // when ALLOW_KEY_VAL_COLUMNS set to true
        execSql("INSERT INTO test (_key, val) VALUES (?, ?)", bob.build(), 0);

        bob.setField("id0", 1);
        bob.setField("id1", 1);
        execSql("MERGE INTO test (_key, val) VALUES (?, ?)", bob.build(), 1);
    }

    /**
     *
     */
    @Test
    public void testUseValField_Allow() {
        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        execSql("CREATE TABLE test (id integer primary key, val varchar)");

        final BinaryObjectBuilder bob = grid().binary().builder("val");
        bob.setField("val", "0");

        // Invalid usage, but allowed for backward compatibility
        // when ALLOW_KEY_VAL_COLUMNS set to true
        execSql("INSERT INTO test (id, _val) VALUES (?, ?)", 0, bob.build());
        execSql("MERGE INTO test (id, _val) VALUES (?, ?)", 0, bob.build());
        execSql("UPDATE test SET _val=?", bob.build());
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     * @return Results.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return ((IgniteEx)ignite).context().query().querySqlFields(qry, false).getAll();
    }

    /**
     * @param sql Sql.
     * @param args Args.
     * @return Query results.
     */
    private List<List<?>> execSql(String sql, Object... args) {
        return execSql(grid(), sql, args);
    }
}
