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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
@SuppressWarnings("ThrowableNotThrown")
public class IgniteSqlDefaultValueTest extends GridCommonAbstractTest {
    /** Name of client node. */
    private static final String NODE_CLIENT = "client";

    /** Number of server nodes. */
    private static final int NODE_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)c.getDiscoverySpi()).setForceServerMode(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        startClientGrid(NODE_CLIENT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Collection<String> tblNames = new ArrayList<>();

        for (String cacheName : grid(0).context().cache().publicCacheNames()) {
            for (GridQueryTypeDescriptor table : grid(0).context().query().types(cacheName))
                tblNames.add(table.tableName());
        }

        for (String tbl : tblNames)
            sql("DROP TABLE " + tbl);

        super.afterTest();
    }

    /**
     */
    @Test
    public void testDefaultValueColumn() {
        sql("CREATE TABLE TEST (id int, val0 varchar DEFAULT 'default-val', primary key (id))");
        sql("INSERT INTO TEST (id) VALUES (?)", 1);
        sql("INSERT INTO TEST (id, val0) VALUES (?, ?)", 2, null);
        sql("INSERT INTO TEST (id, val0) VALUES (?, ?)", 3, "test-val");

        List<List<Object>> exp = Arrays.asList(
            Arrays.<Object>asList(1, "default-val"),
            Arrays.<Object>asList(2, null),
            Arrays.<Object>asList(3, "test-val")
        );

        List<List<?>> res = sql("select id, val0 from TEST");

        checkResults(exp, res);
    }

    /**
     */
    @Test
    public void testDefaultValueColumnAfterUpdate() {
        sql("CREATE TABLE TEST (id int, val0 varchar DEFAULT 'default-val', val1 varchar, primary key (id))");
        sql("INSERT INTO TEST (id, val1) VALUES (?, ?)", 1, "val-10");
        sql("INSERT INTO TEST (id, val1) VALUES (?, ?)", 2, "val-20");
        sql("INSERT INTO TEST (id, val1) VALUES (?, ?)", 3, "val-30");

        List<List<Object>> exp = Arrays.asList(
            Arrays.<Object>asList(1, "default-val", "val-10"),
            Arrays.<Object>asList(2, "default-val", "val-20"),
            Arrays.<Object>asList(3, "default-val", "val-30")
        );

        List<List<?>> res = sql("select id, val0, val1 from TEST");

        checkResults(exp, res);

        sql("UPDATE TEST SET val1=? where id=?", "val-21", 2);

        List<List<Object>> expAfterUpdate = Arrays.asList(
            Arrays.<Object>asList(1, "default-val", "val-10"),
            Arrays.<Object>asList(2, "default-val", "val-21"),
            Arrays.<Object>asList(3, "default-val", "val-30")
        );

        List<List<?>> resAfterUpdate = sql("select id, val0, val1 from TEST");

        checkResults(expAfterUpdate, resAfterUpdate);
    }

    /**
     */
    @Test
    public void testEmptyValueNullDefaults() {
        sql("CREATE TABLE TEST (id int, val0 varchar, primary key (id))");
        sql("INSERT INTO TEST (id) VALUES (?)", 1);
        sql("INSERT INTO TEST (id, val0) VALUES (?, ?)", 2, "test-val");

        List<List<Object>> expected = Arrays.asList(
            Arrays.<Object>asList(1, null),
            Arrays.<Object>asList(2, "test-val")
        );

        List<List<?>> res = sql("select id, val0 from TEST");

        checkResults(expected, res);
    }

    /**
     */
    @Test
    public void testAddColumnWithDefaults() {
        sql("CREATE TABLE TEST (id int, val0 varchar, primary key (id))");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() {
                    sql("ALTER TABLE TEST ADD COLUMN val1 varchar DEFAULT 'default-val'");

                    return null;
                }
            }, IgniteSQLException.class, "ALTER TABLE ADD COLUMN with DEFAULT value is not supported");
    }

    /**
     */
    @Test
    public void testDefaultTypes() {
        assertEquals("Check tinyint", (byte)28, getDefaultObject("TINYINT", "28"));
        assertEquals("Check smallint", (short)28, getDefaultObject("SMALLINT", "28"));
        assertEquals("Check int", 28, getDefaultObject("INT", "28"));
        assertEquals("Check double", 28.25, getDefaultObject("DOUBLE", "28.25"));
        assertEquals("Check float", 28.25, getDefaultObject("FLOAT", "28.25"));
        assertEquals("Check decimal", BigDecimal.valueOf(28.25), getDefaultObject("DECIMAL", "28.25"));
        assertEquals("Check varchar", "test value", getDefaultObject("VARCHAR", "'test value'"));
        assertEquals("Check time", Time.valueOf("14:01:01"), getDefaultObject("TIME", "'14:01:01'"));
        assertEquals("Check date", Date.valueOf("2017-12-29"), getDefaultObject("DATE", "'2017-12-29'"));
        assertEquals("Check timestamp", Timestamp.valueOf("2017-12-29 14:01:01"),
            getDefaultObject("TIMESTAMP", "'2017-12-29 14:01:01'"));
    }

    /**
     * @param sqlType SQL type.
     * @param dfltVal Value string representation.
     * @return Object is returned by SELECT query.
     */
    private Object getDefaultObject(String sqlType, String dfltVal) {
        sql(String.format("CREATE TABLE TEST (id int, val %s default %s, primary key (id))",
            sqlType, dfltVal));

        sql("INSERT INTO TEST (id) VALUES (1)");

        List<List<?>> res = sql("SELECT val FROM TEST WHERE id=1");

        sql("DROP TABLE TEST");

        return res.get(0).get(0);
    }

    /**
     * @param exp Expected results.
     * @param actual Actual results.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private void checkResults(Collection<List<Object>> exp, Collection<List<?>> actual) {
        assertEquals(exp.size(), actual.size());

        for (List<?> row : actual) {
            if (!exp.contains(row))
                fail("Unexpected results: [row=" + row + ']');
        }
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     * @return Results set.
     */
    private List<List<?>> sql(String sql, Object... args) {
        return grid(NODE_CLIENT).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
