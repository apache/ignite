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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for schemas.
 */
public class SqlHugeResultTest extends GridCommonAbstractTest {
    /** Rows count. */
    private static final int ROWS_COUNT = 100;

    /** Original value for the H2 property 'h2.maxMemoryRows'. */
    private static String origMaxMemoryRows;

    /** Original value for the H2 property 'h2.mvStore'. */
    private static String origMvStore;

    /** Node. */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        origMaxMemoryRows = System.getProperty("h2.maxMemoryRows");
        origMvStore = System.getProperty("h2.mvStore");

        System.setProperty("h2.maxMemoryRows", Integer.toString(10));
        System.setProperty("h2.mvStore", "false");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.setProperty("h2.maxMemoryRows", origMaxMemoryRows);
        System.setProperty("h2.mvStore", origMvStore);

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = (IgniteEx)startGrid();

        startGrid(2);

        sql("CREATE TABLE test(id INTEGER PRIMARY KEY, int_val INTEGER, str_val VARCHAR)");

        for (int i = 0; i < ROWS_COUNT; ++i)
            sql("INSERT INTO test(id, int_val, str_val) VALUES (?, ?, ?)", i, i + 1, "val " + i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     */
    public void testPlainResult() throws Exception {
        sql("SELECT " +
            "t0.id, t0.int_val, t0.str_val, " +
            "t1.id, t1.int_val, t1.str_val, " +
            "t2.id, t1.int_val, t2.str_val " +
            "FROM test as t0, test as t1, test as t2");
    }

    /**
     */
    public void testOrderedResult() throws Exception {
        sql("SELECT " +
            "t0.id, t0.int_val, t0.str_val, " +
            "t1.id, t1.int_val, t1.str_val, " +
            "t2.id, t1.int_val, t2.str_val " +
            "FROM test as t0, test as t1, test as t2 " +
            "ORDER BY t0.str_val");

        Thread.sleep(100000);
    }

    /**
     * @param sql SQL query.
     * @param args Parameters' values.
     * @return Results.
     */
    private List<List<?>> sql(String sql, Object... args) {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args);

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
