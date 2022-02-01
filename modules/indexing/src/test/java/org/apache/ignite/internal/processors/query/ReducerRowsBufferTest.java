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

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.twostep.ReduceBlockList;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.result.Row;
import org.junit.Test;

/** */
public class ReducerRowsBufferTest extends GridCommonAbstractTest {
    /** Table size. */
    private static final int TBL_SIZE = 1_000;

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        createSchema();

        populateData();
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** */
    @Test
    public void plainQuery() {
        Iterator<List<?>> it = query("select * from TEST", true).iterator();

        it.next();

        ReduceBlockList<Row> fetched;

        Object innerIt = GridTestUtils.getFieldValue(it, "cursor", "iter");

        if (innerIt instanceof QueryCursorImpl.LazyIterator)
            fetched = GridTestUtils.getFieldValue(innerIt, "delegate", "iter", "cursor", "this$0", "fetched");
        else
            fetched = GridTestUtils.getFieldValue(innerIt, "iter", "cursor", "this$0", "fetched");

        int cnt = 1;
        while (it.hasNext()) {
            it.next();

            assertEquals(0, fetched.size());

            cnt++;
        }

        assertEquals(TBL_SIZE, cnt);
    }

    /** */
    private void populateData() {
        for (int i = 0; i < TBL_SIZE; ++i)
            execSql("insert into TEST VALUES (?, ?, ?)", i, i % 100, UUID.randomUUID().toString());
    }

    /** */
    private void createSchema() {
        execSql("create table TEST (id int primary key, ref_key int, name varchar)");
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     */
    private void execSql(String sql, Object... args) {
        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /**
     * @param sql SQL query
     * @return Results set.
     */
    FieldsQueryCursor<List<?>> query(String sql, boolean lazy) {
        return grid(0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null)
                .setLazy(lazy)
                .setEnforceJoinOrder(true)
                .setPageSize(100), false);
    }
}
