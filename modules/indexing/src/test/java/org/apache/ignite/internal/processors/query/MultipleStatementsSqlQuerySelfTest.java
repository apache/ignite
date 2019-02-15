/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for schemas.
 */
public class MultipleStatementsSqlQuerySelfTest extends AbstractIndexingCommonTest {
    /** Node. */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = (IgniteEx)startGrid();

        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test query without caches.
     */
    @Test
    public void testQuery() {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(
            "create table test(ID int primary key, NAME varchar(20)); " +
                "insert into test (ID, NAME) values (1, 'name_1');" +
                "insert into test (ID, NAME) values (2, 'name_2'), (3, 'name_3');" +
                "select * from test;")
            .setSchema("PUBLIC");

        List<FieldsQueryCursor<List<?>>> res = qryProc.querySqlFields(qry, true, false);

        assert res.size() == 4 : "Unexpected cursors count: " + res.size();

        assert !((QueryCursorImpl)res.get(0)).isQuery() : "Results of DDL statement is expected ";

        List<List<?>> rows = res.get(1).getAll();

        assert !((QueryCursorImpl)res.get(1)).isQuery() : "Results of DDL statement is expected ";
        assert Long.valueOf(1).equals(rows.get(0).get(0)) : "1 row must be updated. [actual=" + rows.get(0).get(0) + ']';

        rows = res.get(2).getAll();

        assert !((QueryCursorImpl)res.get(2)).isQuery() : "Results of DML statement is expected ";
        assert Long.valueOf(2).equals(rows.get(0).get(0)) : "2 row must be updated";

        rows = res.get(3).getAll();

        assert ((QueryCursorImpl)res.get(3)).isQuery() : "Results of SELECT statement is expected ";

        assert rows.size() == 3 : "Invalid rows count: " + rows.size();

        for (int i = 0; i < rows.size(); ++i) {
            assert Integer.valueOf(1).equals(rows.get(i).get(0))
                || Integer.valueOf(2).equals(rows.get(i).get(0))
                || Integer.valueOf(3).equals(rows.get(i).get(0))
                : "Invalid ID: " + rows.get(i).get(0);
        }
    }

    /**
     * Test query without caches.
     */
    @Test
    public void testQueryWithParameters() {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(
            "create table test(ID int primary key, NAME varchar(20)); " +
                "insert into test (ID, NAME) values (?, ?);" +
                "insert into test (ID, NAME) values (?, ?), (?, ?);" +
                "select * from test;")
            .setSchema("PUBLIC")
            .setArgs(1, "name_1", 2, "name2", 3, "name_3");

        List<FieldsQueryCursor<List<?>>> res = qryProc.querySqlFields(qry, true, false);

        assert res.size() == 4 : "Unexpected cursors count: " + res.size();

        assert !((QueryCursorImpl)res.get(0)).isQuery() : "Results of DDL statement is expected ";

        List<List<?>> rows = res.get(1).getAll();

        assert !((QueryCursorImpl)res.get(1)).isQuery() : "Results of DDL statement is expected ";
        assert Long.valueOf(1).equals(rows.get(0).get(0)) : "1 row must be updated. [actual=" + rows.get(0).get(0) + ']';

        rows = res.get(2).getAll();

        assert !((QueryCursorImpl)res.get(2)).isQuery() : "Results of DML statement is expected ";
        assert Long.valueOf(2).equals(rows.get(0).get(0)) : "2 row must be updated";

        rows = res.get(3).getAll();

        assert ((QueryCursorImpl)res.get(3)).isQuery() : "Results of SELECT statement is expected ";

        assert rows.size() == 3 : "Invalid rows count: " + rows.size();

        for (int i = 0; i < rows.size(); ++i) {
            assert Integer.valueOf(1).equals(rows.get(i).get(0))
                || Integer.valueOf(2).equals(rows.get(i).get(0))
                || Integer.valueOf(3).equals(rows.get(i).get(0))
                : "Invalid ID: " + rows.get(i).get(0);
        }
    }

    /**
     */
    @Test
    public void testQueryMultipleStatementsFailed() {
        final SqlFieldsQuery qry = new SqlFieldsQuery("select 1; select 1;").setSchema("PUBLIC");

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    node.context().query().querySqlFields(qry, true, true);

                    return null;
                }
            }, IgniteSQLException.class, "Multiple statements queries are not supported");
    }

    /**
     * Check cached two-steps query.
     */
    @Test
    public void testCachedTwoSteps() {
        List<FieldsQueryCursor<List<?>>> curs = sql("SELECT 1; SELECT 2");

        assertEquals(2, curs.size());
        assertEquals(1, curs.get(0).getAll().get(0).get(0));
        assertEquals(2, curs.get(1).getAll().get(0).get(0));

        curs = sql("SELECT 1; SELECT 2");

        assertEquals(2, curs.size());
        assertEquals(1, curs.get(0).getAll().get(0).get(0));
        assertEquals(2, curs.get(1).getAll().get(0).get(0));
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    private List<FieldsQueryCursor<List<?>>> sql(String sql) {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true, false);
    }
}
