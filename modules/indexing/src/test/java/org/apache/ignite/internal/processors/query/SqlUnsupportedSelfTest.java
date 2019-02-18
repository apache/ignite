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
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for ignite SQL system views.
 */
public class SqlUnsupportedSelfTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
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
     */
    private List<List<?>> execSql(String sql, Object... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param sql Sql.
     */
    private void assertSqlUnsupported(final String sql) {
        Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() {
                execSql(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assert sqlE != null;

        if (IgniteQueryErrorCode.UNSUPPORTED_OPERATION != sqlE.statusCode()) {
            log.error("Unexpected exception", t);

            fail("Unexpected exception. See above");
        }
    }

    /**
     * Test for unsupported SQL statements.
     *
     * @throws Exception On fails.
     */
    @Test
    public void testUnsupportedSqlStatements() throws Exception {
        startGrid(getConfiguration());

//        assertSqlUnsupported("CREATE SCHEMA my_schema");
//        assertSqlUnsupported("DROP SCHEMA my_schema");
//
//        assertSqlUnsupported("RUNSCRIPT FROM 'q.sql'");
//        assertSqlUnsupported("SCRIPT NODATA");
//
//        assertSqlUnsupported("SCRIPT NODATA");

        assertSqlUnsupported("SHOW SCHEMAS");
        assertSqlUnsupported("SHOW TABLES");

//        assertSqlUnsupported("WITH RECURSIVE temp (n, fact) AS \n" +
//            "(SELECT 0, 1 \n" +
//            "  UNION ALL \n" +
//            " SELECT n+1, (n+1)*fact FROM temp  \n" +
//            "        WHERE n < 9)\n" +
//            "SELECT * FROM temp;");
//
//        execSql(
//            "WITH temp (A, B) AS (SELECT 1, 2) " +
//                "SELECT * from temp");
//
//        execSql(
//            "create table tree_sample ( " +
//                "id integer primary key, " +
//                "id_parent integer, " +
//                "nm varchar)");
//
//        assertSqlUnsupported("with recursive tree (nm, id, level, pathstr)\n" +
//            "as (select nm, id, 0, cast('' as text)\n" +
//            "   from tree_sample\n" +
//            "   where id_parent is null\n" +
//            "union all\n" +
//            "   select tree_sample.nm, tree_sample.id, tree.level + 1, tree.pathstr + tree_sample.nm\n" +
//            "   from tree_sample\n" +
//            "     inner join tree on tree.id = tree_sample.id_parent)\n" +
//            "select id, space( level ) + nm as nm\n" +
//            "from tree\n" +
//            "order by pathstr");
    }
}
