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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for unsupported SQL statements.
 */
public class SqlUnsupportedSelfTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        startGrid(getConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test for unsupported SQL statements in CREATE TABLE statement.
     *
     * @throws Exception On fails.
     */
    @Test
    public void testUnsupportedCreateTable() throws Exception {
        assertSqlUnsupported("CREATE MEMORY TABLE unsupported_tbl0 (id integer primary key, val integer)");
        assertSqlUnsupported("CREATE GLOBAL TEMPORARY TABLE unsupported_tbl1 (id integer primary key, val integer)");
        assertSqlUnsupported("CREATE LOCAL TEMPORARY TABLE unsupported_tbl2 (id integer primary key, val integer)");
        assertSqlUnsupported("CREATE TEMPORARY TABLE unsupported_tbl3 (id integer primary key, val integer)");
        assertSqlUnsupported("CREATE TABLE unsupported_tbl4 (id integer primary key, val integer) HIDDEN");
    }

    /**
     * Test for unsupported SQL statements in CREATE TABLE statement.
     */
    @Test
    public void testUnsupportedMerge() {
        execSql(
            "create table test ( " +
                "id integer primary key, " +
                "val varchar DEFAULT 'test_val')");

        assertSqlUnsupported("MERGE INTO test (id, val) VALUES (0, DEFAULT)");
    }

    /**
     * Test for unsupported SQL statements in CREATE TABLE statement.
     */
    @Test
    public void testUnsupportedInsert() {
        execSql(
            "create table test ( " +
                "id integer primary key, " +
                "val varchar DEFAULT 'test_val')");

        assertSqlUnsupported("INSERT INTO test (id, val) VALUES (0, DEFAULT)");
    }

    /**
     * Test for unsupported SQL statements in CREATE TABLE statement.
     */
    @Test
    public void testUnsupportedAlterTableAlterColumn() {
        execSql(
            "CREATE TABLE test ( " +
                "id integer primary key, " +
                "val varchar DEFAULT 'test_val')");

        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val SELECTIVITY 1");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val SET DEFAULT 'new val'");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val DROP DEFAULT");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val SET ON UPDATE 'new val'");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val DROP ON UPDATE");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val SET NULL");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val SET NOT NULL");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val SET VISIBLE");
        assertSqlUnsupported("ALTER TABLE test ALTER COLUMN val SET INVISIBLE");

        assertSqlUnsupported("ALTER TABLE test ADD COLUMN (q integer) FIRST");
    }

    /**
     * Test for unsupported SQL statements in CREATE TABLE statement.
     */
    @Test
    public void testUnsupportedCTE() {
        // Simple CTE supports
        execSql(
            "WITH temp (A, B) AS (SELECT 1, 2) " +
                "SELECT * FROM temp");

        assertSqlUnsupported(
            "WITH RECURSIVE temp (n, fact) AS " +
            "(SELECT 0, 1 " +
            "UNION ALL " +
            "SELECT n+1, (n+1)*fact FROM temp WHERE n < 9) " +
            "SELECT * FROM temp;");

        execSql(
            "CREATE TABLE test ( " +
                "id integer primary key, " +
                "parent integer DEFAULT 0, " +
                "nm varchar)");

        assertSqlUnsupported(
            "WITH RECURSIVE tree (nm, id, level, pathstr) AS " +
            "(SELECT nm, id, 0, CAST('' AS text) FROM test WHERE parent IS NULL " +
            "UNION ALL " +
            "SELECT test.nm, test.id, tree.level + 1, tree.pathstr + test.nm " +
            "FROM TEST " +
            "INNER JOIN tree ON tree.id = test.parent) " +
            "SELECT id, space( level ) + nm AS nm FROM tree ORDER BY pathstr");
    }

    /**
     * Test for unsupported SQL statements.
     */
    @Test
    public void testUnsupportedSqlStatements() {
        execSql(
            "create table test ( " +
                "id integer primary key, " +
                "val varchar DEFAULT 'test_val')");

        assertSqlUnsupported("CREATE SCHEMA my_schema");
        assertSqlUnsupported("DROP SCHEMA my_schema");
        assertSqlUnsupported("ALTER SCHEMA public RENAME TO private");

        assertSqlUnsupported("ANALYZE TABLE test");

        assertSqlUnsupported("ALTER INDEX idx0 RENAME TO idx1");

        assertSqlUnsupported("ALTER TABLE test RENAME TO new_test");

        assertSqlUnsupported("CREATE VIEW test_view AS SELECT * FROM test WHERE id < 100");
        assertSqlUnsupported("DROP VIEW test_view");

        assertSqlUnsupported("CREATE SEQUENCE SEQ_0");
        assertSqlUnsupported("DROP SEQUENCE SEQ_0");

        assertSqlUnsupported("CREATE TRIGGER trig_0 BEFORE INSERT ON TEST FOR EACH ROW CALL \"MyTrigger\"");
        assertSqlUnsupported("DROP TRIGGER trig_0");

        assertSqlUnsupported("CREATE ROLE newRole");
        assertSqlUnsupported("DROP ROLE newRole");

        assertSqlUnsupported("RUNSCRIPT FROM 'q.sql'");
        assertSqlUnsupported("SCRIPT NODATA");
        assertSqlUnsupported("SCRIPT NODATA");
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
}
