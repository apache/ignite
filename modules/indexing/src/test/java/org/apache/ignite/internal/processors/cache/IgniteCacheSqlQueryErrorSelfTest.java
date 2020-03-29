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

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Java API query error messages test.
 */
public class IgniteCacheSqlQueryErrorSelfTest  extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * Checks wrong table name select error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSelectWrongTable() throws Exception {
        checkSqlErrorMessage("select from wrong",
            "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name select error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSelectWrongColumnName() throws Exception {
        checkSqlErrorMessage("select wrong from test",
            "Failed to parse query. Column \"WRONG\" not found");
    }

    /**
     * Checks wrong syntax select error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSelectWrongSyntax() throws Exception {
        checkSqlErrorMessage("select from test where",
            "Failed to parse query. Syntax error in SQL statement \"SELECT FROM TEST WHERE[*]");
    }

    /**
     * Checks wrong table name DML error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDmlWrongTable() throws Exception {
        checkSqlErrorMessage("insert into wrong (id, val) values (3, 'val3')",
            "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("merge into wrong (id, val) values (3, 'val3')",
            "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("update wrong set val = 'val3' where id = 2",
            "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("delete from wrong where id = 2",
            "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name DML error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDmlWrongColumnName() throws Exception {
        checkSqlErrorMessage("insert into test (id, wrong) values (3, 'val3')",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("merge into test (id, wrong) values (3, 'val3')",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("update test set wrong = 'val3' where id = 2",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("delete from test where wrong = 2",
            "Failed to parse query. Column \"WRONG\" not found");
    }

    /**
     * Checks wrong syntax DML error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDmlWrongSyntax() throws Exception {
        checkSqlErrorMessage("insert test (id, val) values (3, 'val3')",
            "Failed to parse query. Syntax error in SQL statement \"INSERT TEST[*] (ID, VAL)");

        checkSqlErrorMessage("merge test (id, val) values (3, 'val3')",
            "Failed to parse query. Syntax error in SQL statement \"MERGE TEST[*] (ID, VAL)");

        checkSqlErrorMessage("update test val = 'val3' where id = 2",
            "Failed to parse query. Syntax error in SQL statement \"UPDATE TEST VAL =[*] 'val3' WHERE ID = 2");

        checkSqlErrorMessage("delete from test 1where id = 2",
            "Failed to parse query. Syntax error in SQL statement \"DELETE FROM TEST 1[*]WHERE ID = 2 ");
    }

    /**
     * Checks wrong table name DDL error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDdlWrongTable() throws Exception {
        checkSqlErrorMessage("create table test (id int primary key, val varchar)",
            "Table already exists: TEST");

        checkSqlErrorMessage("drop table wrong",
            "Table doesn't exist: WRONG");

        checkSqlErrorMessage("create index idx1 on wrong (val)",
            "Table doesn't exist: WRONG");

        checkSqlErrorMessage("drop index wrong",
            "Index doesn't exist: WRONG");

        checkSqlErrorMessage("alter table wrong drop column val",
            "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name DDL error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDdlWrongColumnName() throws Exception {
        checkSqlErrorMessage("create index idx1 on test (wrong)",
            "Column doesn't exist: WRONG");

        checkSqlErrorMessage("alter table test drop column wrong",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("create table test(id integer primary key, AgE integer, AGe integer)",
            "Duplicate column name: AGE");

        checkSqlErrorMessage("create table test(\"id\" integer primary key, \"age\" integer, \"age\" integer)",
            "Duplicate column name: age");

        checkSqlErrorMessage("create table test(id integer primary key, age integer, age varchar)",
            "Duplicate column name: AGE");
    }

    /**
     * Checks wrong syntax DDL error message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDdlWrongSyntax() throws Exception {
        checkSqlErrorMessage("create table wrong (id int wrong key, val varchar)",
            "Failed to parse query. Syntax error in SQL statement \"CREATE TABLE WRONG (ID INT WRONG[*]");

        checkSqlErrorMessage("drop table test on",
            "Failed to parse query. Syntax error in SQL statement \"DROP TABLE TEST ON[*]");

        checkSqlErrorMessage("create index idx1 test (val)",
            "Failed to parse query. Syntax error in SQL statement \"CREATE INDEX IDX1 TEST[*]");

        checkSqlErrorMessage("drop index",
            "Failed to parse query. Syntax error in SQL statement \"DROP INDEX [*]");

        checkSqlErrorMessage("alter table test drop column",
            "Failed to parse query. Syntax error in SQL statement \"ALTER TABLE TEST DROP COLUMN [*]");
    }

    /**
     * Checks SQL error message.
     *
     * @param sql SQL command.
     * @param expMsg Expected error message.
     */
    private void checkSqlErrorMessage(final String sql, String expMsg) {
        execute("DROP TABLE IF EXISTS wrong");
        execute("DROP TABLE IF EXISTS test");

        execute("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");

        execute("INSERT INTO test (id, val) VALUES (1, 'val1')");
        execute("INSERT INTO test (id, val) VALUES (2, 'val2')");

        GridTestUtils.assertThrows(null, new Callable<Object>() {

            @Override public Object call() throws Exception {
                execute(sql);

                fail("Exception is expected");

                return null;
            }
        }, CacheException.class, expMsg);
    }

    /**
     * Executes SQL command.
     *
     * @param sql SQL command.
     */
    private void execute(String sql) {
       jcache().query(new SqlFieldsQuery(sql).setSchema("PUBLIC")).getAll();
    }

}
