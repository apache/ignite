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

package org.apache.ignite.internal.processors.sql;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class IgniteSQLColumnConstraintsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        execSql("CREATE TABLE varchar_table(id INT PRIMARY KEY, str VARCHAR(5))");

        //execSql("INSERT INTO varchar_table VALUES(?, ?)", 1, "12345");

        //TODO: check DmlStatementsProcess#798
        execSql("CREATE TABLE char_table(id INT PRIMARY KEY, str CHAR(5))");

        //execSql("INSERT INTO char_table VALUES(?, ?)", 1, "12345");
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertTooLongVarchar() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            execSql("INSERT INTO varchar_table VALUES(?, ?)", 2, "123456");
            
            return 0;
        }, IgniteException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertTooLongChar() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
                execSql("INSERT INTO char_table VALUES(?, ?)", 2, "123456");

                return 0;
        }, IgniteException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateTooLongVarchar() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            execSql("UPDATE varchar_table SET str = ? WHERE id = ?", "123456", 1);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateTooLongChar() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            execSql("UPDATE char_table SET str = ? WHERE id = ?", "123456", 1);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeTooLongVarchar() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            execSql("MERGE INTO varchar_table(id, str) VALUES(?, ?)", 1, "123456");

            return 0;
        }, IgniteException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeTooLongChar() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            execSql("MERGE INTO char_table(id, str) VALUES(?, ?)", 1, "123456");

            return 0;
        }, IgniteException.class);
    }

    /** */
    private List<?> execSql(String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args);

        return grid(0).context().query().querySqlFields(qry, true).getAll();
    }

    /**
     * Organization.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** ID. */
        private final int id;

        /** Name. */
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
