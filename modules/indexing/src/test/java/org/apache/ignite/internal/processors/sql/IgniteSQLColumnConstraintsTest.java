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

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.odbc.SqlStateCode.CONSTRAINT_VIOLATION;

/**
 */
public class IgniteSQLColumnConstraintsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        execSQL("CREATE TABLE varchar_table(id INT PRIMARY KEY, str VARCHAR(5))");

        execSQL("INSERT INTO varchar_table VALUES(?, ?)", 1, "12345");

        execSQL("CREATE TABLE decimal_table(id INT PRIMARY KEY, val DECIMAL(4, 2))");

        execSQL("INSERT INTO decimal_table VALUES(?, ?)", 1, 12.34);

        execSQL("CREATE TABLE char_table(id INT PRIMARY KEY, str CHAR(5))");

        execSQL("INSERT INTO char_table VALUES(?, ?)", 1, "12345");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateTableWithTooLongCharDefault() throws Exception {
        checkSQLThrows("CREATE TABLE too_long_default(id INT PRIMARY KEY, str CHAR(5) DEFAULT '123456')");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateTableWithTooLongScaleDecimalDefault() throws Exception {
        checkSQLThrows("CREATE TABLE too_long_decimal_default_scale(id INT PRIMARY KEY, val DECIMAL(4,2) DEFAULT 1.345)");
    }

    public void testCreateTableWithTooLongDecimalDefault() throws Exception {
        checkSQLThrows("CREATE TABLE too_long_decimal_default(id INT PRIMARY KEY, val DECIMAL(4,2) NOT NULL DEFAULT 123.45)");
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertTooLongDecimal() throws Exception {
        checkSQLThrows("INSERT INTO decimal_table VALUES(?, ?)", 2, 123.45);

        checkSQLThrows("UPDATE decimal_table SET val = ? WHERE id = ?", 123.45, 1);

        checkSQLThrows("MERGE INTO decimal_table(id, val) VALUES(?, ?)", 1, 123.45);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertTooLongScaleDecimal() throws Exception {
        checkSQLThrows("INSERT INTO decimal_table VALUES(?, ?)", 3, 1.234);

        checkSQLThrows("UPDATE decimal_table SET val = ? WHERE id = ?", 1.234, 1);

        checkSQLThrows("MERGE INTO decimal_table(id, val) VALUES(?, ?)", 1, 1.234);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertTooLongVarchar() throws Exception {
        checkSQLThrows("INSERT INTO varchar_table VALUES(?, ?)", 2, "123456");

        checkSQLThrows("UPDATE varchar_table SET str = ? WHERE id = ?", "123456", 1);

        checkSQLThrows("MERGE INTO varchar_table(id, str) VALUES(?, ?)", 1, "123456");
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertTooLongChar() throws Exception {
        checkSQLThrows("INSERT INTO char_table VALUES(?, ?)", 2, "123456");

        checkSQLThrows("UPDATE char_table SET str = ? WHERE id = ?", "123456", 1);

        checkSQLThrows("MERGE INTO char_table(id, str) VALUES(?, ?)", 1, "123456");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharConstraintsAfterAlterTable() throws Exception {
        execSQL("CREATE TABLE char_table_2(id INT PRIMARY KEY, field INTEGER)");

        execSQL("ALTER TABLE char_table_2 ADD COLUMN str CHAR(5) NOT NULL");

        execSQL("INSERT INTO char_table_2(id, str) VALUES(?, ?)", 1, "1");

        checkSQLThrows("INSERT INTO char_table_2(id, str) VALUES(?, ?)", 2, "123456");

        checkSQLThrows("UPDATE char_table_2 SET str = ? WHERE id = ?", "123456", 1);

        checkSQLThrows("MERGE INTO char_table_2(id, str) VALUES(?, ?)", 1, "123456");
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimalConstraintsAfterAlterTable() throws Exception {
        execSQL("CREATE TABLE decimal_table_2(id INT PRIMARY KEY, field INTEGER)");

        execSQL("ALTER TABLE decimal_table_2 ADD COLUMN val DECIMAL(4, 2) NOT NULL");

        execSQL("INSERT INTO decimal_table_2(id, val) VALUES(?, ?)", 1, 12.34);

        checkSQLThrows("INSERT INTO cdecimal_table_2(id, val) VALUES(?, ?)", 2, 12.3456);

        checkSQLThrows("UPDATE decimal_table_2 SET val = ? WHERE id = ?", 12.3456, 1);

        checkSQLThrows("MERGE INTO decimal_table_2(id, val) VALUES(?, ?)", 1, 12.3456);

        checkSQLThrows("INSERT INTO cdecimal_table_2(id, val) VALUES(?, ?)", 3, 1.234);

        checkSQLThrows("UPDATE decimal_table_2 SET val = ? WHERE id = ?", 1.234, 1);

        checkSQLThrows("MERGE INTO decimal_table_2(id, val) VALUES(?, ?)", 1, 1.234);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharDropColumnWithConstraint() throws Exception {
        execSQL("CREATE TABLE char_table_3(id INT PRIMARY KEY, field CHAR(5), field2 INTEGER)");

        execSQL("INSERT INTO char_table_3(id, field, field2) VALUES(?, ?, ?)", 1, "12345", 1);

        checkSQLThrows("INSERT INTO char_table_3(id, field, field2) VALUES(?, ?, ?)", 2, "123456", 1);

        execSQL("ALTER TABLE char_table_3 DROP COLUMN field");

        execSQL("INSERT INTO char_table_3(id, field2) VALUES(?, ?)", 3, 3);
    }

    public void testDecimalDropColumnWithConstraint() throws Exception {
        execSQL("CREATE TABLE decimal_table_3(id INT PRIMARY KEY, field DECIMAL(4, 2), field2 INTEGER)");

        execSQL("INSERT INTO decimal_table_3(id, field, field2) VALUES(?, ?, ?)", 1, 12.34, 1);

        checkSQLThrows("INSERT INTO decimal_table_3(id, field, field2) VALUES(?, ?, ?)", 2, 12.3456, 1);

        execSQL("ALTER TABLE decimal_table_3 DROP COLUMN field");

        execSQL("INSERT INTO decimal_table_3(id, field2) VALUES(?, ?)", 3, 3);
    }

    public void testCharSqlState() throws Exception {
        execSQL("CREATE TABLE char_table_4(id INT PRIMARY KEY, field CHAR(5))");

        IgniteSQLException err = (IgniteSQLException)
            checkSQLThrows("INSERT INTO char_table_4(id, field) VALUES(?, ?)", 1, "123456");

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);

        execSQL("INSERT INTO char_table_4(id, field) VALUES(?, ?)", 2, "12345");

        err = (IgniteSQLException)
            checkSQLThrows("UPDATE char_table_4 SET field = ? WHERE id = ?", "123456", 2);

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);

        err = (IgniteSQLException)
            checkSQLThrows("MERGE INTO char_table_4(id, field) VALUES(?, ?)", 2, "123456");

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);
    }

    public void testDecimalSqlState() throws Exception {
        execSQL("CREATE TABLE decimal_table_4(id INT PRIMARY KEY, field DECIMAL(4, 2))");

        IgniteSQLException err = (IgniteSQLException)
            checkSQLThrows("INSERT INTO decimal_table_4(id, field) VALUES(?, ?)", 1, 12.3456);

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);

        err = (IgniteSQLException)
            checkSQLThrows("INSERT INTO decimal_table_4(id, field) VALUES(?, ?)", 1, 1.345);

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);

        execSQL("INSERT INTO decimal_table_4(id, field) VALUES(?, ?)", 2, 12.34);

        err = (IgniteSQLException)
            checkSQLThrows("UPDATE decimal_table_4 SET field = ? WHERE id = ?", 12.3456, 2);

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);

        err = (IgniteSQLException)
            checkSQLThrows("MERGE INTO decimal_table_4(id, field) VALUES(?, ?)", 2, 12.3456);

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);

        err = (IgniteSQLException)
            checkSQLThrows("UPDATE decimal_table_4 SET field = ? WHERE id = ?", 1.345, 2);

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);

        err = (IgniteSQLException)
            checkSQLThrows("MERGE INTO decimal_table_4(id, field) VALUES(?, ?)", 2, 1.345);

        assertEquals(err.sqlState(), CONSTRAINT_VIOLATION);
    }

    /** */
    private Throwable checkSQLThrows(String sql, Object... args) {
        return GridTestUtils.assertThrowsWithCause(() -> {
            execSQL(sql, args);

            return 0;
        }, IgniteSQLException.class);
    }

    /** */
    private List<?> execSQL(String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args);

        return grid(0).context().query().querySqlFields(qry, true).getAll();
    }
}
