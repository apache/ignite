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

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.odbc.SqlStateCode.CONSTRAINT_VIOLATION;
import static org.apache.ignite.internal.processors.odbc.SqlStateCode.INTERNAL_ERROR;

/**
 */
public class IgniteSQLColumnConstraintsTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        String mvccQry = mvccEnabled() ? " WITH \"atomicity=transactional_snapshot\"" : "";

        runSQL("CREATE TABLE varchar_table(id INT PRIMARY KEY, str VARCHAR(5))" + mvccQry);

        execSQL("INSERT INTO varchar_table VALUES(?, ?)", 1, "12345");

        checkSQLResults("SELECT * FROM varchar_table WHERE id = 1", 1, "12345");

        runSQL("CREATE TABLE decimal_table(id INT PRIMARY KEY, val DECIMAL(4, 2))" + mvccQry);

        execSQL("INSERT INTO decimal_table VALUES(?, ?)", 1, 12.34);

        checkSQLResults("SELECT * FROM decimal_table WHERE id = 1", 1, BigDecimal.valueOf(12.34));

        runSQL("CREATE TABLE char_table(id INT PRIMARY KEY, str CHAR(5))" + mvccQry);

        execSQL("INSERT INTO char_table VALUES(?, ?)", 1, "12345");

        checkSQLResults("SELECT * FROM char_table WHERE id = 1", 1, "12345");

        runSQL("CREATE TABLE decimal_table_4(id INT PRIMARY KEY, field DECIMAL(4, 2))" + mvccQry);

        runSQL("CREATE TABLE char_table_2(id INT PRIMARY KEY, field INTEGER)" + mvccQry);

        runSQL("CREATE TABLE decimal_table_2(id INT PRIMARY KEY, field INTEGER)" + mvccQry);

        runSQL("CREATE TABLE char_table_3(id INT PRIMARY KEY, field CHAR(5), field2 INTEGER)" + mvccQry);

        runSQL("CREATE TABLE decimal_table_3(id INT PRIMARY KEY, field DECIMAL(4, 2), field2 INTEGER)" + mvccQry);

        runSQL("CREATE TABLE char_table_4(id INT PRIMARY KEY, field CHAR(5))" + mvccQry);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateTableWithTooLongCharDefault() throws Exception {
        checkSQLThrows("CREATE TABLE too_long_default(id INT PRIMARY KEY, str CHAR(5) DEFAULT '123456')",
            INTERNAL_ERROR);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateTableWithTooLongScaleDecimalDefault() throws Exception {
        checkSQLThrows("CREATE TABLE too_long_decimal_default_scale(id INT PRIMARY KEY, val DECIMAL(4, 2)" +
            " DEFAULT 1.345)", INTERNAL_ERROR);
    }

    @Test
    public void testCreateTableWithTooLongDecimalDefault() throws Exception {
        checkSQLThrows("CREATE TABLE too_long_decimal_default(id INT PRIMARY KEY, val DECIMAL(4, 2)" +
            " DEFAULT 123.45)", INTERNAL_ERROR);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertTooLongDecimal() throws Exception {
        checkSQLThrows("INSERT INTO decimal_table VALUES(?, ?)", CONSTRAINT_VIOLATION, 2, 123.45);

        assertTrue(execSQL("SELECT * FROM  decimal_table WHERE id = ?", 2).isEmpty());

        checkSQLThrows("UPDATE decimal_table SET val = ? WHERE id = ?", CONSTRAINT_VIOLATION, 123.45, 1);

        checkSQLResults("SELECT * FROM decimal_table WHERE id = 1", 1, BigDecimal.valueOf(12.34));

        checkSQLThrows("MERGE INTO decimal_table(id, val) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, 123.45);

        checkSQLResults("SELECT * FROM decimal_table WHERE id = 1", 1, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertTooLongScaleDecimal() throws Exception {
        checkSQLThrows("INSERT INTO decimal_table VALUES(?, ?)", CONSTRAINT_VIOLATION, 3, 1.234);

        assertTrue(execSQL("SELECT * FROM  decimal_table WHERE id = ?", 3).isEmpty());

        checkSQLThrows("UPDATE decimal_table SET val = ? WHERE id = ?", CONSTRAINT_VIOLATION, 1.234, 1);

        checkSQLResults("SELECT * FROM decimal_table WHERE id = 1", 1, BigDecimal.valueOf(12.34));

        checkSQLThrows("MERGE INTO decimal_table(id, val) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, 1.234);

        checkSQLResults("SELECT * FROM decimal_table WHERE id = 1", 1, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertTooLongVarchar() throws Exception {
        checkSQLThrows("INSERT INTO varchar_table VALUES(?, ?)", CONSTRAINT_VIOLATION, 2, "123456");

        assertTrue(execSQL("SELECT * FROM  varchar_table WHERE id = ?", 2).isEmpty());

        checkSQLThrows("UPDATE varchar_table SET str = ? WHERE id = ?", CONSTRAINT_VIOLATION, "123456", 1);

        checkSQLResults("SELECT * FROM varchar_table WHERE id = 1", 1, "12345");

        checkSQLThrows("MERGE INTO varchar_table(id, str) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, "123456");

        checkSQLResults("SELECT * FROM varchar_table WHERE id = 1", 1, "12345");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertTooLongChar() throws Exception {
        checkSQLThrows("INSERT INTO char_table VALUES(?, ?)", CONSTRAINT_VIOLATION, 2, "123456");

        assertTrue(execSQL("SELECT * FROM  char_table WHERE id = ?", 2).isEmpty());

        checkSQLThrows("UPDATE char_table SET str = ? WHERE id = ?", CONSTRAINT_VIOLATION, "123456", 1);

        checkSQLResults("SELECT * FROM char_table WHERE id = 1", 1, "12345");

        checkSQLThrows("MERGE INTO char_table(id, str) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, "123456");

        checkSQLResults("SELECT * FROM char_table WHERE id = 1", 1, "12345");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCharConstraintsAfterAlterTable() throws Exception {
        execSQL("ALTER TABLE char_table_2 ADD COLUMN str CHAR(5) NOT NULL");

        execSQL("INSERT INTO char_table_2(id, str) VALUES(?, ?)", 1, "1");

        checkSQLResults("SELECT * FROM char_table_2 WHERE id = 1", 1, null, "1");

        checkSQLThrows("INSERT INTO char_table_2(id, str) VALUES(?, ?)", CONSTRAINT_VIOLATION, 2, "123456");

        assertTrue(execSQL("SELECT * FROM decimal_table_2 WHERE id = ?", 2).isEmpty());

        checkSQLThrows("UPDATE char_table_2 SET str = ? WHERE id = ?", CONSTRAINT_VIOLATION, "123456", 1);

        checkSQLResults("SELECT * FROM char_table_2 WHERE id = 1", 1, null, "1");

        checkSQLThrows("MERGE INTO char_table_2(id, str) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, "123456");

        checkSQLResults("SELECT * FROM char_table_2 WHERE id = 1", 1, null, "1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalConstraintsAfterAlterTable() throws Exception {
        execSQL("ALTER TABLE decimal_table_2 ADD COLUMN val DECIMAL(4, 2) NOT NULL");

        execSQL("INSERT INTO decimal_table_2(id, val) VALUES(?, ?)", 1, 12.34);

        checkSQLResults("SELECT * FROM decimal_table_2 WHERE id = 1", 1, null, BigDecimal.valueOf(12.34));

        checkSQLThrows("INSERT INTO decimal_table_2(id, val) VALUES(?, ?)", CONSTRAINT_VIOLATION, 2, 1234.56);

        assertTrue(execSQL("SELECT * FROM decimal_table_2 WHERE id = ?", 2).isEmpty());

        checkSQLThrows("UPDATE decimal_table_2 SET val = ? WHERE id = ?", CONSTRAINT_VIOLATION, 1234.56, 1);

        checkSQLResults("SELECT * FROM decimal_table_2 WHERE id = 1", 1, null, BigDecimal.valueOf(12.34));

        checkSQLThrows("MERGE INTO decimal_table_2(id, val) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, 12345.6);

        checkSQLResults("SELECT * FROM decimal_table_2 WHERE id = 1", 1, null, BigDecimal.valueOf(12.34));

        checkSQLThrows("INSERT INTO decimal_table_2(id, val) VALUES(?, ?)", CONSTRAINT_VIOLATION, 3, 1.234);

        checkSQLResults("SELECT * FROM decimal_table_2 WHERE id = 1", 1, null, BigDecimal.valueOf(12.34));

        checkSQLThrows("UPDATE decimal_table_2 SET val = ? WHERE id = ?", CONSTRAINT_VIOLATION, 1.234, 1);

        checkSQLResults("SELECT * FROM decimal_table_2 WHERE id = 1", 1, null, BigDecimal.valueOf(12.34));

        checkSQLThrows("MERGE INTO decimal_table_2(id, val) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, 1.234);

        checkSQLResults("SELECT * FROM decimal_table_2 WHERE id = 1", 1, null, BigDecimal.valueOf(12.34));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCharDropColumnWithConstraint() throws Exception {
        execSQL("INSERT INTO char_table_3(id, field, field2) VALUES(?, ?, ?)", 1, "12345", 1);

        checkSQLResults("SELECT * FROM char_table_3 WHERE id = 1", 1, "12345", 1);

        checkSQLThrows("INSERT INTO char_table_3(id, field, field2) VALUES(?, ?, ?)", CONSTRAINT_VIOLATION,
            2, "123456", 1);

        assertTrue(execSQL("SELECT * FROM decimal_table_3 WHERE id = ?", 2).isEmpty());

        execSQL("ALTER TABLE char_table_3 DROP COLUMN field");

        execSQL("INSERT INTO char_table_3(id, field2) VALUES(?, ?)", 3, 3);

        checkSQLResults("SELECT * FROM char_table_3 WHERE id = 3", 3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalDropColumnWithConstraint() throws Exception {
        execSQL("INSERT INTO decimal_table_3(id, field, field2) VALUES(?, ?, ?)", 1, 12.34, 1);

        checkSQLResults("SELECT * FROM decimal_table_3 WHERE id = 1", 1, BigDecimal.valueOf(12.34), 1);

        checkSQLThrows("INSERT INTO decimal_table_3(id, field, field2) VALUES(?, ?, ?)", CONSTRAINT_VIOLATION,
            2, 12.3456, 1);

        assertTrue(execSQL("SELECT * FROM decimal_table_3 WHERE id = ?", 2).isEmpty());

        execSQL("ALTER TABLE decimal_table_3 DROP COLUMN field");

        execSQL("INSERT INTO decimal_table_3(id, field2) VALUES(?, ?)", 3, 3);

        checkSQLResults("SELECT * FROM decimal_table_3 WHERE id = 3", 3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCharSqlState() throws Exception {
        checkSQLThrows("INSERT INTO char_table_4(id, field) VALUES(?, ?)", CONSTRAINT_VIOLATION, 1, "123456");

        assertTrue(execSQL("SELECT * FROM decimal_table_4 WHERE id = ?", 1).isEmpty());

        execSQL("INSERT INTO char_table_4(id, field) VALUES(?, ?)", 2, "12345");

        checkSQLResults("SELECT * FROM char_table_4 WHERE id = 2", 2, "12345");

        checkSQLThrows("UPDATE char_table_4 SET field = ? WHERE id = ?", CONSTRAINT_VIOLATION, "123456", 2);

        checkSQLResults("SELECT * FROM char_table_4 WHERE id = 2", 2, "12345");

        checkSQLThrows("MERGE INTO char_table_4(id, field) VALUES(?, ?)", CONSTRAINT_VIOLATION, 2, "123456");

        checkSQLResults("SELECT * FROM char_table_4 WHERE id = 2", 2, "12345");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalSqlState() throws Exception {
        checkSQLThrows("INSERT INTO decimal_table_4 VALUES(?, ?)", CONSTRAINT_VIOLATION,
            1, BigDecimal.valueOf(1234.56));

        assertTrue(execSQL("SELECT * FROM decimal_table_4 WHERE id = ?", 1).isEmpty());

        checkSQLThrows("INSERT INTO decimal_table_4 VALUES(?, ?)", CONSTRAINT_VIOLATION,
            1, BigDecimal.valueOf(1.345));

        assertTrue(execSQL("SELECT * FROM decimal_table_4 WHERE id = ?", 1).isEmpty());

        execSQL("INSERT INTO decimal_table_4 (id, field) VALUES(?, ?)", 2, 12.34);

        checkSQLResults("SELECT * FROM decimal_table_4 WHERE id = 2", 2, BigDecimal.valueOf(12.34));

        checkSQLThrows("UPDATE decimal_table_4 SET field = ? WHERE id = ?", CONSTRAINT_VIOLATION,
            BigDecimal.valueOf(1234.56), 2);

        checkSQLResults("SELECT * FROM decimal_table_4 WHERE id = 2", 2, BigDecimal.valueOf(12.34));

        checkSQLThrows("MERGE INTO decimal_table_4(id, field) VALUES(?, ?)", CONSTRAINT_VIOLATION,
            2, BigDecimal.valueOf(1234.56));

        checkSQLResults("SELECT * FROM decimal_table_4 WHERE id = 2", 2, BigDecimal.valueOf(12.34));

        checkSQLThrows("UPDATE decimal_table_4 SET field = ? WHERE id = ?", CONSTRAINT_VIOLATION,
            BigDecimal.valueOf(1.345), 2);

        checkSQLResults("SELECT * FROM decimal_table_4 WHERE id = 2", 2, BigDecimal.valueOf(12.34));

        checkSQLThrows("MERGE INTO decimal_table_4(id, field) VALUES(?, ?)", CONSTRAINT_VIOLATION,
            2, BigDecimal.valueOf(1.345));

        checkSQLResults("SELECT * FROM decimal_table_4 WHERE id = 2", 2, BigDecimal.valueOf(12.34));
    }

    /** */
    protected void checkSQLThrows(String sql, String sqlStateCode, Object... args) {
        IgniteSQLException err = (IgniteSQLException)GridTestUtils.assertThrowsWithCause(() -> {
            execSQL(sql, args);

            return 0;
        }, IgniteSQLException.class);

        assertEquals(err.sqlState(), sqlStateCode);
    }

    /** */
    protected List<?> execSQL(String sql, Object... args) {
        return runSQL(sql, args);
    }

    /** */
    protected List<?> runSQL(String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args);

        return grid(0).context().query().querySqlFields(qry, true).getAll();
    }

    /** */
    protected void checkSQLResults(String sql, Object... args) {
        List<?> rows = execSQL(sql);

        assertNotNull(rows);

        assertTrue(!rows.isEmpty());

        assertEquals(rows.size(), 1);

        List<?> row = (List<?>)rows.get(0);

        assertEquals(row.size(), args.length);

        for (int i = 0; i < args.length; i++)
            assertTrue(args[i] + " != " + row.get(i), Objects.equals(args[i], row.get(i)));
    }

    /** */
    protected boolean mvccEnabled() {
        return false;
    }
}
