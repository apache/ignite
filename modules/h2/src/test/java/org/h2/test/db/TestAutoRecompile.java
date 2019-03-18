/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests if prepared statements are re-compiled when required.
 */
public class TestAutoRecompile extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("autoRecompile");
        Connection conn = getConnection("autoRecompile");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST");
        assertEquals(1, prep.executeQuery().getMetaData().getColumnCount());
        stat.execute("ALTER TABLE TEST ADD COLUMN NAME VARCHAR(255)");
        assertEquals(2, prep.executeQuery().getMetaData().getColumnCount());
        stat.execute("DROP TABLE TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, X INT, Y INT)");
        assertEquals(3, prep.executeQuery().getMetaData().getColumnCount());
        // TODO test auto-recompile with insert..select, views and so on

        prep = conn.prepareStatement("INSERT INTO TEST VALUES(1, 2, 3)");
        stat.execute("ALTER TABLE TEST ADD COLUMN Z INT");
        assertThrows(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH, prep).execute();
        assertThrows(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH, prep).execute();
        conn.close();
        deleteDb("autoRecompile");
    }

}
