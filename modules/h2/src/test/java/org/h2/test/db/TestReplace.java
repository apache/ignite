/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Cemo
 */
package org.h2.test.db;

import org.h2.test.TestBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test the MySQL-compatibility REPLACE command.
 *
 * @author Cemo
 */
public class TestReplace extends TestBase {

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
        deleteDb("replace");
        Connection conn = getConnection("replace");
        testReplace(conn);
        conn.close();
        deleteDb("replace");
    }

    private void testReplace(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE TABLE_WORD (" +
                "  WORD_ID int(11) NOT NULL AUTO_INCREMENT," +
                "  WORD varchar(128) NOT NULL," +
                "  PRIMARY KEY (WORD_ID)" +
                ");");

        stat.execute("REPLACE INTO TABLE_WORD " +
                "( WORD ) VALUES ('aaaaaaaaaa')");
        stat.execute("REPLACE INTO TABLE_WORD " +
                "( WORD ) VALUES ('bbbbbbbbbb')");
        stat.execute("REPLACE INTO TABLE_WORD " +
                "( WORD_ID, WORD ) VALUES (3, 'cccccccccc')");

        rs = stat.executeQuery("SELECT WORD " +
                "FROM TABLE_WORD where WORD_ID = 1");
        rs.next();
        assertEquals("aaaaaaaaaa", rs.getNString(1));

        stat.execute("REPLACE INTO TABLE_WORD " +
                "(  WORD_ID, WORD ) VALUES (1, 'REPLACED')");
        rs = stat.executeQuery("SELECT WORD FROM TABLE_WORD where WORD_ID = 1");
        rs.next();
        assertEquals("REPLACED", rs.getNString(1));
    }

}
