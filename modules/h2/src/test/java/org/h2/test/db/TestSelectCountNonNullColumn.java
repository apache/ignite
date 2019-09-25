/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * Test that count(column) is converted to count(*) if the column is not
 * nullable.
 */
public class TestSelectCountNonNullColumn extends TestBase {

    private static final String DBNAME = "selectCountNonNullColumn";
    private Statement stat;

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

        deleteDb(DBNAME);
        Connection conn = getConnection(DBNAME);
        stat = conn.createStatement();

        stat.execute("CREATE TABLE SIMPLE(KEY VARCHAR(25) " +
                "PRIMARY KEY, NAME VARCHAR(25))");
        stat.execute("INSERT INTO SIMPLE(KEY) VALUES('k1')");
        stat.execute("INSERT INTO SIMPLE(KEY,NAME) VALUES('k2','name2')");

        checkKeyCount(-1);
        checkNameCount(-1);
        checkStarCount(-1);

        checkKeyCount(2);
        checkNameCount(1);
        checkStarCount(2);

        conn.close();

    }

    private void checkStarCount(long expect) throws SQLException {
        String sql = "SELECT COUNT(*) FROM SIMPLE";
        if (expect < 0) {
            sql = "EXPLAIN " + sql;
        }
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        if (expect >= 0) {
            assertEquals(expect, rs.getLong(1));
        } else {
            // System.out.println(rs.getString(1));
            assertEquals("SELECT\n" + "    COUNT(*)\n" + "FROM PUBLIC.SIMPLE\n"
                    + "    /* PUBLIC.SIMPLE.tableScan */\n"
                    + "/* direct lookup */", rs.getString(1));
        }
    }

    private void checkKeyCount(long expect) throws SQLException {
        String sql = "SELECT COUNT(KEY) FROM SIMPLE";
        if (expect < 0) {
            sql = "EXPLAIN " + sql;
        }
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        if (expect >= 0) {
            assertEquals(expect, rs.getLong(1));
        } else {
            assertEquals("SELECT\n"
                    + "    COUNT(KEY)\n"
                    + "FROM PUBLIC.SIMPLE\n"
                    + "    /* PUBLIC.PRIMARY_KEY_9 */\n"
                    + "/* direct lookup */", rs.getString(1));
        }
    }

    private void checkNameCount(long expect) throws SQLException {
        String sql = "SELECT COUNT(NAME) FROM SIMPLE";
        if (expect < 0) {
            sql = "EXPLAIN " + sql;
        }
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        if (expect >= 0) {
            assertEquals(expect, rs.getLong(1));
        } else {
            // System.out.println(rs.getString(1));
            assertEquals("SELECT\n" + "    COUNT(NAME)\n" + "FROM PUBLIC.SIMPLE\n"
                    + "    /* PUBLIC.SIMPLE.tableScan */", rs.getString(1));
        }
    }

}
