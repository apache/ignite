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
import java.util.ArrayList;

import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Tests for the two-phase-commit feature.
 */
public class TestTwoPhaseCommit extends TestBase {

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
        if (config.memory || config.networked) {
            return;
        }

        deleteDb("twoPhaseCommit");

        prepare();
        openWith(true);
        test(true);

        prepare();
        openWith(false);
        test(false);

        if (!config.mvStore) {
            testLargeTransactionName();
        }
        deleteDb("twoPhaseCommit");
    }

    private void testLargeTransactionName() throws SQLException {
        Connection conn = getConnection("twoPhaseCommit");
        Statement stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("CREATE TABLE TEST2(ID INT)");
        String name = "tx12345678";
        try {
            while (true) {
                stat.execute("INSERT INTO TEST2 VALUES(1)");
                name += "x";
                stat.execute("PREPARE COMMIT " + name);
            }
        } catch (SQLException e) {
            assertKnownException(e);
        }
        conn.close();
    }

    private void test(boolean rolledBack) throws SQLException {
        Connection conn = getConnection("twoPhaseCommit");
        Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        if (!rolledBack) {
            rs.next();
            assertEquals(2, rs.getInt(1));
            assertEquals("World", rs.getString(2));
        }
        assertFalse(rs.next());
        conn.close();
    }

    private void openWith(boolean rollback) throws SQLException {
        Connection conn = getConnection("twoPhaseCommit");
        Statement stat = conn.createStatement();
        ArrayList<String> list = New.arrayList();
        ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT");
        while (rs.next()) {
            list.add(rs.getString("TRANSACTION"));
        }
        for (String s : list) {
            if (rollback) {
                stat.execute("ROLLBACK TRANSACTION " + s);
            } else {
                stat.execute("COMMIT TRANSACTION " + s);
            }
        }
        conn.close();
    }

    private void prepare() throws SQLException {
        deleteDb("twoPhaseCommit");
        Connection conn = getConnection("twoPhaseCommit");
        Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        conn.setAutoCommit(false);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.commit();
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        stat.execute("PREPARE COMMIT XID_TEST_TRANSACTION_WITH_LONG_NAME");
        crash(conn);
    }
}
