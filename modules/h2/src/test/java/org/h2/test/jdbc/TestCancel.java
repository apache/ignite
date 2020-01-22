/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests Statement.cancel
 */
public class TestCancel extends TestBase {

    private static int lastVisited;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    /**
     * This thread cancels a statement after some time.
     */
    static class CancelThread extends Thread {
        private final Statement cancel;
        private final int wait;
        private volatile boolean stop;

        CancelThread(Statement cancel, int wait) {
            this.cancel = cancel;
            this.wait = wait;
        }

        /**
         * Stop the test now.
         */
        public void stopNow() {
            this.stop = true;
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(wait);
                    cancel.cancel();
                    Thread.yield();
                } catch (SQLException e) {
                    // ignore errors on closed statements
                } catch (Exception e) {
                    TestBase.logError("sleep", e);
                }
            }
        }
    }

    @Override
    public void test() throws Exception {
        testQueryTimeoutInTransaction();
        testReset();
        testMaxQueryTimeout();
        testQueryTimeout();
        testJdbcQueryTimeout();
        testCancelStatement();
        deleteDb("cancel");
    }

    private void testReset() throws SQLException {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        stat.execute("set query_timeout 1");
        assertThrows(ErrorCode.STATEMENT_WAS_CANCELED, stat).
                execute("select count(*) from system_range(1, 1000000), " +
                        "system_range(1, 1000000)");
        stat.execute("set query_timeout 0");
        stat.execute("select count(*) from system_range(1, 1000), " +
                "system_range(1, 1000)");
        conn.close();
    }

    private void testQueryTimeoutInTransaction() throws SQLException {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        conn.setAutoCommit(false);
        stat.execute("INSERT INTO TEST VALUES(1)");
        Savepoint sp = conn.setSavepoint();
        stat.execute("INSERT INTO TEST VALUES(2)");
        stat.setQueryTimeout(1);
        conn.rollback(sp);
        conn.commit();
        conn.close();
    }

    private void testJdbcQueryTimeout() throws SQLException {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        assertEquals(0, stat.getQueryTimeout());
        stat.setQueryTimeout(1);
        assertEquals(1, stat.getQueryTimeout());
        Statement s2 = conn.createStatement();
        assertEquals(1, s2.getQueryTimeout());
        ResultSet rs = s2.executeQuery("SELECT VALUE " +
                "FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = 'QUERY_TIMEOUT'");
        rs.next();
        assertEquals(1000, rs.getInt(1));
        assertThrows(ErrorCode.STATEMENT_WAS_CANCELED, stat).
                executeQuery("SELECT MAX(RAND()) " +
                        "FROM SYSTEM_RANGE(1, 100000000)");
        stat.setQueryTimeout(0);
        stat.execute("SET QUERY_TIMEOUT 1100");
        // explicit changes are not detected except, as documented
        assertEquals(0, stat.getQueryTimeout());
        conn.close();
    }

    private void testQueryTimeout() throws SQLException {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        stat.execute("SET QUERY_TIMEOUT 10");
        assertThrows(ErrorCode.STATEMENT_WAS_CANCELED, stat).
                executeQuery("SELECT MAX(RAND()) " +
                        "FROM SYSTEM_RANGE(1, 100000000)");
        conn.close();
    }

    private void testMaxQueryTimeout() throws SQLException {
        deleteDb("cancel");
        Connection conn = getConnection("cancel;MAX_QUERY_TIMEOUT=10");
        Statement stat = conn.createStatement();
        assertThrows(ErrorCode.STATEMENT_WAS_CANCELED, stat).
                executeQuery("SELECT MAX(RAND()) " +
                        "FROM SYSTEM_RANGE(1, 100000000)");
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param x the value
     * @return the value
     */
    public static int visit(int x) {
        lastVisited = x;
        return x;
    }

    private void testCancelStatement() throws Exception {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE  ALIAS VISIT FOR \"" + getClass().getName() + ".visit\"");
        stat.execute("CREATE  MEMORY TABLE TEST" +
                "(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        trace("insert");
        int len = getSize(10, 1000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            // prep.setString(2, "Test Value "+i);
            prep.setString(2, "hi");
            prep.execute();
        }
        trace("inserted");
        // TODO test insert into ... select
        for (int i = 1;;) {
            Statement query = conn.createStatement();
            CancelThread cancel = new CancelThread(query, i);
            visit(0);
            cancel.start();
            try {
                Thread.yield();
                assertThrows(ErrorCode.STATEMENT_WAS_CANCELED, query,
                        "SELECT VISIT(ID), (SELECT SUM(X) " +
                        "FROM SYSTEM_RANGE(1, 100000) WHERE X<>ID) FROM TEST ORDER BY ID");
            } finally {
                cancel.stopNow();
                cancel.join();
            }
            if (lastVisited == 0) {
                i += 10;
            } else {
                break;
            }
        }
        conn.close();
    }

}
