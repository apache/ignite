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
 * Tests the meta data tables information_schema.locks and sessions.
 */
public class TestSessionsLocks extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testCancelStatement();
        if (!config.mvcc) {
            testLocks();
        }
        deleteDb("sessionsLocks");
    }

    private void testLocks() throws SQLException {
        deleteDb("sessionsLocks");
        Connection conn = getConnection("sessionsLocks;MULTI_THREADED=1");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from information_schema.locks " +
                "order by session_id");
        assertFalse(rs.next());
        Connection conn2 = getConnection("sessionsLocks");
        Statement stat2 = conn2.createStatement();
        stat2.execute("create table test(id int primary key, name varchar)");
        conn2.setAutoCommit(false);
        stat2.execute("insert into test values(1, 'Hello')");
        rs = stat.executeQuery("select * from information_schema.locks " +
                "order by session_id");
        rs.next();
        assertEquals("PUBLIC", rs.getString("TABLE_SCHEMA"));
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        rs.getString("SESSION_ID");
        if (config.mvcc || config.mvStore) {
            assertEquals("READ", rs.getString("LOCK_TYPE"));
        } else {
            assertEquals("WRITE", rs.getString("LOCK_TYPE"));
        }
        assertFalse(rs.next());
        conn2.commit();
        conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        stat2.execute("SELECT * FROM TEST");
        rs = stat.executeQuery("select * from information_schema.locks " +
                "order by session_id");
        if (!config.mvcc && !config.mvStore) {
            rs.next();
            assertEquals("PUBLIC", rs.getString("TABLE_SCHEMA"));
            assertEquals("TEST", rs.getString("TABLE_NAME"));
            rs.getString("SESSION_ID");
            assertEquals("READ", rs.getString("LOCK_TYPE"));
        }
        assertFalse(rs.next());
        conn2.commit();
        rs = stat.executeQuery("select * from information_schema.locks " +
                "order by session_id");
        assertFalse(rs.next());
        conn.close();
        conn2.close();
    }

    private void testCancelStatement() throws Exception {
        deleteDb("sessionsLocks");
        Connection conn = getConnection("sessionsLocks;MULTI_THREADED=1");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from information_schema.sessions " +
                "order by SESSION_START, ID");
        rs.next();
        int sessionId = rs.getInt("ID");
        rs.getString("USER_NAME");
        rs.getTimestamp("SESSION_START");
        rs.getString("STATEMENT");
        rs.getTimestamp("STATEMENT_START");
        assertFalse(rs.next());
        Connection conn2 = getConnection("sessionsLocks");
        final Statement stat2 = conn2.createStatement();
        rs = stat.executeQuery("select * from information_schema.sessions " +
                "order by SESSION_START, ID");
        assertTrue(rs.next());
        assertEquals(sessionId, rs.getInt("ID"));
        assertTrue(rs.next());
        int otherId = rs.getInt("ID");
        assertTrue(otherId != sessionId);
        assertFalse(rs.next());
        stat2.execute("set throttle 1");
        final boolean[] done = { false };
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    stat2.execute("select count(*) from " +
                            "system_range(1, 10000000) t1, system_range(1, 10000000) t2");
                    new Error("Unexpected success").printStackTrace();
                } catch (SQLException e) {
                    done[0] = true;
                }
            }
        };
        new Thread(runnable).start();
        while (true) {
            Thread.sleep(100);
            rs = stat.executeQuery("CALL CANCEL_SESSION(" + otherId + ")");
            rs.next();
            if (rs.getBoolean(1)) {
                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);
                    if (done[0]) {
                        break;
                    }
                }
                assertTrue(done[0]);
                break;
            }
        }
        conn2.close();
        conn.close();
    }

}
