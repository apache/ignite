/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Additional MVCC (multi version concurrency) test cases.
 */
public class TestMvcc4 extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.lockTimeout = 20000;
        test.config.memory = true;
        test.test();
    }

    @Override
    public boolean isEnabled() {
        if (config.networked || !config.mvStore) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws SQLException {
        testSelectForUpdateAndUpdateConcurrency();
    }

    private void testSelectForUpdateAndUpdateConcurrency() throws SQLException {
        deleteDb("mvcc4");
        Connection setup = getConnection("mvcc4;MULTI_THREADED=TRUE");
        setup.setAutoCommit(false);

        {
            Statement s = setup.createStatement();
            s.executeUpdate("CREATE TABLE test ("
                    + "entity_id VARCHAR(100) NOT NULL PRIMARY KEY, "
                    + "lastUpdated TIMESTAMP NOT NULL)");

            PreparedStatement ps = setup.prepareStatement(
                    "INSERT INTO test (entity_id, lastUpdated) VALUES (?, ?)");
            for (int i = 0; i < 2; i++) {
                String id = "" + i;
                ps.setString(1, id);
                ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                ps.executeUpdate();
            }
            setup.commit();
        }

        //Create a connection from thread 1
        Connection c1 = getConnection("mvcc4;LOCK_TIMEOUT=10000");
        c1.setAutoCommit(false);

        //Fire off a concurrent update.
        final CountDownLatch executedUpdate = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                try {
                    Connection c2 = getConnection("mvcc4");
                    c2.setAutoCommit(false);

                    PreparedStatement ps = c2.prepareStatement(
                            "SELECT * FROM test WHERE entity_id = ? FOR UPDATE");
                    ps.setString(1, "1");
                    ps.executeQuery().next();

                    executedUpdate.countDown();
                    // interrogate new "blocker_id" metatable field instead of
                    // relying on stacktraces!? to determine when session is blocking
                    PreparedStatement stmt = c2.prepareStatement(
                            "SELECT * FROM INFORMATION_SCHEMA.SESSIONS WHERE BLOCKER_ID = SESSION_ID()");
                    ResultSet resultSet;
                    do {
                        resultSet = stmt.executeQuery();
                    } while(!resultSet.next());

                    c2.commit();
                    c2.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        //Wait until the concurrent update has executed, but not yet committed
        try {
            executedUpdate.await();
        } catch (InterruptedException e) {
            // ignore
        }

        // Execute an update. This should initially fail, and enter the waiting
        // for lock case.
        PreparedStatement ps = c1.prepareStatement("UPDATE test SET lastUpdated = ?");
        ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        assertEquals(2, ps.executeUpdate());

        c1.commit();
        c1.close();

        Connection verify = getConnection("mvcc4");

        verify.setAutoCommit(false);
        ps = verify.prepareStatement("SELECT COUNT(*) FROM test");
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        verify.commit();
        verify.close();

        setup.close();
    }
}




