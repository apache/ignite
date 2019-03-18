/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.h2.test.TestBase;

/**
 * Additional MVCC (multi version concurrency) test cases.
 */
public class TestMvcc4 extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.mvcc = true;
        test.config.lockTimeout = 20000;
        test.config.memory = true;
        test.test();
    }

    @Override
    public void test() throws SQLException {
        if (config.networked) {
            return;
        }
        testSelectForUpdateAndUpdateConcurrency();
    }

    private void testSelectForUpdateAndUpdateConcurrency() throws SQLException {
        Connection setup = getConnection("mvcc4");
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
        final Thread mainThread = Thread.currentThread();
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
                    waitForThreadToBlockOnDB(mainThread);

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
        ps.executeUpdate();

        c1.commit();
        c1.close();

        Connection verify = getConnection("mvcc4");

        verify.setAutoCommit(false);
        ps = verify.prepareStatement("SELECT COUNT(*) FROM test");
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.getInt(1) == 2);
        verify.commit();
        verify.close();

        setup.close();
    }

    /**
     * Wait for the given thread to block on synchronizing on the database
     * object.
     *
     * @param t the thread
     */
    void waitForThreadToBlockOnDB(Thread t) {
        while (true) {
            // sleep the first time through the loop so we give the main thread
            // a chance
            try {
                Thread.sleep(20);
            } catch (InterruptedException e1) {
                // ignore
            }
            // TODO must not use getAllStackTraces, as the method names are
            // implementation details
            Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
            StackTraceElement[] elements = threadMap.get(t);
            if (elements != null
                    &&
                    elements.length > 1 &&
                    (config.multiThreaded ? "sleep".equals(elements[0]
                            .getMethodName()) : "wait".equals(elements[0]
                            .getMethodName())) &&
                    "filterConcurrentUpdate"
                            .equals(elements[1].getMethodName())) {
                return;
            }
        }
    }
}




