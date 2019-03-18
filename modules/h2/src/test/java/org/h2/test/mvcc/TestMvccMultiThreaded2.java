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
import java.util.ArrayList;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;

/**
 * Additional MVCC (multi version concurrency) test cases.
 */
public class TestMvccMultiThreaded2 extends TestBase {

    private static final int TEST_THREAD_COUNT = 100;
    private static final int TEST_TIME_SECONDS = 60;
    private static final boolean DISPLAY_STATS = false;

    private static final String URL = ";MVCC=TRUE;LOCK_TIMEOUT=120000;MULTI_THREADED=TRUE";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.mvcc = true;
        test.config.lockTimeout = 120000;
        test.config.memory = true;
        test.config.multiThreaded = true;
        test.test();
    }

    @Override
    public void test() throws SQLException, InterruptedException {
        testSelectForUpdateConcurrency();
    }

    private void testSelectForUpdateConcurrency()
            throws SQLException, InterruptedException {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName() + URL);
        conn.setAutoCommit(false);

        String sql = "CREATE TABLE test ("
                + "entity_id INTEGER NOT NULL PRIMARY KEY, "
                + "lastUpdated INTEGER NOT NULL)";

        Statement smtm = conn.createStatement();
        smtm.executeUpdate(sql);

        PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test (entity_id, lastUpdated) VALUES (?, ?)");
        ps.setInt(1, 1);
        ps.setInt(2, 100);
        ps.executeUpdate();
        ps.setInt(1, 2);
        ps.setInt(2, 200);
        ps.executeUpdate();
        conn.commit();

        ArrayList<SelectForUpdate> threads = new ArrayList<>();
        for (int i = 0; i < TEST_THREAD_COUNT; i++) {
            SelectForUpdate sfu = new SelectForUpdate();
            sfu.setName("Test SelectForUpdate Thread#"+i);
            threads.add(sfu);
            sfu.start();
        }

        // give any of the 100 threads a chance to start by yielding the processor to them
        Thread.yield();

        // gather stats on threads after they finished
        @SuppressWarnings("unused")
        int minProcessed = Integer.MAX_VALUE, maxProcessed = 0, totalProcessed = 0;

        boolean allOk = true;
        for (SelectForUpdate sfu : threads) {
            // make sure all threads have stopped by joining with them
            sfu.join();
            allOk &= sfu.ok;
            totalProcessed += sfu.iterationsProcessed;
            if (sfu.iterationsProcessed > maxProcessed) {
                maxProcessed = sfu.iterationsProcessed;
            }
            if (sfu.iterationsProcessed < minProcessed) {
                minProcessed = sfu.iterationsProcessed;
            }
        }

        if (DISPLAY_STATS) {
            System.out.println(String.format(
                    "+ INFO: TestMvccMultiThreaded2 RUN STATS threads=%d, minProcessed=%d, maxProcessed=%d, "
                            + "totalProcessed=%d, averagePerThread=%d, averagePerThreadPerSecond=%d\n",
                    TEST_THREAD_COUNT, minProcessed, maxProcessed, totalProcessed, totalProcessed / TEST_THREAD_COUNT,
                    totalProcessed / (TEST_THREAD_COUNT * TEST_TIME_SECONDS)));
        }

        IOUtils.closeSilently(conn);
        deleteDb(getTestName());

        assertTrue(allOk);
    }

    /**
     *  Worker test thread selecting for update
     */
    private class SelectForUpdate extends Thread {

        public int iterationsProcessed;

        public boolean ok;

        SelectForUpdate() {
        }

        @Override
        public void run() {
            final long start = System.currentTimeMillis();
            boolean done = false;
            Connection conn = null;
            try {
                conn = getConnection(getTestName() + URL);
                conn.setAutoCommit(false);

                // give the other threads a chance to start up before going into our work loop
                Thread.yield();

                while (!done) {
                    try {
                        PreparedStatement ps = conn.prepareStatement(
                                "SELECT * FROM test WHERE entity_id = ? FOR UPDATE");
                        String id;
                        int value;
                        if ((iterationsProcessed & 1) == 0) {
                            id = "1";
                            value = 100;
                        } else {
                            id = "2";
                            value = 200;
                        }
                        ps.setString(1, id);
                        ResultSet rs = ps.executeQuery();

                        assertTrue(rs.next());
                        assertTrue(rs.getInt(2) == value);

                        conn.commit();
                        iterationsProcessed++;

                        long now = System.currentTimeMillis();
                        if (now - start > 1000 * TEST_TIME_SECONDS) {
                            done = true;
                        }
                    } catch (JdbcSQLException e1) {
                        throw e1;
                    }
                }
            } catch (SQLException e) {
                TestBase.logError("SQL error from thread "+getName(), e);
                throw DbException.convert(e);
            } catch (Exception e) {
                TestBase.logError("General error from thread "+getName(), e);
                throw e;
            }
            IOUtils.closeSilently(conn);
            ok = true;
        }
    }
}
