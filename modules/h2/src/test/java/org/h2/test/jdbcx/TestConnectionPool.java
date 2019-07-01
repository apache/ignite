/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbcx;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * This class tests the JdbcConnectionPool.
 */
public class TestConnectionPool extends TestBase {

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
        deleteDb("connectionPool");
        testShutdown();
        testWrongUrl();
        testTimeout();
        testUncommittedTransaction();
        testPerformance();
        testKeepOpen();
        testConnect();
        testThreads();
        deleteDb("connectionPool");
        deleteDb("connectionPool2");
    }

    private void testShutdown() throws SQLException {
        String url = getURL("connectionPool2", true), user = getUser();
        String password = getPassword();
        JdbcConnectionPool cp = JdbcConnectionPool.create(url, user, password);
        StringWriter w = new StringWriter();
        cp.setLogWriter(new PrintWriter(w));
        Connection conn1 = cp.getConnection();
        Connection conn2 = cp.getConnection();
        conn1.close();
        conn2.createStatement().execute("shutdown immediately");
        cp.dispose();
        assertTrue(w.toString().length() > 0);
        cp.dispose();
    }

    private void testWrongUrl() {
        JdbcConnectionPool cp = JdbcConnectionPool.create(
                "jdbc:wrong:url", "", "");
        try {
            cp.getConnection();
        } catch (SQLException e) {
            assertEquals(8001, e.getErrorCode());
        }
        cp.dispose();
    }

    private void testTimeout() throws Exception {
        String url = getURL("connectionPool", true), user = getUser();
        String password = getPassword();
        final JdbcConnectionPool man = JdbcConnectionPool.create(url, user, password);
        man.setLoginTimeout(1);
        createClassProxy(man.getClass());
        assertThrows(IllegalArgumentException.class, man).
                setMaxConnections(-1);
        man.setMaxConnections(2);
        // connection 1 (of 2)
        Connection conn = man.getConnection();
        Task t = new Task() {
            @Override
            public void call() {
                while (!stop) {
                    // this calls notifyAll
                    man.setMaxConnections(1);
                    man.setMaxConnections(2);
                }
            }
        };
        t.execute();
        long time = System.nanoTime();
        Connection conn2 = null;
        try {
            // connection 2 (of 1 or 2) may fail
            conn2 = man.getConnection();
            // connection 3 (of 1 or 2) must fail
            man.getConnection();
            fail();
        } catch (SQLException e) {
            if (conn2 != null) {
                conn2.close();
            }
            assertContains(e.toString().toLowerCase(), "timeout");
            time = System.nanoTime() - time;
            assertTrue("timeout after " + TimeUnit.NANOSECONDS.toMillis(time) +
                    " ms", time > TimeUnit.SECONDS.toNanos(1));
        } finally {
            conn.close();
            t.get();
        }

        man.dispose();
    }

    private void testUncommittedTransaction() throws SQLException {
        String url = getURL("connectionPool", true), user = getUser();
        String password = getPassword();
        JdbcConnectionPool man = JdbcConnectionPool.create(url, user, password);

        assertEquals(30, man.getLoginTimeout());
        man.setLoginTimeout(1);
        assertEquals(1, man.getLoginTimeout());
        man.setLoginTimeout(0);
        assertEquals(30, man.getLoginTimeout());
        assertEquals(10, man.getMaxConnections());

        PrintWriter old = man.getLogWriter();
        PrintWriter pw = new PrintWriter(new StringWriter());
        man.setLogWriter(pw);
        assertTrue(pw == man.getLogWriter());
        man.setLogWriter(old);

        Connection conn1 = man.getConnection();
        assertTrue(conn1.getAutoCommit());
        conn1.setAutoCommit(false);
        conn1.close();
        assertTrue(conn1.isClosed());

        Connection conn2 = man.getConnection();
        assertTrue(conn2.getAutoCommit());
        conn2.close();

        man.dispose();
    }

    private void testPerformance() throws SQLException {
        String url = getURL("connectionPool", true), user = getUser();
        String password = getPassword();
        JdbcConnectionPool man = JdbcConnectionPool.create(url, user, password);
        Connection conn = man.getConnection();
        int len = 1000;
        long time = System.nanoTime();
        for (int i = 0; i < len; i++) {
            man.getConnection().close();
        }
        man.dispose();
        trace((int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
        time = System.nanoTime();
        for (int i = 0; i < len; i++) {
            DriverManager.getConnection(url, user, password).close();
        }
        trace((int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
        conn.close();
    }

    private void testKeepOpen() throws Exception {
        JdbcConnectionPool man = getConnectionPool(1);
        Connection conn = man.getConnection();
        Statement stat = conn.createStatement();
        stat.execute("create local temporary table test(id int)");
        conn.close();
        conn = man.getConnection();
        stat = conn.createStatement();
        stat.execute("select * from test");
        conn.close();
        man.dispose();
    }

    private void testThreads() throws Exception {
        final int len = getSize(4, 20);
        final JdbcConnectionPool man = getConnectionPool(len - 2);
        final boolean[] stop = { false };

        /**
         * This class gets and returns connections from the pool.
         */
        class TestRunner implements Runnable {
            @Override
            public void run() {
                try {
                    while (!stop[0]) {
                        Connection conn = man.getConnection();
                        if (man.getActiveConnections() >= len + 1) {
                            throw new Exception("a: " +
                                    man.getActiveConnections()  +
                                    " is not smaller than b: " + (len + 1));
                        }
                        Statement stat = conn.createStatement();
                        stat.execute("SELECT 1 FROM DUAL");
                        conn.close();
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new Thread(new TestRunner());
            threads[i].start();
        }
        Thread.sleep(1000);
        stop[0] = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        assertEquals(0, man.getActiveConnections());
        man.dispose();
    }

    private JdbcConnectionPool getConnectionPool(int poolSize) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(getURL("connectionPool", true));
        ds.setUser(getUser());
        ds.setPassword(getPassword());
        JdbcConnectionPool pool = JdbcConnectionPool.create(ds);
        pool.setMaxConnections(poolSize);
        return pool;
    }

    private void testConnect() throws SQLException {
        JdbcConnectionPool pool = getConnectionPool(3);
        for (int i = 0; i < 100; i++) {
            Connection conn = pool.getConnection();
            conn.close();
        }
        pool.dispose();
        DataSource ds = pool;
        assertThrows(IllegalStateException.class, ds).
                getConnection();
        assertThrows(UnsupportedOperationException.class, ds).
                getConnection(null, null);
    }

}
