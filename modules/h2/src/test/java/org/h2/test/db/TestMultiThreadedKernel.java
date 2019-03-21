/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.JdbcUtils;
import org.h2.util.Task;

/**
 * A multi-threaded test case.
 */
public class TestMultiThreadedKernel extends TestDb {

    /**
     * Stop the current thread.
     */
    volatile boolean stop;

    /**
     * The exception that occurred in the thread.
     */
    Exception exception;

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
        deleteDb("multiThreadedKernel");
        testConcurrentRead();
        testCache();
        deleteDb("multiThreadedKernel");
        final String url = getURL("multiThreadedKernel;DB_CLOSE_DELAY=-1", true);
        final String user = getUser(), password = getPassword();
        int len = 3;
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection conn = null;
                    try {
                        for (int j = 0; j < 100 && !stop; j++) {
                            conn = DriverManager.getConnection(
                                    url, user, password);
                            work(conn);
                        }
                    } catch (Exception e) {
                        exception = e;
                    } finally {
                        JdbcUtils.closeSilently(conn);
                    }
                }
                private void work(Connection conn) throws SQLException {
                    Statement stat = conn.createStatement();
                    stat.execute(
                            "create local temporary table temp(id identity)");
                    stat.execute(
                            "insert into temp values(1)");
                    conn.close();
                }
            });
        }
        for (int i = 0; i < len; i++) {
            threads[i].start();
        }
        Thread.sleep(1000);
        stop = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.createStatement().execute("shutdown");
        conn.close();
        if (exception != null) {
            throw exception;
        }
        deleteDb("multiThreadedKernel");
    }

    private void testConcurrentRead() throws Exception {
        int size = 2;
        final int count = 1000;
        ArrayList<Task> list = new ArrayList<>(size);
        final Connection[] connections = new Connection[count];
        String url = getURL("multiThreadedKernel;CACHE_SIZE=16", true);
        for (int i = 0; i < size; i++) {
            final Connection conn = DriverManager.getConnection(
                    url, getUser(), getPassword());
            connections[i] = conn;
            if (i == 0) {
                Statement stat = conn.createStatement();
                stat.execute("drop table test if exists");
                stat.execute("create table test(id int primary key, name varchar) "
                        + "as select x, x || space(10) from system_range(1, " + count + ")");
            }
            final Random random = new Random(i);
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    PreparedStatement prep = conn.prepareStatement(
                            "select * from test where id = ?");
                    while (!stop) {
                        prep.setInt(1, random.nextInt(count));
                        prep.execute();
                    }
                }
            };
            t.execute();
            list.add(t);
        }
        Thread.sleep(1000);
        for (Task t : list) {
            t.get();
        }
        for (int i = 0; i < size; i++) {
            connections[i].close();
        }
    }

    private void testCache() throws Exception {
        int size = 3;
        final int count = 100;
        ArrayList<Task> list = new ArrayList<>(size);
        final Connection[] connections = new Connection[count];
        String url = getURL("multiThreadedKernel;CACHE_SIZE=1", true);
        for (int i = 0; i < size; i++) {
            final Connection conn = DriverManager.getConnection(
                    url, getUser(), getPassword());
            connections[i] = conn;
            if (i == 0) {
                Statement stat = conn.createStatement();
                stat.execute("drop table test if exists");
                stat.execute("create table test(id int primary key, name varchar) "
                        + "as select x, space(3000) from system_range(1, " + count + ")");
            }
            final Random random = new Random(i);
            Task t = new Task() {
                @Override
                public void call() throws SQLException {
                    PreparedStatement prep = conn.prepareStatement(
                            "select * from test where id = ?");
                    while (!stop) {
                        prep.setInt(1, random.nextInt(count));
                        prep.execute();
                    }
                }
            };
            t.execute();
            list.add(t);
        }
        Thread.sleep(1000);
        for (Task t : list) {
            t.get();
        }
        for (int i = 0; i < size; i++) {
            connections[i].close();
        }
    }

    @Override
    protected String getURL(String name, boolean admin) {
        return super.getURL(name + ";MULTI_THREADED=1;LOCK_TIMEOUT=2000", admin);
    }
}
