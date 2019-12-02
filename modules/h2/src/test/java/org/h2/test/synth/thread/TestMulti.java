/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.thread;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.test.TestBase;

/**
 * Starts multiple threads and performs random operations on each thread.
 */
public class TestMulti extends TestBase {

    /**
     * If set, the test should stop.
     */
    public volatile boolean stop;

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
        org.h2.Driver.load();
        deleteDb("openClose");

        // int len = getSize(5, 100);
        int len = 10;
        TestMultiThread[] threads = new TestMultiThread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new TestMultiNews(this);
        }
        threads[0].first();
        for (int i = 0; i < len; i++) {
            threads[i].start();
        }
        Thread.sleep(10000);
        this.stop = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        threads[0].finalTest();
    }

    Connection getConnection() throws SQLException {
        final String url = "jdbc:h2:" + getBaseDir() +
                "/openClose;LOCK_MODE=3;DB_CLOSE_DELAY=-1";
        Connection conn = DriverManager.getConnection(url, "sa", "");
        return conn;
    }

}
