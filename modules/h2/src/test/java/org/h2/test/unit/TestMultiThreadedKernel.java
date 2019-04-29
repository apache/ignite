/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests the multi-threaded kernel feature.
 */
public class TestMultiThreadedKernel extends TestDb implements Runnable {

    private String url, user, password;
    private int id;
    private TestMultiThreadedKernel master;
    private volatile boolean stop;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public boolean isEnabled() {
        if (config.networked) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        deleteDb("multiThreadedKernel");
        int count = getSize(2, 5);
        Thread[] list = new Thread[count];
        for (int i = 0; i < count; i++) {
            TestMultiThreadedKernel r = new TestMultiThreadedKernel();
            r.url = getURL("multiThreadedKernel;MULTI_THREADED=1", true);
            r.user = getUser();
            r.password = getPassword();
            r.master = this;
            r.id = i;
            Thread thread = new Thread(r);
            thread.setName("Thread " + i);
            thread.start();
            list[i] = thread;
        }
        Thread.sleep(getSize(2000, 5000));
        stop = true;
        for (int i = 0; i < count; i++) {
            list[i].join();
        }
        deleteDb("multiThreadedKernel");
    }

    @Override
    public void run() {
        try {
            org.h2.Driver.load();
            Connection conn = DriverManager.getConnection(url +
                    ";MULTI_THREADED=1;LOCK_MODE=3;WRITE_DELAY=0",
                    user, password);
            conn.createStatement().execute(
                    "CREATE TABLE TEST" + id +
                    "(COL1 BIGINT AUTO_INCREMENT PRIMARY KEY, COL2 BIGINT)");
            PreparedStatement prep = conn.prepareStatement(
                    "insert into TEST" + id + "(col2) values (?)");
            for (int i = 0; !master.stop; i++) {
                prep.setLong(1, i);
                prep.execute();
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
