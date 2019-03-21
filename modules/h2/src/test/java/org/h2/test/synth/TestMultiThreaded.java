/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests the multi-threaded mode.
 */
public class TestMultiThreaded extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        org.h2.test.TestAll config = new org.h2.test.TestAll();
        config.memory = true;
        config.big = true;
        System.out.println(config);
        TestBase test = createCaller().init(config);
        for (int i = 0; i < 100; i++) {
            System.out.println("Pass #" + i);
            test.config.beforeTest();
            test.test();
            test.config.afterTest();
        }
    }

    /**
     * Processes random operations.
     */
    private class Processor extends Thread {
        private final int id;
        private final Statement stat;
        private final Random random;
        private volatile Throwable exception;
        private boolean stop;

        Processor(Connection conn, int id) throws SQLException {
            this.id = id;
            stat = conn.createStatement();
            random = new Random(id);
        }
        public Throwable getException() {
            return exception;
        }
        @Override
        public void run() {
            int count = 0;
            ResultSet rs;
            try {
                while (!stop) {
                    switch (random.nextInt(6)) {
                    case 0:
                        // insert a row for this connection
                        traceThread("insert " + id + " count: " + count);
                        stat.execute("INSERT INTO TEST(NAME) VALUES('"+ id +"')");
                        traceThread("insert done");
                        count++;
                        break;
                    case 1:
                        // delete a row for this connection
                        if (count > 0) {
                            traceThread("delete " + id + " count: " + count);
                            int updateCount = stat.executeUpdate(
                                    "DELETE FROM TEST " +
                                    "WHERE NAME = '"+ id +"' AND ROWNUM()<2");
                            traceThread("delete done");
                            if (updateCount != 1) {
                                throw new AssertionError(
                                        "Expected: 1 Deleted: " + updateCount);
                            }
                            count--;
                        }
                        break;
                    case 2:
                        // select the number of rows of this connection
                        traceThread("select " + id + " count: " + count);
                        rs = stat.executeQuery("SELECT COUNT(*) " +
                                "FROM TEST WHERE NAME = '"+ id +"'");
                        traceThread("select done");
                        rs.next();
                        int got = rs.getInt(1);
                        if (got != count) {
                            throw new AssertionError("Expected: " + count + " got: " + got);
                        }
                        break;
                    case 3:
                        traceThread("insert");
                        stat.execute("INSERT INTO TEST(NAME) VALUES(NULL)");
                        traceThread("insert done");
                        break;
                    case 4:
                        traceThread("delete");
                        stat.execute("DELETE FROM TEST WHERE NAME IS NULL");
                        traceThread("delete done");
                        break;
                    case 5:
                        traceThread("select");
                        rs = stat.executeQuery("SELECT * FROM TEST WHERE NAME IS NULL");
                        traceThread("select done");
                        while (rs.next()) {
                            rs.getString(1);
                        }
                        break;
                    }
                }
            } catch (Throwable e) {
                exception = e;
            }
        }

        private void traceThread(String s) {
            if (config.traceTest) {
                trace(id + " " + s);
            }
        }
        public void stopNow() {
            this.stop = true;
        }
    }

    @Override
    public void test() throws Exception {
        deleteDb("multiThreaded");
        int size = getSize(2, 20);
        Connection[] connList = new Connection[size];
        for (int i = 0; i < size; i++) {
            connList[i] = getConnection("multiThreaded;MULTI_THREADED=1");
        }
        Connection conn = connList[0];
        Statement stat = conn.createStatement();
        stat.execute("CREATE SEQUENCE TEST_SEQ");
        stat.execute("CREATE TABLE TEST" +
                "(ID BIGINT DEFAULT NEXT VALUE FOR TEST_SEQ, NAME VARCHAR)");
        // stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        // stat.execute("CREATE INDEX IDX_TEST_NAME ON TEST(NAME)");
        trace("init done");
        Processor[] processors = new Processor[size];
        for (int i = 0; i < size; i++) {
            conn = connList[i];
            conn.createStatement().execute("SET LOCK_TIMEOUT 1000");
            processors[i] = new Processor(conn, i);
            processors[i].start();
            trace("started " + i);
            Thread.sleep(100);
        }
        try {
            Thread.sleep(2000);
        } finally {
            trace("stopping");
            for (int i = 0; i < size; i++) {
                Processor p = processors[i];
                p.stopNow();
            }
            for (int i = 0; i < size; i++) {
                Processor p = processors[i];
                p.join(1000);
            }
            trace("close");
            for (int i = 0; i < size; i++) {
                connList[i].close();
            }
            deleteDb("multiThreaded");
        }

        boolean success = true;
        for (int i = 0; i < size; i++) {
            Processor p = processors[i];
            p.join(10000);
            Throwable exception = p.getException();
            if (exception != null) {
                logError("", exception);
                success = false;
            }
        }
        assert success;
    }
}
