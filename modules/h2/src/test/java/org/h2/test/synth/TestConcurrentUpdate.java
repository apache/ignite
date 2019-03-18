/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * A concurrent test.
 */
public class TestConcurrentUpdate extends TestBase {

    private static final int THREADS = 3;
    private static final int ROW_COUNT = 10;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase t = TestBase.createCaller().init();
        t.config.memory = true;
        t.test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("concurrent");
        final String url = getURL("concurrent;MULTI_THREADED=TRUE", true);
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");

        Task[] tasks = new Task[THREADS];
        for (int i = 0; i < THREADS; i++) {
            final int threadId = i;
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    Random r = new Random(threadId);
                    Connection conn = getConnection(url);
                    PreparedStatement insert = conn.prepareStatement(
                            "insert into test values(?, ?)");
                    PreparedStatement update = conn.prepareStatement(
                            "update test set name = ? where id = ?");
                    PreparedStatement delete = conn.prepareStatement(
                            "delete from test where id = ?");
                    PreparedStatement select = conn.prepareStatement(
                            "select * from test where id = ?");
                    while (!stop) {
                        try {
                            int x = r.nextInt(ROW_COUNT);
                            String data = "x" + r.nextInt(ROW_COUNT);
                            switch (r.nextInt(3)) {
                            case 0:
                                insert.setInt(1, x);
                                insert.setString(2, data);
                                insert.execute();
                                break;
                            case 1:
                                update.setString(1, data);
                                update.setInt(2, x);
                                update.execute();
                                break;
                            case 2:
                                delete.setInt(1, x);
                                delete.execute();
                                break;
                            case 4:
                                select.setInt(1, x);
                                ResultSet rs = select.executeQuery();
                                while (rs.next()) {
                                    rs.getString(2);
                                }
                                break;
                            }
                        } catch (SQLException e) {
                            handleException(e);
                        }
                    }
                    conn.close();
                }

            };
            tasks[i] = t;
            t.execute();
        }
        // test 2 seconds
        for (int i = 0; i < 200; i++) {
            Thread.sleep(10);
            for (Task t : tasks) {
                if (t.isFinished()) {
                    i = 1000;
                    break;
                }
            }
        }
        for (Task t : tasks) {
            t.get();
        }
        conn.close();
    }

    /**
     * Handle or ignore the exception.
     *
     * @param e the exception
     */
    void handleException(SQLException e) throws SQLException {
        switch (e.getErrorCode()) {
        case ErrorCode.CONCURRENT_UPDATE_1:
        case ErrorCode.DUPLICATE_KEY_1:
        case ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1:
        case ErrorCode.LOCK_TIMEOUT_1:
            break;
        default:
            throw e;
        }
    }

}
