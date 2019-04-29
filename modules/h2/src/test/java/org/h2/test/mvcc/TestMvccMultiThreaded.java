/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Task;

/**
 * Multi-threaded MVCC (multi version concurrency) test cases.
 */
public class TestMvccMultiThreaded extends TestDb {

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
        if (!config.mvStore) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        testConcurrentSelectForUpdate();
        testMergeWithUniqueKeyViolation();
        testConcurrentMerge();
        testConcurrentUpdate();
    }

    private void testConcurrentSelectForUpdate() throws Exception {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName() + ";MULTI_THREADED=TRUE");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int not null primary key, updated int not null)");
        stat.execute("insert into test(id, updated) values(1, 100)");
        ArrayList<Task> tasks = new ArrayList<>();
        int count = 3;
        for (int i = 0; i < count; i++) {
            Task task = new Task() {
                @Override
                public void call() throws Exception {
                    try (Connection conn = getConnection(getTestName())) {
                        Statement stat = conn.createStatement();
                        while (!stop) {
                            stat.execute("select * from test where id=1 for update");
                        }
                    }
                }
            }.execute();
            tasks.add(task);
        }
        for (int i = 0; i < 10; i++) {
            Thread.sleep(100);
            ResultSet rs = stat.executeQuery("select * from test");
            assertTrue(rs.next());
        }
        for (Task t : tasks) {
            t.get();
        }
        conn.close();
        deleteDb(getTestName());
    }

    private void testMergeWithUniqueKeyViolation() throws Exception {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test(x int primary key, y int unique)");
        stat.execute("insert into test values(1, 1)");
        assertThrows(ErrorCode.DUPLICATE_KEY_1, stat).
                execute("merge into test values(2, 1)");
        stat.execute("merge into test values(1, 2)");
        conn.close();

    }

    private void testConcurrentMerge() throws Exception {
        deleteDb(getTestName());
        int len = 3;
        final Connection[] connList = new Connection[len];
        for (int i = 0; i < len; i++) {
            Connection conn = getConnection(
                    getTestName() + ";LOCK_TIMEOUT=500");
            connList[i] = conn;
        }
        Connection conn = connList[0];
        conn.createStatement().execute(
                "create table test(id int primary key, name varchar)");
        Task[] tasks = new Task[len];
        for (int i = 0; i < len; i++) {
            final Connection c = connList[i];
            c.setAutoCommit(false);
            tasks[i] = new Task() {
                @Override
                public void call() throws Exception {
                    while (!stop) {
                        c.createStatement().execute(
                                "merge into test values(1, 'x')");
                        c.commit();
                    }
                }
            };
            tasks[i].execute();
        }
        Thread.sleep(1000);
        for (int i = 0; i < len; i++) {
            tasks[i].get();
        }
        for (int i = 0; i < len; i++) {
            connList[i].close();
        }
        deleteDb(getTestName());
    }

    private void testConcurrentUpdate() throws Exception {
        deleteDb(getTestName());
        int len = 2;
        final Connection[] connList = new Connection[len];
        for (int i = 0; i < len; i++) {
            connList[i] = getConnection(getTestName());
        }
        Connection conn = connList[0];
        conn.createStatement().execute(
                "create table test(id int primary key, value int)");
        conn.createStatement().execute(
                "insert into test values(0, 0)");
        final int count = 1000;
        Task[] tasks = new Task[len];

        final CyclicBarrier barrier = new CyclicBarrier(len);

        for (int i = 0; i < len; i++) {
            final int x = i;
            // Recent changes exposed a race condition in this test itself.
            // Without preliminary record locking, counter will be off.
            connList[x].setAutoCommit(false);
            tasks[i] = new Task() {
                @Override
                public void call() throws Exception {
                    for (int a = 0; a < count; a++) {
                        ResultSet rs = connList[x].createStatement().executeQuery(
                                "select value from test for update");
                        assertTrue(rs.next());
                        connList[x].createStatement().execute(
                                "update test set value=value+1");
                        connList[x].commit();
                        barrier.await();
                    }
                }
            };
            tasks[i].execute();
        }
        for (int i = 0; i < len; i++) {
            tasks[i].get();
        }
        ResultSet rs = conn.createStatement().executeQuery("select value from test");
        rs.next();
        assertEquals(count * len, rs.getInt(1));
        for (int i = 0; i < len; i++) {
            connList[i].close();
        }
    }

}
