/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Test for the exclusive mode.
 */
public class TestExclusive extends TestBase {

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
        deleteDb("exclusive");
        Connection conn = getConnection("exclusive");
        Statement stat = conn.createStatement();
        stat.execute("set exclusive true");
        assertThrows(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE, this).
                getConnection("exclusive");

        stat.execute("set exclusive false");
        Connection conn2 = getConnection("exclusive");
        final Statement stat2 = conn2.createStatement();
        stat.execute("set exclusive true");
        final AtomicInteger state = new AtomicInteger(0);
        Task task = new Task() {
            @Override
            public void call() throws SQLException {
                stat2.execute("select * from dual");
                if (state.get() != 1) {
                    new Error("unexpected state: " + state.get()).printStackTrace();
                }
            }
        };
        task.execute();
        state.set(1);
        stat.execute("set exclusive false");
        task.get();
        stat.execute("set exclusive true");
        conn.close();

        // check that exclusive mode is off when disconnected
        stat2.execute("select * from dual");
        conn2.close();
        deleteDb("exclusive");
    }

}
