/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.rowlock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Row level locking tests.
 */
public class TestRowLocks extends TestBase {

    /**
     * The statements used in this test.
     */
    Statement s1, s2;

    private Connection c1, c2;

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
        testSetMode();
        testCases();
        deleteDb(getTestName());
    }

    private void testSetMode() throws SQLException {
        deleteDb(getTestName());
        c1 = getConnection(getTestName());
        Statement stat = c1.createStatement();
        stat.execute("SET LOCK_MODE 2");
        ResultSet rs = stat.executeQuery("call lock_mode()");
        rs.next();
        assertEquals("2", rs.getString(1));
        c1.close();
    }

    private void testCases() throws Exception {
        deleteDb(getTestName());
        c1 = getConnection(getTestName() + ";MVCC=TRUE");
        s1 = c1.createStatement();
        s1.execute("SET LOCK_TIMEOUT 10000");
        s1.execute("CREATE TABLE TEST AS " +
                "SELECT X ID, 'Hello' NAME FROM SYSTEM_RANGE(1, 3)");
        c1.commit();
        c1.setAutoCommit(false);
        s1.execute("UPDATE TEST SET NAME='Hallo' WHERE ID=1");

        c2 = getConnection(getTestName());
        c2.setAutoCommit(false);
        s2 = c2.createStatement();

        assertEquals("Hallo", getSingleValue(s1,
                "SELECT NAME FROM TEST WHERE ID=1"));
        assertEquals("Hello", getSingleValue(s2,
                "SELECT NAME FROM TEST WHERE ID=1"));

        s2.execute("UPDATE TEST SET NAME='Hallo' WHERE ID=2");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, s2).
                executeUpdate("UPDATE TEST SET NAME='Hi' WHERE ID=1");
        c1.commit();
        c2.commit();

        assertEquals("Hallo", getSingleValue(s1,
                "SELECT NAME FROM TEST WHERE ID=1"));
        assertEquals("Hallo", getSingleValue(s2,
                "SELECT NAME FROM TEST WHERE ID=1"));

        s2.execute("UPDATE TEST SET NAME='H1' WHERE ID=1");
        Task task = new Task() {
            @Override
            public void call() throws SQLException {
                s1.execute("UPDATE TEST SET NAME='H2' WHERE ID=1");
            }
        };
        task.execute();
        Thread.sleep(100);
        c2.commit();
        task.get();
        c1.commit();
        assertEquals("H2", getSingleValue(s1,
                "SELECT NAME FROM TEST WHERE ID=1"));
        assertEquals("H2", getSingleValue(s2,
                "SELECT NAME FROM TEST WHERE ID=1"));

        c1.close();
        c2.close();
    }

    private static String getSingleValue(Statement stat, String sql)
            throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        return rs.next() ? rs.getString(1) : null;
    }

}
