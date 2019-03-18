/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Additional MVCC (multi version concurrency) test cases.
 */
public class TestMvcc2 extends TestBase {

    private static final String DROP_TABLE =
            "DROP TABLE IF EXISTS EMPLOYEE";
    private static final String CREATE_TABLE =
            "CREATE TABLE EMPLOYEE (id BIGINT, version BIGINT, NAME VARCHAR(255))";
    private static final String INSERT =
            "INSERT INTO EMPLOYEE (id, version, NAME) VALUES (1, 1, 'Jones')";
    private static final String UPDATE =
            "UPDATE EMPLOYEE SET NAME = 'Miller' WHERE version = 1";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.mvcc = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        if (!config.mvcc) {
            return;
        }
        deleteDb("mvcc2");
        testConcurrentInsert();
        testConcurrentUpdate();
        testSelectForUpdate();
        testInsertUpdateRollback();
        testInsertRollback();
        deleteDb("mvcc2");
    }

    private Connection getConnection() throws SQLException {
        return getConnection("mvcc2");
    }

    private void testConcurrentInsert() throws Exception {
        Connection conn = getConnection();
        final Connection conn2 = getConnection();
        Statement stat = conn.createStatement();
        final Statement stat2 = conn2.createStatement();
        stat2.execute("set lock_timeout 1000");
        stat.execute("create table test(id int primary key, name varchar)");
        conn.setAutoCommit(false);
        final AtomicBoolean committed = new AtomicBoolean(false);
        Task t = new Task() {
            @Override
            public void call() throws SQLException {
                try {
//System.out.println("insert2 hallo");
                    stat2.execute("insert into test values(0, 'Hallo')");
//System.out.println("insert2 hallo done");
                } catch (SQLException e) {
//System.out.println("insert2 hallo e " + e);
                    if (!committed.get()) {
                        throw e;
                    }
                }
            }
        };
//System.out.println("insert hello");
        stat.execute("insert into test values(0, 'Hello')");
        t.execute();
        Thread.sleep(500);
//System.out.println("insert hello commit");
        committed.set(true);
        conn.commit();
        t.get();
        ResultSet rs;
        rs = stat.executeQuery("select name from test");
        rs.next();
        assertEquals("Hello", rs.getString(1));
        stat.execute("drop table test");
        conn2.close();
        conn.close();
    }

    private void testConcurrentUpdate() throws Exception {
        Connection conn = getConnection();
        final Connection conn2 = getConnection();
        Statement stat = conn.createStatement();
        final Statement stat2 = conn2.createStatement();
        stat2.execute("set lock_timeout 1000");
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(0, 'Hello')");
        conn.setAutoCommit(false);
        Task t = new Task() {
            @Override
            public void call() throws SQLException {
                stat2.execute("update test set name = 'Hallo'");
            }
        };
        stat.execute("update test set name = 'Hi'");
        t.execute();
        Thread.sleep(500);
        conn.commit();
        t.get();
        ResultSet rs;
        rs = stat.executeQuery("select name from test");
        rs.next();
        assertEquals("Hallo", rs.getString(1));
        stat.execute("drop table test");
        conn2.close();
        conn.close();
    }

    private void testSelectForUpdate() throws SQLException {
        Connection conn = getConnection("mvcc2;SELECT_FOR_UPDATE_MVCC=true");
        Connection conn2 = getConnection("mvcc2;SELECT_FOR_UPDATE_MVCC=true");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        conn.setAutoCommit(false);
        stat.execute("insert into test select x, 'Hello' from system_range(1, 10)");
        stat.execute("select * from test where id = 3 for update");
        conn.commit();
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, stat).
                execute("select sum(id) from test for update");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, stat).
                execute("select distinct id from test for update");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, stat).
                execute("select id from test group by id for update");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, stat).
                execute("select t1.id from test t1, test t2 for update");
        stat.execute("select * from test where id = 3 for update");
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("select * from test where id = 4 for update");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn2.createStatement()).
                execute("select * from test where id = 3 for update");
        conn.close();
        conn2.close();
    }

    private void testInsertUpdateRollback() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute(DROP_TABLE);
        stmt.execute(CREATE_TABLE);
        conn.commit();
        stmt.execute(INSERT);
        stmt.execute(UPDATE);
        conn.rollback();
        conn.close();
    }

    private void testInsertRollback() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute(DROP_TABLE);
        stmt.execute(CREATE_TABLE);
        conn.commit();
        stmt.execute(INSERT);
        conn.rollback();
        conn.close();
    }

}
