/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Task;

/**
 * Additional MVCC (multi version concurrency) test cases.
 */
public class TestMvcc2 extends TestDb {

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
        test.test();
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
        Task t = new Task() {
            @Override
            public void call() {
                try {
                    stat2.execute("insert into test values(0, 'Hallo')");
                    fail();
                } catch (SQLException e) {
                    assertTrue(e.toString(),
                            e.getErrorCode() == ErrorCode.DUPLICATE_KEY_1 ||
                            e.getErrorCode() == ErrorCode.CONCURRENT_UPDATE_1);
                }
            }
        };
        stat.execute("insert into test values(0, 'Hello')");
        t.execute();
        conn.commit();
        t.get();
        ResultSet rs;
        rs = stat.executeQuery("select name from test");
        assertTrue(rs.next());
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
                assertEquals(1, stat2.getUpdateCount());
            }
        };
        stat.execute("update test set name = 'Hi'");
        assertEquals(1, stat.getUpdateCount());
        t.execute();
        conn.commit();
        t.get();
        ResultSet rs;
        rs = stat.executeQuery("select name from test");
        assertTrue(rs.next());
        assertEquals("Hallo", rs.getString(1));
        stat.execute("drop table test");
        conn2.close();
        conn.close();
    }

    private void testSelectForUpdate() throws SQLException {
        Connection conn = getConnection("mvcc2");
        Connection conn2 = getConnection("mvcc2");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        conn.setAutoCommit(false);
        stat.execute("insert into test select x, 'Hello' from system_range(1, 10)");
        stat.execute("select * from test where id = 3 for update");
        conn.commit();
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
