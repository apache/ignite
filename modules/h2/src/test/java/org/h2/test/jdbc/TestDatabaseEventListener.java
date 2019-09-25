/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.h2.Driver;
import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests the DatabaseEventListener interface.
 */
public class TestDatabaseEventListener extends TestBase {

    /**
     * A flag to mark that the given method was called.
     */
    static boolean calledOpened, calledClosingDatabase, calledScan,
            calledCreateIndex, calledStatementStart, calledStatementEnd,
            calledStatementProgress;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        testInit();
        testIndexRebuiltOnce();
        testIndexNotRebuilt();
        testCalled();
        testCloseLog0(false);
        testCloseLog0(true);
        testCalledForStatement();
        deleteDb("databaseEventListener");
    }

    /**
     * Initialize the database after opening.
     */
    public static class Init implements DatabaseEventListener {

        private String databaseUrl;

        @Override
        public void init(String url) {
            databaseUrl = url;
        }

        @Override
        public void opened() {
            try {
                // using DriverManager.getConnection could result in a deadlock
                // when using the server mode, but within the same process
                Properties prop = new Properties();
                prop.setProperty("user", "sa");
                prop.setProperty("password", "sa");
                Connection conn = Driver.load().connect(databaseUrl, prop);
                Statement stat = conn.createStatement();
                stat.execute("create table if not exists test(id int)");
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void closingDatabase() {
            // nothing to do
        }

        @Override
        public void exceptionThrown(SQLException e, String sql) {
            // nothing to do
        }

        @Override
        public void setProgress(int state, String name, int x, int max) {
            // nothing to do
        }

    }

    private void testInit() throws SQLException {
        if (config.cipher != null || config.memory) {
            return;
        }
        deleteDb("databaseEventListener");
        String url = getURL("databaseEventListener", true);
        url += ";DATABASE_EVENT_LISTENER='" + Init.class.getName() + "'";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("select * from test");
        conn.close();
    }

    private void testIndexRebuiltOnce() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("databaseEventListener");
        String url = getURL("databaseEventListener", true);
        String user = getUser(), password = getPassword();
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        Connection conn;
        Statement stat;
        conn = DriverManager.getConnection(url, p);
        stat = conn.createStatement();
        // the old.id index head is at position 0
        stat.execute("create table old(id identity) as select 1");
        // the test.id index head is at position 1
        stat.execute("create table test(id identity) as select 1");
        conn.close();
        conn = DriverManager.getConnection(url, p);
        stat = conn.createStatement();
        // free up space at position 0
        stat.execute("drop table old");
        stat.execute("insert into test values(2)");
        stat.execute("checkpoint sync");
        stat.execute("shutdown immediately");
        assertThrows(ErrorCode.DATABASE_IS_CLOSED, conn).close();
        // now the index should be re-built
        conn = DriverManager.getConnection(url, p);
        conn.close();
        calledCreateIndex = false;
        p.put("DATABASE_EVENT_LISTENER",
                MyDatabaseEventListener.class.getName());
        conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        assertTrue(!calledCreateIndex);
    }

    private void testIndexNotRebuilt() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("databaseEventListener");
        String url = getURL("databaseEventListener", true);
        String user = getUser(), password = getPassword();
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        Connection conn = DriverManager.getConnection(url, p);
        Statement stat = conn.createStatement();
        // the old.id index head is at position 0
        stat.execute("create table old(id identity) as select 1");
        // the test.id index head is at position 1
        stat.execute("create table test(id identity) as select 1");
        conn.close();
        conn = DriverManager.getConnection(url, p);
        stat = conn.createStatement();
        // free up space at position 0
        stat.execute("drop table old");
        // truncate, relocating to position 0
        stat.execute("truncate table test");
        stat.execute("insert into test select 1");
        conn.close();
        calledCreateIndex = false;
        p.put("DATABASE_EVENT_LISTENER",
                MyDatabaseEventListener.class.getName());
        conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        assertTrue(!calledCreateIndex);
    }

    private void testCloseLog0(boolean shutdown) throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("databaseEventListener");
        String url = getURL("databaseEventListener", true);
        String user = getUser(), password = getPassword();
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        Connection conn = DriverManager.getConnection(url, p);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test select x, space(1000) " +
                "from system_range(1,1000)");
        if (shutdown) {
            stat.execute("shutdown");
        }
        conn.close();

        calledOpened = false;
        calledScan = false;
        p.put("DATABASE_EVENT_LISTENER", MyDatabaseEventListener.class.getName());
        conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        if (calledOpened) {
            assertTrue(!calledScan);
        }
    }

    private void testCalled() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", "sa");
        p.setProperty("password", "sa");
        calledOpened = false;
        calledClosingDatabase = false;
        p.put("DATABASE_EVENT_LISTENER", MyDatabaseEventListener.class.getName());
        org.h2.Driver.load();
        String url = "jdbc:h2:mem:databaseEventListener";
        Connection conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        assertTrue(calledOpened);
        assertTrue(calledClosingDatabase);
    }

    private void testCalledForStatement() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", "sa");
        p.setProperty("password", "sa");
        calledStatementStart = false;
        calledStatementEnd = false;
        calledStatementProgress = false;
        p.put("DATABASE_EVENT_LISTENER", MyDatabaseEventListener.class.getName());
        org.h2.Driver.load();
        String url = "jdbc:h2:mem:databaseEventListener";
        Connection conn = org.h2.Driver.load().connect(url, p);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("select * from test");
        conn.close();
        assertTrue(calledStatementStart);
        assertTrue(calledStatementEnd);
        assertTrue(calledStatementProgress);
    }

    /**
     * The database event listener for this test.
     */
    public static final class MyDatabaseEventListener implements
            DatabaseEventListener {

        @Override
        public void closingDatabase() {
            calledClosingDatabase = true;
        }

        @Override
        public void exceptionThrown(SQLException e, String sql) {
            // nothing to do
        }

        @Override
        public void init(String url) {
            // nothing to do
        }

        @Override
        public void opened() {
            calledOpened = true;
        }

        @Override
        public void setProgress(int state, String name, int x, int max) {
            if (state == DatabaseEventListener.STATE_SCAN_FILE) {
                calledScan = true;
            }
            if (state == DatabaseEventListener.STATE_CREATE_INDEX) {
                if (!name.startsWith("SYS:")) {
                    calledCreateIndex = true;
                }
            }
            if (state == STATE_STATEMENT_START) {
                if (name.equals("select * from test")) {
                    calledStatementStart = true;
                }
            }
            if (state == STATE_STATEMENT_END) {
                if (name.equals("select * from test")) {
                    calledStatementEnd = true;
                }
            }
            if (state == STATE_STATEMENT_PROGRESS) {
                if (name.equals("select * from test")) {
                    calledStatementProgress = true;
                }
            }
        }

    }

}
