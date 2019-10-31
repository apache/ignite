/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;

/**
 * Tests the DatabaseEventListener.
 */
public class TestListener extends TestBase implements DatabaseEventListener {

    private long last;
    private int lastState = -1;
    private String databaseUrl;

    public TestListener() {
        start = last = System.nanoTime();
    }

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
        if (config.networked || config.cipher != null) {
            return;
        }
        deleteDb("listener");
        Connection conn;
        conn = getConnection("listener");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, 'Test' || SPACE(100))");
        int len = getSize(100, 100000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.execute();
        }
        crash(conn);

        conn = getConnection("listener;database_event_listener='" +
                getClass().getName() + "'");
        conn.close();
        deleteDb("listener");
    }

    @Override
    public void exceptionThrown(SQLException e, String sql) {
        TestBase.logError("exceptionThrown sql=" + sql, e);
    }

    @Override
    public void setProgress(int state, String name, int current, int max) {
        long time = System.nanoTime();
        if (state == lastState && time < last + TimeUnit.SECONDS.toNanos(1)) {
            return;
        }
        if (state == STATE_STATEMENT_START ||
                state == STATE_STATEMENT_END ||
                state == STATE_STATEMENT_PROGRESS) {
            return;
        }
        if (name.length() > 30) {
            name = "..." + name.substring(name.length() - 30);
        }
        last = time;
        lastState = state;
        String stateName;
        switch (state) {
        case STATE_SCAN_FILE:
            stateName = "Scan " + name;
            break;
        case STATE_CREATE_INDEX:
            stateName = "Create Index " + name;
            break;
        case STATE_RECOVER:
            stateName = "Recover";
            break;
        default:
            TestBase.logError("unknown state: " + state, null);
            stateName = "? " + name;
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            // ignore
        }
        printTime("state: " + stateName + " " +
                (100 * current / max) + " " + TimeUnit.NANOSECONDS.toMillis(time - start));
    }

    @Override
    public void closingDatabase() {
        if (databaseUrl.toUpperCase().contains("CIPHER")) {
            return;
        }

        try (Connection conn = DriverManager.getConnection(databaseUrl,
                getUser(), getPassword())) {
            conn.createStatement().execute("DROP TABLE TEST2");
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init(String url) {
        this.databaseUrl = url;
    }

    @Override
    public void opened() {
        if (databaseUrl.toUpperCase().contains("CIPHER")) {
            return;
        }

        try (Connection conn = DriverManager.getConnection(databaseUrl,
                getUser(), getPassword())) {
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TEST2(ID INT)");
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
