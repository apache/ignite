/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.h2.api.DatabaseEventListener;
import org.h2.jdbc.JdbcConnection;

/**
 * This example application implements a database event listener. This is useful
 * to display progress information while opening a large database, or to log
 * database exceptions.
 */
public class ShowProgress implements DatabaseEventListener {

    private final long startNs;
    private long lastNs;

    /**
     * Create a new instance of this class, and startNs the timer.
     */
    public ShowProgress() {
        startNs = lastNs = System.nanoTime();
    }

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new ShowProgress().test();
    }

    /**
     * Run the progress test.
     */
    void test() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:test", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, 'Test' || SPACE(100))");
        long time;
        time = System.nanoTime();
        int len = 1000;
        for (int i = 0; i < len; i++) {
            long now = System.nanoTime();
            if (now > time + TimeUnit.SECONDS.toNanos(1)) {
                time = now;
                System.out.println("Inserting " + (100L * i / len) + "%");
            }
            prep.setInt(1, i);
            prep.execute();
        }
        boolean abnormalTermination = true;
        if (abnormalTermination) {
            ((JdbcConnection) conn).setPowerOffCount(1);
            try {
                stat.execute("INSERT INTO TEST VALUES(-1, 'Test' || SPACE(100))");
            } catch (SQLException e) {
                // ignore
            }
        } else {
            conn.close();
        }

        System.out.println("Open connection...");
        time = System.nanoTime();
        conn = DriverManager.getConnection(
                "jdbc:h2:test;DATABASE_EVENT_LISTENER='" +
                getClass().getName() + "'", "sa", "");
        time = System.nanoTime() - time;
        System.out.println("Done after " + TimeUnit.NANOSECONDS.toMillis(time) + " ms");
        prep.close();
        stat.close();
        conn.close();

    }

    /**
     * This method is called if an exception occurs in the database.
     *
     * @param e the exception
     * @param sql the SQL statement
     */
    @Override
    public void exceptionThrown(SQLException e, String sql) {
        System.out.println("Error executing " + sql);
        e.printStackTrace();
    }

    /**
     * This method is called when opening the database to notify about the
     * progress.
     *
     * @param state the current state
     * @param name the object name (depends on the state)
     * @param current the current progress
     * @param max the 100% mark
     */
    @Override
    public void setProgress(int state, String name, int current, int max) {
        long time = System.nanoTime();
        if (time < lastNs + TimeUnit.SECONDS.toNanos(5)) {
            return;
        }
        lastNs = time;
        String stateName = "?";
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
            return;
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            // ignore
        }
        System.out.println("State: " + stateName + " " +
                (100 * current / max) + "% (" +
                current + " of " + max + ") "
                + TimeUnit.NANOSECONDS.toMillis(time - startNs) + " ms");
    }

    /**
     * This method is called when the database is closed.
     */
    @Override
    public void closingDatabase() {
        System.out.println("Closing the database");
    }

    /**
     * This method is called just after creating the instance.
     *
     * @param url the database URL
     */
    @Override
    public void init(String url) {
        System.out.println("Initializing the event listener for database " + url);
    }

    /**
     * This method is called when the database is open.
     */
    @Override
    public void opened() {
        // do nothing
    }

}
