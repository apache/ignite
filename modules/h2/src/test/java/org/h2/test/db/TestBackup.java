/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.h2.api.DatabaseEventListener;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.Backup;
import org.h2.tools.Restore;
import org.h2.util.Task;

/**
 * Test for the BACKUP SQL statement.
 */
public class TestBackup extends TestBase {

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
        if (config.memory) {
            return;
        }
        testConcurrentBackup();
        testBackupRestoreLobStatement();
        testBackupRestoreLob();
        testBackup();
        deleteDb("backup");
        FileUtils.delete(getBaseDir() + "/backup.zip");
    }

    private void testConcurrentBackup() throws SQLException {
        if (config.networked || !config.big) {
            return;
        }
        deleteDb("backup");
        String url = getURL("backup;multi_threaded=true", true);
        Connection conn = getConnection(url);
        final Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test select x, 'Hello' from system_range(1, 2)");
        conn.setAutoCommit(false);
        Connection conn1;
        conn1 = getConnection(url);
        final AtomicLong updateEnd = new AtomicLong();
        final Statement stat1 = conn.createStatement();
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    if (System.nanoTime() < updateEnd.get()) {
                        stat.execute("update test set name = 'Hallo'");
                        stat1.execute("checkpoint");
                        stat.execute("update test set name = 'Hello'");
                        stat.execute("commit");
                        stat.execute("checkpoint");
                    } else {
                        Thread.sleep(10);
                    }
                }
            }
        };
        Connection conn2;
        conn2 = getConnection(url + ";database_event_listener='" +
                BackupListener.class.getName() + "'");
        Statement stat2 = conn2.createStatement();
        task.execute();
        for (int i = 0; i < 10; i++) {
            updateEnd.set(System.nanoTime() + TimeUnit.SECONDS.toNanos(2));
            stat2.execute("backup to '"+getBaseDir()+"/backup.zip'");
            stat2.execute("checkpoint");
            Restore.execute(getBaseDir() + "/backup.zip", getBaseDir() + "/t" + i, "backup");
            Connection conn3 = getConnection("t" + i + "/backup");
            Statement stat3 = conn3.createStatement();
            stat3.execute("script");
            ResultSet rs = stat3.executeQuery(
                    "select * from test where name='Hallo'");
            while (rs.next()) {
                fail();
            }
            conn3.close();
        }
        task.get();
        conn2.close();
        conn.close();
        conn1.close();
    }

    /**
     * A backup listener to test concurrent backup.
     */
    public static class BackupListener implements DatabaseEventListener {

        @Override
        public void closingDatabase() {
            // ignore
        }

        @Override
        public void exceptionThrown(SQLException e, String sql) {
            // ignore
        }

        @Override
        public void init(String url) {
            // ignore
        }

        @Override
        public void opened() {
            // ignore
        }

        @Override
        public void setProgress(int state, String name, int x, int max) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
            if (x % 400 == 0) {
                // System.out.println("state: " + state +
                //    " name: " + name + " x:" + x + "/" + max);
            }
        }

    }

    private void testBackupRestoreLob() throws SQLException {
        deleteDb("backup");
        Connection conn = getConnection("backup");
        conn.createStatement().execute(
                "create table test(x clob) as select space(10000)");
        conn.close();
        Backup.execute(getBaseDir() + "/backup.zip",
                getBaseDir(), "backup", true);
        deleteDb("backup");
        Restore.execute(getBaseDir() + "/backup.zip",
                getBaseDir(), "backup");
    }

    private void testBackupRestoreLobStatement() throws SQLException {
        deleteDb("backup");
        Connection conn = getConnection("backup");
        conn.createStatement().execute(
                "create table test(x clob) as select space(10000)");
        conn.createStatement().execute("backup to '" +
                getBaseDir() + "/backup.zip"+"'");
        conn.close();
        deleteDb("backup");
        Restore.execute(getBaseDir() + "/backup.zip",
                getBaseDir(), "backup");
    }

    private void testBackup() throws SQLException {
        deleteDb("backup");
        deleteDb("restored");
        Connection conn1, conn2, conn3;
        Statement stat1, stat2, stat3;
        conn1 = getConnection("backup");
        stat1 = conn1.createStatement();
        stat1.execute("create table test" +
                "(id int primary key, name varchar(255))");
        stat1.execute("insert into test values" +
                "(1, 'first'), (2, 'second')");
        stat1.execute("create table testlob" +
                "(id int primary key, b blob, c clob)");
        stat1.execute("insert into testlob values" +
                "(1, space(10000), repeat('00', 10000))");
        conn2 = getConnection("backup");
        stat2 = conn2.createStatement();
        stat2.execute("insert into test values(3, 'third')");
        conn2.setAutoCommit(false);
        stat2.execute("insert into test values(4, 'fourth (uncommitted)')");
        stat2.execute("insert into testlob values(2, ' ', '00')");

        stat1.execute("backup to '" + getBaseDir() + "/backup.zip'");
        conn2.rollback();
        assertEqualDatabases(stat1, stat2);

        Restore.execute(getBaseDir() + "/backup.zip", getBaseDir(), "restored");
        conn3 = getConnection("restored");
        stat3 = conn3.createStatement();
        assertEqualDatabases(stat1, stat3);

        conn1.close();
        conn2.close();
        conn3.close();
        deleteDb("restored");
    }

}
