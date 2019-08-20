/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.test.TestBase;
import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;

/**
 * A recovery test that checks the consistency of a database (if it exists),
 * then deletes everything and runs in an endless loop executing random
 * operations. This loop is usually stopped by switching off the computer.
 */
public class TestTimer extends TestBase {

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
        validateOld();
        DeleteDbFiles.execute(getBaseDir(), "timer", true);
        loop();
    }

    private void loop() throws SQLException {
        println("loop");
        Connection conn = getConnection("timer");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        Random random = new Random();
        int max = 0;
        int count = 0;
        long startTime = System.nanoTime();
        while (true) {
            int action = random.nextInt(10);
            int x = max == 0 ? 0 : random.nextInt(max);
            switch (action) {
            case 0:
            case 1:
            case 2:
                stat.execute("INSERT INTO TEST VALUES(NULL, 'Hello')");
                ResultSet rs = stat.getGeneratedKeys();
                rs.next();
                int i = rs.getInt(1);
                max = i;
                count++;
                break;
            case 3:
            case 4:
                if (count == 0) {
                    break;
                }
                stat.execute("UPDATE TEST SET NAME=NAME||'+' WHERE ID=" + x);
                break;
            case 5:
            case 6:
                if (count == 0) {
                    break;
                }
                count -= stat.executeUpdate("DELETE FROM TEST WHERE ID=" + x);
                break;
            case 7:
                rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
                rs.next();
                int c = rs.getInt(1);
                assertEquals(count, c);
                long time = System.nanoTime();
                if (time > startTime + TimeUnit.SECONDS.toNanos(5)) {
                    println("rows: " + count);
                    startTime = time;
                }
                break;
            default:
            }
        }
    }

    private void validateOld() {
        println("validate");
        try {
            Connection conn = getConnection("timer");
            // TODO validate transactions
            Statement stat = conn.createStatement();
            stat.execute("CREATE TABLE IF NOT EXISTS TEST(ID IDENTITY, NAME VARCHAR)");
            ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            int count = rs.getInt(1);
            println("row count: " + count);
            int real = 0;
            rs = stat.executeQuery("SELECT * FROM TEST");
            while (rs.next()) {
                real++;
            }
            if (real != count) {
                println("real count: " + real);
                throw new AssertionError("COUNT(*)=" + count + " SELECT=" + real);
            }
            rs = stat.executeQuery("SCRIPT");
            while (rs.next()) {
                rs.getString(1);
            }
            conn.close();
        } catch (Throwable e) {
            logError("validate", e);
            backup();
        }
    }

    private void backup() {
        println("backup");
        for (int i = 0;; i++) {
            String s = "timer." + i + ".zip";
            File f = new File(s);
            if (f.exists()) {
                continue;
            }
            try {
                Backup.execute(s, getBaseDir(), "timer", true);
            } catch (SQLException e) {
                logError("backup", e);
            }
            break;
        }
    }

}
