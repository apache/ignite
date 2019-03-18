/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.poweroff;

import java.io.File;
import java.io.FileDescriptor;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * This test shows the raw file access performance using various file modes.
 * It also tests databases.
 */
public class TestWrite {

    private TestWrite() {
        // utility class
    }

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        testFile("rw", false);
        testFile("rwd", false);
        testFile("rws", false);
        testFile("rw", true);
        testFile("rwd", true);
        testFile("rws", true);
        testDatabase("org.h2.Driver",
                "jdbc:h2:test", "sa", "");
        testDatabase("org.hsqldb.jdbcDriver",
                "jdbc:hsqldb:test4", "sa", "");
        testDatabase("org.apache.derby.jdbc.EmbeddedDriver",
                "jdbc:derby:test;create=true", "sa", "");
        testDatabase("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost/test", "sa", "sa");
        testDatabase("org.postgresql.Driver",
                "jdbc:postgresql:test", "sa", "sa");
    }

    private static void testFile(String mode, boolean flush) throws Exception {
        System.out.println("Testing RandomAccessFile(.., \"" + mode + "\")...");
        if (flush) {
            System.out.println("  with FileDescriptor.sync()");
        }
        RandomAccessFile file = new RandomAccessFile("test.txt", mode);
        file.setLength(0);
        FileDescriptor fd = file.getFD();
        long start = System.nanoTime();
        byte[] data = { 0 };
        file.write(data);
        int i = 0;
        if (flush) {
            for (;; i++) {
                file.seek(0);
                file.write(data);
                fd.sync();
                if ((i & 15) == 0) {
                    long time = System.nanoTime() - start;
                    if (time > TimeUnit.SECONDS.toNanos(5)) {
                        break;
                    }
                }
            }
        } else {
            for (;; i++) {
                file.seek(0);
                file.write(data);
                if ((i & 1023) == 0) {
                    long time = System.nanoTime() - start;
                    if (time > TimeUnit.SECONDS.toNanos(5)) {
                        break;
                    }
                }
            }
        }
        long time = System.nanoTime() - start;
        System.out.println("Time: " + TimeUnit.NANOSECONDS.toMillis(time));
        System.out.println("Operations: " + i);
        System.out.println("Operations/second: " + (i * TimeUnit.SECONDS.toNanos(1) / time));
        System.out.println();
        file.close();
        new File("test.txt").delete();
    }

    private static void testDatabase(String driver, String url, String user,
            String password) throws Exception {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, user, password);
        System.out.println("Testing Database, URL=" + url);
        Statement stat = conn.createStatement();
        try {
            stat.execute("DROP TABLE TEST");
        } catch (SQLException e) {
            // ignore
        }
        stat.execute("CREATE TABLE TEST(ID INT)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?)");
        long start = System.nanoTime();
        int i = 0;
        for (;; i++) {
            prep.setInt(1, i);
            // autocommit is on by default, so this commits as well
            prep.execute();
            if ((i & 15) == 0) {
                long time = System.nanoTime() - start;
                if (time > TimeUnit.SECONDS.toNanos(5)) {
                    break;
                }
            }
        }
        long time = System.nanoTime() - start;
        System.out.println("Time: " + TimeUnit.NANOSECONDS.toMillis(time));
        System.out.println("Operations: " + i);
        System.out.println("Operations/second: " + (i * TimeUnit.SECONDS.toNanos(1) / time));
        System.out.println();
        stat.execute("DROP TABLE TEST");
        conn.close();
    }

}
