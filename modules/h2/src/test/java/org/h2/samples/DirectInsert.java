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

import org.h2.tools.DeleteDbFiles;

/**
 * Demonstrates the benefit of using the CREATE TABLE ... AS SELECT
 * optimization.
 */
public class DirectInsert {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        DeleteDbFiles.execute("~", "test", true);
        String url = "jdbc:h2:~/test";
        initialInsert(url, 200_000);
        for (int i = 0; i < 3; i++) {
            createAsSelect(url, true);
            createAsSelect(url, false);
        }
    }

    private static void initialInsert(String url, int len) throws SQLException {
        Connection conn = DriverManager.getConnection(url + ";LOG=0");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, 'Test' || SPACE(100))");
        long time = System.nanoTime();
        for (int i = 0; i < len; i++) {
            long now = System.nanoTime();
            if (now > time + TimeUnit.SECONDS.toNanos(1)) {
                time = now;
                System.out.println("Inserting " + (100L * i / len) + "%");
            }
            prep.setInt(1, i);
            prep.execute();
        }
        conn.commit();
        prep.close();
        stat.close();
        conn.close();
    }

    private static void createAsSelect(String url, boolean optimize)
            throws SQLException {
        Connection conn = DriverManager.getConnection(url +
                ";OPTIMIZE_INSERT_FROM_SELECT=" + optimize);
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST2");
        System.out.println("CREATE TABLE ... AS SELECT " +
                (optimize ? "(optimized)" : ""));
        long time = System.nanoTime();
        stat.execute("CREATE TABLE TEST2 AS SELECT * FROM TEST");
        System.out.printf("%.3f sec.\n", (double) (System.nanoTime() - time) /
                TimeUnit.SECONDS.toNanos(1));
        stat.execute("INSERT INTO TEST2 SELECT * FROM TEST2");
        stat.close();
        conn.close();
    }

}
