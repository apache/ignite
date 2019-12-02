/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.todo;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.h2.jdbc.JdbcConnection;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;
import org.h2.util.JdbcUtils;

/**
 * A test to detect disk space leaks when killing a process.
 */
public class TestDiskSpaceLeak {

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        DeleteDbFiles.execute("data", null, true);
        Class.forName("org.h2.Driver");
        Connection conn;
        long before = 0;
        for (int i = 0; i < 10; i++) {
            conn = DriverManager.getConnection("jdbc:h2:data/test");
            ResultSet rs;
            rs = conn.createStatement().executeQuery(
                    "select count(*) from information_schema.lobs");
            rs.next();
            System.out.println("lobs: " + rs.getInt(1));
            rs = conn.createStatement().executeQuery(
                    "select count(*) from information_schema.lob_map");
            rs.next();
            System.out.println("lob_map: " + rs.getInt(1));
            rs = conn.createStatement().executeQuery(
                    "select count(*) from information_schema.lob_data");
            rs.next();
            System.out.println("lob_data: " + rs.getInt(1));
            conn.close();
            Recover.execute("data", "test");
            new File("data/test.h2.sql").renameTo(new File("data/test." + i + ".sql"));
            conn = DriverManager.getConnection("jdbc:h2:data/test");
            // ((JdbcConnection) conn).setPowerOffCount(i);
            ((JdbcConnection) conn).setPowerOffCount(28);
            String last = "connect";
            try {
                conn.createStatement().execute("drop table test if exists");
                last = "drop";
                conn.createStatement().execute("create table test(id identity, b blob)");
                last = "create";
                conn.createStatement().execute("insert into test values(1, space(10000))");
                last = "insert";
                conn.createStatement().execute("delete from test");
                last = "delete";
                conn.createStatement().execute("insert into test values(1, space(10000))");
                last = "insert2";
                conn.createStatement().execute("delete from test");
                last = "delete2";
            } catch (SQLException e) {
                // ignore
            } finally {
                JdbcUtils.closeSilently(conn);
            }
            long now = new File("data/test.h2.db").length();
            long diff = now - before;
            before = now;
            System.out.println(now + " " + diff + " " + i + " " + last);
        }
    }

}
