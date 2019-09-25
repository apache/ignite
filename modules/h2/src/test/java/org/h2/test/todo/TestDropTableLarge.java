/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.todo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.Profiler;

/**
 * Test the performance of dropping large tables
 */
public class TestDropTableLarge {

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        // System.setProperty("h2.largeTransactions", "true");
        TestDropTableLarge.test();
    }

    private static void test() throws SQLException {
        DeleteDbFiles.execute("data", "test", true);
        Connection conn = DriverManager.getConnection("jdbc:h2:data/test");
        Statement stat = conn.createStatement();
        stat.execute("create table test1(id identity, name varchar)");
        stat.execute("create table test2(id identity, name varchar)");
        conn.setAutoCommit(true);
        // use two tables to make sure the data stored on disk is not too simple
        PreparedStatement prep1 = conn.prepareStatement(
                "insert into test1(name) values(space(255))");
        PreparedStatement prep2 = conn.prepareStatement(
                "insert into test2(name) values(space(255))");
        for (int i = 0; i < 50000; i++) {
            if (i % 7 != 0) {
                prep1.execute();
            } else {
                prep2.execute();
            }
        }
        Profiler prof = new Profiler();
        prof.startCollecting();
        stat.execute("DROP TABLE test1");
        prof.stopCollecting();
        System.out.println(prof.getTop(3));
        conn.close();
    }

}
