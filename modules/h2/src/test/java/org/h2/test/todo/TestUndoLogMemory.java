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

/**
 * A test to reproduce out of memory using a large operation.
 */
public class TestUndoLogMemory {

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        TestUndoLogMemory.test(10, "null");
        TestUndoLogMemory.test(100, "space(100000)");
        // new TestUndoLogMemory().test(100000, "null");
        // new TestUndoLogMemory().test(1000, "space(100000)");
    }

    private static void test(int count, String defaultValue) throws SQLException {

        // -Xmx1m -XX:+HeapDumpOnOutOfMemoryError
        DeleteDbFiles.execute("data", "test", true);
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:data/test;large_transactions=true");
        Statement stat = conn.createStatement();
        stat.execute("set cache_size 32");
        stat.execute("SET max_operation_memory 100");
        stat.execute("SET max_memory_undo 100");
        conn.setAutoCommit(false);

        // also a problem: tables without unique index
        System.out.println("create--- " + count + " " + defaultValue);
        stat.execute("create table test(id int, name varchar default " +
                defaultValue + " )");
        System.out.println("insert---");
        stat.execute("insert into test(id) select x from system_range(1, " +
                count + ")");
        System.out.println("rollback---");
        conn.rollback();

        System.out.println("drop---");
        stat.execute("drop table test");
        System.out.println("create---");
        stat.execute("create table test" +
                "(id int primary key, name varchar default " +
                defaultValue + " )");

        // INSERT problem
        System.out.println("insert---");
        stat.execute(
            "insert into test(id) select x from system_range(1, "+count+")");
        System.out.println("delete---");
        stat.execute("delete from test");

        // DELETE problem
        System.out.println("insert---");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test(id) values(?)");
        for (int i = 0; i < count; i++) {
            prep.setInt(1, i);
            prep.execute();
        }
        System.out.println("delete---");
        stat.execute("delete from test");

        System.out.println("close---");
        conn.close();
    }

}
