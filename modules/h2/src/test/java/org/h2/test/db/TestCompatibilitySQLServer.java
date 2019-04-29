/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test MSSQLServer compatibility mode.
 */
public class TestCompatibilitySQLServer extends TestDb {

    /**
     * Run just this test.
     *
     * @param s ignored
     */
    public static void main(String... s) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("sqlserver");

        final Connection conn = getConnection("sqlserver;MODE=MSSQLServer");
        try {
            testDiscardTableHints(conn);
            testUseIdentityAsAutoIncrementAlias(conn);
        } finally {
            conn.close();
            deleteDb("sqlserver");
        }
    }

    private void testDiscardTableHints(Connection conn) throws SQLException {
        final Statement stat = conn.createStatement();

        stat.execute("create table parent(id int primary key, name varchar(255))");
        stat.execute("create table child(" +
                            "id int primary key, " +
                            "parent_id int, " +
                            "name varchar(255), " +
                            "foreign key (parent_id) references public.parent(id))");

        stat.execute("select * from parent");
        stat.execute("select * from parent with(nolock)");
        stat.execute("select * from parent with(nolock, index = id)");
        stat.execute("select * from parent with(nolock, index(id, name))");

        stat.execute("select * from parent p " +
                            "join child ch on ch.parent_id = p.id");
        stat.execute("select * from parent p with(nolock) " +
                            "join child ch with(nolock) on ch.parent_id = p.id");
        stat.execute("select * from parent p with(nolock) " +
                            "join child ch with(nolock, index = id) on ch.parent_id = p.id");
        stat.execute("select * from parent p with(nolock) " +
                            "join child ch with(nolock, index(id, name)) on ch.parent_id = p.id");
    }

    private void testUseIdentityAsAutoIncrementAlias(Connection conn) throws SQLException {
        final Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key identity, expected_id int)");
        stat.execute("insert into test (expected_id) VALUES (1), (2), (3)");

        final ResultSet results = stat.executeQuery("select * from test");
        while (results.next()) {
            assertEquals(results.getInt("expected_id"), results.getInt("id"));
        }

        stat.execute("create table test2 (id int primary key not null identity)");
    }

}
