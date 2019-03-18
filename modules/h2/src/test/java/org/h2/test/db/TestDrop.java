/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * Test DROP statement
 */
public class TestDrop extends TestBase {

    private Connection conn;
    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("drop");
        conn = getConnection("drop");
        stat = conn.createStatement();

        testTableDependsOnView();
        testComputedColumnDependency();
        testInterSchemaDependency();

        conn.close();
        deleteDb("drop");
    }

    private void testTableDependsOnView() throws SQLException {
        stat.execute("drop all objects");
        stat.execute("create table a(x int)");
        stat.execute("create view b as select * from a");
        stat.execute("create table c(y int check (select count(*) from b) = 0)");
        stat.execute("drop all objects");
    }

    private void testComputedColumnDependency() throws SQLException {
        stat.execute("DROP ALL OBJECTS");
        stat.execute("CREATE TABLE A (A INT);");
        stat.execute("CREATE TABLE B (B INT AS SELECT A FROM A);");
        stat.execute("DROP ALL OBJECTS");
        stat.execute("CREATE SCHEMA TEST_SCHEMA");
        stat.execute("CREATE TABLE TEST_SCHEMA.A (A INT);");
        stat.execute("CREATE TABLE TEST_SCHEMA.B " +
                "(B INT AS SELECT A FROM TEST_SCHEMA.A);");
        stat.execute("DROP SCHEMA TEST_SCHEMA CASCADE");
    }

    private void testInterSchemaDependency() throws SQLException {
        stat.execute("drop all objects;");
        stat.execute("create schema table_view");
        stat.execute("set schema table_view");
        stat.execute("create table test1 (id int, name varchar(20))");
        stat.execute("create view test_view_1 as (select * from test1)");
        stat.execute("set schema public");
        stat.execute("create schema test_run");
        stat.execute("set schema test_run");
        stat.execute("create table test2 (id int, address varchar(20), " +
                "constraint a_cons check (id in (select id from table_view.test1)))");
        stat.execute("set schema public");
        stat.execute("drop all objects");
    }
}
