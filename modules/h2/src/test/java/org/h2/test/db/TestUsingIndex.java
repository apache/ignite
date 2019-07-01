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
import org.h2.test.TestBase;
import org.h2.value.DataType;

/**
 * Tests the "create index ... using" syntax.
 *
 * @author Erwan Bocher Atelier SIG, IRSTV FR CNRS 2488
 */
public class TestUsingIndex extends TestBase {

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
    public void test() throws SQLException {
        deleteDb("using_index");
        testUsingBadSyntax();
        testUsingGoodSyntax();
        testHashIndex();
        testSpatialIndex();
        testBadSpatialSyntax();
    }

    private void testHashIndex() throws SQLException {
        conn = getConnection("using_index");
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create index idx_name on test(id) using hash");
        stat.execute("insert into test select x from system_range(1, 1000)");
        ResultSet rs = stat.executeQuery("select * from test where id=100");
        assertTrue(rs.next());
        assertFalse(rs.next());
        stat.execute("delete from test where id=100");
        rs = stat.executeQuery("select * from test where id=100");
        assertFalse(rs.next());
        rs = stat.executeQuery("select min(id), max(id) from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1000, rs.getInt(2));
        stat.execute("drop table test");
        conn.close();
        deleteDb("using_index");
    }

    private void testUsingBadSyntax() throws SQLException {
        conn = getConnection("using_index");
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        assertFalse(isSupportedSyntax(stat,
                "create hash index idx_name_1 on test(id) using hash"));
        assertFalse(isSupportedSyntax(stat,
                "create hash index idx_name_2 on test(id) using btree"));
        assertFalse(isSupportedSyntax(stat,
                "create index idx_name_3 on test(id) using hash_tree"));
        assertFalse(isSupportedSyntax(stat,
                "create unique hash index idx_name_4 on test(id) using hash"));
        assertFalse(isSupportedSyntax(stat,
                "create index idx_name_5 on test(id) using hash table"));
        conn.close();
        deleteDb("using_index");
    }

    private void testUsingGoodSyntax() throws SQLException {
        conn = getConnection("using_index");
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        assertTrue(isSupportedSyntax(stat,
                "create index idx_name_1 on test(id) using hash"));
        assertTrue(isSupportedSyntax(stat,
                "create index idx_name_2 on test(id) using btree"));
        assertTrue(isSupportedSyntax(stat,
                "create unique index idx_name_3 on test(id) using hash"));
        conn.close();
        deleteDb("using_index");
    }

    /**
     * Return if the syntax is supported otherwise false
     *
     * @param stat the statement
     * @param sql the SQL statement
     * @return true if the query works, false if it fails
     */
    private static boolean isSupportedSyntax(Statement stat, String sql) {
        try {
            stat.execute(sql);
            return true;
        } catch (SQLException ex) {
            return false;
        }
    }

    private void testSpatialIndex() throws SQLException {
        if (!config.mvStore && config.mvcc) {
            return;
        }
        if (config.memory && config.mvcc) {
            return;
        }
        if (DataType.GEOMETRY_CLASS == null) {
            return;
        }
        deleteDb("spatial");
        conn = getConnection("spatial");
        stat = conn.createStatement();
        stat.execute("create table test"
                + "(id int primary key, poly geometry)");
        stat.execute("insert into test values(1, "
                + "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        stat.execute("insert into test values(2,null)");
        stat.execute("insert into test values(3, "
                + "'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
        stat.execute("insert into test values(4,null)");
        stat.execute("insert into test values(5, "
                + "'POLYGON ((1 3, 1 4, 2 4, 1 3))')");
        stat.execute("create index on test(poly) using rtree");

        ResultSet rs = stat.executeQuery(
                "select * from test "
                        + "where poly && 'POINT (1.5 1.5)'::Geometry");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());
        rs.close();
        conn.close();
        deleteDb("spatial");
    }

    private void testBadSpatialSyntax() throws SQLException {
        if (!config.mvStore && config.mvcc) {
            return;
        }
        if (config.memory && config.mvcc) {
            return;
        }
        if (DataType.GEOMETRY_CLASS == null) {
            return;
        }
        deleteDb("spatial");
        conn = getConnection("spatial");
        stat = conn.createStatement();
        stat.execute("create table test"
                + "(id int primary key, poly geometry)");
        stat.execute("insert into test values(1, "
                + "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        assertFalse(isSupportedSyntax(stat,
                "create spatial index on test(poly) using rtree"));
        conn.close();
        deleteDb("spatial");
    }

}