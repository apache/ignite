/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests for overloaded user defined functions.
 *
 * @author Gary Tong
 */
public class TestFunctionOverload extends TestBase {

    private static final String ME = TestFunctionOverload.class.getName();
    private Connection conn;
    private DatabaseMetaData meta;

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
        deleteDb("functionOverload");
        conn = getConnection("functionOverload");
        meta = conn.getMetaData();
        testControl();
        testOverload();
        testOverloadNamedArgs();
        testOverloadWithConnection();
        testOverloadError();
        conn.close();
        deleteDb("functionOverload");
    }

    private void testOverloadError() throws SQLException {
        Statement stat = conn.createStatement();
        assertThrows(ErrorCode.METHODS_MUST_HAVE_DIFFERENT_PARAMETER_COUNTS_2, stat).
                execute("create alias overloadError for \"" + ME + ".overloadError\"");
    }

    private void testControl() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("create alias overload0 for \"" + ME + ".overload0\"");
        ResultSet rs = stat.executeQuery("select overload0() from dual");
        assertTrue(rs.next());
        assertEquals("0 args", 0, rs.getInt(1));
        assertFalse(rs.next());
        rs = meta.getProcedures(null, null, "OVERLOAD0");
        rs.next();
        assertFalse(rs.next());
    }

    private void testOverload() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("create alias overload1or2 for \"" + ME + ".overload1or2\"");
        ResultSet rs = stat.executeQuery("select overload1or2(1) from dual");
        rs.next();
        assertEquals("1 arg", 1, rs.getInt(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("select overload1or2(1, 2) from dual");
        rs.next();
        assertEquals("2 args", 3, rs.getInt(1));
        assertFalse(rs.next());
        rs = meta.getProcedures(null, null, "OVERLOAD1OR2");
        rs.next();
        assertEquals(1, rs.getInt("NUM_INPUT_PARAMS"));
        rs.next();
        assertEquals(2, rs.getInt("NUM_INPUT_PARAMS"));
        assertFalse(rs.next());
    }

    private void testOverloadNamedArgs() throws SQLException {
        Statement stat = conn.createStatement();

        stat.execute("create alias overload1or2Named for \"" + ME +
                ".overload1or2(int)\"");

        ResultSet rs = stat.executeQuery("select overload1or2Named(1) from dual");
        assertTrue("First Row", rs.next());
        assertEquals("1 arg", 1, rs.getInt(1));
        assertFalse("Second Row", rs.next());
        rs.close();
        assertThrows(ErrorCode.METHOD_NOT_FOUND_1, stat).
                executeQuery("select overload1or2Named(1, 2) from dual");
        stat.close();
    }

    private void testOverloadWithConnection() throws SQLException {
        Statement stat = conn.createStatement();

        stat.execute("create alias overload1or2WithConn for \"" + ME +
                ".overload1or2WithConn\"");

        ResultSet rs = stat.executeQuery("select overload1or2WithConn(1) from dual");
        rs.next();
        assertEquals("1 arg", 1, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();

        rs = stat.executeQuery("select overload1or2WithConn(1, 2) from dual");
        rs.next();
        assertEquals("2 args", 3, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();

        stat.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return 0
     */
    public static int overload0() {
        return 0;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param one the value
     * @return the value
     */
    public static int overload1or2(int one) {
        return one;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param one the first value
     * @param two the second value
     * @return the sum of both
     */
    public static int overload1or2(int one, int two) {
        return one + two;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param one the value
     * @return the value
     */
    public static int overload1or2WithConn(Connection conn, int one)
            throws SQLException {
        conn.createStatement().executeQuery("select 1 from dual");
        return one;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param one the first value
     * @param two the second value
     * @return the sum of both
     */
    public static int overload1or2WithConn(int one, int two) {
        return one + two;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param one the first value
     * @param two the second value
     * @return the sum of both
     */
    public static int overloadError(int one, int two) {
        return one + two;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param one the first value
     * @param two the second value
     * @return the sum of both
     */
    public static int overloadError(double one, double two) {
        return (int) (one + two);
    }

}
