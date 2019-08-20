/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.store.FileLister;
import org.h2.test.TestBase;

/**
 * Test for big result sets.
 */
public class TestBigResult extends TestBase {

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
        if (config.memory) {
            return;
        }
        testLargeSubquery();
        testLargeUpdateDelete();
        testCloseConnectionDelete();
        testOrderGroup();
        testLimitBufferedResult();
        deleteDb("bigResult");
    }

    private void testLargeSubquery() throws SQLException {
        deleteDb("bigResult");
        Connection conn = getConnection("bigResult");
        Statement stat = conn.createStatement();
        int len = getSize(1000, 4000);
        stat.execute("SET MAX_MEMORY_ROWS " + (len / 10));
        stat.execute("CREATE TABLE RECOVERY(TRANSACTION_ID INT, SQL_STMT VARCHAR)");
        stat.execute("INSERT INTO RECOVERY " +
                "SELECT X, CASE MOD(X, 2) WHEN 0 THEN 'commit' ELSE 'begin' END " +
                "FROM SYSTEM_RANGE(1, "+len+")");
        ResultSet rs = stat.executeQuery("SELECT * FROM RECOVERY " +
                "WHERE SQL_STMT LIKE 'begin%' AND " +
                "TRANSACTION_ID NOT IN(SELECT TRANSACTION_ID FROM RECOVERY " +
                "WHERE SQL_STMT='commit' OR SQL_STMT='rollback')");
        int count = 0, last = 1;
        while (rs.next()) {
            assertEquals(last, rs.getInt(1));
            last += 2;
            count++;
        }
        assertEquals(len / 2, count);
        conn.close();
    }

    private void testLargeUpdateDelete() throws SQLException {
        deleteDb("bigResult");
        Connection conn = getConnection("bigResult");
        Statement stat = conn.createStatement();
        int len = getSize(10000, 100000);
        stat.execute("SET MAX_OPERATION_MEMORY 4096");
        stat.execute("CREATE TABLE TEST AS SELECT * FROM SYSTEM_RANGE(1, " + len + ")");
        stat.execute("UPDATE TEST SET X=X+1");
        stat.execute("DELETE FROM TEST");
        conn.close();
    }

    private void testCloseConnectionDelete() throws SQLException {
        deleteDb("bigResult");
        Connection conn = getConnection("bigResult");
        Statement stat = conn.createStatement();
        stat.execute("SET MAX_MEMORY_ROWS 2");
        ResultSet rs = stat.executeQuery("SELECT * FROM SYSTEM_RANGE(1, 100)");
        while (rs.next()) {
            // ignore
        }
        // rs.close();
        conn.close();
        deleteDb("bigResult");
        ArrayList<String> files = FileLister.getDatabaseFiles(getBaseDir(),
                "bigResult", true);
        if (files.size() > 0) {
            fail("file not deleted: " + files.get(0));
        }
    }

    private void testLimitBufferedResult() throws SQLException {
        deleteDb("bigResult");
        Connection conn = getConnection("bigResult");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT)");
        for (int i = 0; i < 200; i++) {
            stat.execute("INSERT INTO TEST(ID) VALUES(" + i + ")");
        }
        stat.execute("SET MAX_MEMORY_ROWS 100");
        ResultSet rs;
        rs = stat.executeQuery("select id from test order by id limit 10 offset 85");
        for (int i = 85; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
        }
        rs = stat.executeQuery("select id from test order by id limit 10 offset 95");
        for (int i = 95; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
        }
        rs = stat.executeQuery("select id from test order by id limit 10 offset 105");
        for (int i = 105; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
        }
        conn.close();
    }

    private void testOrderGroup() throws SQLException {
        deleteDb("bigResult");
        Connection conn = getConnection("bigResult");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(" +
                "ID INT PRIMARY KEY, " +
                "Name VARCHAR(255), " +
                "FirstName VARCHAR(255), " +
                "Points INT," +
                "LicenseID INT)");
        int len = getSize(10, 5000);
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?, ?, ?, ?)");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Name " + i);
            prep.setString(3, "First Name " + i);
            prep.setInt(4, i * 10);
            prep.setInt(5, i * i);
            prep.execute();
        }
        conn.close();
        conn = getConnection("bigResult");
        stat = conn.createStatement();
        stat.setMaxRows(len + 1);
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        for (int i = 0; i < len; i++) {
            rs.next();
            assertEquals(i, rs.getInt(1));
            assertEquals("Name " + i, rs.getString(2));
            assertEquals("First Name " + i, rs.getString(3));
            assertEquals(i * 10, rs.getInt(4));
            assertEquals(i * i, rs.getInt(5));
        }

        stat.setMaxRows(len + 1);
        rs = stat.executeQuery("SELECT * FROM TEST WHERE ID >= 1000 ORDER BY ID");
        for (int i = 1000; i < len; i++) {
            rs.next();
            assertEquals(i, rs.getInt(1));
            assertEquals("Name " + i, rs.getString(2));
            assertEquals("First Name " + i, rs.getString(3));
            assertEquals(i * 10, rs.getInt(4));
            assertEquals(i * i, rs.getInt(5));
        }

        stat.execute("SET MAX_MEMORY_ROWS 2");
        rs = stat.executeQuery("SELECT Name, SUM(ID) FROM TEST GROUP BY NAME");
        while (rs.next()) {
            rs.getString(1);
            rs.getInt(2);
        }

        conn.setAutoCommit(false);
        stat.setMaxRows(0);
        stat.execute("SET MAX_MEMORY_ROWS 0");
        stat.execute("CREATE TABLE DATA(ID INT, NAME VARCHAR_IGNORECASE(255))");
        prep = conn.prepareStatement("INSERT INTO DATA VALUES(?, ?)");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setString(2, "" + i / 200);
            prep.execute();
        }
        Statement s2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        rs = s2.executeQuery("SELECT NAME FROM DATA");
        rs.last();
        conn.setAutoCommit(true);

        rs = s2.executeQuery("SELECT NAME FROM DATA ORDER BY ID");
        while (rs.next()) {
            // do nothing
        }

        conn.close();
    }

}
