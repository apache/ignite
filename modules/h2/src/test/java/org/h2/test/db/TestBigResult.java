/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import org.h2.message.TraceSystem;
import org.h2.store.FileLister;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test for big result sets.
 */
public class TestBigResult extends TestDb {

    /**
     * Run just this test.
     *
     * @param a
     *              ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public boolean isEnabled() {
        if (config.memory) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws SQLException {
        testLargeSubquery();
        testSortingAndDistinct();
        testLOB();
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
        stat.execute("INSERT INTO RECOVERY " + "SELECT X, CASE MOD(X, 2) WHEN 0 THEN 'commit' ELSE 'begin' END "
                + "FROM SYSTEM_RANGE(1, " + len + ")");
        ResultSet rs = stat.executeQuery("SELECT * FROM RECOVERY " + "WHERE SQL_STMT LIKE 'begin%' AND "
                + "TRANSACTION_ID NOT IN(SELECT TRANSACTION_ID FROM RECOVERY "
                + "WHERE SQL_STMT='commit' OR SQL_STMT='rollback')");
        int count = 0, last = 1;
        while (rs.next()) {
            assertEquals(last, rs.getInt(1));
            last += 2;
            count++;
        }
        assertEquals(len / 2, count);
        conn.close();
    }

    private void testSortingAndDistinct() throws SQLException {
        deleteDb("bigResult");
        Connection conn = getConnection("bigResult");
        Statement stat = conn.createStatement();
        int count = getSize(1000, 4000);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE INT NOT NULL)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");
        for (int i = 0; i < count; i++) {
            ps.setInt(1, i);
            ps.setInt(2, count - i);
            ps.executeUpdate();
        }
        // local result
        testSortingAndDistinct1(stat, count, count);
        // external result
        testSortingAndDistinct1(stat, 10, count);
        stat.execute("DROP TABLE TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE1 INT NOT NULL, VALUE2 INT NOT NULL)");
        ps = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?, ?)");
        int partCount = count / 10;
        for (int i = 0; i < count; i++) {
            ps.setInt(1, i);
            int a = i / 10;
            int b = i % 10;
            ps.setInt(2, partCount - a);
            ps.setInt(3, 10 - b);
            ps.executeUpdate();
        }
        String sql;
        /*
         * Sorting only
         */
        sql = "SELECT VALUE2, VALUE1 FROM (SELECT ID, VALUE2, VALUE1 FROM TEST ORDER BY VALUE2)";
        // local result
        testSortingAndDistinct2(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct2(stat, sql, 10, partCount);
        /*
         * Distinct only
         */
        sql = "SELECT VALUE2, VALUE1 FROM (SELECT DISTINCT ID, VALUE2, VALUE1 FROM TEST)";
        // local result
        testSortingAndDistinct2DistinctOnly(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct2DistinctOnly(stat, sql, 10, partCount);
        /*
         * Sorting and distinct
         */
        sql = "SELECT VALUE2, VALUE1 FROM (SELECT DISTINCT ID, VALUE2, VALUE1 FROM TEST ORDER BY VALUE2)";
        // local result
        testSortingAndDistinct2(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct2(stat, sql, 10, partCount);
        /*
         * One more distinct only
         */
        sql = "SELECT VALUE1 FROM (SELECT DISTINCT VALUE1 FROM TEST)";
        // local result
        testSortingAndDistinct3DistinctOnly(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct3DistinctOnly(stat, sql, 1, partCount);
        /*
         * One more sorting and distinct
         */
        sql = "SELECT VALUE1 FROM (SELECT DISTINCT VALUE1 FROM TEST ORDER BY VALUE1)";
        // local result
        testSortingAndDistinct3(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct3(stat, sql, 1, partCount);
        stat.execute("DROP TABLE TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE INT)");
        ps = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");
        for (int i = 0; i < count; i++) {
            ps.setInt(1, i);
            int j = i / 10;
            if (j == 0) {
                ps.setNull(2, Types.INTEGER);
            } else {
                ps.setInt(2, j);
            }
            ps.executeUpdate();
        }
        /*
         * Sorting and distinct
         */
        sql = "SELECT DISTINCT VALUE FROM TEST ORDER BY VALUE";
        // local result
        testSortingAndDistinct4(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct4(stat, sql, 1, partCount);
        /*
         * Distinct only
         */
        sql = "SELECT DISTINCT VALUE FROM TEST";
        // local result
        testSortingAndDistinct4DistinctOnly(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct4DistinctOnly(stat, sql, 1, partCount);
        /*
         * Sorting only
         */
        sql = "SELECT VALUE FROM TEST ORDER BY VALUE";
        // local result
        testSortingAndDistinct4SortingOnly(stat, sql, count, partCount);
        // external result
        testSortingAndDistinct4SortingOnly(stat, sql, 1, partCount);
        conn.close();
    }

    private void testSortingAndDistinct1(Statement stat, int maxRows, int count) throws SQLException {
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        ResultSet rs = stat.executeQuery("SELECT VALUE FROM (SELECT DISTINCT ID, VALUE FROM TEST ORDER BY VALUE)");
        for (int i = 1; i <= count; i++) {
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), i);
        }
        assertFalse(rs.next());
    }

    private void testSortingAndDistinct2(Statement stat, String sql, int maxRows, int partCount) throws SQLException {
        ResultSet rs;
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        rs = stat.executeQuery(sql);
        BitSet set = new BitSet(partCount);
        for (int i = 1; i <= 10; i++) {
            set.clear();
            for (int j = 1; j <= partCount; j++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
                set.set(rs.getInt(2));
            }
            assertEquals(partCount + 1, set.nextClearBit(1));
        }
        assertFalse(rs.next());
    }

    private void testSortingAndDistinct2DistinctOnly(Statement stat, String sql, int maxRows, int partCount)
            throws SQLException {
        ResultSet rs;
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        rs = stat.executeQuery(sql);
        BitSet set = new BitSet(partCount * 10);
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= partCount; j++) {
                assertTrue(rs.next());
                set.set(rs.getInt(1) * partCount + rs.getInt(2));
            }
        }
        assertEquals(partCount * 11 + 1, set.nextClearBit(partCount + 1));
        assertFalse(rs.next());
    }

    private void testSortingAndDistinct3(Statement stat, String sql, int maxRows, int partCount) throws SQLException {
        ResultSet rs;
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        rs = stat.executeQuery(sql);
        for (int i = 1; i <= partCount; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    private void testSortingAndDistinct3DistinctOnly(Statement stat, String sql, int maxRows, int partCount)
            throws SQLException {
        ResultSet rs;
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        rs = stat.executeQuery(sql);
        BitSet set = new BitSet(partCount);
        for (int i = 1; i <= partCount; i++) {
            assertTrue(rs.next());
            set.set(rs.getInt(1));
        }
        assertEquals(partCount + 1, set.nextClearBit(1));
        assertFalse(rs.next());
    }

    private void testSortingAndDistinct4(Statement stat, String sql, int maxRows, int count) throws SQLException {
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        ResultSet rs = stat.executeQuery(sql);
        for (int i = 0; i < count; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            if (i == 0) {
                assertTrue(rs.wasNull());
            }
        }
        assertFalse(rs.next());
    }

    private void testSortingAndDistinct4DistinctOnly(Statement stat, String sql, int maxRows, int count)
            throws SQLException {
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        ResultSet rs = stat.executeQuery(sql);
        BitSet set = new BitSet();
        for (int i = 0; i < count; i++) {
            assertTrue(rs.next());
            int v = rs.getInt(1);
            if (v == 0) {
                assertTrue(rs.wasNull());
            }
            assertFalse(set.get(v));
            set.set(v);
        }
        assertFalse(rs.next());
        assertEquals(count, set.nextClearBit(0));
    }

    private void testSortingAndDistinct4SortingOnly(Statement stat, String sql, int maxRows, int count)
            throws SQLException {
        stat.execute("SET MAX_MEMORY_ROWS " + maxRows);
        ResultSet rs = stat.executeQuery(sql);
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < 10; j++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
                if (i == 0) {
                    assertTrue(rs.wasNull());
                }
            }
        }
        assertFalse(rs.next());
    }

    private void testLOB() throws SQLException {
        if (config.traceLevelFile == TraceSystem.DEBUG) {
            // Trace system on this level can throw OOME with such large
            // arguments as used in this test.
            return;
        }
        deleteDb("bigResult");
        Connection conn = getConnection("bigResult");
        Statement stat = conn.createStatement();
        stat.execute("SET MAX_MEMORY_ROWS " + 1);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE BLOB NOT NULL)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");
        int length = 1_000_000;
        byte[] data = new byte[length];
        for (int i = 1; i <= 10; i++) {
            ps.setInt(1, i);
            Arrays.fill(data, (byte) i);
            ps.setBytes(2, data);
            ps.executeUpdate();
        }
        Blob[] blobs = new Blob[10];
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        for (int i = 1; i <= 10; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            blobs[i - 1] = rs.getBlob(2);
        }
        assertFalse(rs.next());
        rs.close();
        for (int i = 1; i <= 10; i++) {
            Blob b = blobs[i - 1];
            byte[] bytes = b.getBytes(1, (int) b.length());
            Arrays.fill(data, (byte) i);
            assertEquals(data, bytes);
            b.free();
        }
        stat.execute("DROP TABLE TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE CLOB NOT NULL)");
        ps = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");
        char[] cdata = new char[length];
        for (int i = 1; i <= 10; i++) {
            ps.setInt(1, i);
            Arrays.fill(cdata, (char) i);
            ps.setString(2, new String(cdata));
            ps.executeUpdate();
        }
        Clob[] clobs = new Clob[10];
        rs = stat.executeQuery("SELECT * FROM TEST");
        for (int i = 1; i <= 10; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            clobs[i - 1] = rs.getClob(2);
        }
        assertFalse(rs.next());
        rs.close();
        for (int i = 1; i <= 10; i++) {
            Clob c = clobs[i - 1];
            String string = c.getSubString(1, (int) c.length());
            Arrays.fill(cdata, (char) i);
            assertEquals(new String(cdata), string);
            c.free();
        }
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
        ArrayList<String> files = FileLister.getDatabaseFiles(getBaseDir(), "bigResult", true);
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
        stat.execute("CREATE TABLE TEST(" + "ID INT PRIMARY KEY, " + "Name VARCHAR(255), " + "FirstName VARCHAR(255), "
                + "Points INT," + "LicenseID INT)");
        int len = getSize(10, 5000);
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?, ?, ?)");
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
        Statement s2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
