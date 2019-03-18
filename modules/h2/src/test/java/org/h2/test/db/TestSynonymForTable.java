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
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcSQLException;
import org.h2.test.TestBase;

/**
 * Tests for table synonyms.
 */
public class TestSynonymForTable extends TestBase {

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
        testSelectFromSynonym();
        testInsertIntoSynonym();
        testInsertWithColumnNameIntoSynonym();
        testUpdate();
        testDeleteFromSynonym();
        testTruncateSynonym();
        testExistingTableName();
        testCreateForUnknownTable();
        testMetaData();
        testCreateOrReplace();
        testCreateOrReplaceExistingTable();
        testSynonymInDifferentSchema();
        testReopenDatabase();
        testDropSynonym();
        testDropTable();
        testDropSchema();
    }

    private void testUpdate() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);
        insertIntoSynonym(conn, 25);

        Statement stmnt = conn.createStatement();
        assertEquals(1, stmnt.executeUpdate("UPDATE testsynonym set id = 30 WHERE id = 25"));

        assertSynonymContains(conn, 30);

        conn.close();
    }

    private void testDropSchema() throws SQLException {
        Connection conn = getConnection("synonym");
        Statement stat = conn.createStatement();

        stat.execute("CREATE SCHEMA IF NOT EXISTS s1");
        stat.execute("CREATE TABLE IF NOT EXISTS s1.backingtable(id INT PRIMARY KEY)");
        stat.execute("CREATE OR REPLACE SYNONYM testsynonym FOR s1.backingtable");
        stat.execute("DROP SCHEMA s1 CASCADE");

        assertThrows(JdbcSQLException.class, stat).execute("SELECT id FROM testsynonym");
        conn.close();
    }

    private void testDropTable() throws SQLException  {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE backingtable");

        // Backing table does not exist anymore.
        assertThrows(JdbcSQLException.class, stat).execute("SELECT id FROM testsynonym");

        // Synonym should be dropped as well
        ResultSet synonyms = conn.createStatement().executeQuery(
                "SELECT * FROM INFORMATION_SCHEMA.SYNONYMS WHERE SYNONYM_NAME='TESTSYNONYM'");
        assertFalse(synonyms.next());
        conn.close();

        // Reopening should work with dropped synonym
        Connection conn2 = getConnection("synonym");
        assertThrows(JdbcSQLException.class, stat).execute("SELECT id FROM testsynonym");
        conn2.close();
    }

    private void testDropSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);
        Statement stat = conn.createStatement();

        stat.execute("DROP SYNONYM testsynonym");

        // Synonym does not exist anymore.
        assertThrows(JdbcSQLException.class, stat).execute("SELECT id FROM testsynonym");

        // Dropping with "if exists" should succeed even if the synonym does not exist anymore.
        stat.execute("DROP SYNONYM IF EXISTS testsynonym");

        // Without "if exists" the command should fail if the synonym does not exist.
        assertThrows(JdbcSQLException.class, stat).execute("DROP SYNONYM testsynonym");
        conn.close();
    }

    private void testSynonymInDifferentSchema() throws SQLException {
        Connection conn = getConnection("synonym");
        Statement stat = conn.createStatement();

        stat.execute("CREATE SCHEMA IF NOT EXISTS s1");
        stat.execute("CREATE TABLE IF NOT EXISTS s1.backingtable(id INT PRIMARY KEY)");
        stat.execute("TRUNCATE TABLE s1.backingtable");
        stat.execute("CREATE OR REPLACE SYNONYM testsynonym FOR s1.backingtable");
        stat.execute("INSERT INTO s1.backingtable VALUES(15)");
        assertSynonymContains(conn, 15);
        conn.close();
    }

    private void testCreateOrReplaceExistingTable() throws SQLException {
        Connection conn = getConnection("synonym");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS backingtable(id INT PRIMARY KEY)");

        assertThrows(JdbcSQLException.class, stat).execute("CREATE OR REPLACE SYNONYM backingtable FOR backingtable");
        conn.close();
    }

    private void testCreateOrReplace() throws SQLException {
        // start with a fresh db so the first create or replace has to actually create the synonym.
        deleteDb("synonym");
        Connection conn = getConnection("synonym");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS backingtable(id INT PRIMARY KEY)");
        stat.execute("CREATE TABLE IF NOT EXISTS backingtable2(id INT PRIMARY KEY)");
        stat.execute("CREATE OR REPLACE SYNONYM testsynonym FOR backingtable");
        insertIntoBackingTable(conn, 17);

        ResultSet rs = stat.executeQuery("SELECT id FROM testsynonym");
        assertTrue(rs.next());
        assertEquals(17, rs.getInt(1));

        stat.execute("CREATE OR REPLACE SYNONYM testsynonym FOR backingtable2");

        // Should not return a result, since backingtable2 is empty.
        ResultSet rs2 = stat.executeQuery("SELECT id FROM testsynonym");
        assertFalse(rs2.next());
        conn.close();

        deleteDb("synonym");
    }

    private void testMetaData() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        ResultSet tables = conn.getMetaData().getTables(null, Constants.SCHEMA_MAIN, null,
                new String[]{"SYNONYM"});
        assertTrue(tables.next());
        assertEquals(tables.getString("TABLE_NAME"), "TESTSYNONYM");
        assertEquals(tables.getString("TABLE_TYPE"), "SYNONYM");
        assertFalse(tables.next());

        ResultSet columns = conn.getMetaData().getColumns(null, Constants.SCHEMA_MAIN, "TESTSYNONYM", null);
        assertTrue(columns.next());
        assertEquals(columns.getString("TABLE_NAME"), "TESTSYNONYM");
        assertEquals(columns.getString("COLUMN_NAME"), "ID");
        assertFalse(columns.next());

        ResultSet synonyms = conn.createStatement().executeQuery("SELECT * FROM INFORMATION_SCHEMA.SYNONYMS");
        assertTrue(synonyms.next());
        assertEquals("SYNONYM", synonyms.getString("SYNONYM_CATALOG"));
        assertEquals("PUBLIC", synonyms.getString("SYNONYM_SCHEMA"));
        assertEquals("TESTSYNONYM", synonyms.getString("SYNONYM_NAME"));
        assertEquals("BACKINGTABLE", synonyms.getString("SYNONYM_FOR"));
        assertEquals("VALID", synonyms.getString("STATUS"));
        assertEquals("", synonyms.getString("REMARKS"));
        assertTrue(synonyms.getString("ID") != null);
        assertFalse(synonyms.next());
        conn.close();
    }

    private void testCreateForUnknownTable() throws SQLException {
        Connection conn = getConnection("synonym");
        Statement stat = conn.createStatement();

        assertThrows(JdbcSQLException.class, stat).execute("CREATE SYNONYM someSynonym FOR nonexistingTable");
        conn.close();
    }

    private void testExistingTableName() throws SQLException {
        Connection conn = getConnection("synonym");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS backingtable(id INT PRIMARY KEY)");

        assertThrows(JdbcSQLException.class, stat).execute("CREATE SYNONYM backingtable FOR backingtable");
        conn.close();
    }

    /**
     * Make sure, that the schema changes are persisted when reopening the database
     */
    private void testReopenDatabase() throws SQLException {
        if(!config.memory) {
            deleteDb("synonym");
            Connection conn = getConnection("synonym");
            createTableWithSynonym(conn);
            insertIntoBackingTable(conn, 9);
            conn.close();
            Connection conn2 = getConnection("synonym");
            assertSynonymContains(conn2, 9);
            conn2.close();
        }
    }

    private void testTruncateSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        insertIntoBackingTable(conn, 7);
        assertBackingTableContains(conn, 7);

        conn.createStatement().execute("TRUNCATE TABLE testsynonym");

        assertBackingTableIsEmpty(conn);
        conn.close();
    }

    private void testDeleteFromSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        insertIntoBackingTable(conn, 7);
        assertBackingTableContains(conn, 7);
        deleteFromSynonym(conn, 7);

        assertBackingTableIsEmpty(conn);
        conn.close();
    }

    private static void deleteFromSynonym(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "DELETE FROM testsynonym WHERE id = ?");
        prep.setInt(1, id);
        prep.execute();
    }

    private void assertBackingTableIsEmpty(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT id FROM backingtable");
        assertFalse(rs.next());
    }

    private void testInsertIntoSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        insertIntoSynonym(conn, 5);
        assertBackingTableContains(conn, 5);
        conn.close();
    }

    private void testInsertWithColumnNameIntoSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO testsynonym (id) VALUES(?)");
        prep.setInt(1, 55);
        prep.execute();
        assertBackingTableContains(conn, 55);
        conn.close();
    }

    private void assertBackingTableContains(Connection conn, int testValue) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT id FROM backingtable");
        assertTrue(rs.next());
        assertEquals(testValue, rs.getInt(1));
        assertFalse(rs.next());
    }

    private void testSelectFromSynonym() throws SQLException {
        deleteDb("synonym");
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);
        insertIntoBackingTable(conn, 1);
        assertSynonymContains(conn, 1);
        conn.close();
    }

    private void assertSynonymContains(Connection conn, int id) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT id FROM testsynonym");
        assertTrue(rs.next());
        assertEquals(id, rs.getInt(1));
        assertFalse(rs.next());
    }

    private static void insertIntoSynonym(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO testsynonym VALUES(?)");
        prep.setInt(1, id);
        prep.execute();
    }

    private static void insertIntoBackingTable(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO backingtable VALUES(?)");
        prep.setInt(1, id);
        prep.execute();
    }

    private static void createTableWithSynonym(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS backingtable(id INT PRIMARY KEY)");
        stat.execute("CREATE OR REPLACE SYNONYM testsynonym FOR backingtable");
        stat.execute("TRUNCATE TABLE backingtable");
    }

}
