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
import org.h2.test.TestBase;

/**
 * Tests for the ON DUPLICATE KEY UPDATE in the Insert class.
 */
public class TestDuplicateKeyUpdate extends TestBase {

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
        deleteDb("duplicateKeyUpdate");
        Connection conn = getConnection("duplicateKeyUpdate;MODE=MySQL");
        testDuplicateOnPrimary(conn);
        testDuplicateOnUnique(conn);
        testDuplicateCache(conn);
        testDuplicateExpression(conn);
        testOnDuplicateKeyInsertBatch(conn);
        testOnDuplicateKeyInsertMultiValue(conn);
        testPrimaryKeyAndUniqueKey(conn);
        conn.close();
        deleteDb("duplicateKeyUpdate");
    }

    private void testDuplicateOnPrimary(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE table_test (\n" +
                "  id bigint(20) NOT NULL ,\n" +
                "  a_text varchar(254) NOT NULL,\n" +
                "  some_text varchar(254) NULL,\n" +
                "  PRIMARY KEY (id)\n" +
                ") ;");

        stat.execute("INSERT INTO table_test ( id, a_text, some_text ) VALUES " +
                "(1, 'aaaaaaaaaa', 'aaaaaaaaaa'), " +
                "(2, 'bbbbbbbbbb', 'bbbbbbbbbb'), "+
                "(3, 'cccccccccc', 'cccccccccc'), " +
                "(4, 'dddddddddd', 'dddddddddd'), " +
                "(5, 'eeeeeeeeee', 'eeeeeeeeee')");

        stat.execute("INSERT INTO table_test ( id , a_text, some_text ) " +
                "VALUES (1, 'zzzzzzzzzz', 'abcdefghij') " +
                "ON DUPLICATE KEY UPDATE some_text='UPDATE'");

        rs = stat.executeQuery("SELECT some_text FROM table_test where id = 1");
        rs.next();
        assertEquals("UPDATE", rs.getNString(1));

        stat.execute("INSERT INTO table_test ( id , a_text, some_text ) " +
                "VALUES (3, 'zzzzzzzzzz', 'SOME TEXT') " +
                "ON DUPLICATE KEY UPDATE some_text=values(some_text)");
        rs = stat.executeQuery("SELECT some_text FROM table_test where id = 3");
        rs.next();
        assertEquals("SOME TEXT", rs.getNString(1));
    }

    private void testDuplicateOnUnique(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE table_test2 (\n"
                + "  id bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  a_text varchar(254) NOT NULL,\n"
                + "  some_text varchar(254) NOT NULL,\n"
                + "  updatable_text varchar(254) NULL,\n"
                + "  PRIMARY KEY (id)\n" + ") ;");

        stat.execute("CREATE UNIQUE INDEX index_name \n"
                + "ON table_test2 (a_text, some_text);");

        stat.execute("INSERT INTO table_test2 " +
                "( a_text, some_text, updatable_text ) VALUES ('a', 'a', '1')");
        stat.execute("INSERT INTO table_test2 " +
                "( a_text, some_text, updatable_text ) VALUES ('b', 'b', '2')");
        stat.execute("INSERT INTO table_test2 " +
                "( a_text, some_text, updatable_text ) VALUES ('c', 'c', '3')");
        stat.execute("INSERT INTO table_test2 " +
                "( a_text, some_text, updatable_text ) VALUES ('d', 'd', '4')");
        stat.execute("INSERT INTO table_test2 " +
                "( a_text, some_text, updatable_text ) VALUES ('e', 'e', '5')");

        stat.execute("INSERT INTO table_test2 ( a_text, some_text ) " +
                "VALUES ('e', 'e') ON DUPLICATE KEY UPDATE updatable_text='UPDATE'");

        rs = stat.executeQuery("SELECT updatable_text " +
                "FROM table_test2 where a_text = 'e'");
        rs.next();
        assertEquals("UPDATE", rs.getNString(1));

        stat.execute("INSERT INTO table_test2 (a_text, some_text, updatable_text ) " +
                "VALUES ('b', 'b', 'test'), ('c', 'c', 'test2') " +
                "ON DUPLICATE KEY UPDATE updatable_text=values(updatable_text)");
        rs = stat.executeQuery("SELECT updatable_text " +
                "FROM table_test2 where a_text in ('b', 'c') order by a_text");
        rs.next();
        assertEquals("test", rs.getNString(1));
        rs.next();
        assertEquals("test2", rs.getNString(1));
    }

    private void testDuplicateCache(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE table_test3 (\n" +
                "  id bigint(20) NOT NULL ,\n" +
                "  a_text varchar(254) NOT NULL,\n" +
                "  some_text varchar(254) NULL,\n" +
                "  PRIMARY KEY (id)\n" +
                ") ;");

        stat.execute("INSERT INTO table_test3 ( id, a_text, some_text ) " +
                "VALUES (1, 'aaaaaaaaaa', 'aaaaaaaaaa')");

        stat.execute("INSERT INTO table_test3 ( id , a_text, some_text ) " +
                "VALUES (1, 'zzzzzzzzzz', 'SOME TEXT') " +
                "ON DUPLICATE KEY UPDATE some_text=values(some_text)");
        rs = stat.executeQuery("SELECT some_text FROM table_test3 where id = 1");
        rs.next();
        assertEquals("SOME TEXT", rs.getNString(1));

        // Execute twice the same query to use the one from cache without
        // parsing, caused the values parameter to be seen as ambiguous
        stat.execute("INSERT INTO table_test3 ( id , a_text, some_text ) " +
                "VALUES (1, 'zzzzzzzzzz', 'SOME TEXT') " +
                "ON DUPLICATE KEY UPDATE some_text=values(some_text)");
        rs = stat.executeQuery("SELECT some_text FROM table_test3 where id = 1");
        rs.next();
        assertEquals("SOME TEXT", rs.getNString(1));
    }

    private void testDuplicateExpression(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("CREATE TABLE table_test4 (\n" +
                "  id bigint(20) NOT NULL ,\n" +
                "  a_text varchar(254) NOT NULL,\n" +
                "  some_value int(10) NULL,\n" +
                "  PRIMARY KEY (id)\n" +
                ") ;");

        stat.execute("INSERT INTO table_test4 ( id, a_text, some_value ) " +
                "VALUES (1, 'aaaaaaaaaa', 5)");
        stat.execute("INSERT INTO table_test4 ( id, a_text, some_value ) " +
                "VALUES (2, 'aaaaaaaaaa', 5)");

        stat.execute("INSERT INTO table_test4 ( id , a_text, some_value ) " +
                "VALUES (1, 'b', 1) " +
                "ON DUPLICATE KEY UPDATE some_value=some_value + values(some_value)");
        stat.execute("INSERT INTO table_test4 ( id , a_text, some_value ) " +
                "VALUES (1, 'b', 1) " +
                "ON DUPLICATE KEY UPDATE some_value=some_value + 100");
        stat.execute("INSERT INTO table_test4 ( id , a_text, some_value ) " +
                "VALUES (2, 'b', 1) " +
                "ON DUPLICATE KEY UPDATE some_value=values(some_value) + 1");
        rs = stat.executeQuery("SELECT some_value FROM table_test4 where id = 1");
        rs.next();
        assertEquals(106, rs.getInt(1));
        rs = stat.executeQuery(
                "SELECT some_value FROM table_test4 where id = 2");
        rs.next();
        assertEquals(2, rs.getInt(1));
    }

    private void testOnDuplicateKeyInsertBatch(Connection conn)
            throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("create table test " +
                "(key varchar(1) primary key, count int not null)");

        // Insert multiple values as a batch
        for (int i = 0; i <= 2; ++i) {
            PreparedStatement prep = conn.prepareStatement(
                    "insert into test(key, count) values(?, ?) " +
                    "on duplicate key update count = count + 1");
            prep.setString(1, "a");
            prep.setInt(2, 1);
            prep.addBatch();
            prep.setString(1, "b");
            prep.setInt(2, 1);
            prep.addBatch();
            prep.setString(1, "b");
            prep.setInt(2, 1);
            prep.addBatch();
            prep.executeBatch();
        }

        // Check result
        ResultSet rs = stat.executeQuery(
                "select count from test where key = 'a'");
        rs.next();
        assertEquals(3, rs.getInt(1));

        stat.execute("drop table test");
    }

    private void testOnDuplicateKeyInsertMultiValue(Connection conn)
            throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("create table test" +
                "(key varchar(1) primary key, count int not null)");

        // Insert multiple values in single insert operation
        for (int i = 0; i <= 2; ++i) {
            PreparedStatement prep = conn.prepareStatement(
                    "insert into test(key, count) values(?, ?), (?, ?), (?, ?) " +
                    "on duplicate key update count = count + 1");
            prep.setString(1, "a");
            prep.setInt(2, 1);
            prep.setString(3, "b");
            prep.setInt(4, 1);
            prep.setString(5, "b");
            prep.setInt(6, 1);
            prep.executeUpdate();
        }
        conn.commit();

        // Check result
        ResultSet rs = stat.executeQuery("select count from test where key = 'a'");
        rs.next();
        assertEquals(3, rs.getInt(1));

        stat.execute("drop table test");
    }

    private void testPrimaryKeyAndUniqueKey(Connection conn) throws SQLException
    {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE test (id INT, dup INT, " +
                "counter INT, PRIMARY KEY(id), UNIQUE(dup))");
        stat.execute("INSERT INTO test (id, dup, counter) VALUES (1, 1, 1)");
        stat.execute("INSERT INTO test (id, dup, counter) VALUES (2, 1, 1) " +
                "ON DUPLICATE KEY UPDATE counter = counter + VALUES(counter)");

        // Check result
        ResultSet rs = stat.executeQuery("SELECT counter FROM test ORDER BY id");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(false, rs.next());

        stat.execute("drop table test");
    }
}
