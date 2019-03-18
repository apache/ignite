/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.h2.api.Trigger;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.jdbc.JdbcStatement;
import org.h2.test.TestBase;

/**
 * Tests for the {@link Statement#getGeneratedKeys()}.
 */
public class TestGetGeneratedKeys extends TestBase {

    public static class TestGetGeneratedKeysTrigger implements Trigger {

        @Override
        public void close() throws SQLException {
        }

        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
            if (newRow[0] == null) {
                newRow[0] = UUID.randomUUID();
            }
        }

        @Override
        public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                int type) throws SQLException {
        }

        @Override
        public void remove() throws SQLException {
        }

    }

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("getGeneratedKeys");
        Connection conn = getConnection("getGeneratedKeys");
        testBatchAndMergeInto(conn);
        testCalledSequences(conn);
        testInsertWithSelect(conn);
        testMergeUsing(conn);
        testMultithreaded(conn);
        testNameCase(conn);

        testPrepareStatement_Execute(conn);
        testPrepareStatement_ExecuteBatch(conn);
        testPrepareStatement_ExecuteLargeBatch(conn);
        testPrepareStatement_ExecuteLargeUpdate(conn);
        testPrepareStatement_ExecuteUpdate(conn);
        testPrepareStatement_int_Execute(conn);
        testPrepareStatement_int_ExecuteBatch(conn);
        testPrepareStatement_int_ExecuteLargeBatch(conn);
        testPrepareStatement_int_ExecuteLargeUpdate(conn);
        testPrepareStatement_int_ExecuteUpdate(conn);
        testPrepareStatement_intArray_Execute(conn);
        testPrepareStatement_intArray_ExecuteBatch(conn);
        testPrepareStatement_intArray_ExecuteLargeBatch(conn);
        testPrepareStatement_intArray_ExecuteLargeUpdate(conn);
        testPrepareStatement_intArray_ExecuteUpdate(conn);
        testPrepareStatement_StringArray_Execute(conn);
        testPrepareStatement_StringArray_ExecuteBatch(conn);
        testPrepareStatement_StringArray_ExecuteLargeBatch(conn);
        testPrepareStatement_StringArray_ExecuteLargeUpdate(conn);
        testPrepareStatement_StringArray_ExecuteUpdate(conn);

        testStatementExecute(conn);
        testStatementExecute_int(conn);
        testStatementExecute_intArray(conn);
        testStatementExecute_StringArray(conn);
        testStatementExecuteLargeUpdate(conn);
        testStatementExecuteLargeUpdate_int(conn);
        testStatementExecuteLargeUpdate_intArray(conn);
        testStatementExecuteLargeUpdate_StringArray(conn);
        testStatementExecuteUpdate(conn);
        testStatementExecuteUpdate_int(conn);
        testStatementExecuteUpdate_intArray(conn);
        testStatementExecuteUpdate_StringArray(conn);

        testTrigger(conn);
        conn.close();
        deleteDb("getGeneratedKeys");
    }

    /**
     * Test for batch updates and MERGE INTO operator.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testBatchAndMergeInto(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID BIGINT AUTO_INCREMENT, UID UUID DEFAULT RANDOM_UUID(), VALUE INT)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (?), (?)",
                Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.setInt(1, 4);
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(1L, rs.getLong(1));
        UUID u1 = (UUID) rs.getObject(2);
        assertTrue(u1 != null);
        rs.next();
        assertEquals(2L, rs.getLong(1));
        UUID u2 = (UUID) rs.getObject(2);
        assertTrue(u2 != null);
        rs.next();
        assertEquals(3L, rs.getLong(1));
        UUID u3 = (UUID) rs.getObject(2);
        assertTrue(u3 != null);
        rs.next();
        assertEquals(4L, rs.getLong(1));
        UUID u4 = (UUID) rs.getObject(2);
        assertTrue(u4 != null);
        assertFalse(rs.next());
        assertFalse(u1.equals(u2));
        assertFalse(u2.equals(u3));
        assertFalse(u3.equals(u4));
        prep = conn.prepareStatement("MERGE INTO TEST(ID, VALUE) KEY(ID) VALUES (?, ?)",
                Statement.RETURN_GENERATED_KEYS);
        prep.setInt(1, 2);
        prep.setInt(2, 10);
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        prep.setInt(1, 5);
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test for keys generated by sequences.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testCalledSequences(Connection conn) throws Exception {
        Statement stat = conn.createStatement();

        stat.execute("CREATE SEQUENCE SEQ");
        stat.execute("CREATE TABLE TEST(ID INT)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", Statement.RETURN_GENERATED_KEYS);
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", Statement.RETURN_GENERATED_KEYS);
        prep.execute();
        rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new int[] { 1 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new String[] { "ID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        prep.execute();
        rs = prep.getGeneratedKeys();
        rs.next();
        assertFalse(rs.next());

        stat.execute("DROP TABLE TEST");
        stat.execute("DROP SEQUENCE SEQ");

        stat.execute("CREATE TABLE TEST(ID BIGINT)");
        stat.execute("CREATE SEQUENCE SEQ");
        prep = conn.prepareStatement("INSERT INTO TEST VALUES (30), (NEXT VALUE FOR SEQ),"
                + " (NEXT VALUE FOR SEQ), (NEXT VALUE FOR SEQ), (20)", Statement.RETURN_GENERATED_KEYS);
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        rs.next();
        assertEquals(1L, rs.getLong(1));
        rs.next();
        assertEquals(2L, rs.getLong(1));
        rs.next();
        assertEquals(3L, rs.getLong(1));
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        stat.execute("DROP SEQUENCE SEQ");
    }

    /**
     * Test method for INSERT ... SELECT operator.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testInsertWithSelect(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT, VALUE INT NOT NULL)");

        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) SELECT 10",
                Statement.RETURN_GENERATED_KEYS);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
        rs.close();

        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for MERGE USING operator.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testMergeUsing(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE SOURCE (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + " UID INT NOT NULL UNIQUE, VALUE INT NOT NULL)");
        stat.execute("CREATE TABLE DESTINATION (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + " UID INT NOT NULL UNIQUE, VALUE INT NOT NULL)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO SOURCE(UID, VALUE) VALUES (?, ?)");
        for (int i = 1; i <= 100; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 10 + 5);
            ps.executeUpdate();
        }
        // Insert first half of a rows with different values
        ps = conn.prepareStatement("INSERT INTO DESTINATION(UID, VALUE) VALUES (?, ?)");
        for (int i = 1; i <= 50; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 10);
            ps.executeUpdate();
        }
        // And merge second half into it, first half will be updated with a new values
        ps = conn.prepareStatement(
                "MERGE INTO DESTINATION USING SOURCE ON (DESTINATION.UID = SOURCE.UID)"
                        + " WHEN MATCHED THEN UPDATE SET VALUE = SOURCE.VALUE"
                        + " WHEN NOT MATCHED THEN INSERT (UID, VALUE) VALUES (SOURCE.UID, SOURCE.VALUE)",
                Statement.RETURN_GENERATED_KEYS);
        // All rows should be either updated or inserted
        assertEquals(100, ps.executeUpdate());
        ResultSet rs = ps.getGeneratedKeys();
        // Only 50 keys for inserted rows should be generated
        for (int i = 1; i <= 50; i++) {
            assertTrue(rs.next());
            assertEquals(i + 50, rs.getLong(1));
        }
        assertFalse(rs.next());
        rs.close();
        // Check merged data
        rs = stat.executeQuery("SELECT ID, UID, VALUE FROM DESTINATION ORDER BY ID");
        for (int i = 1; i <= 100; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getLong(1));
            assertEquals(i, rs.getInt(2));
            assertEquals(i * 10 + 5, rs.getInt(3));
        }
        assertFalse(rs.next());
        stat.execute("DROP TABLE SOURCE");
        stat.execute("DROP TABLE DESTINATION");
    }

    /**
     * Test method for shared connection between several statements in different
     * threads.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testMultithreaded(final Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT," + "VALUE INT NOT NULL)");
        final int count = 4, iterations = 10_000;
        Thread[] threads = new Thread[count];
        final long[] keys = new long[count * iterations];
        for (int i = 0; i < count; i++) {
            final int num = i;
            threads[num] = new Thread("getGeneratedKeys-" + num) {
                @Override
                public void run() {
                    try {
                        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (?)",
                                Statement.RETURN_GENERATED_KEYS);
                        for (int i = 0; i < iterations; i++) {
                            int value = iterations * num + i;
                            prep.setInt(1, value);
                            prep.execute();
                            ResultSet rs = prep.getGeneratedKeys();
                            rs.next();
                            keys[value] = rs.getLong(1);
                            rs.close();
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            };
        }
        for (int i = 0; i < count; i++) {
            threads[i].start();
        }
        for (int i = 0; i < count; i++) {
            threads[i].join();
        }
        ResultSet rs = stat.executeQuery("SELECT VALUE, ID FROM TEST ORDER BY VALUE");
        for (int i = 0; i < keys.length; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals(keys[i], rs.getLong(2));
        }
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for case of letters in column names.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testNameCase(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "\"id\" UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        // Test columns with only difference in case
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)",
                new String[] { "id", "ID" });
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("id", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(1L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        // Test lower case name of upper case column
        stat.execute("ALTER TABLE TEST DROP COLUMN \"id\"");
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", new String[] { "id" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertFalse(rs.next());
        rs.close();
        // Test upper case name of lower case column
        stat.execute("ALTER TABLE TEST ALTER COLUMN ID RENAME TO \"id\"");
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)", new String[] { "ID" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("id", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)");
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)");
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)");
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)");
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String)}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)");
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(3L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(4L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", Statement.NO_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)",
                Statement.RETURN_GENERATED_KEYS);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(3L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(4L, rs.getLong("ID"));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertEquals(UUID.class, rs.getObject("UID").getClass());
        assertEquals(UUID.class, rs.getObject("UID", UUID.class).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", Statement.NO_GENERATED_KEYS);
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)",
                Statement.RETURN_GENERATED_KEYS);
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int)}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_int_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)",
                Statement.NO_GENERATED_KEYS);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        prep.execute();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", new int[] { 1, 2 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)", new int[] { 2, 1 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", new int[] { 1, 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)", new int[] { 2, 1 });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)",
                new int[] { 1, 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)",
                new int[] { 2, 1 });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)",
                new int[] { 1, 2 });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)",
                new int[] { 2, 1 });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, int[])}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_intArray_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", new int[] { 1, 2 });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)", new int[] { 2, 1 });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#execute()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_Execute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", new String[] { "ID", "UID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)", new String[] { "UID", "ID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new String[] { "UID" });
        prep.execute();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", new String[] { "ID", "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)", new String[] { "UID", "ID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new String[] { "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeLargeBatch()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteLargeBatch(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)",
                new String[] { "ID", "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)",
                new String[] { "UID", "ID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(5L, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(6L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)",
                new String[] { "UID" });
        prep.addBatch();
        prep.addBatch();
        prep.executeLargeBatch();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for
     * {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeLargeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteLargeUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        JdbcPreparedStatement prep = (JdbcPreparedStatement) conn
                .prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        prep.executeLargeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)",
                new String[] { "ID", "UID" });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)",
                new String[] { "UID", "ID" });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = (JdbcPreparedStatement) conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)",
                new String[] { "UID" });
        prep.executeLargeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Connection#prepareStatement(String, String[])}
     * .{@link PreparedStatement#executeUpdate()}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testPrepareStatement_StringArray_ExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (20)", new String[] { "ID", "UID" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (30)", new String[] { "UID", "ID" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        prep = conn.prepareStatement("INSERT INTO TEST(VALUE) VALUES (40)", new String[] { "UID" });
        prep.executeUpdate();
        rs = prep.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#execute(String)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.execute("INSERT INTO TEST(VALUE) VALUES (10)");
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#execute(String, int)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute_int(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.execute("INSERT INTO TEST(VALUE) VALUES (10)", Statement.NO_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(VALUE) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#execute(String, int[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute_intArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.execute("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(VALUE) VALUES (20)", new int[] { 1, 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(VALUE) VALUES (30)", new int[] { 2, 1 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, String[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecute_StringArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.execute("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(VALUE) VALUES (20)", new String[] { "ID", "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(VALUE) VALUES (30)", new String[] { "UID", "ID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.execute("INSERT INTO TEST(VALUE) VALUES (40)", new String[] { "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate(Connection conn) throws Exception {
        JdbcStatement stat = (JdbcStatement) conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (10)");
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String, int)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate_int(Connection conn) throws Exception {
        JdbcStatement stat = (JdbcStatement) conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (10)", Statement.NO_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String, int[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate_intArray(Connection conn) throws Exception {
        JdbcStatement stat = (JdbcStatement) conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (20)", new int[] { 1, 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (30)", new int[] { 2, 1 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeLargeUpdate(String, String[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteLargeUpdate_StringArray(Connection conn) throws Exception {
        JdbcStatement stat = (JdbcStatement) conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (20)", new String[] { "ID", "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (30)", new String[] { "UID", "ID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeLargeUpdate("INSERT INTO TEST(VALUE) VALUES (40)", new String[] { "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (10)");
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, int)}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate_int(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (10)", Statement.NO_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (20)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, int[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate_intArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (10)", new int[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (20)", new int[] { 1, 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (30)", new int[] { 2, 1 });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (40)", new int[] { 2 });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test method for {@link Statement#executeUpdate(String, String[])}.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testStatementExecuteUpdate_StringArray(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST (ID BIGINT PRIMARY KEY AUTO_INCREMENT,"
                + "UID UUID NOT NULL DEFAULT RANDOM_UUID(), VALUE INT NOT NULL)");
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (10)", new String[0]);
        ResultSet rs = stat.getGeneratedKeys();
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (20)", new String[] { "ID", "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("UID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(UUID.class, rs.getObject(2).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (30)", new String[] { "UID", "ID" });
        rs = stat.getGeneratedKeys();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertEquals("ID", rs.getMetaData().getColumnName(2));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertEquals(3L, rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (40)", new String[] { "UID" });
        rs = stat.getGeneratedKeys();
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("UID", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        assertEquals(UUID.class, rs.getObject(1).getClass());
        assertFalse(rs.next());
        rs.close();
        stat.execute("DROP TABLE TEST");
    }

    /**
     * Test for keys generated by trigger.
     *
     * @param conn
     *            connection
     * @throws Exception
     *             on exception
     */
    private void testTrigger(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID UUID, VALUE INT)");
        stat.execute("CREATE TRIGGER TEST_INSERT BEFORE INSERT ON TEST FOR EACH ROW CALL \""
                + TestGetGeneratedKeysTrigger.class.getName() + '"');
        stat.executeUpdate("INSERT INTO TEST(VALUE) VALUES (10), (20)", Statement.RETURN_GENERATED_KEYS);
        ResultSet rs = stat.getGeneratedKeys();
        rs.next();
        UUID u1 = (UUID) rs.getObject(1);
        rs.next();
        UUID u2 = (UUID) rs.getObject(1);
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT ID FROM TEST ORDER BY VALUE");
        rs.next();
        assertEquals(u1, rs.getObject(1));
        rs.next();
        assertEquals(u2, rs.getObject(1));
        stat.execute("DROP TRIGGER TEST_INSERT");
        stat.execute("DROP TABLE TEST");
    }

}
