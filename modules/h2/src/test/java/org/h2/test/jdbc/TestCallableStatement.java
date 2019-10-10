/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.util.Utils;

/**
 * Tests for the CallableStatement class.
 */
public class TestCallableStatement extends TestBase {

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
        deleteDb("callableStatement");
        Connection conn = getConnection("callableStatement");
        testOutParameter(conn);
        testUnsupportedOperations(conn);
        testGetters(conn);
        testCallWithResultSet(conn);
        testPreparedStatement(conn);
        testCallWithResult(conn);
        testPrepare(conn);
        testClassLoader(conn);
        testArrayArgument(conn);
        testArrayReturnValue(conn);
        conn.close();
        deleteDb("callableStatement");
    }

    private void testOutParameter(Connection conn) throws SQLException {
        conn.createStatement().execute(
                "create table test(id identity) as select null");
        for (int i = 1; i < 20; i++) {
            CallableStatement cs = conn.prepareCall("{ ? = call IDENTITY()}");
            cs.registerOutParameter(1, Types.BIGINT);
            cs.execute();
            long id = cs.getLong(1);
            assertEquals(1, id);
            cs.close();
        }
        conn.createStatement().execute(
                "drop table test");
    }

    private void testUnsupportedOperations(Connection conn) throws SQLException {
        CallableStatement call;
        call = conn.prepareCall("select 10 as a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getURL(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getObject(1, Collections.<String, Class<?>>emptyMap());
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getRef(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getRowId(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getSQLXML(1);

        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getURL("a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getObject("a", Collections.<String, Class<?>>emptyMap());
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getRef("a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getRowId("a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                getSQLXML("a");

        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                setURL(1, (URL) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                setRef(1, (Ref) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                setRowId(1, (RowId) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                setSQLXML(1, (SQLXML) null);

        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                setURL("a", (URL) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                setRowId("a", (RowId) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).
                setSQLXML("a", (SQLXML) null);

    }

    private void testCallWithResultSet(Connection conn) throws SQLException {
        CallableStatement call;
        ResultSet rs;
        call = conn.prepareCall("select 10 as a");
        call.execute();
        rs = call.getResultSet();
        rs.next();
        assertEquals(10, rs.getInt(1));
    }

    private void testPreparedStatement(Connection conn) throws SQLException {
        // using a callable statement like a prepared statement
        CallableStatement call;
        call = conn.prepareCall("create table test(id int)");
        call.executeUpdate();
        call = conn.prepareCall("insert into test values(1), (2)");
        assertEquals(2, call.executeUpdate());
        call = conn.prepareCall("drop table test");
        call.executeUpdate();
    }

    private void testGetters(Connection conn) throws SQLException {
        CallableStatement call;
        call = conn.prepareCall("{?=call ?}");
        call.setLong(2, 1);
        call.registerOutParameter(1, Types.BIGINT);
        call.execute();
        assertEquals(1, call.getLong(1));
        assertEquals(1, call.getByte(1));
        assertEquals(1, ((Long) call.getObject(1)).longValue());
        assertEquals(1, call.getObject(1, Long.class).longValue());
        assertFalse(call.wasNull());

        call.setFloat(2, 1.1f);
        call.registerOutParameter(1, Types.REAL);
        call.execute();
        assertEquals(1.1f, call.getFloat(1));

        call.setDouble(2, Math.PI);
        call.registerOutParameter(1, Types.DOUBLE);
        call.execute();
        assertEquals(Math.PI, call.getDouble(1));

        call.setBytes(2, new byte[11]);
        call.registerOutParameter(1, Types.BINARY);
        call.execute();
        assertEquals(11, call.getBytes(1).length);
        assertEquals(11, call.getBlob(1).length());

        call.setDate(2, java.sql.Date.valueOf("2000-01-01"));
        call.registerOutParameter(1, Types.DATE);
        call.execute();
        assertEquals("2000-01-01", call.getDate(1).toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2000-01-01", call.getObject(1,
                            LocalDateTimeUtils.LOCAL_DATE).toString());
        }

        call.setTime(2, java.sql.Time.valueOf("01:02:03"));
        call.registerOutParameter(1, Types.TIME);
        call.execute();
        assertEquals("01:02:03", call.getTime(1).toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("01:02:03", call.getObject(1,
                            LocalDateTimeUtils.LOCAL_TIME).toString());
        }

        call.setTimestamp(2, java.sql.Timestamp.valueOf(
                "2001-02-03 04:05:06.789"));
        call.registerOutParameter(1, Types.TIMESTAMP);
        call.execute();
        assertEquals("2001-02-03 04:05:06.789", call.getTimestamp(1).toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2001-02-03T04:05:06.789", call.getObject(1,
                            LocalDateTimeUtils.LOCAL_DATE_TIME).toString());
        }

        call.setBoolean(2, true);
        call.registerOutParameter(1, Types.BIT);
        call.execute();
        assertEquals(true, call.getBoolean(1));

        call.setShort(2, (short) 123);
        call.registerOutParameter(1, Types.SMALLINT);
        call.execute();
        assertEquals(123, call.getShort(1));

        call.setBigDecimal(2, BigDecimal.TEN);
        call.registerOutParameter(1, Types.DECIMAL);
        call.execute();
        assertEquals("10", call.getBigDecimal(1).toString());
    }

    private void testCallWithResult(Connection conn) throws SQLException {
        CallableStatement call;
        for (String s : new String[]{"{?= call abs(?)}",
                " { ? = call abs(?)}", " {? = call abs(?)}"}) {
            call = conn.prepareCall(s);
            call.setInt(2, -3);
            call.registerOutParameter(1, Types.INTEGER);
            call.execute();
            assertEquals(3, call.getInt(1));
            call.executeUpdate();
            assertEquals(3, call.getInt(1));
        }
    }

    private void testPrepare(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        CallableStatement call;
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        call = conn.prepareCall("INSERT INTO TEST VALUES(?, ?)");
        call.setInt(1, 1);
        call.setString(2, "Hello");
        call.execute();
        call = conn.prepareCall("SELECT * FROM TEST",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        rs = call.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        call = conn.prepareCall("SELECT * FROM TEST",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        rs = call.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("CREATE ALIAS testCall FOR \"" +
                    getClass().getName() + ".testCall\"");
        call = conn.prepareCall("{CALL testCall(?, ?, ?, ?)}");
        call.setInt("A", 50);
        call.setString("B", "abc");
        long t = System.currentTimeMillis();
        call.setTimestamp("C", new Timestamp(t));
        call.setTimestamp("D", Timestamp.valueOf("2001-02-03 10:20:30.0"));
        call.registerOutParameter(1, Types.INTEGER);
        call.registerOutParameter("B", Types.VARCHAR);
        call.executeUpdate();
        try {
            call.getTimestamp("C");
            fail("not registered out parameter accessible");
        } catch (SQLException e) {
            // expected exception
        }
        call.registerOutParameter(3, Types.TIMESTAMP);
        call.registerOutParameter(4, Types.TIMESTAMP);
        call.executeUpdate();

        assertEquals(t + 1, call.getTimestamp(3).getTime());
        assertEquals(t + 1, call.getTimestamp("C").getTime());

        assertEquals("2001-02-03 10:20:30.0", call.getTimestamp(4).toString());
        assertEquals("2001-02-03 10:20:30.0", call.getTimestamp("D").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2001-02-03T10:20:30", call.getObject(4,
                            LocalDateTimeUtils.LOCAL_DATE_TIME).toString());
            assertEquals("2001-02-03T10:20:30", call.getObject("D",
                            LocalDateTimeUtils.LOCAL_DATE_TIME).toString());
        }
        assertEquals("10:20:30", call.getTime(4).toString());
        assertEquals("10:20:30", call.getTime("D").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("10:20:30", call.getObject(4,
                            LocalDateTimeUtils.LOCAL_TIME).toString());
            assertEquals("10:20:30", call.getObject("D",
                            LocalDateTimeUtils.LOCAL_TIME).toString());
        }
        assertEquals("2001-02-03", call.getDate(4).toString());
        assertEquals("2001-02-03", call.getDate("D").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2001-02-03", call.getObject(4,
                            LocalDateTimeUtils.LOCAL_DATE).toString());
            assertEquals("2001-02-03", call.getObject("D",
                            LocalDateTimeUtils.LOCAL_DATE).toString());
        }

        assertEquals(100, call.getInt(1));
        assertEquals(100, call.getInt("A"));
        assertEquals(100, call.getLong(1));
        assertEquals(100, call.getLong("A"));
        assertEquals("100", call.getBigDecimal(1).toString());
        assertEquals("100", call.getBigDecimal("A").toString());
        assertEquals(100, call.getFloat(1));
        assertEquals(100, call.getFloat("A"));
        assertEquals(100, call.getDouble(1));
        assertEquals(100, call.getDouble("A"));
        assertEquals(100, call.getByte(1));
        assertEquals(100, call.getByte("A"));
        assertEquals(100, call.getShort(1));
        assertEquals(100, call.getShort("A"));
        assertTrue(call.getBoolean(1));
        assertTrue(call.getBoolean("A"));

        assertEquals("ABC", call.getString(2));
        Reader r = call.getCharacterStream(2);
        assertEquals("ABC", IOUtils.readStringAndClose(r, -1));
        r = call.getNCharacterStream(2);
        assertEquals("ABC", IOUtils.readStringAndClose(r, -1));
        assertEquals("ABC", call.getString("B"));
        assertEquals("ABC", call.getNString(2));
        assertEquals("ABC", call.getNString("B"));
        assertEquals("ABC", call.getClob(2).getSubString(1, 3));
        assertEquals("ABC", call.getClob("B").getSubString(1, 3));
        assertEquals("ABC", call.getNClob(2).getSubString(1, 3));
        assertEquals("ABC", call.getNClob("B").getSubString(1, 3));

        try {
            call.getString(100);
            fail("incorrect parameter index value");
        } catch (SQLException e) {
            // expected exception
        }
        try {
            call.getString(0);
            fail("incorrect parameter index value");
        } catch (SQLException e) {
            // expected exception
        }
        try {
            call.getBoolean("X");
            fail("incorrect parameter name value");
        } catch (SQLException e) {
            // expected exception
        }

        call.setCharacterStream("B",
                new StringReader("xyz"));
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setCharacterStream("B",
                new StringReader("xyz-"), 3);
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setCharacterStream("B",
                new StringReader("xyz-"), 3L);
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setAsciiStream("B",
                new ByteArrayInputStream("xyz".getBytes(StandardCharsets.UTF_8)));
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setAsciiStream("B",
                new ByteArrayInputStream("xyz-".getBytes(StandardCharsets.UTF_8)), 3);
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setAsciiStream("B",
                new ByteArrayInputStream("xyz-".getBytes(StandardCharsets.UTF_8)), 3L);
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));

        call.setClob("B", new StringReader("xyz"));
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setClob("B", new StringReader("xyz-"), 3);
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));

        call.setNClob("B", new StringReader("xyz"));
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setNClob("B", new StringReader("xyz-"), 3);
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));

        call.setString("B", "xyz");
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));
        call.setNString("B", "xyz");
        call.executeUpdate();
        assertEquals("XYZ", call.getString("B"));

        // test for exceptions after closing
        call.close();
        assertThrows(ErrorCode.OBJECT_CLOSED, call).
                executeUpdate();
        assertThrows(ErrorCode.OBJECT_CLOSED, call).
                registerOutParameter(1, Types.INTEGER);
        assertThrows(ErrorCode.OBJECT_CLOSED, call).
                getString("X");
    }

    private void testClassLoader(Connection conn) throws SQLException {
        Utils.ClassFactory myFactory = new TestClassFactory();
        JdbcUtils.addClassFactory(myFactory);
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_CLASSLOADER FOR \"TestClassFactory.testClassF\"");
            ResultSet rs = stat.executeQuery("SELECT T_CLASSLOADER(true)");
            assertTrue(rs.next());
            assertEquals(false, rs.getBoolean(1));
        } finally {
            JdbcUtils.removeClassFactory(myFactory);
        }
    }

    private void testArrayArgument(Connection connection) throws SQLException {
        Array array = connection.createArrayOf("Int", new Object[] {0, 1, 2});
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE ALIAS getArrayLength FOR \"" +
                            getClass().getName() + ".getArrayLength\"");

            // test setArray
            try (CallableStatement callableStatement = connection
                    .prepareCall("{call getArrayLength(?)}")) {
                callableStatement.setArray(1, array);
                assertTrue(callableStatement.execute());

                try (ResultSet resultSet = callableStatement.getResultSet()) {
                    assertTrue(resultSet.next());
                    assertEquals(3, resultSet.getInt(1));
                    assertFalse(resultSet.next());
                }
            }

            // test setObject
            try (CallableStatement callableStatement = connection
                    .prepareCall("{call getArrayLength(?)}")) {
                callableStatement.setObject(1, array);
                assertTrue(callableStatement.execute());

                try (ResultSet resultSet = callableStatement.getResultSet()) {
                    assertTrue(resultSet.next());
                    assertEquals(3, resultSet.getInt(1));
                    assertFalse(resultSet.next());
                }
            }
        } finally {
            array.free();
        }
    }

    private void testArrayReturnValue(Connection connection) throws SQLException {
        Object[][] arraysToTest = new Object[][] {
            new Object[] {0, 1, 2},
            new Object[] {0, "1", 2},
            new Object[] {0, null, 2},
            new Object[] {0, new Object[] {"s", 1}, new Object[] {null, 1L}},
        };
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE ALIAS arrayIdentiy FOR \"" +
                            getClass().getName() + ".arrayIdentiy\"");

            for (Object[] arrayToTest : arraysToTest) {
                Array sqlInputArray = connection.createArrayOf("ignored", arrayToTest);
                try {
                    try (CallableStatement callableStatement = connection
                            .prepareCall("{call arrayIdentiy(?)}")) {
                        callableStatement.setArray(1, sqlInputArray);
                        assertTrue(callableStatement.execute());

                        try (ResultSet resultSet = callableStatement.getResultSet()) {
                            assertTrue(resultSet.next());

                            // test getArray()
                            Array sqlReturnArray = resultSet.getArray(1);
                            try {
                                assertEquals(
                                        (Object[]) sqlInputArray.getArray(),
                                        (Object[]) sqlReturnArray.getArray());
                            } finally {
                                sqlReturnArray.free();
                            }

                            // test getObject(Array.class)
                            sqlReturnArray = resultSet.getObject(1, Array.class);
                            try {
                                assertEquals(
                                        (Object[]) sqlInputArray.getArray(),
                                        (Object[]) sqlReturnArray.getArray());
                            } finally {
                                sqlReturnArray.free();
                            }

                            assertFalse(resultSet.next());
                        }
                    }
                } finally {
                    sqlInputArray.free();
                }

            }
        }
    }

    /**
     * Class factory unit test
     * @param b boolean value
     * @return !b
     */
    public static Boolean testClassF(Boolean b) {
        return !b;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param array the array
     * @return the length of the array
     */
    public static int getArrayLength(Object[] array) {
        return array == null ? 0 : array.length;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param array the array
     * @return the array
     */
    public static Object[] arrayIdentiy(Object[] array) {
        return array;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param a the value a
     * @param b the value b
     * @param c the value c
     * @param d the value d
     * @return a result set
     */
    public static ResultSet testCall(Connection conn, int a, String b,
            Timestamp c, Timestamp d) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("A", Types.INTEGER, 0, 0);
        rs.addColumn("B", Types.VARCHAR, 0, 0);
        rs.addColumn("C", Types.TIMESTAMP, 0, 0);
        rs.addColumn("D", Types.TIMESTAMP, 0, 0);
        if ("jdbc:columnlist:connection".equals(conn.getMetaData().getURL())) {
            return rs;
        }
        rs.addRow(a * 2, b.toUpperCase(), new Timestamp(c.getTime() + 1), d);
        return rs;
    }

    /**
     * A class factory used for testing.
     */
    static class TestClassFactory implements Utils.ClassFactory {

        @Override
        public boolean match(String name) {
            return name.equals("TestClassFactory");
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return TestCallableStatement.class;
        }
    }

}
