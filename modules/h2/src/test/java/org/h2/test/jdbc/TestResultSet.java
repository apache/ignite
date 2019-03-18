/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.TimeZone;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Tests for the ResultSet implementation.
 */
public class TestResultSet extends TestBase {

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
        deleteDb("resultSet");
        conn = getConnection("resultSet");

        stat = conn.createStatement();

        testUnwrap();
        testReuseSimpleResult();
        testUnsupportedOperations();
        testAmbiguousColumnNames();
        testInsertRowWithUpdatableResultSetDefault();
        testBeforeFirstAfterLast();
        testParseSpecialValues();
        testSubstringPrecision();
        testSubstringDataType();
        testColumnLabelColumnName();
        testAbsolute();
        testFetchSize();
        testOwnUpdates();
        testUpdatePrimaryKey();
        testFindColumn();
        testColumnLength();
        testArray();
        testLimitMaxRows();

        trace("max rows=" + stat.getMaxRows());
        stat.setMaxRows(6);
        trace("max rows after set to 6=" + stat.getMaxRows());
        assertTrue(stat.getMaxRows() == 6);

        testInt();
        testSmallInt();
        testBigInt();
        testVarchar();
        testDecimal();
        testDoubleFloat();
        testDatetime();
        testDatetimeWithCalendar();
        testBlob();
        testClob();
        testAutoIncrement();

        conn.close();
        deleteDb("resultSet");

    }

    private void testUnwrap() throws SQLException {
        ResultSet rs = stat.executeQuery("select 1");
        assertTrue(rs.isWrapperFor(Object.class));
        assertTrue(rs.isWrapperFor(ResultSet.class));
        assertTrue(rs.isWrapperFor(rs.getClass()));
        assertFalse(rs.isWrapperFor(Integer.class));
        assertTrue(rs == rs.unwrap(Object.class));
        assertTrue(rs == rs.unwrap(ResultSet.class));
        assertTrue(rs == rs.unwrap(rs.getClass()));
        assertThrows(ErrorCode.INVALID_VALUE_2, rs).
                unwrap(Integer.class);
    }

    private void testReuseSimpleResult() throws SQLException {
        ResultSet rs = stat.executeQuery("select table(x array=((1)))");
        while (rs.next()) {
            rs.getString(1);
        }
        rs.close();
        rs = stat.executeQuery("select table(x array=((1)))");
        while (rs.next()) {
            rs.getString(1);
        }
        rs.close();
    }

    @SuppressWarnings("deprecation")
    private void testUnsupportedOperations() throws SQLException {
        ResultSet rs = stat.executeQuery("select 1 as x from dual");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
        getUnicodeStream(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
        getUnicodeStream("x");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getObject(1, Collections.<String, Class<?>>emptyMap());
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getObject("x", Collections.<String, Class<?>>emptyMap());
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getRef(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getRef("x");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getURL(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getURL("x");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getRowId(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getRowId("x");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getSQLXML(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getSQLXML("x");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateRef(1, (Ref) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateRef("x", (Ref) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateArray(1, (Array) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateArray("x", (Array) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateRowId(1, (RowId) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateRowId("x", (RowId) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateNClob(1, (NClob) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateNClob("x", (NClob) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateSQLXML(1, (SQLXML) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                updateSQLXML("x", (SQLXML) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                getCursorName();
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, rs).
                setFetchDirection(ResultSet.FETCH_REVERSE);
    }

    private void testAmbiguousColumnNames() throws SQLException {
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(1)");
        ResultSet rs = stat.executeQuery(
                "select 1 x, 2 x, 3 x, 4 x, 5 x, 6 x from test");
        rs.next();
        assertEquals(1, rs.getInt("x"));
        stat.execute("drop table test");
    }

    private void testInsertRowWithUpdatableResultSetDefault() throws Exception {
        stat.execute("create table test(id int primary key, " +
                "data varchar(255) default 'Hello')");
        PreparedStatement prep = conn.prepareStatement("select * from test",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = prep.executeQuery();
        rs.moveToInsertRow();
        rs.updateInt(1, 1);
        rs.insertRow();
        rs.close();
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals("Hello", rs.getString(2));
        assertEquals("Hello", rs.getString("data"));
        assertEquals("Hello", rs.getNString(2));
        assertEquals("Hello", rs.getNString("data"));
        assertEquals("Hello", IOUtils.readStringAndClose(
                rs.getNCharacterStream(2), -1));
        assertEquals("Hello", IOUtils.readStringAndClose(
                rs.getNCharacterStream("data"), -1));
        assertEquals("Hello", IOUtils.readStringAndClose(
                rs.getNClob(2).getCharacterStream(), -1));
        assertEquals("Hello", IOUtils.readStringAndClose(
                rs.getNClob("data").getCharacterStream(), -1));

        rs = prep.executeQuery();

        rs.moveToInsertRow();
        rs.updateInt(1, 2);
        rs.updateNString(2, "Hello");
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 3);
        rs.updateNString("data", "Hello");
        rs.insertRow();

        Clob c;
        Writer w;

        rs.moveToInsertRow();
        rs.updateInt(1, 4);
        c = conn.createClob();
        w = c.setCharacterStream(1);
        w.write("Hello");
        w.close();
        rs.updateClob(2, c);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 5);
        c = conn.createClob();
        w = c.setCharacterStream(1);
        w.write("Hello");
        w.close();
        rs.updateClob("data", c);
        rs.insertRow();

        InputStream in;

        rs.moveToInsertRow();
        rs.updateInt(1, 6);
        in = new ByteArrayInputStream("Hello".getBytes(StandardCharsets.UTF_8));
        rs.updateAsciiStream(2, in);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 7);
        in = new ByteArrayInputStream("Hello".getBytes(StandardCharsets.UTF_8));
        rs.updateAsciiStream("data", in);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 8);
        in = new ByteArrayInputStream("Hello-".getBytes(StandardCharsets.UTF_8));
        rs.updateAsciiStream(2, in, 5);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 9);
        in = new ByteArrayInputStream("Hello-".getBytes(StandardCharsets.UTF_8));
        rs.updateAsciiStream("data", in, 5);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 10);
        in = new ByteArrayInputStream("Hello-".getBytes(StandardCharsets.UTF_8));
        rs.updateAsciiStream(2, in, 5L);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 11);
        in = new ByteArrayInputStream("Hello-".getBytes(StandardCharsets.UTF_8));
        rs.updateAsciiStream("data", in, 5L);
        rs.insertRow();

        rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            assertEquals("Hello", rs.getString(2));
        }

        stat.execute("drop table test");
    }

    private void testBeforeFirstAfterLast() throws SQLException {
        stat.executeUpdate("create table test(id int)");
        stat.executeUpdate("insert into test values(1)");
        // With a result
        ResultSet rs = stat.executeQuery("select * from test");
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        rs.close();
        // With no result
        rs = stat.executeQuery("select * from test where 1 = 2");
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        rs.close();
        stat.execute("drop table test");
    }

    private void testParseSpecialValues() throws SQLException {
        for (int i = -10; i < 10; i++) {
            testParseSpecialValue("" + ((long) Integer.MIN_VALUE + i));
            testParseSpecialValue("" + ((long) Integer.MAX_VALUE + i));
            BigInteger bi = BigInteger.valueOf(i);
            testParseSpecialValue(bi.add(BigInteger.valueOf(Long.MIN_VALUE)).toString());
            testParseSpecialValue(bi.add(BigInteger.valueOf(Long.MAX_VALUE)).toString());
        }
    }

    private void testParseSpecialValue(String x) throws SQLException {
        Object expected;
        expected = new BigDecimal(x);
        try {
            expected = Long.decode(x);
            expected = Integer.decode(x);
        } catch (Exception e) {
            // ignore
        }
        ResultSet rs = stat.executeQuery("call " + x);
        rs.next();
        Object o = rs.getObject(1);
        assertEquals(expected.getClass().getName(), o.getClass().getName());
        assertTrue(expected.equals(o));
    }

    private void testSubstringDataType() throws SQLException {
        ResultSet rs = stat.executeQuery("select substr(x, 1, 1) from dual");
        rs.next();
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
    }

    private void testColumnLabelColumnName() throws SQLException {
        ResultSet rs = stat.executeQuery("select x as y from dual");
        rs.next();
        rs.getString("x");
        rs.getString("y");
        rs.close();
        rs = conn.getMetaData().getColumns(null, null, null, null);
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        String[] columnName = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            // columnName[i - 1] = meta.getColumnLabel(i);
            columnName[i - 1] = meta.getColumnName(i);
        }
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                rs.getObject(columnName[i]);
            }
        }
    }

    private void testAbsolute() throws SQLException {
        // stat.execute("SET MAX_MEMORY_ROWS 90");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        // there was a problem when more than MAX_MEMORY_ROWS where in the
        // result set
        stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 200)");
        Statement s2 = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s2.executeQuery("SELECT * FROM TEST ORDER BY ID");
        for (int i = 100; i > 0; i--) {
            rs.absolute(i);
            assertEquals(i, rs.getInt(1));
        }
        stat.execute("DROP TABLE TEST");
    }

    private void testFetchSize() throws SQLException {
        if (!config.networked || config.memory) {
            return;
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM SYSTEM_RANGE(1, 100)");
        int a = stat.getFetchSize();
        int b = rs.getFetchSize();
        assertEquals(a, b);
        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        rs.setFetchSize(b + 1);
        b = rs.getFetchSize();
        assertEquals(a + 1, b);
    }

    private void testOwnUpdates() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        for (int i = 0; i < 3; i++) {
            int type = i == 0 ? ResultSet.TYPE_FORWARD_ONLY :
                i == 1 ? ResultSet.TYPE_SCROLL_INSENSITIVE :
                ResultSet.TYPE_SCROLL_SENSITIVE;
            assertTrue(meta.ownUpdatesAreVisible(type));
            assertFalse(meta.ownDeletesAreVisible(type));
            assertFalse(meta.ownInsertsAreVisible(type));
        }
        stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        ResultSet rs;
        rs = stat.executeQuery("SELECT ID, NAME FROM TEST ORDER BY ID");
        rs.next();
        rs.next();
        rs.updateString(2, "Hallo");
        rs.updateRow();
        assertEquals("Hallo", rs.getString(2));
        stat.execute("DROP TABLE TEST");
    }

    private void testUpdatePrimaryKey() throws SQLException {
        stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        rs.updateInt(1, 2);
        rs.updateRow();
        rs.updateInt(1, 3);
        rs.updateRow();
        stat.execute("DROP TABLE TEST");
    }

    private void checkPrecision(int expected, String sql) throws SQLException {
        ResultSetMetaData meta = stat.executeQuery(sql).getMetaData();
        assertEquals(expected, meta.getPrecision(1));
    }

    private void testSubstringPrecision() throws SQLException {
        trace("testSubstringPrecision");
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR(10))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'WorldPeace')");
        checkPrecision(0, "SELECT SUBSTR(NAME, 12, 4) FROM TEST");
        checkPrecision(9, "SELECT SUBSTR(NAME, 2) FROM TEST");
        checkPrecision(10, "SELECT SUBSTR(NAME, ID) FROM TEST");
        checkPrecision(4, "SELECT SUBSTR(NAME, 2, 4) FROM TEST");
        checkPrecision(3, "SELECT SUBSTR(NAME, 8, 4) FROM TEST");
        checkPrecision(4, "SELECT SUBSTR(NAME, 7, 4) FROM TEST");
        checkPrecision(8, "SELECT SUBSTR(NAME, 3, ID*0) FROM TEST");
        stat.execute("DROP TABLE TEST");
    }

    private void testFindColumn() throws SQLException {
        trace("testFindColumn");
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        rs = stat.executeQuery("SELECT * FROM TEST");
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(2, rs.findColumn("NAME"));
        assertEquals(1, rs.findColumn("id"));
        assertEquals(2, rs.findColumn("name"));
        assertEquals(1, rs.findColumn("Id"));
        assertEquals(2, rs.findColumn("Name"));
        assertEquals(1, rs.findColumn("TEST.ID"));
        assertEquals(2, rs.findColumn("TEST.NAME"));
        assertEquals(1, rs.findColumn("Test.Id"));
        assertEquals(2, rs.findColumn("Test.Name"));
        stat.execute("DROP TABLE TEST");

        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, DATA VARCHAR)");
        rs = stat.executeQuery("SELECT * FROM TEST");
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(2, rs.findColumn("NAME"));
        assertEquals(3, rs.findColumn("DATA"));
        assertEquals(1, rs.findColumn("id"));
        assertEquals(2, rs.findColumn("name"));
        assertEquals(3, rs.findColumn("data"));
        assertEquals(1, rs.findColumn("Id"));
        assertEquals(2, rs.findColumn("Name"));
        assertEquals(3, rs.findColumn("Data"));
        assertEquals(1, rs.findColumn("TEST.ID"));
        assertEquals(2, rs.findColumn("TEST.NAME"));
        assertEquals(3, rs.findColumn("TEST.DATA"));
        assertEquals(1, rs.findColumn("Test.Id"));
        assertEquals(2, rs.findColumn("Test.Name"));
        assertEquals(3, rs.findColumn("Test.Data"));
        stat.execute("DROP TABLE TEST");

    }

    private void testColumnLength() throws SQLException {
        trace("testColumnDisplayLength");
        ResultSet rs;
        ResultSetMetaData meta;

        stat.execute("CREATE TABLE one (ID INT, NAME VARCHAR(255))");
        rs = stat.executeQuery("select * from one");
        meta = rs.getMetaData();
        assertEquals("ID", meta.getColumnLabel(1));
        assertEquals(11, meta.getColumnDisplaySize(1));
        assertEquals("NAME", meta.getColumnLabel(2));
        assertEquals(255, meta.getColumnDisplaySize(2));
        stat.execute("DROP TABLE one");

        rs = stat.executeQuery("select 1, 'Hello' union select 2, 'Hello World!'");
        meta = rs.getMetaData();
        assertEquals(11, meta.getColumnDisplaySize(1));
        assertEquals(12, meta.getColumnDisplaySize(2));

        rs = stat.executeQuery("explain select * from dual");
        meta = rs.getMetaData();
        assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

        rs = stat.executeQuery("script");
        meta = rs.getMetaData();
        assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

        rs = stat.executeQuery("select group_concat(table_name) " +
                "from information_schema.tables");
        rs.next();
        meta = rs.getMetaData();
        assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

    }

    private void testLimitMaxRows() throws SQLException {
        trace("Test LimitMaxRows");
        ResultSet rs;
        stat.execute("CREATE TABLE one (C CHARACTER(10))");
        rs = stat.executeQuery("SELECT C || C FROM one;");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(20, md.getPrecision(1));
        ResultSet rs2 = stat.executeQuery("SELECT UPPER (C)  FROM one;");
        ResultSetMetaData md2 = rs2.getMetaData();
        assertEquals(10, md2.getPrecision(1));
        rs = stat.executeQuery("SELECT UPPER (C), CHAR(10), " +
                "CONCAT(C,C,C), HEXTORAW(C), RAWTOHEX(C) FROM one");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(10, meta.getPrecision(1));
        assertEquals(1, meta.getPrecision(2));
        assertEquals(30, meta.getPrecision(3));
        assertEquals(3, meta.getPrecision(4));
        assertEquals(40, meta.getPrecision(5));
        stat.execute("DROP TABLE one");
    }

    private void testAutoIncrement() throws SQLException {
        trace("Test AutoIncrement");
        stat.execute("DROP TABLE IF EXISTS TEST");
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID IDENTITY NOT NULL, NAME VARCHAR NULL)");

        stat.execute("INSERT INTO TEST(NAME) VALUES('Hello')",
                Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        stat.execute("INSERT INTO TEST(NAME) VALUES('World')",
                Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = stat.executeQuery("SELECT ID AS I, NAME AS N, ID+1 AS IP1 FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.isAutoIncrement(1));
        assertFalse(meta.isAutoIncrement(2));
        assertFalse(meta.isAutoIncrement(3));
        assertEquals(ResultSetMetaData.columnNoNulls, meta.isNullable(1));
        assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(2));
        assertEquals(ResultSetMetaData.columnNullableUnknown, meta.isNullable(3));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

    }

    private void testInt() throws SQLException {
        trace("Test INT");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE INT)");
        stat.execute("INSERT INTO TEST VALUES(1,-1)");
        stat.execute("INSERT INTO TEST VALUES(2,0)");
        stat.execute("INSERT INTO TEST VALUES(3,1)");
        stat.execute("INSERT INTO TEST VALUES(4," + Integer.MAX_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(5," + Integer.MIN_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(6,NULL)");
        // this should not be read - maxrows=6
        stat.execute("INSERT INTO TEST VALUES(7,NULL)");

        // MySQL compatibility (is this required?)
        // rs=stat.executeQuery("SELECT * FROM TEST T ORDER BY ID");
        // check(rs.findColumn("T.ID"), 1);
        // check(rs.findColumn("T.NAME"), 2);

        rs = stat.executeQuery("SELECT *, NULL AS N FROM TEST ORDER BY ID");

        // MySQL compatibility
        assertEquals(1, rs.findColumn("TEST.ID"));
        assertEquals(2, rs.findColumn("TEST.VALUE"));

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(3, meta.getColumnCount());
        assertEquals("resultSet".toUpperCase(), meta.getCatalogName(1));
        assertTrue("PUBLIC".equals(meta.getSchemaName(2)));
        assertTrue("TEST".equals(meta.getTableName(1)));
        assertTrue("ID".equals(meta.getColumnName(1)));
        assertTrue("VALUE".equals(meta.getColumnName(2)));
        assertTrue(!meta.isAutoIncrement(1));
        assertTrue(meta.isCaseSensitive(1));
        assertTrue(meta.isSearchable(1));
        assertFalse(meta.isCurrency(1));
        assertTrue(meta.getColumnDisplaySize(1) > 0);
        assertTrue(meta.isSigned(1));
        assertTrue(meta.isSearchable(2));
        assertEquals(ResultSetMetaData.columnNoNulls, meta.isNullable(1));
        assertFalse(meta.isReadOnly(1));
        assertTrue(meta.isWritable(1));
        assertFalse(meta.isDefinitelyWritable(1));
        assertTrue(meta.getColumnDisplaySize(1) > 0);
        assertTrue(meta.getColumnDisplaySize(2) > 0);
        assertEquals(null, meta.getColumnClassName(3));

        assertTrue(rs.getRow() == 0);
        assertResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" },
                new int[] { Types.INTEGER, Types.INTEGER,
                Types.NULL }, new int[] { 10, 10, 1 }, new int[] { 0, 0, 0 });
        rs.next();
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        trace("default fetch size=" + rs.getFetchSize());
        // 0 should be an allowed value (but it's not defined what is actually
        // means)
        rs.setFetchSize(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, rs).setFetchSize(-1);
        // fetch size 100 is bigger than maxrows - not allowed
        assertThrows(ErrorCode.INVALID_VALUE_2, rs).setFetchSize(100);
        rs.setFetchSize(6);

        assertTrue(rs.getRow() == 1);
        assertEquals(2, rs.findColumn("VALUE"));
        assertEquals(2, rs.findColumn("value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(1, rs.findColumn("id"));
        assertEquals(1, rs.findColumn("Id"));
        assertEquals(1, rs.findColumn("iD"));
        assertTrue(rs.getInt(2) == -1 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == -1 && !rs.wasNull());
        assertTrue(rs.getInt("value") == -1 && !rs.wasNull());
        assertTrue(rs.getInt("Value") == -1 && !rs.wasNull());
        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());

        o = rs.getObject("value");
        trace(o.getClass().getName());
        assertTrue(o instanceof Integer);
        assertTrue(((Integer) o).intValue() == -1);
        o = rs.getObject("value", Integer.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Integer);
        assertTrue(((Integer) o).intValue() == -1);
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof Integer);
        assertTrue(((Integer) o).intValue() == -1);
        o = rs.getObject(2, Integer.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Integer);
        assertTrue(((Integer) o).intValue() == -1);
        assertTrue(rs.getBoolean("Value"));
        assertTrue(rs.getByte("Value") == (byte) -1);
        assertTrue(rs.getShort("Value") == (short) -1);
        assertTrue(rs.getLong("Value") == -1);
        assertTrue(rs.getFloat("Value") == -1.0);
        assertTrue(rs.getDouble("Value") == -1.0);

        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());
        assertTrue(rs.getInt("ID") == 1 && !rs.wasNull());
        assertTrue(rs.getInt("id") == 1 && !rs.wasNull());
        assertTrue(rs.getInt("Id") == 1 && !rs.wasNull());
        assertTrue(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 2);
        assertTrue(rs.getInt(2) == 0 && !rs.wasNull());
        assertTrue(!rs.getBoolean(2));
        assertTrue(rs.getByte(2) == 0);
        assertTrue(rs.getShort(2) == 0);
        assertTrue(rs.getLong(2) == 0);
        assertTrue(rs.getFloat(2) == 0.0);
        assertTrue(rs.getDouble(2) == 0.0);
        assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 3);
        assertTrue(rs.getInt("ID") == 3 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == 1 && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 4);
        assertTrue(rs.getInt("ID") == 4 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == Integer.MAX_VALUE && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 5);
        assertTrue(rs.getInt("id") == 5 && !rs.wasNull());
        assertTrue(rs.getInt("value") == Integer.MIN_VALUE && !rs.wasNull());
        assertTrue(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 6);
        assertTrue(rs.getInt("id") == 6 && !rs.wasNull());
        assertTrue(rs.getInt("value") == 0 && rs.wasNull());
        assertTrue(rs.getInt(2) == 0 && rs.wasNull());
        assertTrue(rs.getInt(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(1).equals("6") && !rs.wasNull());
        assertTrue(rs.getString(2) == null && rs.wasNull());
        o = rs.getObject(2);
        assertTrue(o == null);
        assertTrue(rs.wasNull());
        o = rs.getObject(2, Integer.class);
        assertTrue(o == null);
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
        assertEquals(0, rs.getRow());
        // there is one more row, but because of setMaxRows we don't get it

        stat.execute("DROP TABLE TEST");
        stat.setMaxRows(0);
    }

    private void testSmallInt() throws SQLException {
        trace("Test SMALLINT");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE SMALLINT)");
        stat.execute("INSERT INTO TEST VALUES(1,-1)");
        stat.execute("INSERT INTO TEST VALUES(2,0)");
        stat.execute("INSERT INTO TEST VALUES(3,1)");
        stat.execute("INSERT INTO TEST VALUES(4," + Short.MAX_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(5," + Short.MIN_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(6,NULL)");

        // MySQL compatibility (is this required?)
        // rs=stat.executeQuery("SELECT * FROM TEST T ORDER BY ID");
        // check(rs.findColumn("T.ID"), 1);
        // check(rs.findColumn("T.NAME"), 2);

        rs = stat.executeQuery("SELECT *, NULL AS N FROM TEST ORDER BY ID");

        // MySQL compatibility
        assertEquals(1, rs.findColumn("TEST.ID"));
        assertEquals(2, rs.findColumn("TEST.VALUE"));

        assertTrue(rs.getRow() == 0);
        assertResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" },
                        new int[] { Types.INTEGER, Types.SMALLINT,
                                Types.NULL }, new int[] { 10, 5, 1 }, new int[] { 0, 0, 0 });
        rs.next();

        assertTrue(rs.getRow() == 1);
        assertEquals(2, rs.findColumn("VALUE"));
        assertEquals(2, rs.findColumn("value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(1, rs.findColumn("id"));
        assertEquals(1, rs.findColumn("Id"));
        assertEquals(1, rs.findColumn("iD"));
        assertTrue(rs.getShort(2) == -1 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == -1 && !rs.wasNull());
        assertTrue(rs.getShort("value") == -1 && !rs.wasNull());
        assertTrue(rs.getShort("Value") == -1 && !rs.wasNull());
        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());

        o = rs.getObject("value");
        trace(o.getClass().getName());
        assertTrue(o.getClass() == (SysProperties.OLD_RESULT_SET_GET_OBJECT ? Short.class : Integer.class));
        assertTrue(((Number) o).intValue() == -1);
        o = rs.getObject("value", Short.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Short);
        assertTrue(((Short) o).shortValue() == -1);
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o.getClass() == (SysProperties.OLD_RESULT_SET_GET_OBJECT ? Short.class : Integer.class));
        assertTrue(((Number) o).intValue() == -1);
        o = rs.getObject(2, Short.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Short);
        assertTrue(((Short) o).shortValue() == -1);
        assertTrue(rs.getBoolean("Value"));
        assertTrue(rs.getByte("Value") == (byte) -1);
        assertTrue(rs.getInt("Value") == -1);
        assertTrue(rs.getLong("Value") == -1);
        assertTrue(rs.getFloat("Value") == -1.0);
        assertTrue(rs.getDouble("Value") == -1.0);

        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());
        assertTrue(rs.getShort("ID") == 1 && !rs.wasNull());
        assertTrue(rs.getShort("id") == 1 && !rs.wasNull());
        assertTrue(rs.getShort("Id") == 1 && !rs.wasNull());
        assertTrue(rs.getShort(1) == 1 && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 2);
        assertTrue(rs.getShort(2) == 0 && !rs.wasNull());
        assertTrue(!rs.getBoolean(2));
        assertTrue(rs.getByte(2) == 0);
        assertTrue(rs.getInt(2) == 0);
        assertTrue(rs.getLong(2) == 0);
        assertTrue(rs.getFloat(2) == 0.0);
        assertTrue(rs.getDouble(2) == 0.0);
        assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
        assertTrue(rs.getShort(1) == 2 && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 3);
        assertTrue(rs.getShort("ID") == 3 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == 1 && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 4);
        assertTrue(rs.getShort("ID") == 4 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == Short.MAX_VALUE && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 5);
        assertTrue(rs.getShort("id") == 5 && !rs.wasNull());
        assertTrue(rs.getShort("value") == Short.MIN_VALUE && !rs.wasNull());
        assertTrue(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 6);
        assertTrue(rs.getShort("id") == 6 && !rs.wasNull());
        assertTrue(rs.getShort("value") == 0 && rs.wasNull());
        assertTrue(rs.getShort(2) == 0 && rs.wasNull());
        assertTrue(rs.getShort(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(1).equals("6") && !rs.wasNull());
        assertTrue(rs.getString(2) == null && rs.wasNull());
        o = rs.getObject(2);
        assertTrue(o == null);
        assertTrue(rs.wasNull());
        o = rs.getObject(2, Short.class);
        assertTrue(o == null);
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
        assertEquals(0, rs.getRow());

        stat.execute("DROP TABLE TEST");
        stat.setMaxRows(0);
    }

    private void testBigInt() throws SQLException {
        trace("Test SMALLINT");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE BIGINT)");
        stat.execute("INSERT INTO TEST VALUES(1,-1)");
        stat.execute("INSERT INTO TEST VALUES(2,0)");
        stat.execute("INSERT INTO TEST VALUES(3,1)");
        stat.execute("INSERT INTO TEST VALUES(4," + Long.MAX_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(5," + Long.MIN_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(6,NULL)");

        // MySQL compatibility (is this required?)
        // rs=stat.executeQuery("SELECT * FROM TEST T ORDER BY ID");
        // check(rs.findColumn("T.ID"), 1);
        // check(rs.findColumn("T.NAME"), 2);

        rs = stat.executeQuery("SELECT *, NULL AS N FROM TEST ORDER BY ID");

        // MySQL compatibility
        assertEquals(1, rs.findColumn("TEST.ID"));
        assertEquals(2, rs.findColumn("TEST.VALUE"));

        assertTrue(rs.getRow() == 0);
        assertResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" },
                        new int[] { Types.INTEGER, Types.BIGINT,
                                Types.NULL }, new int[] { 10, 19, 1 }, new int[] { 0, 0, 0 });
        rs.next();

        assertTrue(rs.getRow() == 1);
        assertEquals(2, rs.findColumn("VALUE"));
        assertEquals(2, rs.findColumn("value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(1, rs.findColumn("id"));
        assertEquals(1, rs.findColumn("Id"));
        assertEquals(1, rs.findColumn("iD"));
        assertTrue(rs.getLong(2) == -1 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == -1 && !rs.wasNull());
        assertTrue(rs.getLong("value") == -1 && !rs.wasNull());
        assertTrue(rs.getLong("Value") == -1 && !rs.wasNull());
        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());

        o = rs.getObject("value");
        trace(o.getClass().getName());
        assertTrue(o instanceof Long);
        assertTrue(((Long) o).longValue() == -1);
        o = rs.getObject("value", Long.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Long);
        assertTrue(((Long) o).longValue() == -1);
        o = rs.getObject("value", BigInteger.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof BigInteger);
        assertTrue(((BigInteger) o).longValue() == -1);
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof Long);
        assertTrue(((Long) o).longValue() == -1);
        o = rs.getObject(2, Long.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Long);
        assertTrue(((Long) o).longValue() == -1);
        o = rs.getObject(2, BigInteger.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof BigInteger);
        assertTrue(((BigInteger) o).longValue() == -1);
        assertTrue(rs.getBoolean("Value"));
        assertTrue(rs.getByte("Value") == (byte) -1);
        assertTrue(rs.getShort("Value") == -1);
        assertTrue(rs.getInt("Value") == -1);
        assertTrue(rs.getFloat("Value") == -1.0);
        assertTrue(rs.getDouble("Value") == -1.0);

        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());
        assertTrue(rs.getLong("ID") == 1 && !rs.wasNull());
        assertTrue(rs.getLong("id") == 1 && !rs.wasNull());
        assertTrue(rs.getLong("Id") == 1 && !rs.wasNull());
        assertTrue(rs.getLong(1) == 1 && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 2);
        assertTrue(rs.getLong(2) == 0 && !rs.wasNull());
        assertTrue(!rs.getBoolean(2));
        assertTrue(rs.getByte(2) == 0);
        assertTrue(rs.getShort(2) == 0);
        assertTrue(rs.getInt(2) == 0);
        assertTrue(rs.getFloat(2) == 0.0);
        assertTrue(rs.getDouble(2) == 0.0);
        assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
        assertTrue(rs.getLong(1) == 2 && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 3);
        assertTrue(rs.getLong("ID") == 3 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == 1 && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 4);
        assertTrue(rs.getLong("ID") == 4 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == Long.MAX_VALUE && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 5);
        assertTrue(rs.getLong("id") == 5 && !rs.wasNull());
        assertTrue(rs.getLong("value") == Long.MIN_VALUE && !rs.wasNull());
        assertTrue(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();

        assertTrue(rs.getRow() == 6);
        assertTrue(rs.getLong("id") == 6 && !rs.wasNull());
        assertTrue(rs.getLong("value") == 0 && rs.wasNull());
        assertTrue(rs.getLong(2) == 0 && rs.wasNull());
        assertTrue(rs.getLong(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(1).equals("6") && !rs.wasNull());
        assertTrue(rs.getString(2) == null && rs.wasNull());
        o = rs.getObject(2);
        assertTrue(o == null);
        assertTrue(rs.wasNull());
        o = rs.getObject(2, Long.class);
        assertTrue(o == null);
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
        assertEquals(0, rs.getRow());

        stat.execute("DROP TABLE TEST");
        stat.setMaxRows(0);
    }

    private void testVarchar() throws SQLException {
        trace("Test VARCHAR");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1,'')");
        stat.execute("INSERT INTO TEST VALUES(2,' ')");
        stat.execute("INSERT INTO TEST VALUES(3,'  ')");
        stat.execute("INSERT INTO TEST VALUES(4,NULL)");
        stat.execute("INSERT INTO TEST VALUES(5,'Hi')");
        stat.execute("INSERT INTO TEST VALUES(6,' Hi ')");
        stat.execute("INSERT INTO TEST VALUES(7,'Joe''s')");
        stat.execute("INSERT INTO TEST VALUES(8,'{escape}')");
        stat.execute("INSERT INTO TEST VALUES(9,'\\n')");
        stat.execute("INSERT INTO TEST VALUES(10,'\\''')");
        stat.execute("INSERT INTO TEST VALUES(11,'\\%')");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" },
                new int[] { Types.INTEGER, Types.VARCHAR }, new int[] {
                10, 255 }, new int[] { 0, 0 });
        String value;
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <>)");
        assertTrue(value != null && value.equals("") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: < >)");
        assertTrue(rs.getString(2).equals(" ") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <  >)");
        assertTrue(rs.getString(2).equals("  ") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 3 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <null>)");
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getInt(1) == 4 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <Hi>)");
        assertTrue(rs.getInt(1) == 5 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("Hi") && !rs.wasNull());
        o = rs.getObject("value");
        trace(o.getClass().getName());
        assertTrue(o instanceof String);
        assertTrue(o.toString().equals("Hi"));
        o = rs.getObject("value", String.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof String);
        assertTrue(o.equals("Hi"));
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: < Hi >)");
        assertTrue(rs.getInt(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(2).equals(" Hi ") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <Joe's>)");
        assertTrue(rs.getInt(1) == 7 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("Joe's") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <{escape}>)");
        assertTrue(rs.getInt(1) == 8 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("{escape}") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\n>)");
        assertTrue(rs.getInt(1) == 9 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\n") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\'>)");
        assertTrue(rs.getInt(1) == 10 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\'") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\%>)");
        assertTrue(rs.getInt(1) == 11 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\%") && !rs.wasNull());
        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDecimal() throws SQLException {
        int numericType;
        if (SysProperties.BIG_DECIMAL_IS_DECIMAL) {
            numericType = Types.DECIMAL;
        } else {
            numericType = Types.NUMERIC;
        }
        trace("Test DECIMAL");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE DECIMAL(10,2))");
        stat.execute("INSERT INTO TEST VALUES(1,-1)");
        stat.execute("INSERT INTO TEST VALUES(2,.0)");
        stat.execute("INSERT INTO TEST VALUES(3,1.)");
        stat.execute("INSERT INTO TEST VALUES(4,12345678.89)");
        stat.execute("INSERT INTO TEST VALUES(6,99999998.99)");
        stat.execute("INSERT INTO TEST VALUES(7,-99999998.99)");
        stat.execute("INSERT INTO TEST VALUES(8,NULL)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" },
                new int[] { Types.INTEGER, numericType }, new int[] {
                10, 10 }, new int[] { 0, 2 });
        BigDecimal bd;

        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == -1);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
        assertTrue(!rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof BigDecimal);
        assertTrue(((BigDecimal) o).compareTo(new BigDecimal("-1.00")) == 0);
        o = rs.getObject(2, BigDecimal.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof BigDecimal);
        assertTrue(((BigDecimal) o).compareTo(new BigDecimal("-1.00")) == 0);

        rs.next();
        assertTrue(rs.getInt(1) == 2);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == 0);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertTrue(!rs.wasNull());

        rs.next();
        checkColumnBigDecimal(rs, 2, 1, "1.00");

        rs.next();
        checkColumnBigDecimal(rs, 2, 12345679, "12345678.89");

        rs.next();
        checkColumnBigDecimal(rs, 2, 99999999, "99999998.99");

        rs.next();
        checkColumnBigDecimal(rs, 2, -99999999, "-99999998.99");

        rs.next();
        checkColumnBigDecimal(rs, 2, 0, null);

        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE DECIMAL(22,2))");
        stat.execute("INSERT INTO TEST VALUES(1,-12345678909876543210)");
        stat.execute("INSERT INTO TEST VALUES(2,12345678901234567890.12345)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(new BigDecimal("-12345678909876543210.00"), rs.getBigDecimal(2));
        assertEquals(new BigInteger("-12345678909876543210"), rs.getObject(2, BigInteger.class));
        rs.next();
        assertEquals(new BigDecimal("12345678901234567890.12"), rs.getBigDecimal(2));
        assertEquals(new BigInteger("12345678901234567890"), rs.getObject(2, BigInteger.class));
        stat.execute("DROP TABLE TEST");
    }

    private void testDoubleFloat() throws SQLException {
        trace("Test DOUBLE - FLOAT");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, D DOUBLE, R REAL)");
        stat.execute("INSERT INTO TEST VALUES(1, -1, -1)");
        stat.execute("INSERT INTO TEST VALUES(2,.0, .0)");
        stat.execute("INSERT INTO TEST VALUES(3, 1., 1.)");
        stat.execute("INSERT INTO TEST VALUES(4, 12345678.89, 12345678.89)");
        stat.execute("INSERT INTO TEST VALUES(6, 99999999.99, 99999999.99)");
        stat.execute("INSERT INTO TEST VALUES(7, -99999999.99, -99999999.99)");
        stat.execute("INSERT INTO TEST VALUES(8, NULL, NULL)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 3, new String[] { "ID", "D", "R" },
                new int[] { Types.INTEGER, Types.DOUBLE, Types.REAL },
                new int[] { 10, 17, 7 }, new int[] { 0, 0, 0 });
        BigDecimal bd;
        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == -1);
        assertTrue(rs.getInt(3) == -1);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
        assertTrue(!rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof Double);
        assertTrue(((Double) o).compareTo(-1d) == 0);
        o = rs.getObject(2, Double.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Double);
        assertTrue(((Double) o).compareTo(-1d) == 0);
        o = rs.getObject(3);
        trace(o.getClass().getName());
        assertTrue(o instanceof Float);
        assertTrue(((Float) o).compareTo(-1f) == 0);
        o = rs.getObject(3, Float.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof Float);
        assertTrue(((Float) o).compareTo(-1f) == 0);
        rs.next();
        assertTrue(rs.getInt(1) == 2);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == 0);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(3) == 0);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(3);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertTrue(!rs.wasNull());
        rs.next();
        assertEquals(1.0, rs.getDouble(2));
        assertEquals(1.0f, rs.getFloat(3));
        rs.next();
        assertEquals(12345678.89, rs.getDouble(2));
        assertEquals(12345678.89f, rs.getFloat(3));
        rs.next();
        assertEquals(99999999.99, rs.getDouble(2));
        assertEquals(99999999.99f, rs.getFloat(3));
        rs.next();
        assertEquals(-99999999.99, rs.getDouble(2));
        assertEquals(-99999999.99f, rs.getFloat(3));
        rs.next();
        checkColumnBigDecimal(rs, 2, 0, null);
        checkColumnBigDecimal(rs, 3, 0, null);
        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDatetime() throws SQLException {
        trace("Test DATETIME");
        ResultSet rs;
        Object o;

        rs = stat.executeQuery("call date '99999-12-23'");
        rs.next();
        assertEquals("99999-12-23", rs.getString(1));
        rs = stat.executeQuery("call timestamp '99999-12-23 01:02:03.000'");
        rs.next();
        assertEquals("99999-12-23 01:02:03", rs.getString(1));
        rs = stat.executeQuery("call date '-99999-12-23'");
        rs.next();
        assertEquals("-99999-12-23", rs.getString(1));
        rs = stat.executeQuery("call timestamp '-99999-12-23 01:02:03.000'");
        rs.next();
        assertEquals("-99999-12-23 01:02:03", rs.getString(1));

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE DATETIME)");
        stat.execute("INSERT INTO TEST VALUES(1,DATE '2011-11-11')");
        stat.execute("INSERT INTO TEST VALUES(2,TIMESTAMP '2002-02-02 02:02:02')");
        stat.execute("INSERT INTO TEST VALUES(3,TIMESTAMP '1800-1-1 0:0:0')");
        stat.execute("INSERT INTO TEST VALUES(4,TIMESTAMP '9999-12-31 23:59:59')");
        stat.execute("INSERT INTO TEST VALUES(5,NULL)");
        rs = stat.executeQuery("SELECT 0 ID, " +
                "TIMESTAMP '9999-12-31 23:59:59' VALUE FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" },
                new int[] { Types.INTEGER, Types.TIMESTAMP },
                new int[] { 10, 29 }, new int[] { 0, 9 });
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" },
                new int[] { Types.INTEGER, Types.TIMESTAMP },
                new int[] { 10, 26 }, new int[] { 0, 6 });
        rs.next();
        java.sql.Date date;
        java.sql.Time time;
        java.sql.Timestamp ts;
        date = rs.getDate(2);
        assertTrue(!rs.wasNull());
        time = rs.getTime(2);
        assertTrue(!rs.wasNull());
        ts = rs.getTimestamp(2);
        assertTrue(!rs.wasNull());
        trace("Date: " + date.toString() + " Time:" + time.toString() +
                " Timestamp:" + ts.toString());
        trace("Date ms: " + date.getTime() + " Time ms:" + time.getTime() +
                " Timestamp ms:" + ts.getTime());
        trace("1970 ms: " + java.sql.Timestamp.valueOf(
                "1970-01-01 00:00:00.0").getTime());
        assertEquals(java.sql.Timestamp.valueOf(
                "2011-11-11 00:00:00.0").getTime(), date.getTime());
        assertEquals(java.sql.Timestamp.valueOf(
                "1970-01-01 00:00:00.0").getTime(), time.getTime());
        assertEquals(java.sql.Timestamp.valueOf(
                "2011-11-11 00:00:00.0").getTime(), ts.getTime());
        assertTrue(date.equals(
                java.sql.Date.valueOf("2011-11-11")));
        assertTrue(time.equals(
                java.sql.Time.valueOf("00:00:00")));
        assertTrue(ts.equals(
                java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertFalse(rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof java.sql.Timestamp);
        assertTrue(((java.sql.Timestamp) o).equals(
                java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertFalse(rs.wasNull());
        o = rs.getObject(2, java.sql.Timestamp.class);
        trace(o.getClass().getName());
        assertTrue(o instanceof java.sql.Timestamp);
        assertTrue(((java.sql.Timestamp) o).equals(
                        java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertFalse(rs.wasNull());
        o = rs.getObject(2, java.util.Date.class);
        assertTrue(o.getClass() == java.util.Date.class);
        assertEquals(((java.util.Date) o).getTime(),
                        java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        o = rs.getObject(2, Calendar.class);
        assertTrue(o instanceof Calendar);
        assertEquals(((Calendar) o).getTimeInMillis(),
                        java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        rs.next();

        date = rs.getDate("VALUE");
        assertTrue(!rs.wasNull());
        time = rs.getTime("VALUE");
        assertTrue(!rs.wasNull());
        ts = rs.getTimestamp("VALUE");
        assertTrue(!rs.wasNull());
        trace("Date: " + date.toString() +
                " Time:" + time.toString() + " Timestamp:" + ts.toString());
        assertEquals("2002-02-02", date.toString());
        assertEquals("02:02:02", time.toString());
        assertEquals("2002-02-02 02:02:02.0", ts.toString());
        rs.next();

        assertEquals("1800-01-01", rs.getDate("value").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("1800-01-01", rs.getObject("value",
                            LocalDateTimeUtils.LOCAL_DATE).toString());
        }
        assertEquals("00:00:00", rs.getTime("value").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("00:00", rs.getObject("value",
                            LocalDateTimeUtils.LOCAL_TIME).toString());
        }
        assertEquals("1800-01-01 00:00:00.0", rs.getTimestamp("value").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("1800-01-01T00:00", rs.getObject("value",
                            LocalDateTimeUtils.LOCAL_DATE_TIME).toString());
        }
        rs.next();

        assertEquals("9999-12-31", rs.getDate("Value").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("9999-12-31", rs.getObject("Value",
                            LocalDateTimeUtils.LOCAL_DATE).toString());
        }
        assertEquals("23:59:59", rs.getTime("Value").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("23:59:59", rs.getObject("Value",
                            LocalDateTimeUtils.LOCAL_TIME).toString());
        }
        assertEquals("9999-12-31 23:59:59.0", rs.getTimestamp("Value").toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("9999-12-31T23:59:59", rs.getObject("Value",
                            LocalDateTimeUtils.LOCAL_DATE_TIME).toString());
        }
        rs.next();

        assertTrue(rs.getDate("Value") == null && rs.wasNull());
        assertTrue(rs.getTime("vALUe") == null && rs.wasNull());
        assertTrue(rs.getTimestamp(2) == null && rs.wasNull());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertTrue(rs.getObject(2,
                            LocalDateTimeUtils.LOCAL_DATE_TIME) == null && rs.wasNull());
        }
        assertTrue(!rs.next());

        rs = stat.executeQuery("SELECT DATE '2001-02-03' D, " +
                "TIME '14:15:16', " +
                "TIMESTAMP '2007-08-09 10:11:12.141516171' TS FROM TEST");
        rs.next();

        date = (Date) rs.getObject(1);
        time = (Time) rs.getObject(2);
        ts = (Timestamp) rs.getObject(3);
        assertEquals("2001-02-03", date.toString());
        assertEquals("14:15:16", time.toString());
        assertEquals("2007-08-09 10:11:12.141516171", ts.toString());
        date = rs.getObject(1, Date.class);
        time = rs.getObject(2, Time.class);
        ts = rs.getObject(3, Timestamp.class);
        assertEquals("2001-02-03", date.toString());
        assertEquals("14:15:16", time.toString());
        assertEquals("2007-08-09 10:11:12.141516171", ts.toString());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2001-02-03", rs.getObject(1,
                            LocalDateTimeUtils.LOCAL_DATE).toString());
        }
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("14:15:16", rs.getObject(2,
                            LocalDateTimeUtils.LOCAL_TIME).toString());
        }
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2007-08-09T10:11:12.141516171",
                    rs.getObject(3, LocalDateTimeUtils.LOCAL_DATE_TIME)
                            .toString());
        }

        stat.execute("DROP TABLE TEST");
    }

    private void testDatetimeWithCalendar() throws SQLException {
        trace("Test DATETIME with Calendar");
        ResultSet rs;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "D DATE, T TIME, TS TIMESTAMP(9))");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?, ?, ?)");
        Calendar regular = DateTimeUtils.createGregorianCalendar();
        Calendar other = null;
        // search a locale that has a _different_ raw offset
        long testTime = java.sql.Date.valueOf("2001-02-03").getTime();
        for (String s : TimeZone.getAvailableIDs()) {
            TimeZone zone = TimeZone.getTimeZone(s);
            long rawOffsetDiff = regular.getTimeZone().getRawOffset() -
                    zone.getRawOffset();
            // must not be the same timezone (not 0 h and not 24 h difference
            // as for Pacific/Auckland and Etc/GMT+12)
            if (rawOffsetDiff != 0 && rawOffsetDiff != 1000 * 60 * 60 * 24) {
                if (regular.getTimeZone().getOffset(testTime) !=
                        zone.getOffset(testTime)) {
                    other = DateTimeUtils.createGregorianCalendar(zone);
                    break;
                }
            }
        }

        trace("regular offset = " + regular.getTimeZone().getRawOffset() +
                " other = " + other.getTimeZone().getRawOffset());

        prep.setInt(1, 0);
        prep.setDate(2, null, regular);
        prep.setTime(3, null, regular);
        prep.setTimestamp(4, null, regular);
        prep.execute();

        prep.setInt(1, 1);
        prep.setDate(2, null, other);
        prep.setTime(3, null, other);
        prep.setTimestamp(4, null, other);
        prep.execute();

        prep.setInt(1, 2);
        prep.setDate(2, java.sql.Date.valueOf("2001-02-03"), regular);
        prep.setTime(3, java.sql.Time.valueOf("04:05:06"), regular);
        prep.setTimestamp(4,
                java.sql.Timestamp.valueOf("2007-08-09 10:11:12.131415"), regular);
        prep.execute();

        prep.setInt(1, 3);
        prep.setDate(2, java.sql.Date.valueOf("2101-02-03"), other);
        prep.setTime(3, java.sql.Time.valueOf("14:05:06"), other);
        prep.setTimestamp(4,
                java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"), other);
        prep.execute();

        prep.setInt(1, 4);
        prep.setDate(2, java.sql.Date.valueOf("2101-02-03"));
        prep.setTime(3, java.sql.Time.valueOf("14:05:06"));
        prep.setTimestamp(4,
                java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"));
        prep.execute();

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 4,
                new String[] { "ID", "D", "T", "TS" },
                new int[] { Types.INTEGER, Types.DATE,
                Types.TIME, Types.TIMESTAMP },
                new int[] { 10, 10, 8, 29 }, new int[] { 0, 0, 0, 9 });

        rs.next();
        assertEquals(0, rs.getInt(1));
        assertTrue(rs.getDate(2, regular) == null && rs.wasNull());
        assertTrue(rs.getTime(3, regular) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(3, regular) == null && rs.wasNull());

        rs.next();
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.getDate(2, other) == null && rs.wasNull());
        assertTrue(rs.getTime(3, other) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(3, other) == null && rs.wasNull());

        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("2001-02-03", rs.getDate(2, regular).toString());
        assertEquals("04:05:06", rs.getTime(3, regular).toString());
        assertFalse(rs.getTime(3, other).toString().equals("04:05:06"));
        assertEquals("2007-08-09 10:11:12.131415",
                rs.getTimestamp(4, regular).toString());
        assertFalse(rs.getTimestamp(4, other).toString().
                equals("2007-08-09 10:11:12.131415"));

        rs.next();
        assertEquals(3, rs.getInt("ID"));
        assertFalse(rs.getTimestamp("TS", regular).toString().
                equals("2107-08-09 10:11:12.131415"));
        assertEquals("2107-08-09 10:11:12.131415",
                rs.getTimestamp("TS", other).toString());
        assertFalse(rs.getTime("T", regular).toString().equals("14:05:06"));
        assertEquals("14:05:06",
                rs.getTime("T", other).toString());
        // checkFalse(rs.getDate(2, regular).toString(), "2101-02-03");
        // check(rs.getDate("D", other).toString(), "2101-02-03");

        rs.next();
        assertEquals(4, rs.getInt("ID"));
        assertEquals("2107-08-09 10:11:12.131415",
                rs.getTimestamp("TS").toString());
        assertEquals("14:05:06", rs.getTime("T").toString());
        assertEquals("2101-02-03", rs.getDate("D").toString());

        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testBlob() throws SQLException {
        trace("Test BLOB");
        ResultSet rs;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE BLOB)");
        stat.execute("INSERT INTO TEST VALUES(1,X'01010101')");
        stat.execute("INSERT INTO TEST VALUES(2,X'02020202')");
        stat.execute("INSERT INTO TEST VALUES(3,X'00')");
        stat.execute("INSERT INTO TEST VALUES(4,X'ffffff')");
        stat.execute("INSERT INTO TEST VALUES(5,X'0bcec1')");
        stat.execute("INSERT INTO TEST VALUES(6,X'03030303')");
        stat.execute("INSERT INTO TEST VALUES(7,NULL)");
        byte[] random = new byte[0x10000];
        MathUtils.randomBytes(random);
        stat.execute("INSERT INTO TEST VALUES(8, X'" + StringUtils.convertBytesToHex(random) + "')");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" },
                new int[] { Types.INTEGER, Types.BLOB }, new int[] {
                10, Integer.MAX_VALUE }, new int[] { 0, 0 });
        rs.next();

        assertEqualsWithNull(new byte[] { (byte) 0x01, (byte) 0x01,
                (byte) 0x01, (byte) 0x01 },
                rs.getBytes(2));
        assertTrue(!rs.wasNull());
        assertEqualsWithNull(new byte[] { (byte) 0x01, (byte) 0x01,
                (byte) 0x01, (byte) 0x01 },
                rs.getObject(2, byte[].class));
        assertTrue(!rs.wasNull());
        rs.next();

        assertEqualsWithNull(new byte[] { (byte) 0x02, (byte) 0x02,
                (byte) 0x02, (byte) 0x02 },
                rs.getBytes("value"));
        assertTrue(!rs.wasNull());
        assertEqualsWithNull(new byte[] { (byte) 0x02, (byte) 0x02,
                (byte) 0x02, (byte) 0x02 },
                rs.getObject("value", byte[].class));
        assertTrue(!rs.wasNull());
        rs.next();

        assertEqualsWithNull(new byte[] { (byte) 0x00 },
                readAllBytes(rs.getBinaryStream(2)));
        assertTrue(!rs.wasNull());
        rs.next();

        assertEqualsWithNull(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff },
                readAllBytes(rs.getBinaryStream("VaLuE")));
        assertTrue(!rs.wasNull());
        rs.next();

        InputStream in = rs.getBinaryStream("value");
        byte[] b = readAllBytes(in);
        assertEqualsWithNull(new byte[] { (byte) 0x0b, (byte) 0xce, (byte) 0xc1 }, b);
        Blob blob = rs.getObject("value", Blob.class);
        try {
            assertTrue(blob != null);
            assertEqualsWithNull(new byte[] { (byte) 0x0b, (byte) 0xce, (byte) 0xc1 },
                    readAllBytes(blob.getBinaryStream()));
            assertEqualsWithNull(new byte[] { (byte) 0xce,
                    (byte) 0xc1 }, readAllBytes(blob.getBinaryStream(2, 2)));
            assertTrue(!rs.wasNull());
        } finally {
            blob.free();
        }
        assertTrue(!rs.wasNull());
        rs.next();

        blob = rs.getObject("value", Blob.class);
        try {
            assertTrue(blob != null);
            assertEqualsWithNull(new byte[] { (byte) 0x03, (byte) 0x03,
                    (byte) 0x03, (byte) 0x03 }, readAllBytes(blob.getBinaryStream()));
            assertEqualsWithNull(new byte[] { (byte) 0x03,
                    (byte) 0x03 }, readAllBytes(blob.getBinaryStream(2, 2)));
            assertTrue(!rs.wasNull());
            assertThrows(ErrorCode.INVALID_VALUE_2, blob).getBinaryStream(5, 1);
        } finally {
            blob.free();
        }
        rs.next();

        assertEqualsWithNull(null, readAllBytes(rs.getBinaryStream("VaLuE")));
        assertTrue(rs.wasNull());
        rs.next();

        blob = rs.getObject("value", Blob.class);
        try {
            assertTrue(blob != null);
            assertEqualsWithNull(random, readAllBytes(blob.getBinaryStream()));
            byte[] expected = Arrays.copyOfRange(random, 100, 50102);
            byte[] got = readAllBytes(blob.getBinaryStream(101, 50002));
            assertEqualsWithNull(expected, got);
            assertTrue(!rs.wasNull());
            assertThrows(ErrorCode.INVALID_VALUE_2, blob).getBinaryStream(0x10001, 1);
            assertThrows(ErrorCode.INVALID_VALUE_2, blob).getBinaryStream(0x10002, 0);
        } finally {
            blob.free();
        }

        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testClob() throws SQLException {
        trace("Test CLOB");
        ResultSet rs;
        String string;
        stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE CLOB)");
        stat.execute("INSERT INTO TEST VALUES(1,'Test')");
        stat.execute("INSERT INTO TEST VALUES(2,'Hello')");
        stat.execute("INSERT INTO TEST VALUES(3,'World!')");
        stat.execute("INSERT INTO TEST VALUES(4,'Hallo')");
        stat.execute("INSERT INTO TEST VALUES(5,'Welt!')");
        stat.execute("INSERT INTO TEST VALUES(6,'Test2')");
        stat.execute("INSERT INTO TEST VALUES(7,NULL)");
        stat.execute("INSERT INTO TEST VALUES(8,NULL)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" },
                new int[] { Types.INTEGER, Types.CLOB }, new int[] {
                10, Integer.MAX_VALUE }, new int[] { 0, 0 });
        rs.next();
        Object obj = rs.getObject(2);
        assertTrue(obj instanceof java.sql.Clob);
        string = rs.getString(2);
        assertTrue(string != null && string.equals("Test"));
        assertTrue(!rs.wasNull());
        rs.next();
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(rs.getAsciiStream(2), StandardCharsets.ISO_8859_1);
        } catch (Exception e) {
            assertTrue(false);
        }
        string = readString(reader);
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("Hello"));
        rs.next();
        try {
            reader = new InputStreamReader(rs.getAsciiStream("value"), StandardCharsets.ISO_8859_1);
        } catch (Exception e) {
            assertTrue(false);
        }
        string = readString(reader);
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("World!"));
        rs.next();

        string = readString(rs.getCharacterStream(2));
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("Hallo"));
        Clob clob = rs.getClob(2);
        try {
            assertEquals("all", readString(clob.getCharacterStream(2, 3)));
            assertThrows(ErrorCode.INVALID_VALUE_2, clob).getCharacterStream(6, 1);
            assertThrows(ErrorCode.INVALID_VALUE_2, clob).getCharacterStream(7, 0);
        } finally {
            clob.free();
        }
        rs.next();

        string = readString(rs.getCharacterStream("value"));
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("Welt!"));
        rs.next();

        clob = rs.getObject("value", Clob.class);
        try {
            assertTrue(clob != null);
            string = readString(clob.getCharacterStream());
            assertTrue(string != null && string.equals("Test2"));
            assertTrue(!rs.wasNull());
        } finally {
            clob.free();
        }
        rs.next();

        assertTrue(rs.getCharacterStream(2) == null);
        assertTrue(rs.wasNull());
        rs.next();

        assertTrue(rs.getAsciiStream("Value") == null);
        assertTrue(rs.wasNull());

        assertTrue(rs.getStatement() == stat);
        assertTrue(rs.getWarnings() == null);
        rs.clearWarnings();
        assertTrue(rs.getWarnings() == null);
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs.next();
        stat.execute("DROP TABLE TEST");
    }

    private void testArray() throws SQLException {
        trace("Test ARRAY");
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE ARRAY)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        prep.setInt(1, 1);
        prep.setObject(2, new Object[] { 1, 2 });
        prep.execute();
        prep.setInt(1, 2);
        prep.setObject(2, new Object[] { 11, 12 });
        prep.execute();
        prep.close();
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(1, rs.getInt(1));
        Object[] list = (Object[]) rs.getObject(2);
        assertEquals(1, ((Integer) list[0]).intValue());
        assertEquals(2, ((Integer) list[1]).intValue());

        Array array = rs.getArray(2);
        Object[] list2 = (Object[]) array.getArray();
        assertEquals(1, ((Integer) list2[0]).intValue());
        assertEquals(2, ((Integer) list2[1]).intValue());
        list2 = (Object[]) array.getArray(2, 1);
        assertEquals(2, ((Integer) list2[0]).intValue());
        rs.next();
        assertEquals(2, rs.getInt(1));
        list = (Object[]) rs.getObject(2);
        assertEquals(11, ((Integer) list[0]).intValue());
        assertEquals(12, ((Integer) list[1]).intValue());

        array = rs.getArray("VALUE");
        list2 = (Object[]) array.getArray();
        assertEquals(11, ((Integer) list2[0]).intValue());
        assertEquals(12, ((Integer) list2[1]).intValue());
        list2 = (Object[]) array.getArray(2, 1);
        assertEquals(12, ((Integer) list2[0]).intValue());

        list2 = (Object[]) array.getArray(Collections.<String, Class<?>>emptyMap());
        assertEquals(11, ((Integer) list2[0]).intValue());

        assertEquals(Types.NULL, array.getBaseType());
        assertEquals("NULL", array.getBaseTypeName());

        assertTrue(array.toString().endsWith(": (11, 12)"));

        // free
        array.free();
        assertEquals("null", array.toString());
        assertThrows(ErrorCode.OBJECT_CLOSED, array).getBaseType();
        assertThrows(ErrorCode.OBJECT_CLOSED, array).getBaseTypeName();
        assertThrows(ErrorCode.OBJECT_CLOSED, array).getResultSet();

        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private byte[] readAllBytes(InputStream in) {
        if (in == null) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            while (true) {
                int b = in.read();
                if (b == -1) {
                    break;
                }
                out.write(b);
            }
            return out.toByteArray();
        } catch (IOException e) {
            assertTrue(false);
            return null;
        }
    }

    private void assertEqualsWithNull(byte[] expected, byte[] got) {
        if (got == null || expected == null) {
            assertTrue(got == expected);
        } else {
            assertEquals(got, expected);
        }
    }

    private void checkColumnBigDecimal(ResultSet rs, int column, int i,
            String bd) throws SQLException {
        BigDecimal bd1 = rs.getBigDecimal(column);
        int i1 = rs.getInt(column);
        if (bd == null) {
            trace("should be: null");
            assertTrue(rs.wasNull());
        } else {
            trace("BigDecimal i=" + i + " bd=" + bd + " ; i1=" + i1 + " bd1=" + bd1);
            assertTrue(!rs.wasNull());
            assertTrue(i1 == i);
            assertTrue(bd1.compareTo(new BigDecimal(bd)) == 0);
        }
    }

}
