/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.Csv;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * CSVREAD and CSVWRITE tests.
 *
 * @author Thomas Mueller
 * @author Sylvain Cuaz (testNull)
 */
public class TestCsv extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        testWriteColumnHeader();
        testCaseSensitiveColumnNames();
        testWriteResultSetDataType();
        testPreserveWhitespace();
        testChangeData();
        testOptions();
        testPseudoBom();
        testWriteRead();
        testColumnNames();
        testSpaceSeparated();
        testNull();
        testRandomData();
        testEmptyFieldDelimiter();
        testFieldDelimiter();
        testAsTable();
        testRead();
        testPipe();
        deleteDb("csv");
    }

    private void testWriteColumnHeader() throws Exception {
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + getBaseDir() +
                "/test.tsv', 'select x from dual', 'writeColumnHeader=false')");
        String x = IOUtils.readStringAndClose(IOUtils.getReader(
                FileUtils.newInputStream(getBaseDir() + "/test.tsv")), -1);
        assertEquals("\"1\"", x.trim());
        stat.execute("call csvwrite('" + getBaseDir() +
                "/test.tsv', 'select x from dual', 'writeColumnHeader=true')");
        x = IOUtils.readStringAndClose(IOUtils.getReader(
                FileUtils.newInputStream(getBaseDir() + "/test.tsv")), -1);
        x = x.trim();
        assertTrue(x.startsWith("\"X\""));
        assertTrue(x.endsWith("\"1\""));
        conn.close();
    }


    private void testWriteResultSetDataType() throws Exception {
        // Oracle: ResultSet.getString on a date or time column returns a
        // strange result (2009-6-30.16.17. 21. 996802000 according to a
        // customer)
        StringWriter writer = new StringWriter();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "select timestamp '-100-01-01 12:00:00.0' ts, null n");
        Csv csv = new Csv();
        csv.setFieldDelimiter((char) 0);
        csv.setLineSeparator(";");
        csv.write(writer, rs);
        conn.close();
        // getTimestamp().getString() needs to be used (not for H2, but for
        // Oracle)
        assertEquals("TS,N;0101-01-01 12:00:00.0,;", writer.toString());
    }

    private void testCaseSensitiveColumnNames() throws Exception {
        OutputStream out = FileUtils.newOutputStream(
                getBaseDir() + "/test.tsv", false);
        out.write("lower,Mixed,UPPER\n 1 , 2, 3 \n".getBytes());
        out.close();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from csvread('" +
                getBaseDir() + "/test.tsv')");
        rs.next();
        assertEquals("LOWER", rs.getMetaData().getColumnName(1));
        assertEquals("MIXED", rs.getMetaData().getColumnName(2));
        assertEquals("UPPER", rs.getMetaData().getColumnName(3));
        rs = stat.executeQuery("select * from csvread('" +
                getBaseDir() +
                "/test.tsv', null, 'caseSensitiveColumnNames=true')");
        rs.next();
        assertEquals("lower", rs.getMetaData().getColumnName(1));
        assertEquals("Mixed", rs.getMetaData().getColumnName(2));
        assertEquals("UPPER", rs.getMetaData().getColumnName(3));
        conn.close();
    }

    private void testPreserveWhitespace() throws Exception {
        OutputStream out = FileUtils.newOutputStream(
                getBaseDir() + "/test.tsv", false);
        out.write("a,b\n 1 , 2 \n".getBytes());
        out.close();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from csvread('" +
                getBaseDir() + "/test.tsv')");
        rs.next();
        assertEquals("1", rs.getString(1));
        assertEquals("2", rs.getString(2));
        rs = stat.executeQuery("select * from csvread('" +
                getBaseDir() + "/test.tsv', null, 'preserveWhitespace=true')");
        rs.next();
        assertEquals(" 1 ", rs.getString(1));
        assertEquals(" 2 ", rs.getString(2));
        conn.close();
    }

    private void testChangeData() throws Exception {
        OutputStream out = FileUtils.newOutputStream(
                getBaseDir() + "/test.tsv", false);
        out.write("a,b,c,d,e,f,g\n1".getBytes());
        out.close();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from csvread('" +
                getBaseDir() + "/test.tsv')");
        assertEquals(7, rs.getMetaData().getColumnCount());
        assertEquals("A", rs.getMetaData().getColumnLabel(1));
        rs.next();
        assertEquals(1, rs.getInt(1));
        out = FileUtils.newOutputStream(getBaseDir() + "/test.tsv", false);
        out.write("x".getBytes());
        out.close();
        rs = stat.executeQuery("select * from csvread('" +
                getBaseDir() + "/test.tsv')");
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("X", rs.getMetaData().getColumnLabel(1));
        assertFalse(rs.next());
        conn.close();
    }

    private void testOptions() {
        Csv csv = new Csv();
        assertEquals(",", csv.getFieldSeparatorWrite());
        assertEquals(SysProperties.LINE_SEPARATOR, csv.getLineSeparator());
        assertEquals("", csv.getNullString());
        assertEquals('\"', csv.getEscapeCharacter());
        assertEquals('"', csv.getFieldDelimiter());
        assertEquals(',', csv.getFieldSeparatorRead());
        assertEquals(",", csv.getFieldSeparatorWrite());
        assertEquals(0, csv.getLineCommentCharacter());
        assertEquals(false, csv.getPreserveWhitespace());

        String charset;

        charset = csv.setOptions("escape=\\  fieldDelimiter=\\\\ fieldSeparator=\n " +
                "lineComment=\" lineSeparator=\\ \\\\\\ ");
        assertEquals(' ', csv.getEscapeCharacter());
        assertEquals('\\', csv.getFieldDelimiter());
        assertEquals('\n', csv.getFieldSeparatorRead());
        assertEquals("\n", csv.getFieldSeparatorWrite());
        assertEquals('"', csv.getLineCommentCharacter());
        assertEquals(" \\ ", csv.getLineSeparator());
        assertFalse(csv.getPreserveWhitespace());
        assertFalse(csv.getCaseSensitiveColumnNames());

        charset = csv.setOptions("escape=1x fieldDelimiter=2x " +
                "fieldSeparator=3x " + "lineComment=4x lineSeparator=5x " +
                "null=6x charset=7x " +
                "preserveWhitespace=true caseSensitiveColumnNames=true");
        assertEquals('1', csv.getEscapeCharacter());
        assertEquals('2', csv.getFieldDelimiter());
        assertEquals('3', csv.getFieldSeparatorRead());
        assertEquals("3x", csv.getFieldSeparatorWrite());
        assertEquals('4', csv.getLineCommentCharacter());
        assertEquals("5x", csv.getLineSeparator());
        assertEquals("6x", csv.getNullString());
        assertEquals("7x", charset);
        assertTrue(csv.getPreserveWhitespace());
        assertTrue(csv.getCaseSensitiveColumnNames());

        charset = csv.setOptions("escape= fieldDelimiter= " +
                "fieldSeparator= " + "lineComment= lineSeparator=\r\n " +
                "null=\0 charset=");
        assertEquals(0, csv.getEscapeCharacter());
        assertEquals(0, csv.getFieldDelimiter());
        assertEquals(0, csv.getFieldSeparatorRead());
        assertEquals("", csv.getFieldSeparatorWrite());
        assertEquals(0, csv.getLineCommentCharacter());
        assertEquals("\r\n", csv.getLineSeparator());
        assertEquals("\0", csv.getNullString());
        assertEquals("", charset);

        createClassProxy(Csv.class);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, csv).
            setOptions("escape=a error=b");
        assertEquals('a', csv.getEscapeCharacter());
    }

    private void testPseudoBom() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // UTF-8 "BOM" / marker
        out.write(StringUtils.convertHexToBytes("ef" + "bb" + "bf"));
        out.write("\"ID\", \"NAME\"\n1, Hello".getBytes(StandardCharsets.UTF_8));
        byte[] buff = out.toByteArray();
        Reader r = new InputStreamReader(new ByteArrayInputStream(buff), StandardCharsets.UTF_8);
        ResultSet rs = new Csv().read(r, null);
        assertEquals("ID", rs.getMetaData().getColumnLabel(1));
        assertEquals("NAME", rs.getMetaData().getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
    }

    private void testColumnNames() throws Exception {
        ResultSet rs;
        rs = new Csv().read(new StringReader("Id,First Name,2x,_x2\n1,2,3"), null);
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("First Name", rs.getMetaData().getColumnName(2));
        assertEquals("2x", rs.getMetaData().getColumnName(3));
        assertEquals("_X2", rs.getMetaData().getColumnName(4));

        rs = new Csv().read(new StringReader("a,a\n1,2"), null);
        assertEquals("A", rs.getMetaData().getColumnName(1));
        assertEquals("A1", rs.getMetaData().getColumnName(2));

        rs = new Csv().read(new StringReader("1,2"), new String[] { "", null });
        assertEquals("C1", rs.getMetaData().getColumnName(1));
        assertEquals("C2", rs.getMetaData().getColumnName(2));
    }

    private void testSpaceSeparated() throws SQLException {
        deleteDb("csv");
        File f = new File(getBaseDir() + "/testSpace.csv");
        FileUtils.delete(f.getAbsolutePath());

        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("create temporary table test (a int, b int, c int)");
        stat.execute("insert into test values(1,2,3)");
        stat.execute("insert into test values(4,null,5)");
        stat.execute("call csvwrite('" + getBaseDir() +
                "/test.tsv','select * from test',null,' ')");
        ResultSet rs1 = stat.executeQuery("select * from test");
        assertResultSetOrdered(rs1, new String[][] {
                new String[] { "1", "2", "3" }, new String[] { "4", null, "5" } });
        ResultSet rs2 = stat.executeQuery("select * from csvread('" +
                getBaseDir() + "/test.tsv',null,null,' ')");
        assertResultSetOrdered(rs2, new String[][] {
                new String[] { "1", "2", "3" }, new String[] { "4", null, "5" } });
        conn.close();
        FileUtils.delete(f.getAbsolutePath());
        FileUtils.delete(getBaseDir() + "/test.tsv");
    }

    /**
     * Test custom NULL string.
     */
    private void testNull() throws Exception {
        deleteDb("csv");

        String fileName = getBaseDir() + "/testNull.csv";
        FileUtils.delete(fileName);

        OutputStream out = FileUtils.newOutputStream(fileName, false);
        String csvContent = "\"A\",\"B\",\"C\",\"D\"\n\\N,\"\",\"\\N\",";
        byte[] b = csvContent.getBytes(StandardCharsets.UTF_8);
        out.write(b, 0, b.length);
        out.close();
        Csv csv = new Csv();
        csv.setNullString("\\N");
        ResultSet rs = csv.read(fileName, null, "UTF8");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(4, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertEquals("C", meta.getColumnLabel(3));
        assertEquals("D", meta.getColumnLabel(4));
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("", rs.getString(2));
        // null is never quoted
        assertEquals("\\N", rs.getString(3));
        // an empty string is always parsed as null
        assertEquals(null, rs.getString(4));
        assertFalse(rs.next());

        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + fileName +
                "', 'select NULL as a, '''' as b, ''\\N'' as c, NULL as d', " +
                "'UTF8', ',', '\"', NULL, '\\N', '\n')");
        InputStreamReader reader = new InputStreamReader(
                FileUtils.newInputStream(fileName));
        // on read, an empty string is treated like null,
        // but on write a null is always written with the nullString
        String data = IOUtils.readStringAndClose(reader, -1);
        assertEquals(csvContent + "\\N", data.trim());
        conn.close();

        FileUtils.delete(fileName);
    }

    private void testRandomData() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, a varchar, b varchar)");
        int len = getSize(1000, 10000);
        PreparedStatement prep = conn.prepareStatement(
                "insert into test(a, b) values(?, ?)");
        ArrayList<String[]> list = New.arrayList();
        Random random = new Random(1);
        for (int i = 0; i < len; i++) {
            String a = randomData(random), b = randomData(random);
            prep.setString(1, a);
            prep.setString(2, b);
            list.add(new String[] { a, b });
            prep.execute();
        }
        stat.execute("call csvwrite('" + getBaseDir() +
                "/test.csv', 'select a, b from test order by id', 'UTF-8', '|', '#')");
        Csv csv = new Csv();
        csv.setFieldSeparatorRead('|');
        csv.setFieldDelimiter('#');
        ResultSet rs = csv.read(getBaseDir() + "/test.csv", null, "UTF-8");
        for (int i = 0; i < len; i++) {
            assertTrue(rs.next());
            String[] pair = list.get(i);
            assertEquals(pair[0], rs.getString(1));
            assertEquals(pair[1], rs.getString(2));
        }
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(getBaseDir() + "/test.csv");
    }

    private static String randomData(Random random) {
        if (random.nextInt(10) == 1) {
            return null;
        }
        int len = random.nextInt(5);
        StringBuilder buff = new StringBuilder();
        String chars = "\\\'\",\r\n\t ;.-123456|#";
        for (int i = 0; i < len; i++) {
            buff.append(chars.charAt(random.nextInt(chars.length())));
        }
        return buff.toString();
    }

    private void testEmptyFieldDelimiter() throws Exception {
        String fileName = getBaseDir() + "/test.csv";
        FileUtils.delete(fileName);
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + fileName
                + "', 'select 1 id, ''Hello'' name', null, '|', '', null, null, chr(10))");
        InputStreamReader reader = new InputStreamReader(
                FileUtils.newInputStream(fileName));
        String text = IOUtils.readStringAndClose(reader, -1).trim();
        text = text.replace('\n', ' ');
        assertEquals("ID|NAME 1|Hello", text);
        ResultSet rs = stat.executeQuery("select * from csvread('" +
                fileName + "', null, null, '|', '')");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("ID", meta.getColumnLabel(1));
        assertEquals("NAME", meta.getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(fileName);
    }

    private void testFieldDelimiter() throws Exception {
        String fileName = getBaseDir() + "/test.csv";
        String fileName2 = getBaseDir() + "/test2.csv";
        FileUtils.delete(fileName);
        OutputStream out = FileUtils.newOutputStream(fileName, false);
        byte[] b = "'A'; 'B'\n\'It\\'s nice\'; '\nHello\\*\n'".getBytes();
        out.write(b, 0, b.length);
        out.close();
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from csvread('" +
                fileName + "', null, null, ';', '''', '\\')");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals("It's nice", rs.getString(1));
        assertEquals("\nHello*\n", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("call csvwrite('" + fileName2 +
                "', 'select * from csvread(''" + fileName +
                "'', null, null, '';'', '''''''', ''\\'')', null, '+', '*', '#')");
        rs = stat.executeQuery("select * from csvread('" + fileName2 +
                "', null, null, '+', '*', '#')");
        meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertTrue(rs.next());
        assertEquals("It's nice", rs.getString(1));
        assertEquals("\nHello*\n", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(fileName);
        FileUtils.delete(fileName2);
    }

    private void testPipe() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + getBaseDir() +
                "/test.csv', 'select 1 id, ''Hello'' name', 'utf-8', '|')");
        ResultSet rs = stat.executeQuery("select * from csvread('" +
                getBaseDir() + "/test.csv', null, 'utf-8', '|')");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        new File(getBaseDir() + "/test.csv").delete();

        // PreparedStatement prep = conn.prepareStatement("select * from
        // csvread(?, null, ?, ?)");
        // prep.setString(1, BASE_DIR+"/test.csv");
        // prep.setString(2, "utf-8");
        // prep.setString(3, "|");
        // rs = prep.executeQuery();

        conn.close();
        FileUtils.delete(getBaseDir() + "/test.csv");
    }

    private void testAsTable() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + getBaseDir() +
                "/test.csv', 'select 1 id, ''Hello'' name')");
        ResultSet rs = stat.executeQuery("select name from csvread('" +
                getBaseDir() + "/test.csv')");
        assertTrue(rs.next());
        assertEquals("Hello", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("call csvread('" + getBaseDir() + "/test.csv')");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        new File(getBaseDir() + "/test.csv").delete();
        conn.close();
    }

    private void testRead() throws Exception {
        String fileName = getBaseDir() + "/test.csv";
        FileUtils.delete(fileName);
        OutputStream out = FileUtils.newOutputStream(fileName, false);
        byte[] b = ("a,b,c,d\n201,-2,0,18\n, \"abc\"\"\" ," +
                ",\"\"\n 1 ,2 , 3, 4 \n5, 6, 7, 8").getBytes();
        out.write(b, 0, b.length);
        out.close();
        ResultSet rs = new Csv().read(fileName, null, "UTF8");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(4, meta.getColumnCount());
        assertEquals("A", meta.getColumnLabel(1));
        assertEquals("B", meta.getColumnLabel(2));
        assertEquals("C", meta.getColumnLabel(3));
        assertEquals("D", meta.getColumnLabel(4));
        assertTrue(rs.next());
        assertEquals("201", rs.getString(1));
        assertEquals("-2", rs.getString(2));
        assertEquals("0", rs.getString(3));
        assertEquals("18", rs.getString(4));
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("abc\"", rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertEquals("", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals("3", rs.getString(3));
        assertEquals("4", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("5", rs.getString(1));
        assertEquals("6", rs.getString(2));
        assertEquals("7", rs.getString(3));
        assertEquals("8", rs.getString(4));
        assertFalse(rs.next());

        // a,b,c,d
        // 201,-2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        FileUtils.delete(fileName);
    }

    private void testWriteRead() throws SQLException {
        deleteDb("csv");
        Connection conn = getConnection("csv");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        // int len = 100000;
        int len = 100;
        for (int i = 0; i < len; i++) {
            stat.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        }
        long time;
        time = System.nanoTime();
        new Csv().write(conn, getBaseDir() + "/testRW.csv",
                "SELECT X ID, 'Ruebezahl' NAME FROM SYSTEM_RANGE(1, " + len + ")", "UTF8");
        trace("write: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
        ResultSet rs;
        time = System.nanoTime();
        for (int i = 0; i < 30; i++) {
            rs = new Csv().read(getBaseDir() + "/testRW.csv", null, "UTF8");
            while (rs.next()) {
                // ignore
            }
        }
        trace("read: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
        rs = new Csv().read(getBaseDir() + "/testRW.csv", null, "UTF8");
        // stat.execute("CREATE ALIAS CSVREAD FOR \"org.h2.tools.Csv.read\"");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        for (int i = 0; i < len; i++) {
            rs.next();
            assertEquals("" + (i + 1), rs.getString("ID"));
            assertEquals("Ruebezahl", rs.getString("NAME"));
        }
        assertFalse(rs.next());
        rs.close();
        conn.close();
        FileUtils.delete(getBaseDir() + "/testRW.csv");
    }

}
