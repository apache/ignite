/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;

/**
 * Test the Blob, Clob, and NClob implementations.
 */
public class TestLobApi extends TestBase {

    private JdbcConnection conn;
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
        deleteDb(getTestName());
        testUnsupportedOperations();
        testLobStaysOpenUntilCommitted();
        testInputStreamThrowsException(true);
        testInputStreamThrowsException(false);
        conn = (JdbcConnection) getConnection(getTestName());
        stat = conn.createStatement();
        stat.execute("create table test(id int, x blob)");
        testBlob(0);
        testBlob(1);
        testBlob(100);
        testBlob(100000);
        stat.execute("drop table test");
        stat.execute("create table test(id int, x clob)");
        testClob(0);
        testClob(1);
        testClob(100);
        testClob(100000);
        stat.execute("drop table test");
        conn.close();
    }

    private void testUnsupportedOperations() throws Exception {
        Connection conn = getConnection(getTestName());
        stat = conn.createStatement();
        stat.execute("create table test(id int, c clob, b blob)");
        stat.execute("insert into test values(1, 'x', x'00')");
        ResultSet rs = stat.executeQuery("select * from test order by id");
        rs.next();
        Clob clob = rs.getClob(2);
        byte[] data = IOUtils.readBytesAndClose(clob.getAsciiStream(), -1);
        assertEquals("x", new String(data, StandardCharsets.UTF_8));
        assertTrue(clob.toString().endsWith("'x'"));
        clob.free();
        assertTrue(clob.toString().endsWith("null"));

        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, clob).
                truncate(0);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, clob).
                setAsciiStream(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, clob).
                position("", 0);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, clob).
                position((Clob) null, 0);

        Blob blob = rs.getBlob(3);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, blob).
                truncate(0);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, blob).
                position(new byte[1], 0);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, blob).
                position((Blob) null, 0);
        assertTrue(blob.toString().endsWith("X'00'"));
        blob.free();
        assertTrue(blob.toString().endsWith("null"));

        stat.execute("drop table test");
        conn.close();
    }

    /**
     * According to the JDBC spec, BLOB and CLOB objects must stay open even if
     * the result set is closed (see ResultSet.close).
     */
    private void testLobStaysOpenUntilCommitted() throws Exception {
        Connection conn = getConnection(getTestName());
        stat = conn.createStatement();
        stat.execute("create table test(id identity, c clob, b blob)");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(null, ?, ?)");
        prep.setString(1, "");
        prep.setBytes(2, new byte[0]);
        prep.execute();

        Random r = new Random(1);

        char[] charsSmall = new char[20];
        for (int i = 0; i < charsSmall.length; i++) {
            charsSmall[i] = (char) r.nextInt(10000);
        }
        String dSmall = new String(charsSmall);
        prep.setCharacterStream(1, new StringReader(dSmall), -1);
        byte[] bytesSmall = new byte[20];
        r.nextBytes(bytesSmall);
        prep.setBinaryStream(2, new ByteArrayInputStream(bytesSmall), -1);
        prep.execute();

        char[] chars = new char[100000];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) r.nextInt(10000);
        }
        String d = new String(chars);
        prep.setCharacterStream(1, new StringReader(d), -1);
        byte[] bytes = new byte[100000];
        r.nextBytes(bytes);
        prep.setBinaryStream(2, new ByteArrayInputStream(bytes), -1);
        prep.execute();

        conn.setAutoCommit(false);
        ResultSet rs = stat.executeQuery("select * from test order by id");
        rs.next();
        Clob c1 = rs.getClob(2);
        Blob b1 = rs.getBlob(3);
        rs.next();
        Clob c2 = rs.getClob(2);
        Blob b2 = rs.getBlob(3);
        rs.next();
        Clob c3 = rs.getClob(2);
        Blob b3 = rs.getBlob(3);
        assertFalse(rs.next());
        // now close
        rs.close();
        // but the LOBs must stay open
        assertEquals(0, c1.length());
        assertEquals(0, b1.length());
        assertEquals("", c1.getSubString(1, 0));
        assertEquals(new byte[0], b1.getBytes(1, 0));

        assertEquals(charsSmall.length, c2.length());
        assertEquals(bytesSmall.length, b2.length());
        assertEquals(dSmall, c2.getSubString(1, (int) c2.length()));
        assertEquals(bytesSmall, b2.getBytes(1, (int) b2.length()));

        assertEquals(chars.length, c3.length());
        assertEquals(bytes.length, b3.length());
        assertEquals(d, c3.getSubString(1, (int) c3.length()));
        assertEquals(bytes, b3.getBytes(1, (int) b3.length()));
        stat.execute("drop table test");
        conn.close();
    }

    private void testInputStreamThrowsException(final boolean ioException)
            throws Exception {
        Connection conn = getConnection(getTestName());
        stat = conn.createStatement();
        stat.execute("create table test(id identity, c clob, b blob)");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(null, ?, ?)");

        assertThrows(ErrorCode.IO_EXCEPTION_1, prep).
                setCharacterStream(1, new Reader() {
                    int pos;
                    @Override
                    public int read(char[] buff, int off, int len) throws IOException {
                        pos += len;
                        if (pos > 100001) {
                            if (ioException) {
                                throw new IOException("");
                            }
                            throw new IllegalStateException();
                        }
                        return len;
                    }
                    @Override
                    public void close() throws IOException {
                        // nothing to do
                    }
                }, -1);

        prep.setString(1, new String(new char[10000]));
        prep.setBytes(2, new byte[0]);
        prep.execute();
        prep.setString(1, "");

        assertThrows(ErrorCode.IO_EXCEPTION_1, prep).
                setBinaryStream(2, new InputStream() {
                    int pos;
                    @Override
                    public int read() throws IOException {
                        pos++;
                        if (pos > 100001) {
                            if (ioException) {
                                throw new IOException("");
                            }
                            throw new IllegalStateException();
                        }
                        return 0;
                    }
                }, -1);

        prep.setBytes(2, new byte[10000]);
        prep.execute();
        ResultSet rs = stat.executeQuery("select c, b from test order by id");
        rs.next();
        assertEquals(new String(new char[10000]), rs.getString(1));
        assertEquals(new byte[0], rs.getBytes(2));
        rs.next();
        assertEquals("", rs.getString(1));
        assertEquals(new byte[10000], rs.getBytes(2));
        stat.execute("drop table test");
        conn.close();
    }

    private void testBlob(int length) throws Exception {
        Random r = new Random(length);
        byte[] data = new byte[length];
        r.nextBytes(data);
        Blob b = conn.createBlob();
        OutputStream out = b.setBinaryStream(1);
        out.write(data, 0, data.length);
        out.close();
        stat.execute("delete from test");

        PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?)");
        prep.setInt(1, 1);
        prep.setBlob(2, b);
        prep.execute();

        prep.setInt(1, 2);
        b = conn.createBlob();
        assertEquals(length, b.setBytes(1, data));
        prep.setBlob(2, b);
        prep.execute();

        prep.setInt(1, 3);
        Blob b2 = conn.createBlob();
        byte[] xdata = new byte[length + 2];
        System.arraycopy(data, 0, xdata, 1, length);
        assertEquals(length, b2.setBytes(1, xdata, 1, length));
        prep.setBlob(2, b2);
        prep.execute();

        prep.setInt(1, 4);
        prep.setBlob(2, new ByteArrayInputStream(data));
        prep.execute();

        prep.setInt(1, 5);
        prep.setBlob(2, new ByteArrayInputStream(data), -1);
        prep.execute();

        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        Blob b3 = rs.getBlob(2);
        assertEquals(length, b3.length());
        byte[] bytes = b.getBytes(1, length);
        byte[] bytes2 = b3.getBytes(1, length);
        assertEquals(bytes, bytes2);
        rs.next();
        b3 = rs.getBlob(2);
        assertEquals(length, b3.length());
        bytes2 = b3.getBytes(1, length);
        assertEquals(bytes, bytes2);
        rs.next();
        b3 = rs.getBlob(2);
        assertEquals(length, b3.length());
        bytes2 = b3.getBytes(1, length);
        assertEquals(bytes, bytes2);
        while (rs.next()) {
            bytes2 = rs.getBytes(2);
            assertEquals(bytes, bytes2);
        }
    }

    private void testClob(int length) throws Exception {
        Random r = new Random(length);
        char[] data = new char[length];

        // Unicode problem:
        // The UCS code values 0xd800-0xdfff (UTF-16 surrogates)
        // as well as 0xfffe and 0xffff (UCS non-characters)
        // should not appear in conforming UTF-8 streams.
        // (String.getBytes("UTF-8") only returns 1 byte for 0xd800-0xdfff)
        for (int i = 0; i < length; i++) {
            char c;
            do {
                c = (char) r.nextInt();
            } while (c >= 0xd800 && c <= 0xdfff);
            data[i] = c;
        }
        Clob c = conn.createClob();
        Writer out = c.setCharacterStream(1);
        out.write(data, 0, data.length);
        out.close();
        stat.execute("delete from test");
        PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?)");

        prep.setInt(1, 1);
        prep.setClob(2, c);
        prep.execute();

        c = conn.createClob();
        c.setString(1, new String(data));
        prep.setInt(1, 2);
        prep.setClob(2, c);
        prep.execute();

        prep.setInt(1, 3);
        prep.setCharacterStream(2, new StringReader(new String(data)));
        prep.execute();

        prep.setInt(1, 4);
        prep.setCharacterStream(2, new StringReader(new String(data)), -1);
        prep.execute();

        NClob nc;
        nc = conn.createNClob();
        assertEquals(length, nc.setString(1, new String(data)));
        prep.setInt(1, 5);
        prep.setNClob(2, nc);
        prep.execute();

        nc = conn.createNClob();
        char[] xdata = new char[length + 2];
        System.arraycopy(data, 0, xdata, 1, length);
        assertEquals(length, nc.setString(1, new String(xdata), 1, length));
        prep.setInt(1, 6);
        prep.setNClob(2, nc);
        prep.execute();

        prep.setInt(1, 7);
        prep.setNClob(2, new StringReader(new String(data)));
        prep.execute();

        prep.setInt(1, 8);
        prep.setNClob(2, new StringReader(new String(data)), -1);
        prep.execute();

        prep.setInt(1, 9);
        prep.setNString(2, new String(data));
        prep.execute();

        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        Clob c2 = rs.getClob(2);
        assertEquals(length, c2.length());
        String s = c.getSubString(1, length);
        String s2 = c2.getSubString(1, length);
        while (rs.next()) {
            c2 = rs.getClob(2);
            assertEquals(length, c2.length());
            s2 = c2.getSubString(1, length);
            assertEquals(s, s2);
        }
    }

}
