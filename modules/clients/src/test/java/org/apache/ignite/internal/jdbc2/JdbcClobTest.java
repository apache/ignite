/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.jdbc2;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Test for JDBC CLOB.
 */
public class JdbcClobTest {
    /** */
    static final String ERROR_CLOB_FREE = "Clob instance can't be used after free() has been called.";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLength() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        assertEquals(10, clob.length());

        clob.free();
        assertThrows(null, clob::length, SQLException.class, ERROR_CLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSubString() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        assertThrows(null, () -> clob.getSubString(-1, 1), SQLException.class, null);

        assertThrows(null, () -> clob.getSubString(0, 1), SQLException.class, null);

        assertThrows(null, () -> clob.getSubString(1, -1), SQLException.class, null);

        assertThrows(null, () -> clob.getSubString(1, 11), SQLException.class, null);

        assertEquals("", clob.getSubString(3, 0));

        assertEquals("1", clob.getSubString(1, 1));

        assertEquals("0", clob.getSubString(10, 1));

        assertEquals("12345", clob.getSubString(1, 5));

        assertEquals("34567", clob.getSubString(3, 5));

        assertEquals("567890", clob.getSubString(5, 6));

        assertEquals("1234567890", clob.getSubString(1, 10));

        clob.free();
        assertThrows(null, () -> clob.getSubString(1, 10), SQLException.class, ERROR_CLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetCharacterStream() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        Reader cStream = clob.getCharacterStream();
        String res = IOUtils.toString(cStream);
        assertEquals("1234567890", res);

        clob.free();
        assertThrows(null, () -> clob.getCharacterStream(), SQLException.class, ERROR_CLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetCharacterStreamWithParams() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        assertThrows(null, () -> clob.getCharacterStream(-1, 1), SQLException.class, null);

        assertThrows(null, () -> clob.getCharacterStream(0, 1), SQLException.class, null);

        assertThrows(null, () -> clob.getCharacterStream(1, -1), SQLException.class, null);

        assertThrows(null, () -> clob.getCharacterStream(1, 11), SQLException.class, null);

        Reader cStream = clob.getCharacterStream(1, 10);
        String res = IOUtils.toString(cStream);
        assertEquals("1234567890", res);

        cStream = clob.getCharacterStream(1, 1);
        res = IOUtils.toString(cStream);
        assertEquals("1", res);

        cStream = clob.getCharacterStream(10, 1);
        res = IOUtils.toString(cStream);
        assertEquals("0", res);

        cStream = clob.getCharacterStream(3, 5);
        res = IOUtils.toString(cStream);
        assertEquals("34567", res);

        cStream = clob.getCharacterStream(3, 0);
        res = IOUtils.toString(cStream);
        assertEquals("", res);

        clob.free();
        assertThrows(null, () -> clob.getCharacterStream(1, 1), SQLException.class, ERROR_CLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAsciiStream() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");
        byte[] bytes = IOUtils.toByteArray(clob.getAsciiStream());
        Assert.assertArrayEquals("1234567890".getBytes(UTF_8), bytes);

        clob.free();
        assertThrows(null, clob::getAsciiStream, SQLException.class, ERROR_CLOB_FREE);

        Clob emptyClob = new JdbcClob("");
        bytes = IOUtils.toByteArray(emptyClob.getAsciiStream());
        Assert.assertArrayEquals("".getBytes(UTF_8), bytes);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAsciiStreamForNonAsciiDataBufferedRead() throws Exception {
        StringBuilder sb = new StringBuilder();

        // Create string in a way which makes sure that all variants in
        // JdbcClob.Utf8EncodedStringInputStream.encodeNextChunk() are covered.
        // In particular the check for the surrogate element.
        for (int i = 0; i < 3277; i++) {
            sb.append("aa©😀");
        }

        Clob clob = new JdbcClob(sb.toString());

        InputStream stream = clob.getAsciiStream();

        assertThrows(null, () -> stream.read(null, 0, 1), NullPointerException.class, null);

        assertThrows(null, () -> stream.read(new byte[10], -1, 5), IndexOutOfBoundsException.class, null);

        assertThrows(null, () -> stream.read(new byte[10], 5, -1), IndexOutOfBoundsException.class, null);

        assertThrows(null, () -> stream.read(new byte[10], 11, 1), IndexOutOfBoundsException.class, null);

        assertThrows(null, () -> stream.read(new byte[10], 5, 6), IndexOutOfBoundsException.class, null);

        assertEquals(0, stream.read(new byte[10], 5, 0));

        byte[] bytes = IOUtils.toByteArray(stream);

        String reencoded = new String(bytes, UTF_8);

        assertEquals(clob.getSubString(1, (int)clob.length()), reencoded);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAsciiStreamForNonAsciiDataReadByByte() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("aa©😀");
        }

        Clob clob = new JdbcClob(sb.toString());

        InputStream stream = clob.getAsciiStream();

        int i = 0;
        byte[] bytes = new byte[80];

        byte val = (byte)stream.read();

        while (val != -1) {
            bytes[i++] = val;

            val = (byte)stream.read();
        }

        String reencoded = new String(bytes, UTF_8);

        assertEquals(clob.getSubString(1, (int)clob.length()), reencoded);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPositionWithStringPattern() throws Exception {
        JdbcClob clob1 = new JdbcClob("1234567890");

        assertThrows(null, () -> clob1.position("0", 0), SQLException.class, null);

        assertThrows(null, () -> clob1.position("0", -1), SQLException.class, null);

        assertEquals(1, clob1.position("", 1));

        assertEquals(10, clob1.position("", 10));

        assertEquals(11, clob1.position("", 100));

        assertEquals(-1, clob1.position("a", 11));

        assertEquals(1, clob1.position("1", 1));

        assertEquals(5, clob1.position("56", 1));

        assertEquals(5, clob1.position("56", 5));

        assertEquals(-1, clob1.position("56", 6));

        clob1.free();
        assertThrows(null, clob1::getAsciiStream, SQLException.class, ERROR_CLOB_FREE);

        Clob clob2 = new JdbcClob("abbabab");

        assertEquals(5, clob2.position("b", 4));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPositionWithClobPattern() throws Exception {
        Clob clob = new JdbcClob("1234567890");

        Clob patternClob = new JdbcClob("567");

        assertThrows(null, () -> clob.position(patternClob, 0), SQLException.class, null);

        assertThrows(null, () -> clob.position(patternClob, -1), SQLException.class, null);

        assertEquals(5, clob.position(patternClob, 1));

        assertEquals(5, clob.position(patternClob, 5));

        assertEquals(-1, clob.position(patternClob, 6));

        Clob patternClob2 = new JdbcClob("a");

        assertEquals(-1, clob.position(patternClob2, 1));

        clob.free();
        assertThrows(null, () -> clob.position(patternClob2, 5), SQLException.class, ERROR_CLOB_FREE);

        Clob clob2 = new JdbcClob("bbabbabba");

        assertEquals(6, clob2.position(patternClob2, 5));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetString() throws Exception {
        JdbcClob clob1 = new JdbcClob("1234567890");

        assertThrows(null, () -> clob1.setString(-1, "a"), SQLException.class, null);

        assertThrows(null, () -> clob1.setString(0, "a"), SQLException.class, null);

        assertThrows(null, () -> clob1.setString(clob1.length() + 2, "a"), SQLException.class, null);

        assertThrows(null, () -> clob1.setString(1, null), SQLException.class, null);

        int written = clob1.setString(1, "a");
        assertEquals("a", clob1.getSubString(1, 1));
        assertEquals(1, written);

        written = clob1.setString(5, "abc");
        assertEquals("abc", clob1.getSubString(5, 3));
        assertEquals(3, written);

        written = clob1.setString(10, "def");
        assertEquals("def", clob1.getSubString(10, 3));
        assertEquals(3, written);

        clob1.free();
        assertThrows(null, () -> clob1.setString(1, "a"), SQLException.class, ERROR_CLOB_FREE);

        Clob clob2 = new JdbcClob("12345");
        written = clob2.setString(3, "abcd");
        assertEquals("12abcd", clob2.getSubString(1, (int)clob2.length()));
        assertEquals(4, written);

        Clob clob3 = new JdbcClob("12345");
        written = clob3.setString(3, "ab");
        assertEquals("12ab5", clob3.getSubString(1, (int)clob3.length()));
        assertEquals(2, written);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetStringWithSubString() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        assertThrows(null, () -> clob.setString(-1, "a", 0, 1), SQLException.class, null);

        assertThrows(null, () -> clob.setString(0, "a", 0, 1), SQLException.class, null);

        assertThrows(null, () -> clob.setString(clob.length() + 2, "a", 0, 1), SQLException.class, null);

        assertThrows(null, () -> clob.setString(1, null, 0, 1), SQLException.class, null);

        assertThrows(null, () -> clob.setString(1, "a", -1, 1), SQLException.class, null);

        assertThrows(null, () -> clob.setString(1, "a", 0, -1), SQLException.class, null);

        assertThrows(null, () -> clob.setString(1, "abc", 1, 3), SQLException.class, null);

        clob.free();
        assertThrows(null, () -> clob.setString(1, "a", 0, 1), SQLException.class, ERROR_CLOB_FREE);

        Clob clob2 = new JdbcClob("1234567890");
        int written = clob2.setString(3, "abcd", 0, 1);
        assertEquals("12a4567890", clob2.getSubString(1, (int)clob2.length()));
        assertEquals(1, written);

        clob2 = new JdbcClob("1234567890");
        written = clob2.setString(1, "abcd", 0, 3);
        assertEquals("abc4567890", clob2.getSubString(1, (int)clob2.length()));
        assertEquals(3, written);

        clob2 = new JdbcClob("1234567890");
        written = clob2.setString(5, "abcd", 2, 2);
        assertEquals("1234cd7890", clob2.getSubString(1, (int)clob2.length()));
        assertEquals(2, written);

        clob2 = new JdbcClob("1234567890");
        written = clob2.setString(9, "abcd", 0, 4);
        assertEquals("12345678abcd", clob2.getSubString(1, (int)clob2.length()));
        assertEquals(4, written);

        clob2 = new JdbcClob("1234567890");
        written = clob2.setString(11, "abcd", 0, 4);
        assertEquals("1234567890abcd", clob2.getSubString(1, (int)clob2.length()));
        assertEquals(4, written);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTruncate() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        assertThrows(null, () -> {
            clob.truncate(-1);

            return null;
        }, SQLException.class, null);

        assertThrows(null, () -> {
            clob.truncate(clob.length() + 1);

            return null;
        }, SQLException.class, null);

        clob.truncate(9);
        assertEquals("123456789", clob.getSubString(1, (int)clob.length()));

        clob.truncate(5);
        assertEquals("12345", clob.getSubString(1, (int)clob.length()));

        clob.truncate(0);
        assertEquals("", clob.getSubString(1, (int)clob.length()));

        clob.free();
        assertThrows(null, () -> {
            clob.truncate(1);

            return null;
        }, SQLException.class, ERROR_CLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetAsciiStream() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        assertThrows(null, () -> clob.setAsciiStream(1L), SQLFeatureNotSupportedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetCharacterStream() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        assertThrows(null, () -> clob.setCharacterStream(1L), SQLFeatureNotSupportedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFree() throws Exception {
        JdbcClob clob = new JdbcClob("1234567890");

        clob.length();

        clob.free();

        clob.free();

        assertThrows(null, clob::length, SQLException.class, ERROR_CLOB_FREE);
    }
}
