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

import java.io.Reader;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;

/**
 * Test for JDBC CLOB.
 */
public class JdbcClobTest extends TestCase {
    /** Entity for testing. */
    private JdbcClob clob;

    /**
     * CLOB initialization.
     */
    @Override public void setUp() throws Exception {
        super.setUp();

        clob = new JdbcClob("1234567890");
    }

    /**
     * CLOB clearing.
     */
    @Override public void tearDown() throws Exception {
        clob = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLength() throws Exception {
        assertEquals(10, clob.length());

        clob.free();

        try {
            clob.length();

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSubString() throws Exception {
        try {
            clob.getSubString(0, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.getSubString(1, -1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.getSubString(1, 11);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        assertEquals("1", clob.getSubString(1, 1));

        assertEquals("0", clob.getSubString(10, 1));

        assertEquals("12345", clob.getSubString(1, 5));

        assertEquals("34567", clob.getSubString(3, 5));

        assertEquals("567890", clob.getSubString(5, 6));

        assertEquals("1234567890", clob.getSubString(1, 10));

        clob.free();

        try {
            clob.getSubString(1, 10);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCharacterStream() throws Exception {
        Reader cStream = clob.getCharacterStream();
        String res = IOUtils.toString(cStream);
        assertEquals("1234567890", res);

        clob.free();

        try {
            clob.getCharacterStream();

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetCharacterStream1() throws Exception {
        try {
            clob.getCharacterStream(0, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.getCharacterStream(1, -1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.getCharacterStream(1, 11);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

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

        clob.free();

        try {
            clob.getCharacterStream(1, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAsciiStream() throws Exception {
        byte[] bytes = IOUtils.toByteArray(clob.getAsciiStream());

        Assert.assertArrayEquals("1234567890".getBytes(), bytes);

        clob.free();

        try {
            clob.getAsciiStream();

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPosition() throws Exception {
        try {
            clob.position("0", 0);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        assertEquals(-1, clob.position("a", 11));

        assertEquals(1, clob.position("1", 1));

        assertEquals(5, clob.position("56", 1));

        assertEquals(5, clob.position("56", 5));

        assertEquals(-1, clob.position("56", 6));

        clob = new JdbcClob("abbabab");

        assertEquals(5, clob.position("b", 4));

        clob.free();

        try {
            clob.position("1", 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPosition1() throws Exception {
        JdbcClob anotherClob = new JdbcClob("567");

        try {
            clob.position(anotherClob, 0);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        assertEquals(5, clob.position(anotherClob, 1));

        assertEquals(5, clob.position(anotherClob, 5));

        assertEquals(-1, clob.position(anotherClob, 6));

        anotherClob = new JdbcClob("a");

        assertEquals(-1, clob.position(anotherClob, 1));

        clob = new JdbcClob("bbabbabba");

        assertEquals(6, clob.position(anotherClob, 5));

        clob.free();

        try {
            clob.position(anotherClob, 5);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetString() throws Exception {
        try {
            clob.setString(0, "a");

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.setString(clob.length() + 2, "a");

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.setString(1, null);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        clob.setString(1, "a");
        assertEquals("a", clob.getSubString(1, 1));

        clob.setString(5, "abc");
        assertEquals("abc", clob.getSubString(5, 3));

        clob.setString(10, "def");
        assertEquals("def", clob.getSubString(10, 3));

        clob = new JdbcClob("12345");
        clob.setString(3, "abcd");
        assertEquals("12abcd", clob.getSubString(1, (int)clob.length()));

        clob = new JdbcClob("12345");
        clob.setString(3, "ab");
        assertEquals("12ab5", clob.getSubString(1, (int)clob.length()));

        clob.free();

        try {
            clob.setString(1, "a");

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetString1() throws Exception {
        try {
            clob.setString(0, "a", 0, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.setString(clob.length() + 1, "a", 0, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.setString(1, null, 0, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.setString(1, "a", -1, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.setString(1, "a", 0, -1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.setString(1, "abc", 1, 3);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        clob = new JdbcClob("1234567890");
        clob.setString(3, "abcd", 0, 1);
        assertEquals("12a4567890", clob.getSubString(1, (int)clob.length()));

        clob = new JdbcClob("1234567890");
        clob.setString(1, "abcd", 0, 3);
        assertEquals("abc4567890", clob.getSubString(1, (int)clob.length()));

        clob = new JdbcClob("1234567890");
        clob.setString(5, "abcd", 2, 2);
        assertEquals("1234cd7890", clob.getSubString(1, (int)clob.length()));

        clob = new JdbcClob("1234567890");
        clob.setString(9, "abcd", 0, 4);
        assertEquals("12345678abcd", clob.getSubString(1, (int)clob.length()));

        clob.free();

        try {
            clob.setString(1, "a", 0, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTruncate() throws Exception {
        try {
            clob.truncate(-1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.truncate(clob.length() + 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        clob.truncate(9);
        assertEquals("123456789", clob.getSubString(1, (int)clob.length()));

        clob.truncate(5);
        assertEquals("12345", clob.getSubString(1, (int)clob.length()));

        clob.truncate(0);
        assertEquals("", clob.getSubString(1, (int)clob.length()));

        clob.free();

        try {
            clob.truncate(1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetAsciiStream() throws Exception {
        try {
            clob.setAsciiStream(1L);

            fail();
        }
        catch (SQLFeatureNotSupportedException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetCharacterStream() throws Exception {
        try {
            clob.setCharacterStream(1L);

            fail();
        }
        catch (SQLFeatureNotSupportedException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFree() throws Exception {
        clob.length();

        clob.getCharacterStream();

        clob.free();

        clob.free();

        try {
            clob.length();

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            clob.getCharacterStream();

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }
}