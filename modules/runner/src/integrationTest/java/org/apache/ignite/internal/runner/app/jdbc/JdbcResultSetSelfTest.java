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

package org.apache.ignite.internal.runner.app.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import org.apache.ignite.internal.tostring.S;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Result set test.
 */
public class JdbcResultSetSelfTest extends AbstractJdbcSelfTest {
    /** SQL query. */
    private static final String SQL =
        "SELECT 1::INTEGER, true, 1::TINYINT, 1::SMALLINT, 1::INTEGER, 1::BIGINT, 1.0::FLOAT, 1.0::DOUBLE, 1.0::DOUBLE, " +
            "'1', '1', '1901-02-01'::DATE, '01:01:01'::TIME, 1::TIMESTAMP;";

    /** Statement. */
    private Statement stmt;

    /**
     * Create the connection ant statement.
     *
     * @throws Exception if failed.
     */
    @BeforeEach
    public void beforeTest() throws Exception {
        Connection conn = DriverManager.getConnection(URL);

        stmt = conn.createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    /**
     * Close the connection and statement.
     *
     * @throws Exception if failed.
     */
    @AfterEach
    public void afterTest() throws Exception {
        if (stmt != null) {
            stmt.getConnection().close();

            stmt.close();

            assertTrue(stmt.isClosed());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
//                assert rs.getBoolean("boolVal");
                assertTrue(rs.getBoolean(2));
                assertEquals(1, rs.getByte(2));
                assertEquals(1, rs.getInt(2));
                assertEquals(1, rs.getShort(2));
                assertEquals(1, rs.getLong(2));
                assertEquals(1.0, rs.getDouble(2));
                assertEquals(1.0f, rs.getFloat(2));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(2));
                assertEquals(rs.getString(2), "true");

                assertTrue(rs.getObject(2, Boolean.class));
                assertEquals((byte)1, rs.getObject(2, Byte.class));
                assertEquals((short)1, rs.getObject(2, Short.class));
                assertEquals( 1, rs.getObject(2, Integer.class));
                assertEquals( 1, rs.getObject(2, Long.class));
                assertEquals( 1.0f, rs.getObject(2, Float.class));
                assertEquals( 1.0, rs.getObject(2, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(2, BigDecimal.class));
                assertEquals("true", rs.getObject(2, String.class));
            }

            cnt++;
        }

        assertEquals(1, cnt);

        ResultSet rs0 = stmt.executeQuery("select 1");

        assertTrue(rs0.next());
        assertTrue(rs0.getBoolean(1));

        rs0 = stmt.executeQuery("select 0");

        assertTrue(rs0.next());
        assertFalse(rs0.getBoolean(1));

        rs0 = stmt.executeQuery("select '1'");

        assertTrue(rs0.next());
        assertTrue(rs0.getBoolean(1));

        rs0 = stmt.executeQuery("select '0'");

        assertTrue(rs0.next());
        assertFalse(rs0.getBoolean(1));

        assertThrows(SQLException.class, () -> {
            ResultSet badRs = stmt.executeQuery("select ''");

            assertTrue(badRs.next());
            assertTrue(badRs.getBoolean(1));
        }, "Cannot convert to boolean: ");

        assertThrows(SQLException.class, () -> {
            ResultSet badRs = stmt.executeQuery("select 'qwe'");

            assertTrue(badRs.next());
            assertTrue(badRs.getBoolean(1));
        }, "Cannot convert to boolean: qwe");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testByte() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
//                assert rs.getByte("byteVal") == 1;

                assertTrue(rs.getBoolean(3));
                assertEquals(1, rs.getByte(3));
                assertEquals(1, rs.getInt(3));
                assertEquals(1, rs.getShort(3));
                assertEquals(1, rs.getLong(3));
                assertEquals(1.0, rs.getDouble(3));
                assertEquals(1.0f, rs.getFloat(3));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(3));
                assertEquals(rs.getString(3), "1");

                assertTrue(rs.getObject(3, Boolean.class));
                assertEquals((byte)1, rs.getObject(3, Byte.class));
                assertEquals((short)1, rs.getObject(3, Short.class));
                assertEquals(1, rs.getObject(3, Integer.class));
                assertEquals(1, rs.getObject(3, Long.class));
                assertEquals(1.f, rs.getObject(3, Float.class));
                assertEquals(1, rs.getObject(3, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(3, BigDecimal.class));
                assertArrayEquals(new byte[] {1}, rs.getBytes(3));
                assertEquals(rs.getObject(3, String.class), "1");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShort() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
//                assert rs.getShort("shortVal") == 1;

                assertTrue(rs.getBoolean(4));
                assertEquals(1, rs.getByte(4));
                assertEquals(1, rs.getShort(4));
                assertEquals(1, rs.getInt(4));
                assertEquals(1, rs.getLong(4));
                assertEquals(1.0, rs.getDouble(4));
                assertEquals(1.0f, rs.getFloat(4));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(4));
                assertEquals(rs.getString(4), "1");

                assertTrue(rs.getObject(4, Boolean.class));
                assertEquals((byte)1, rs.getObject(4, Byte.class));
                assertEquals((short)1, rs.getObject(4, Short.class));
                assertEquals(1, rs.getObject(4, Integer.class));
                assertEquals(1, rs.getObject(4, Long.class));
                assertEquals(1.f, rs.getObject(4, Float.class));
                assertEquals(1, rs.getObject(4, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(4, BigDecimal.class));
                assertArrayEquals(new byte[] {0, 1}, rs.getBytes(4));
                assertEquals(rs.getObject(4, String.class), "1");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInteger() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
//                assert rs.getInt("intVal") == 1;

                assertTrue(rs.getBoolean(5));
                assertEquals(1, rs.getByte(5));
                assertEquals(1, rs.getShort(5));
                assertEquals(1, rs.getInt(5));
                assertEquals(1, rs.getLong(5));
                assertEquals(1.0, rs.getDouble(5));
                assertEquals(1.0f, rs.getFloat(5));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(5));
                assertEquals(rs.getString(5), "1");

                assertTrue(rs.getObject(5, Boolean.class));
                assertEquals((byte)1, rs.getObject(5, Byte.class));
                assertEquals((short)1, rs.getObject(5, Short.class));
                assertEquals(1, rs.getObject(5, Integer.class));
                assertEquals(1, rs.getObject(5, Long.class));
                assertEquals(1.f, rs.getObject(5, Float.class));
                assertEquals(1, rs.getObject(5, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(5, BigDecimal.class));
                assertArrayEquals(new byte[] {0, 0, 0, 1}, rs.getBytes(5));
                assertEquals(rs.getObject(5, String.class), "1");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLong() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
//                assert rs.getLong("longVal") == 1;

                assertTrue(rs.getBoolean(6));
                assertEquals(1, rs.getByte(6));
                assertEquals(1, rs.getShort(6));
                assertEquals(1, rs.getInt(6));
                assertEquals(1, rs.getLong(6));
                assertEquals(1.0, rs.getDouble(6));
                assertEquals(1.0f, rs.getFloat(6));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(6));
                assertEquals(rs.getString(6), "1");

                assertTrue(rs.getObject(6, Boolean.class));
                assertEquals((byte)1, rs.getObject(6, Byte.class));
                assertEquals((short)1, rs.getObject(6, Short.class));
                assertEquals(1, rs.getObject(6, Integer.class));
                assertEquals(1, rs.getObject(6, Long.class));
                assertEquals(1.f, rs.getObject(6, Float.class));
                assertEquals(1, rs.getObject(6, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(6, BigDecimal.class));
                assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}, rs.getBytes(6));
                assertEquals(rs.getObject(6, String.class), "1");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloat() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
//                assert rs.getFloat("floatVal") == 1.0;

                assertTrue(rs.getBoolean(7));
                assertEquals(1, rs.getByte(7));
                assertEquals(1, rs.getShort(7));
                assertEquals(1, rs.getInt(7));
                assertEquals(1, rs.getLong(7));
                assertEquals(1.0, rs.getDouble(7));
                assertEquals(1.0f, rs.getFloat(7));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(7));
                assertEquals(rs.getString(7), "1.0");

                assertTrue(rs.getObject(7, Boolean.class));
                assertEquals((byte)1, rs.getObject(7, Byte.class));
                assertEquals((short)1, rs.getObject(7, Short.class));
                assertEquals(1, rs.getObject(7, Integer.class));
                assertEquals(1, rs.getObject(7, Long.class));
                assertEquals(1.f, rs.getObject(7, Float.class));
                assertEquals(1, rs.getObject(7, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(7, BigDecimal.class));
                assertArrayEquals(new byte[] {63, -128, 0, 0}, rs.getBytes(7));
                assertEquals(rs.getObject(7, String.class), "1.0");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDouble() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
//                assert rs.getDouble("doubleVal") == 1.0;

                assertTrue(rs.getBoolean(8));
                assertEquals(1, rs.getByte(8));
                assertEquals(1, rs.getShort(8));
                assertEquals(1, rs.getInt(8));
                assertEquals(1, rs.getLong(8));
                assertEquals(1.0, rs.getDouble(8));
                assertEquals(1.0f, rs.getFloat(8));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(8));
                assertEquals(rs.getString(8), "1.0");

                assertTrue(rs.getObject(8, Boolean.class));
                assertEquals((byte)1, rs.getObject(8, Byte.class));
                assertEquals((short)1, rs.getObject(8, Short.class));
                assertEquals(1, rs.getObject(8, Integer.class));
                assertEquals(1, rs.getObject(8, Long.class));
                assertEquals(1.f, rs.getObject(8, Float.class));
                assertEquals(1, rs.getObject(8, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(8, BigDecimal.class));
                assertArrayEquals(new byte[] {63, -16, 0, 0, 0, 0, 0, 0}, rs.getBytes(8));
                assertEquals(rs.getObject(8, String.class), "1.0");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testBigDecimal() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assertEquals(1, rs.getBigDecimal("bigVal").intValue());

                assertTrue(rs.getBoolean(9));
                assertEquals(1, rs.getByte(9));
                assertEquals(1, rs.getShort(9));
                assertEquals(1, rs.getInt(9));
                assertEquals(1, rs.getLong(9));
                assertEquals(1.0, rs.getDouble(9));
                assertEquals(1.0f, rs.getFloat(9));
                assertEquals(new BigDecimal(1), rs.getBigDecimal(9));
                assertEquals(rs.getString(9), "1");

                assertTrue(rs.getObject(9, Boolean.class));
                assertEquals((byte)1, rs.getObject(9, Byte.class));
                assertEquals((short)1, rs.getObject(9, Short.class));
                assertEquals(1, rs.getObject(9, Integer.class));
                assertEquals(1, rs.getObject(9, Long.class));
                assertEquals(1.f, rs.getObject(9, Float.class));
                assertEquals(1, rs.getObject(9, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(9, BigDecimal.class));
                assertEquals(rs.getObject(9, String.class), "1");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * TODO: IGNITE-15163
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testBigDecimalScale() throws Exception {
        assertEquals(convertStringToBigDecimalViaJdbc("0.1234", 2).toString(), "0.12");
        assertEquals(convertStringToBigDecimalViaJdbc("1.0005", 3).toString(), "1.001");
        assertEquals(convertStringToBigDecimalViaJdbc("1205.5", -3).toString(), "1E+3");
        assertEquals(convertStringToBigDecimalViaJdbc("12505.5", -3).toString(), "1.3E+4");
    }

    /**
     * @param strDec String representation of a decimal value.
     * @param scale Scale.
     * @return BigDecimal object.
     * @throws SQLException On error.
     */
    private BigDecimal convertStringToBigDecimalViaJdbc(String strDec, int scale) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select '" + strDec + "'")) {
            assertTrue(rs.next());

            return rs.getBigDecimal(1, scale);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testString() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                //TODO IGNITE-15187
                //assert "1".equals(rs.getString("strVal"));

                assertTrue(rs.getBoolean(10));
                assertEquals(1, rs.getByte(10));
                assertEquals(1, rs.getShort(10));
                assertEquals(1, rs.getInt(10));
                assertEquals(1, rs.getLong(10));
                assertEquals(1.0, rs.getDouble(10));
                assertEquals(1.0f, rs.getFloat(10));
                assertEquals(new BigDecimal("1"), rs.getBigDecimal(10));
                assertEquals(rs.getString(10), "1");

                assertTrue(rs.getObject(10, Boolean.class));
                assertEquals((byte)1, rs.getObject(10, Byte.class));
                assertEquals((short)1, rs.getObject(10, Short.class));
                assertEquals(1, rs.getObject(10, Integer.class));
                assertEquals(1, rs.getObject(10, Long.class));
                assertEquals(1.f, rs.getObject(10, Float.class));
                assertEquals(1, rs.getObject(10, Double.class));
                assertEquals(new BigDecimal(1), rs.getObject(10, BigDecimal.class));
                assertArrayEquals(new byte[] {49}, rs.getBytes(10));
                assertEquals(rs.getObject(10, String.class), "1");
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * TODO: IGNITE-15163
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testArray() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
                assertArrayEquals(new byte[] {1}, rs.getBytes("arrVal"));
                assertArrayEquals(new byte[] {1}, rs.getBytes(11));
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * TODO: IGNITE-15163
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testDate() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
//                assert rs.getDate("dateVal").equals(new Date(1, 1, 1));

                assertEquals(new Date(1, 1, 1), rs.getDate(12));
                assertEquals(new Time(new Date(1, 1, 1).getTime()), rs.getTime(12));
                assertEquals(new Timestamp(new Date(1, 1, 1).getTime()), rs.getTimestamp(12));

                assertEquals(new Date(1, 1, 1), rs.getObject(12, Date.class));
                assertEquals(new Time(new Date(1, 1, 1).getTime()), rs.getObject(12, Time.class));
                assertEquals(new Timestamp(new Date(1, 1, 1).getTime()), rs.getObject(12, Timestamp.class));
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * TODO: IGNITE-15163
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testTime() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
//                assert rs.getTime("timeVal").equals(new Time(1, 1, 1));

                assertEquals(new Date(new Time(1, 1, 1).getTime()), rs.getDate(13));
                assertEquals(new Time(1, 1, 1), rs.getTime(13));
                assertEquals(new Timestamp(new Time(1, 1, 1).getTime()), rs.getTimestamp(13));

                assertEquals(new Date(new Time(1, 1, 1).getTime()), rs.getObject(13, Date.class));
                assertEquals(new Time(1, 1, 1), rs.getObject(13, Time.class));
                assertEquals(new Timestamp(new Time(1, 1, 1).getTime()), rs.getObject(13, Timestamp.class));
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * TODO: IGNITE-15163
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTimestamp() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0) {
//                assert rs.getTimestamp("tsVal").getTime() == 1;
                assertEquals(new Date(new Timestamp(-10800000).getTime()), rs.getDate(14));
                assertEquals(new Time(new Timestamp(-10800000).getTime()), rs.getTime(14));
                assertEquals(new Timestamp(-10800000), rs.getTimestamp(14));

                assertEquals(new Date(new Timestamp(-10800000).getTime()), rs.getObject(14, Date.class));
                assertEquals(new Time(new Timestamp(-10800000).getTime()), rs.getObject(14, Time.class));
                assertEquals(new Timestamp(-10800000), rs.getObject(14, Timestamp.class));
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * TODO Enable when sql engine will be fully integrated.
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testObject() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        int cnt = 0;

        TestObjectField exp = new TestObjectField(100, "AAAA");

        while (rs.next()) {
            if (cnt == 0) {
                assertEquals(exp, rs.getObject("objVal"));

                assertEquals(exp, rs.getObject(15));

                assertEquals(exp, rs.getObject(15, Object.class));

                assertEquals(exp, rs.getObject(15, TestObjectField.class));
            }

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * TODO Enable when sql engine will be fully integrated.
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testNavigation() throws Exception {
        ResultSet rs = stmt.executeQuery("select id from TestObject where id > 0");

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(0, rs.getRow());

        assertTrue(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(1, rs.getRow());

        assertTrue(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertTrue(rs.isLast());
        assertEquals(2, rs.getRow());

        assertFalse(rs.next());

        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertEquals(0, rs.getRow());

        rs = stmt.executeQuery("select id from TestObject where id < 0");

        assertFalse(rs.isBeforeFirst());
    }

    /**
     * TODO IGNITE-15187
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testFindColumn() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        assertNotNull(rs);
        assertTrue(rs.next());

        assertEquals(1, rs.findColumn("id"));

        assertThrows(SQLException.class, () -> rs.findColumn("wrong"), "Column not found: wrong");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotSupportedTypes() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        assertTrue(rs.next());

        checkNotSupported(() -> rs.getArray(1));

        checkNotSupported(() -> rs.getArray("id"));
        
        checkNotSupported(() -> rs.getAsciiStream(1));

        checkNotSupported(() -> rs.getAsciiStream("id"));
        
        checkNotSupported(() -> rs.getBinaryStream(1));
        
        checkNotSupported(() -> rs.getBinaryStream("id"));

        checkNotSupported(() -> rs.getBlob(1));

        checkNotSupported(() -> rs.getBlob("id"));

        checkNotSupported(() -> rs.getClob(1));

        checkNotSupported(() -> rs.getClob("id"));

        checkNotSupported(() -> rs.getCharacterStream(1));

        checkNotSupported(() -> rs.getCharacterStream("id"));

        checkNotSupported(() -> rs.getNCharacterStream(1));

        checkNotSupported(() -> rs.getNCharacterStream("id"));

        checkNotSupported(() -> rs.getNClob(1));

        checkNotSupported(() -> rs.getNClob("id"));

        checkNotSupported(() -> rs.getRef(1));

        checkNotSupported(() -> rs.getRef("id"));

        checkNotSupported(() -> rs.getRowId(1));

        checkNotSupported(() -> rs.getRowId("id"));

        checkNotSupported(() -> rs.getSQLXML(1));

        checkNotSupported(() -> rs.getSQLXML("id"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateNotSupported() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        assertTrue(rs.next());

        checkNotSupported(() -> rs.updateBoolean(1, true));

        checkNotSupported(() -> rs.updateBoolean("id", true));

        checkNotSupported(() -> rs.updateByte(1, (byte)0));

        checkNotSupported(() -> rs.updateByte("id", (byte)0));

        checkNotSupported(() -> rs.updateShort(1, (short)0));

        checkNotSupported(() -> rs.updateShort("id", (short)0));

        checkNotSupported(() -> rs.updateInt(1, 0));

        checkNotSupported(() -> rs.updateInt("id", 0));

        checkNotSupported(() -> rs.updateLong(1, 0));

        checkNotSupported(() -> rs.updateLong("id", 0));

        checkNotSupported(() -> rs.updateFloat(1, (float)0.0));

        checkNotSupported(() -> rs.updateFloat("id", (float)0.0));

        checkNotSupported(() -> rs.updateDouble(1, 0.0));

        checkNotSupported(() -> rs.updateDouble("id", 0.0));

        checkNotSupported(() -> rs.updateString(1, ""));

        checkNotSupported(() -> rs.updateString("id", ""));

        checkNotSupported(() -> rs.updateTime(1, new Time(0)));

        checkNotSupported(() -> rs.updateTime("id", new Time(0)));

        checkNotSupported(() -> rs.updateDate(1, new Date(0)));

        checkNotSupported(() -> rs.updateDate("id", new Date(0)));

        checkNotSupported(() -> rs.updateTimestamp(1, new Timestamp(0)));

        checkNotSupported(() -> rs.updateTimestamp("id", new Timestamp(0)));

        checkNotSupported(() -> rs.updateBytes(1, new byte[]{}));

        checkNotSupported(() -> rs.updateBytes("id", new byte[]{}));

        checkNotSupported(() -> rs.updateArray(1, null));

        checkNotSupported(() -> rs.updateArray("id", null));

        checkNotSupported(() -> rs.updateBlob(1, (Blob)null));

        checkNotSupported(() -> rs.updateBlob(1, (InputStream)null));

        checkNotSupported(() -> rs.updateBlob(1, null, 0L));

        checkNotSupported(() -> rs.updateBlob("id", (Blob)null));

        checkNotSupported(() -> rs.updateBlob("id", (InputStream)null));

        checkNotSupported(() -> rs.updateBlob("id", null, 0L));

        checkNotSupported(() -> rs.updateClob(1, (Clob)null));

        checkNotSupported(() -> rs.updateClob(1, (Reader)null));

        checkNotSupported(() -> rs.updateClob(1, null, 0L));

        checkNotSupported(() -> rs.updateClob("id", (Clob)null));

        checkNotSupported(() -> rs.updateClob("id", (Reader)null));

        checkNotSupported(() -> rs.updateClob("id", null, 0L));

        checkNotSupported(() -> rs.updateNClob(1, (NClob)null));

        checkNotSupported(() -> rs.updateNClob(1, (Reader)null));

        checkNotSupported(() -> rs.updateNClob(1, null, 0L));

        checkNotSupported(() -> rs.updateNClob("id", (NClob)null));

        checkNotSupported(() -> rs.updateNClob("id", (Reader)null));

        checkNotSupported(() -> rs.updateNClob("id", null, 0L));

        checkNotSupported(() -> rs.updateAsciiStream(1, null));

        checkNotSupported(() -> rs.updateAsciiStream(1, null, 0));

        checkNotSupported(() -> rs.updateAsciiStream(1, null, 0L));

        checkNotSupported(() -> rs.updateAsciiStream("id", null));

        checkNotSupported(() -> rs.updateAsciiStream("id", null, 0));

        checkNotSupported(() -> rs.updateAsciiStream("id", null, 0L));

        checkNotSupported(() -> rs.updateCharacterStream(1, null));

        checkNotSupported(() -> rs.updateCharacterStream(1, null, 0));

        checkNotSupported(() -> rs.updateCharacterStream(1, null, 0L));

        checkNotSupported(() -> rs.updateCharacterStream("id", null));

        checkNotSupported(() -> rs.updateCharacterStream("id", null, 0));

        checkNotSupported(() -> rs.updateCharacterStream("id", null, 0L));

        checkNotSupported(() -> rs.updateNCharacterStream(1, null));

        checkNotSupported(() -> rs.updateNCharacterStream(1, null, 0));

        checkNotSupported(() -> rs.updateNCharacterStream(1, null, 0L));

        checkNotSupported(() -> rs.updateNCharacterStream("id", null));

        checkNotSupported(() -> rs.updateNCharacterStream("id", null, 0));

        checkNotSupported(() -> rs.updateNCharacterStream("id", null, 0L));

        checkNotSupported(() -> rs.updateRef(1, null));

        checkNotSupported(() -> rs.updateRef("id", null));

        checkNotSupported(() -> rs.updateRowId(1, null));

        checkNotSupported(() -> rs.updateRowId("id", null));

        checkNotSupported(() -> rs.updateNString(1, null));

        checkNotSupported(() -> rs.updateNString("id", null));

        checkNotSupported(() -> rs.updateSQLXML(1, null));

        checkNotSupported(() -> rs.updateSQLXML("id", null));

        checkNotSupported(() -> rs.updateObject(1, null));

        checkNotSupported(() -> rs.updateObject(1, null, 0));

        checkNotSupported(() -> rs.updateObject("id", null));

        checkNotSupported(() -> rs.updateObject("id", null, 0));

        checkNotSupported(() -> rs.updateBigDecimal(1, null));

        checkNotSupported(() -> rs.updateBigDecimal("id", null));

        checkNotSupported(() -> rs.updateNull(1));

        checkNotSupported(() -> rs.updateNull("id"));

        checkNotSupported(rs::cancelRowUpdates);

        checkNotSupported(rs::updateRow);

        checkNotSupported(rs::deleteRow);

        checkNotSupported(rs::insertRow);

        checkNotSupported(rs::moveToInsertRow);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnClosedResultSet() throws Exception {
        final ResultSet rs = stmt.executeQuery(SQL);

        rs.close();

        // Must do nothing on closed result set
        rs.close();

        checkResultSetClosed(() -> rs.getBoolean(1));

        checkResultSetClosed(() -> rs.getBoolean("id"));

        checkResultSetClosed(() -> rs.getByte(1));

        checkResultSetClosed(() -> rs.getByte("id"));

        checkResultSetClosed(() -> rs.getShort(1));

        checkResultSetClosed(() -> rs.getShort("id"));

        checkResultSetClosed(() -> rs.getInt(1));

        checkResultSetClosed(() -> rs.getInt("id"));

        checkResultSetClosed(() -> rs.getLong(1));

        checkResultSetClosed(() -> rs.getLong("id"));

        checkResultSetClosed(() -> rs.getFloat(1));

        checkResultSetClosed(() -> rs.getFloat("id"));

        checkResultSetClosed(() -> rs.getDouble(1));

        checkResultSetClosed(() -> rs.getDouble("id"));

        checkResultSetClosed(() -> rs.getString(1));

        checkResultSetClosed(() -> rs.getString("id"));

        checkResultSetClosed(() -> rs.getBytes(1));

        checkResultSetClosed(() -> rs.getBytes("id"));

        checkResultSetClosed(() -> rs.getDate(1));

        checkResultSetClosed(() -> rs.getDate(1, new GregorianCalendar()));

        checkResultSetClosed(() -> rs.getDate("id"));

        checkResultSetClosed(() -> rs.getDate("id", new GregorianCalendar()));

        checkResultSetClosed(() -> rs.getTime(1));

        checkResultSetClosed(() -> rs.getTime(1, new GregorianCalendar()));

        checkResultSetClosed(() -> rs.getTime("id"));

        checkResultSetClosed(() -> rs.getTime("id", new GregorianCalendar()));

        checkResultSetClosed(() -> rs.getTimestamp(1));

        checkResultSetClosed(() -> rs.getTimestamp(1, new GregorianCalendar()));

        checkResultSetClosed(() -> rs.getTimestamp("id"));

        checkResultSetClosed(() -> rs.getTimestamp("id", new GregorianCalendar()));

        checkResultSetClosed(() -> rs.getObject("objVal"));

        checkResultSetClosed(() -> rs.getObject("objVal", TestObjectField.class));

        checkResultSetClosed(rs::wasNull);

        checkResultSetClosed(rs::getMetaData);

        checkResultSetClosed(rs::next);

        checkResultSetClosed(rs::last);

        checkResultSetClosed(rs::afterLast);

        checkResultSetClosed(rs::beforeFirst);

        checkResultSetClosed(rs::first);

        checkResultSetClosed(() -> rs.findColumn("id"));

        checkResultSetClosed(rs::getRow);
    }

    /**
     * Test object field.
     */
    @SuppressWarnings("PackageVisibleField")
    private static class TestObjectField implements Serializable {
        /** */
        final int a;

        /** */
        final String b;

        /**
         * @param a A.
         * @param b B.
         */
        private TestObjectField(int a, String b) {
            this.a = a;
            this.b = b;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestObjectField that = (TestObjectField)o;

            return a == that.a && !(b != null ? !b.equals(that.b) : that.b != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = a;

            res = 31 * res + (b != null ? b.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestObjectField.class, this);
        }
    }
}
