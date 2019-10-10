/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests a custom BigDecimal implementation, as well
 * as direct modification of a byte in a byte array.
 */
public class TestZloty extends TestBase {

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
        testZloty();
        testModifyBytes();
        deleteDb("zloty");
    }

    /**
     * This class overrides BigDecimal and implements some strange comparison
     * method.
     */
    private static class ZlotyBigDecimal extends BigDecimal {

        private static final long serialVersionUID = 1L;

        public ZlotyBigDecimal(String s) {
            super(s);
        }

        @Override
        public int compareTo(BigDecimal bd) {
            return -super.compareTo(bd);
        }

    }

    private void testModifyBytes() throws SQLException {
        deleteDb("zloty");
        Connection conn = getConnection("zloty");
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT, DATA BINARY)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        byte[] shared = { 0 };
        prep.setInt(1, 0);
        prep.setBytes(2, shared);
        prep.execute();
        shared[0] = 1;
        prep.setInt(1, 1);
        prep.setBytes(2, shared);
        prep.execute();
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals(0, rs.getBytes(2)[0]);
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getBytes(2)[0]);
        rs.getBytes(2)[0] = 2;
        assertEquals(1, rs.getBytes(2)[0]);
        assertFalse(rs.next());
        conn.close();
    }

    /**
     * H2 destroyer application ;->
     *
     * @author Maciej Wegorkiewicz
     */
    private void testZloty() throws SQLException {
        deleteDb("zloty");
        Connection conn = getConnection("zloty");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT, AMOUNT DECIMAL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        prep.setInt(1, 1);
        prep.setBigDecimal(2, new BigDecimal("10.0"));
        prep.execute();
        prep.setInt(1, 2);
        assertThrows(ErrorCode.INVALID_CLASS_2, prep).
                setBigDecimal(2, new ZlotyBigDecimal("11.0"));

        prep.setInt(1, 3);
        BigDecimal value = new BigDecimal("12.100000") {

            private static final long serialVersionUID = 1L;

            @Override
            public String toString() {
                return "12,100000 EURO";
            }
        };
        assertThrows(ErrorCode.INVALID_CLASS_2, prep).
                setBigDecimal(2, value);

        conn.close();
    }

}
