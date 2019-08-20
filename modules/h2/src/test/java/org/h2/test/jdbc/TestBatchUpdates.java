/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Test for batch updates.
 */
public class TestBatchUpdates extends TestBase {

    private static final String COFFEE_UPDATE =
            "UPDATE TEST SET PRICE=PRICE*20 WHERE TYPE_ID=?";
    private static final String COFFEE_SELECT =
            "SELECT PRICE FROM TEST WHERE KEY_ID=?";
    // private static final String COFFEE_QUERY =
    //  "SELECT C_NAME,PRICE FROM TEST WHERE TYPE_ID=?";
    // private static final String COFFEE_DELETE =
    //  "DELETE FROM TEST WHERE KEY_ID=?";
    private static final String COFFEE_INSERT1 =
            "INSERT INTO TEST VALUES(9,'COFFEE-9',9.0,5)";
    private static final String COFFEE_DELETE1 =
            "DELETE FROM TEST WHERE KEY_ID=9";
    private static final String COFFEE_UPDATE1 =
            "UPDATE TEST SET PRICE=PRICE*20 WHERE TYPE_ID=1";
    private static final String COFFEE_SELECT1 =
            "SELECT PRICE FROM TEST WHERE KEY_ID>4";
    private static final String COFFEE_UPDATE_SET =
            "UPDATE TEST SET KEY_ID=?, C_NAME=? WHERE C_NAME=?";
    private static final String COFFEE_SELECT_CONTINUED =
            "SELECT COUNT(*) FROM TEST WHERE C_NAME='Continue-1'";

    private static final int COFFEE_SIZE = 10;
    private static final int COFFEE_TYPE = 11;

    private Connection conn;
    private Statement stat;
    private PreparedStatement prep;

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
        testRootCause();
        testExecuteCall();
        testException();
        testCoffee();
        deleteDb("batchUpdates");
    }

    private void testRootCause() throws SQLException {
        deleteDb("batchUpdates");
        conn = getConnection("batchUpdates");
        stat = conn.createStatement();
        stat.addBatch("select * from test_x");
        stat.addBatch("select * from test_y");
        try {
            stat.executeBatch();
        } catch (SQLException e) {
            assertContains(e.toString(), "TEST_Y");
            e = e.getNextException();
            assertTrue(e != null);
            assertContains(e.toString(), "TEST_Y");
            e = e.getNextException();
            assertTrue(e != null);
            assertContains(e.toString(), "TEST_X");
            e = e.getNextException();
            assertTrue(e == null);
        }
        stat.execute("create table test(id int)");
        PreparedStatement prep = conn.prepareStatement("insert into test values(?)");
        prep.setString(1, "TEST_X");
        prep.addBatch();
        prep.setString(1, "TEST_Y");
        prep.addBatch();
        try {
            prep.executeBatch();
        } catch (SQLException e) {
            assertContains(e.toString(), "TEST_Y");
            e = e.getNextException();
            assertTrue(e != null);
            assertContains(e.toString(), "TEST_Y");
            e = e.getNextException();
            assertTrue(e != null);
            assertContains(e.toString(), "TEST_X");
            e = e.getNextException();
            assertTrue(e == null);
        }
        stat.execute("drop table test");
        conn.close();
    }

    private void testExecuteCall() throws SQLException {
        deleteDb("batchUpdates");
        conn = getConnection("batchUpdates");
        stat = conn.createStatement();
        stat.execute("CREATE ALIAS updatePrices FOR \"" +
                getClass().getName() + ".updatePrices\"");
        CallableStatement call = conn.prepareCall("{call updatePrices(?, ?)}");
        call.setString(1, "Hello");
        call.setFloat(2, 1.4f);
        call.addBatch();
        call.setString(1, "World");
        call.setFloat(2, 3.2f);
        call.addBatch();
        int[] updateCounts = call.executeBatch();
        int total = 0;
        for (int t : updateCounts) {
            total += t;
        }
        assertEquals(4, total);
        conn.close();
    }

    /**
     * This method is called by the database.
     *
     * @param message the message (currently not used)
     * @param f the float
     * @return the float converted to an int
     */
    public static int updatePrices(@SuppressWarnings("unused") String message, double f) {
        return (int) f;
    }

    private void testException() throws SQLException {
        deleteDb("batchUpdates");
        conn = getConnection("batchUpdates");
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        prep = conn.prepareStatement("insert into test values(?)");
        for (int i = 0; i < 700; i++) {
            prep.setString(1, "x");
            prep.addBatch();
        }
        try {
            prep.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            // expected
        }
        conn.close();
    }

    private void testCoffee() throws SQLException {
        deleteDb("batchUpdates");
        conn = getConnection("batchUpdates");
        stat = conn.createStatement();
        DatabaseMetaData meta = conn.getMetaData();
        assertTrue(meta.supportsBatchUpdates());
        stat.executeUpdate("CREATE TABLE TEST(KEY_ID INT PRIMARY KEY,"
                + "C_NAME VARCHAR(255),PRICE DECIMAL(20,2),TYPE_ID INT)");
        String newName = null;
        float newPrice = 0;
        int newType = 0;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?,?,?,?)");
        int newKey = 1;
        for (int i = 1; i <= COFFEE_TYPE && newKey <= COFFEE_SIZE; i++) {
            for (int j = 1; j <= i && newKey <= COFFEE_SIZE; j++) {
                newName = "COFFEE-" + newKey;
                newPrice = newKey + (float) .00;
                newType = i;
                prep.setInt(1, newKey);
                prep.setString(2, newName);
                prep.setFloat(3, newPrice);
                prep.setInt(4, newType);
                prep.execute();
                newKey = newKey + 1;
            }
        }
        trace("Inserted the Rows ");
        testAddBatch01();
        testAddBatch02();
        testClearBatch01();
        testClearBatch02();
        testExecuteBatch01();
        testExecuteBatch02();
        testExecuteBatch03();
        testExecuteBatch04();
        testExecuteBatch05();
        testExecuteBatch06();
        testExecuteBatch07();
        testContinueBatch01();

        conn.close();
    }

    private void testAddBatch01() throws SQLException {
        trace("testAddBatch01");
        int i = 0;
        int[] retValue = { 0, 0, 0 };
        String s = COFFEE_UPDATE;
        trace("Prepared Statement String:" + s);
        prep = conn.prepareStatement(s);
        assertThrows(ErrorCode.PARAMETER_NOT_SET_1, prep).addBatch();
        prep.setInt(1, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.addBatch();
        prep.setInt(1, 4);
        prep.addBatch();
        int[] updateCount = prep.executeBatch();
        int updateCountLen = updateCount.length;

        // PreparedStatement p;
        // p = conn.prepareStatement(COFFEE_UPDATE);
        // p.setInt(1,2);
        // System.out.println("upc="+p.executeUpdate());
        // p.setInt(1,3);
        // System.out.println("upc="+p.executeUpdate());
        // p.setInt(1,4);
        // System.out.println("upc="+p.executeUpdate());

        trace("updateCount length:" + updateCountLen);
        assertEquals(3, updateCountLen);
        String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=2";
        String query2 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=3";
        String query3 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=4";
        ResultSet rs = stat.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = stat.executeQuery(query2);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = stat.executeQuery(query3);
        rs.next();
        retValue[i++] = rs.getInt(1);
        for (int j = 0; j < updateCount.length; j++) {
            trace("UpdateCount:" + updateCount[j]);
            assertEquals(updateCount[j], retValue[j]);
        }
    }

    private void testAddBatch02() throws SQLException {
        trace("testAddBatch02");
        int i = 0;
        int[] retValue = { 0, 0, 0 };
        int updCountLength = 0;
        String sUpdCoffee = COFFEE_UPDATE1;
        String sDelCoffee = COFFEE_DELETE1;
        String sInsCoffee = COFFEE_INSERT1;
        stat.addBatch(sUpdCoffee);
        stat.addBatch(sDelCoffee);
        stat.addBatch(sInsCoffee);
        int[] updateCount = stat.executeBatch();
        updCountLength = updateCount.length;
        trace("updateCount Length:" + updCountLength);
        assertEquals(3, updCountLength);
        String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=1";
        ResultSet rs = stat.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        // 1 as delete Statement will delete only one row
        retValue[i++] = 1;
        // 1 as insert Statement will insert only one row
        retValue[i++] = 1;
        trace("ReturnValue count : " + retValue.length);
        for (int j = 0; j < updateCount.length; j++) {
            trace("Update Count:" + updateCount[j]);
            trace("Returned Value : " + retValue[j]);
            assertEquals("j:" + j, retValue[j], updateCount[j]);
        }
    }

    private void testClearBatch01() throws SQLException {
        trace("testClearBatch01");
        String sPrepStmt = COFFEE_UPDATE;
        trace("Prepared Statement String:" + sPrepStmt);
        prep = conn.prepareStatement(sPrepStmt);
        prep.setInt(1, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.addBatch();
        prep.setInt(1, 4);
        prep.addBatch();
        prep.clearBatch();
        assertEquals(0, prep.executeBatch().length);
    }

    private void testClearBatch02() throws SQLException {
        trace("testClearBatch02");
        String sUpdCoffee = COFFEE_UPDATE1;
        String sInsCoffee = COFFEE_INSERT1;
        String sDelCoffee = COFFEE_DELETE1;
        stat.addBatch(sUpdCoffee);
        stat.addBatch(sDelCoffee);
        stat.addBatch(sInsCoffee);
        stat.clearBatch();
        assertEquals(0, stat.executeBatch().length);
    }

    private void testExecuteBatch01() throws SQLException {
        trace("testExecuteBatch01");
        int i = 0;
        int[] retValue = { 0, 0, 0 };
        int updCountLength = 0;
        String sPrepStmt = COFFEE_UPDATE;
        trace("Prepared Statement String:" + sPrepStmt);
        // get the PreparedStatement object
        prep = conn.prepareStatement(sPrepStmt);
        prep.setInt(1, 1);
        prep.addBatch();
        prep.setInt(1, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.addBatch();
        int[] updateCount = prep.executeBatch();
        updCountLength = updateCount.length;
        trace("Successfully Updated");
        trace("updateCount Length:" + updCountLength);
        if (updCountLength != 3) {
            fail("executeBatch");
        } else {
            trace("executeBatch executes the Batch of SQL statements");
        }
        // 1 is the number that is set First for Type Id in Prepared Statement
        String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=1";
        // 2 is the number that is set second for Type id in Prepared Statement
        String query2 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=2";
        // 3 is the number that is set Third for Type id in Prepared Statement
        String query3 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=3";
        ResultSet rs = stat.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = stat.executeQuery(query2);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = stat.executeQuery(query3);
        rs.next();
        retValue[i++] = rs.getInt(1);
        trace("retValue length : " + retValue.length);
        for (int j = 0; j < updateCount.length; j++) {
            trace("UpdateCount Value:" + updateCount[j]);
            trace("RetValue : " + retValue[j]);
            if (updateCount[j] != retValue[j]) {
                fail("j=" + j + " right:" + retValue[j]);
            }
        }
    }

    private void testExecuteBatch02() throws SQLException {
        trace("testExecuteBatch02");
        String sPrepStmt = COFFEE_UPDATE;
        trace("Prepared Statement String:" + sPrepStmt);
        prep = conn.prepareStatement(sPrepStmt);
        prep.setInt(1, 1);
        prep.setInt(1, 2);
        prep.setInt(1, 3);
        int[] updateCount = prep.executeBatch();
        int updCountLength = updateCount.length;
        trace("UpdateCount Length : " + updCountLength);
        if (updCountLength == 0) {
            trace("executeBatch does not execute Empty Batch");
        } else {
            fail("executeBatch");
        }
    }

    private void testExecuteBatch03() throws SQLException {
        trace("testExecuteBatch03");
        boolean batchExceptionFlag = false;
        String sPrepStmt = COFFEE_SELECT;
        trace("Prepared Statement String :" + sPrepStmt);
        prep = conn.prepareStatement(sPrepStmt);
        prep.setInt(1, 1);
        prep.addBatch();
        try {
            int[] updateCount = prep.executeBatch();
            trace("Update Count" + updateCount.length);
        } catch (BatchUpdateException b) {
            batchExceptionFlag = true;
        }
        if (batchExceptionFlag) {
            trace("select not allowed; correct");
        } else {
            fail("executeBatch select");
        }
    }

    private void testExecuteBatch04() throws SQLException {
        trace("testExecuteBatch04");
        int i = 0;
        int[] retValue = { 0, 0, 0 };
        int updCountLength = 0;
        String sUpdCoffee = COFFEE_UPDATE1;
        String sInsCoffee = COFFEE_INSERT1;
        String sDelCoffee = COFFEE_DELETE1;
        stat.addBatch(sUpdCoffee);
        stat.addBatch(sDelCoffee);
        stat.addBatch(sInsCoffee);
        int[] updateCount = stat.executeBatch();
        updCountLength = updateCount.length;
        trace("Successfully Updated");
        trace("updateCount Length:" + updCountLength);
        if (updCountLength != 3) {
            fail("executeBatch");
        } else {
            trace("executeBatch executes the Batch of SQL statements");
        }
        String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=1";
        ResultSet rs = stat.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        // 1 as Delete Statement will delete only one row
        retValue[i++] = 1;
        // 1 as Insert Statement will insert only one row
        retValue[i++] = 1;
        for (int j = 0; j < updateCount.length; j++) {
            trace("Update Count : " + updateCount[j]);
            if (updateCount[j] != retValue[j]) {
                fail("j=" + j + " right:" + retValue[j]);
            }
        }
    }

    private void testExecuteBatch05() throws SQLException {
        trace("testExecuteBatch05");
        int updCountLength = 0;
        int[] updateCount = stat.executeBatch();
        updCountLength = updateCount.length;
        trace("updateCount Length:" + updCountLength);
        if (updCountLength == 0) {
            trace("executeBatch Method does not execute the Empty Batch ");
        } else {
            fail("executeBatch 0!=" + updCountLength);
        }
    }

    private void testExecuteBatch06() throws SQLException {
        trace("testExecuteBatch06");
        boolean batchExceptionFlag = false;
        // Insert a row which is already Present
        String sInsCoffee = COFFEE_INSERT1;
        String sDelCoffee = COFFEE_DELETE1;
        stat.addBatch(sInsCoffee);
        stat.addBatch(sInsCoffee);
        stat.addBatch(sDelCoffee);
        try {
            stat.executeBatch();
        } catch (BatchUpdateException b) {
            batchExceptionFlag = true;
            for (int uc : b.getUpdateCounts()) {
                trace("Update counts:" + uc);
            }
        }
        if (batchExceptionFlag) {
            trace("executeBatch insert duplicate; correct");
        } else {
            fail("executeBatch");
        }
    }

    private void testExecuteBatch07() throws SQLException {
        trace("testExecuteBatch07");
        boolean batchExceptionFlag = false;
        String selectCoffee = COFFEE_SELECT1;
        trace("selectCoffee = " + selectCoffee);
        Statement stmt = conn.createStatement();
        stmt.addBatch(selectCoffee);
        try {
            int[] updateCount = stmt.executeBatch();
            trace("updateCount Length : " + updateCount.length);
        } catch (BatchUpdateException be) {
            batchExceptionFlag = true;
        }
        if (batchExceptionFlag) {
            trace("executeBatch select");
        } else {
            fail("executeBatch");
        }
    }

    private void testContinueBatch01() throws SQLException {
        trace("testContinueBatch01");
        int[] batchUpdates = { 0, 0, 0 };
        int buCountLen = 0;
        try {
            String sPrepStmt = COFFEE_UPDATE_SET;
            trace("Prepared Statement String:" + sPrepStmt);
            prep = conn.prepareStatement(sPrepStmt);
            // Now add a legal update to the batch
            prep.setInt(1, 1);
            prep.setString(2, "Continue-1");
            prep.setString(3, "COFFEE-1");
            prep.addBatch();
            // Now add an illegal update to the batch by
            // forcing a unique constraint violation
            // Try changing the key_id of row 3 to 1.
            prep.setInt(1, 1);
            prep.setString(2, "Invalid");
            prep.setString(3, "COFFEE-3");
            prep.addBatch();
            // Now add a second legal update to the batch
            // which will be processed ONLY if the driver supports
            // continued batch processing according to 6.2.2.3
            // of the J2EE platform spec.
            prep.setInt(1, 2);
            prep.setString(2, "Continue-2");
            prep.setString(3, "COFFEE-2");
            prep.addBatch();
            // The executeBatch() method will result in a
            // BatchUpdateException
            prep.executeBatch();
        } catch (BatchUpdateException b) {
            trace("expected BatchUpdateException");
            batchUpdates = b.getUpdateCounts();
            buCountLen = batchUpdates.length;
        }
        if (buCountLen == 1) {
            trace("no continued updates - OK");
            return;
        } else if (buCountLen == 3) {
            trace("Driver supports continued updates.");
            // Check to see if the third row from the batch was added
            String query = COFFEE_SELECT_CONTINUED;
            trace("Query is: " + query);
            ResultSet rs = stat.executeQuery(query);
            rs.next();
            int count = rs.getInt(1);
            rs.close();
            stat.close();
            trace("Count val is: " + count);
            // make sure that we have the correct error code for
            // the failed update.
            if (!(batchUpdates[1] == -3 && count == 1)) {
                fail("insert failed");
            }
        }
    }
}
