/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.h2.test.TestBase;
import org.h2.util.Utils;

/**
 * Test for big databases.
 */
public class TestBigDb extends TestBase {

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
        if (config.memory) {
            return;
        }
        if (config.networked && config.big) {
            return;
        }
        testLargeTable();
        testInsert();
        testLeftSummary();
        deleteDb("bigDb");
    }

    private void testLargeTable() throws SQLException {
        deleteDb("bigDb");
        Connection conn = getConnection("bigDb");
        Statement stat = conn.createStatement();
        stat.execute("CREATE CACHED TABLE TEST("
                + "M_CODE CHAR(1) DEFAULT CAST(RAND()*9 AS INT),"
                + "PRD_CODE CHAR(20) DEFAULT SECURE_RAND(10),"
                + "ORG_CODE_SUPPLIER CHAR(13) DEFAULT SECURE_RAND(6),"
                + "PRD_CODE_1 CHAR(14) DEFAULT SECURE_RAND(7),"
                + "PRD_CODE_2 CHAR(20)  DEFAULT SECURE_RAND(10),"
                + "ORG_CODE CHAR(13)  DEFAULT SECURE_RAND(6),"
                + "SUBSTITUTED_BY CHAR(20) DEFAULT SECURE_RAND(10),"
                + "SUBSTITUTED_BY_2 CHAR(14) DEFAULT SECURE_RAND(7),"
                + "SUBSTITUTION_FOR CHAR(20) DEFAULT SECURE_RAND(10),"
                + "SUBSTITUTION_FOR_2 CHAR(14) DEFAULT SECURE_RAND(7),"
                + "TEST CHAR(2) DEFAULT SECURE_RAND(1),"
                + "TEST_2 CHAR(2) DEFAULT SECURE_RAND(1),"
                + "TEST_3 DECIMAL(7,2) DEFAULT RAND(),"
                + "PRIMARY_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                + "RATE_PRICE_ORDER_UNIT DECIMAL(9,3) DEFAULT RAND(),"
                + "ORDER_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                + "ORDER_QTY_MIN DECIMAL(6,1) DEFAULT RAND(),"
                + "ORDER_QTY_LOT_SIZE DECIMAL(6,1) DEFAULT RAND(),"
                + "ORDER_UNIT_CODE_2 CHAR(3) DEFAULT SECURE_RAND(1),"
                + "PRICE_GROUP CHAR(20) DEFAULT SECURE_RAND(10),"
                + "LEAD_TIME INTEGER DEFAULT RAND(),"
                + "LEAD_TIME_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                + "PRD_GROUP CHAR(10) DEFAULT SECURE_RAND(5),"
                + "WEIGHT_GROSS DECIMAL(7,3) DEFAULT RAND(),"
                + "WEIGHT_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                + "PACK_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                + "PACK_LENGTH DECIMAL(7,3) DEFAULT RAND(),"
                + "PACK_WIDTH DECIMAL(7,3) DEFAULT RAND(),"
                + "PACK_HEIGHT DECIMAL(7,3) DEFAULT RAND(),"
                + "SIZE_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                + "STATUS_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                + "INTRA_STAT_CODE CHAR(12) DEFAULT SECURE_RAND(6),"
                + "PRD_TITLE CHAR(50) DEFAULT SECURE_RAND(25),"
                + "VALID_FROM DATE DEFAULT NOW(),"
                + "MOD_DATUM DATE DEFAULT NOW())");
        int len = getSize(10, 50000);
        try {
            PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO TEST(PRD_CODE) VALUES('abc' || ?)");
            long time = System.nanoTime();
            for (int i = 0; i < len; i++) {
                if ((i % 1000) == 0) {
                    long t = System.nanoTime();
                    if (t - time > TimeUnit.SECONDS.toNanos(1)) {
                        time = t;
                        int free = Utils.getMemoryFree();
                        println("i: " + i + " free: " + free + " used: " + Utils.getMemoryUsed());
                    }
                }
                prep.setInt(1, i);
                prep.execute();
            }
            stat.execute("CREATE INDEX IDX_TEST_PRD_CODE ON TEST(PRD_CODE)");
            ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columns; i++) {
                    rs.getString(i + 1);
                }
            }
        } catch (OutOfMemoryError e) {
            TestBase.logError("memory", e);
            conn.close();
            throw e;
        }
        conn.close();
    }

    private void testLeftSummary() throws SQLException {
        deleteDb("bigDb");
        Connection conn = getConnection("bigDb");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT, NEG INT AS -ID, " +
                "NAME VARCHAR, PRIMARY KEY(ID, NAME))");
        stat.execute("CREATE INDEX IDX_NEG ON TEST(NEG, NAME)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST(ID, NAME) VALUES(?, '1234567890')");
        int len = getSize(10, 1000);
        int block = getSize(3, 10);
        int left, x = 0;
        for (int i = 0; i < len; i++) {
            left = x + block / 2;
            for (int j = 0; j < block; j++) {
                prep.setInt(1, x++);
                prep.execute();
            }
            stat.execute("DELETE FROM TEST WHERE ID>" + left);
            ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            int count = rs.getInt(1);
            trace("count: " + count);
        }
        conn.close();
    }

    private void testInsert() throws SQLException {
        deleteDb("bigDb");
        Connection conn = getConnection("bigDb");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST(NAME) VALUES('Hello World')");
        int len = getSize(1000, 10000);
        for (int i = 0; i < len; i++) {
            if (i % 1000 == 0) {
                println("rows: " + i);
                Thread.yield();
            }
            prep.execute();
        }
        conn.close();
    }

}
