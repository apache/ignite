/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.h2.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A test that runs random join statements against two databases and compares the results.
 */
public class TestHashJoin {
    private static final int LEFT_CNT = 1000;
    private static final int RIGHT_CNT = 100;

    private static Connection connection;

    /**
     * @throws SQLException On error.
     */
    @BeforeClass
    public static void init() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:hashjoin");
        sql("DROP TABLE IF EXISTS A");
        sql("DROP TABLE IF EXISTS B");

        sql("CREATE TABLE A (ID INT PRIMARY KEY, JID INT)");
        sql("CREATE INDEX A_JID ON A(JID)");

        sql("CREATE TABLE B(ID INT PRIMARY KEY, val0 int, val1 VARCHAR(20), A_JID INT, val2 BOOLEAN)");
        sql("CREATE INDEX B_A_JID ON B(A_JID)");
        sql("CREATE INDEX B_VAL0 ON B(VAL0)");

        sql("CREATE TABLE C(ID INT PRIMARY KEY, val0 int, val1 VARCHAR(20), A_JID INT, val2 BOOLEAN)");
        sql("CREATE INDEX C_A_JID ON C(A_JID)");
        sql("CREATE INDEX C_VAL0 ON C(VAL0)");

        for (int i = 0; i < LEFT_CNT; ++i)
            sql("INSERT INTO A VALUES(?, ?)", i, i % 3 == 0 ? null : i % RIGHT_CNT);

        for (int i = 0; i < RIGHT_CNT; ++i)
            sql("INSERT INTO B (ID, A_JID, val0) VALUES(?, ?, ?)",
                i,
                i % 4 == 0 ? null : i,
                i == 0 ? null : i % 10);

        sql("INSERT INTO B (ID, A_JID, val0, val1, val2) VALUES(?, ?, ?, ?, ?)",
            RIGHT_CNT,
            RIGHT_CNT % 4,
            null, null, null);

        for (int i = 0; i < RIGHT_CNT; ++i)
            sql("INSERT INTO C (ID, A_JID, val0) VALUES(?, ?, ?)",
                i,
                i % 4 == 0 ? null : i,
                i == 0 ? null : i % 10);

        sql("SET FORCE_JOIN_ORDER 1");
    }

    /**
     * @throws SQLException On error.
     */
    @AfterClass
    public static void cleanup() throws SQLException {
        sql("DROP TABLE IF EXISTS A");
        sql("DROP TABLE IF EXISTS B");
        sql("DROP TABLE IF EXISTS C");

        connection.close();
    }

    /**
     * Check query plan. HASH_JOIN_IDX index must be chosen.
     * @throws Exception On error.
     */
    @Test
    public void testHashJoin() throws Exception {
//        sql("SET TRACE_LEVEL_SYSTEM_OUT 10");

        String plan = null;

        plan = sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) WHERE A.JID=B.A_JID");
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=B_DATA, hashedCols=[A_JID]]"));

        plan = sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX), C USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID=B.A_JID AND A.JID=C.A_JID");
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=B_DATA, hashedCols=[A_JID]]"));
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=C_DATA, hashedCols=[A_JID]]"));

        plan = sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX), C " +
            "WHERE A.JID=B.A_JID AND A.JID=C.A_JID");

        assertTrue("Unexpected plan: " + plan, plan.contains("PUBLIC.C_A_JID: A_JID = A.JID"));
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=B_DATA, hashedCols=[A_JID]]"));

        plan = sqlStr("EXPLAIN SELECT * FROM A, B, C USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID=B.A_JID AND A.JID=C.A_JID");
        assertTrue("Unexpected plan: " + plan, plan.contains("PUBLIC.B_A_JID: A_JID = A.JID"));
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=C_DATA, hashedCols=[A_JID]]"));

        plan = sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX), C USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID=B.A_JID AND B.ID=C.ID");
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=B_DATA, hashedCols=[A_JID]"));
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=C_DATA, hashedCols=[ID]]"));


        plan = sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX), C " +
            "WHERE A.JID=B.A_JID AND B.VAL0=C.VAL0");
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=B_DATA, hashedCols=[A_JID]"));
        assertTrue("Unexpected plan: " + plan, plan.contains("PUBLIC.C_VAL0: VAL0 = B.VAL0"));

        plan = sqlStr("EXPLAIN SELECT * FROM A, B, C USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID=B.A_JID AND B.VAL0=C.VAL0");
        assertTrue("Unexpected plan: " + plan, plan.contains("PUBLIC.B_A_JID: A_JID = A.JID"));
        assertTrue("Unexpected plan: " + plan, plan.contains("HASH_JOIN_IDX [fillFromIndex=C_DATA, hashedCols=[VAL0]]"));
    }

    /**
     * Check that result of HASH JOIN and NL join are equals.
     * @throws Exception On failed.
     */
    @Test
    public void testHashJoinResults() throws Exception {
        assertResultEquals(
            sql("SELECT * FROM A, B WHERE A.JID=B.A_JID ORDER BY 1, 2, 3"),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) WHERE A.JID=B.A_JID ORDER BY 1, 2, 3")
        );

        assertResultEquals(
            sql("SELECT * FROM A, B, C " +
                "WHERE A.JID=B.A_JID AND A.JID=C.A_JID ORDER BY 1, 2, 3"),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX), C USE INDEX (HASH_JOIN_IDX) " +
                "WHERE A.JID=B.A_JID AND A.JID=C.A_JID ORDER BY 1, 2, 3")
        );

        assertResultEquals(
            sql("SELECT * FROM A, B, C " +
                "WHERE A.JID=B.A_JID AND A.JID=C.A_JID ORDER BY 1, 2, 3"),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX), C " +
                "WHERE A.JID=B.A_JID AND A.JID=C.A_JID ORDER BY 1, 2, 3")
        );

        assertResultEquals(
            sql("SELECT * FROM A, B, C " +
                "WHERE A.JID=B.A_JID AND A.JID=C.A_JID ORDER BY 1, 2, 3"),
            sql("SELECT * FROM A, B, C USE INDEX (HASH_JOIN_IDX) " +
                "WHERE A.JID=B.A_JID AND A.JID=C.A_JID ORDER BY 1, 2, 3")
        );

        assertResultEquals(
            sql("SELECT * FROM A, B " +
                "WHERE A.JID = B.A_JID AND B.val0 > ? ORDER BY 1, 2, 3", 5),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.val0 > ? ORDER BY 1, 2, 3", 5)
        );

        assertResultEquals(
            sql("SELECT * FROM A, B " +
                "WHERE A.JID = B.A_JID AND B.val0 >= ? ORDER BY 1, 2, 3", 5),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.val0 >= ? ORDER BY 1, 2, 3", 5)
        );

        assertResultEquals(
            sql("SELECT * FROM A, B " +
                "WHERE A.JID = B.A_JID AND B.val0 < ? ORDER BY 1, 2, 3", 50),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.val0 < ? ORDER BY 1, 2, 3", 50)
        );

        assertResultEquals(
            sql("SELECT * FROM A, B " +
                "WHERE A.JID = B.A_JID AND B.val0 <= ? ORDER BY 1, 2, 3", 50),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.val0 <= ? ORDER BY 1, 2, 3", 50)
        );

        assertResultEquals(
            sql("SELECT * FROM A, B " +
                "WHERE A.JID = B.A_JID AND B.val0 = ? ORDER BY 1, 2, 3", 4),
            sql("SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.val0 = ? ORDER BY 1, 2, 3", 4)
        );
    }

    /**
     * Check that result of HASH JOIN and NL join are equals.
     * @param expected Expected results.
     * @param actual Actual results.
     */
    public void assertResultEquals(List<List<Object>> expected, List<List<Object>> actual) {
        assertFalse(expected.isEmpty());
        assertEquals(expected, actual);
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testHashJoinFilterCondition() throws Exception {
        assertTrue(sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID = B.A_JID AND B.val0 > ?", 5)
            .contains("HASH_JOIN_IDX [fillFromIndex=B_VAL0, hashedCols=[A_JID], filters=[VAL0 > ?1]]"));
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testNotHashJoin() throws Exception {
        assertFalse(sqlStr("EXPLAIN SELECT * FROM A USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID IN (NULL, NULL)")
                .contains("HASH_JOIN_IDX [fillFromIndex="));

        assertFalse(sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID > B.A_JID")
                .contains("HASH_JOIN_IDX [fillFromIndex="));

        assertFalse(sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX (HASH_JOIN_IDX) " +
            "WHERE A.JID > B.A_JID AND B.A_JID = ?", 5)
            .contains("HASH_JOIN_IDX [fillFromIndex="));

        String plan = sqlStr("EXPLAIN SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX), C USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID > C.A_JID AND B.A_JID = C.A_JID");
        assertTrue(plan.contains("PUBLIC.B.tableScan"));
        assertTrue(plan.contains("HASH_JOIN_IDX [fillFromIndex=C_A_JID, hashedCols=[A_JID], filters=[A_JID < A.JID]]"));
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testHashJoinOnString() throws Exception {
        sql("CREATE TABLE A_STR (ID INT PRIMARY KEY, JID VARCHAR(80))");
        sql("CREATE TABLE B_STR (ID INT PRIMARY KEY, JID VARCHAR(80))");
        sql("CREATE TABLE B_STR_IGNORECASE (ID INT PRIMARY KEY, JID VARCHAR_IGNORECASE(80))");

        sql("INSERT INTO A_STR VALUES (0, 'val_0')");
        sql("INSERT INTO A_STR VALUES (1, 'Val_1')");
        sql("INSERT INTO A_STR VALUES (2, 'VAL_2')");


        sql("INSERT INTO B_STR VALUES (0, 'val_0')");
        sql("INSERT INTO B_STR VALUES (1, 'val_1')");
        sql("INSERT INTO B_STR VALUES (2, 'val_2')");

        sql("INSERT INTO B_STR_IGNORECASE VALUES (0, 'VaL_0')");
        sql("INSERT INTO B_STR_IGNORECASE VALUES (1, 'vaL_1')");
        sql("INSERT INTO B_STR_IGNORECASE VALUES (2, 'val_2')");

        try {
            assertResultEquals(
                sql("SELECT * FROM A_STR, B_STR " +
                    "WHERE A_STR.JID = B_STR.JID ORDER BY 1, 2, 3"),
                sql("SELECT * FROM A_STR, B_STR USE INDEX (HASH_JOIN_IDX) " +
                    "WHERE A_STR.JID = B_STR.JID ORDER BY 1, 2, 3")
            );

            // Reverse order join (B_STR_IGNORECASE -> A) is not use index.
            // See at the end of the method Comparison.createIndexConditions
            assertResultEquals(
                sql("SELECT * FROM A_STR, B_STR_IGNORECASE " +
                    "WHERE A_STR.JID = B_STR_IGNORECASE.JID ORDER BY 1, 2, 3"),
                sql("SELECT * FROM A_STR, B_STR_IGNORECASE USE INDEX (HASH_JOIN_IDX) " +
                    "WHERE A_STR.JID = B_STR_IGNORECASE.JID ORDER BY 1, 2, 3")
            );
        }
        finally {
            sql("DROP TABLE A_STR");
            sql("DROP TABLE B_STR");
        }
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testDifferetColumnTypes() throws Exception {
        sql("CREATE TABLE TEST_1 (ID LONG PRIMARY KEY, VAL VARCHAR(80))");
        sql("CREATE TABLE TEST_2 (ID INT PRIMARY KEY, VAL VARCHAR(80))");

        sql("INSERT INTO TEST_1 VALUES (0, 'val_0')");
        sql("INSERT INTO TEST_1 VALUES (1, 'Val_1')");
        sql("INSERT INTO TEST_1 VALUES (2, 'VAL_2')");


        sql("INSERT INTO TEST_2 VALUES (0, 'val_0')");
        sql("INSERT INTO TEST_2 VALUES (1, 'val_1')");
        sql("INSERT INTO TEST_2 VALUES (2, 'val_2')");

        List<List<Object>> resExpected = sql("SELECT * FROM TEST_1, TEST_2 " +
            "WHERE TEST_1.ID = TEST_2.ID");

        // Convert value on build.
        List<List<Object>> resHj = sql("SELECT * FROM TEST_1, TEST_2 USE INDEX (HASH_JOIN_IDX) " +
            "WHERE TEST_1.ID = TEST_2.ID");

        assertEquals(resExpected.size(), resHj.size());

        // Convert value on find.
        resHj = sql("SELECT * FROM TEST_2, TEST_1 USE INDEX (HASH_JOIN_IDX) " +
            "WHERE TEST_1.ID = TEST_2.ID");

        assertEquals(resExpected.size(), resHj.size());
    }

    /**
     * @param sql SQL query.
     * @param params Parameters.
     * @throws SQLException On error.
     * @return Result set or updated count are printed to string.
     */
    private static String sqlStr(String sql, Object... params) throws SQLException {
        try (PreparedStatement prep = connection.prepareStatement(sql)) {
            for (int j = 0; j < params.length; j++)
                prep.setObject(j + 1, params[j]);

            if (prep.execute()) {
                ResultSet rs = prep.getResultSet();

                return readResult(rs);
            }
            else
                return "UPD: " + prep.getUpdateCount();
        }
    }

    /**
     * @param sql SQL query.
     * @param params Parameters.
     * @throws SQLException On error.
     * @return Result set or updated count are printed to string.
     */
    private static List<List<Object>> sql(String sql, Object... params) throws SQLException {
        try (PreparedStatement prep = connection.prepareStatement(sql)) {
            for (int j = 0; j < params.length; j++)
                prep.setObject(j + 1, params[j]);

            if (prep.execute()) {
                ResultSet rs = prep.getResultSet();

                int colCnt = rs.getMetaData().getColumnCount();

                List<List<Object>> res = new ArrayList<>();

                while (rs.next()) {
                    List<Object> row = new ArrayList<>(colCnt);

                    for (int i = 0; i < colCnt; i++)
                        row.add (rs.getObject(i + 1));

                    res.add(row);
                }

                return res;
            }
            else
                return Collections.singletonList(Collections.singletonList((Object)prep.getUpdateCount()));
        }
    }

    /**
     * @param rs Result set.
     * @return Result set printed to string.
     * @throws SQLException On error.
     */
    private static String readResult(ResultSet rs) throws SQLException {
        StringBuilder b = new StringBuilder();

        ResultSetMetaData meta = rs.getMetaData();

        int columnCount = meta.getColumnCount();

        for (int i = 0; i < columnCount; i++) {
            if (i > 0)
                b.append(",");

            b.append(StringUtils.toUpperEnglish(meta.getColumnLabel(i + 1)));
        }

        b.append(":\n");

        String result = b.toString();

        ArrayList<String> list = new ArrayList<>();

        while (rs.next()) {
            b = new StringBuilder();

            for (int i = 0; i < columnCount; i++) {
                if (i > 0)
                    b.append(",");

                b.append(rs.getString(i + 1));
            }

            list.add(b.toString());
        }
        Collections.sort(list);

        for (int i = 0; i < list.size(); i++)
            result += list.get(i) + "\n";

        return result;
    }
}
