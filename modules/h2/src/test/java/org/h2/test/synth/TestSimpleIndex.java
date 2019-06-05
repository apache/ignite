/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.synth.sql.RandomGen;

/**
 * A test that runs random operations against a table to test the various index
 * implementations.
 */
public class TestSimpleIndex extends TestBase {

    private Connection conn;
    private Statement stat;
    private RandomGen random;

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
        deleteDb("simpleIndex");
        conn = getConnection("simpleIndex");
        random = new RandomGen();
        stat = conn.createStatement();
        for (int i = 0; i < 10000; i++) {
            testIndex(i);
        }
    }

    private void testIndex(int seed) throws SQLException {
        random.setSeed(seed);
        String unique = random.nextBoolean() ? "UNIQUE " : "";
        int len = random.getInt(2) + 1;
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append((char) ('A' + random.getInt(3)));
        }
        String cols = buff.toString();
        execute("CREATE MEMORY TABLE TEST_M(A INT, B INT, C INT, DATA VARCHAR(255))");
        execute("CREATE CACHED TABLE TEST_D(A INT, B INT, C INT, DATA VARCHAR(255))");
        execute("CREATE MEMORY TABLE TEST_MI(A INT, B INT, C INT, DATA VARCHAR(255))");
        execute("CREATE CACHED TABLE TEST_DI(A INT, B INT, C INT, DATA VARCHAR(255))");
        execute("CREATE " + unique + "INDEX M ON TEST_MI(" + cols + ")");
        execute("CREATE " + unique + "INDEX D ON TEST_DI(" + cols + ")");
        for (int i = 0; i < 100; i++) {
            println("i=" + i);
            testRows();
        }
        execute("DROP INDEX M");
        execute("DROP INDEX D");
        execute("DROP TABLE TEST_M");
        execute("DROP TABLE TEST_D");
        execute("DROP TABLE TEST_MI");
        execute("DROP TABLE TEST_DI");
    }

    private void testRows() throws SQLException {
        String a = randomValue(), b = randomValue(), c = randomValue();
        String data = a + "/" + b + "/" + c;
        String sql = "VALUES(" + a + ", " + b + ", " + c + ", '" + data + "')";
        boolean em, ed;
        // if(id==73) {
        // print("halt");
        // }
        try {
            execute("INSERT INTO TEST_MI " + sql);
            em = false;
        } catch (SQLException e) {
            em = true;
        }
        try {
            execute("INSERT INTO TEST_DI " + sql);
            ed = false;
        } catch (SQLException e) {
            ed = true;
        }
        if (em != ed) {
            fail("different result: ");
        }
        if (!em) {
            execute("INSERT INTO TEST_M " + sql);
            execute("INSERT INTO TEST_D " + sql);
        }
        StringBuilder buff = new StringBuilder("WHERE 1=1");
        int len = random.getLog(10);
        for (int i = 0; i < len; i++) {
            buff.append(" AND ");
            buff.append('A' + random.getInt(3));
            switch (random.getInt(10)) {
            case 0:
                buff.append("<");
                buff.append(random.getInt(100) - 50);
                break;
            case 1:
                buff.append("<=");
                buff.append(random.getInt(100) - 50);
                break;
            case 2:
                buff.append(">");
                buff.append(random.getInt(100) - 50);
                break;
            case 3:
                buff.append(">=");
                buff.append(random.getInt(100) - 50);
                break;
            case 4:
                buff.append("<>");
                buff.append(random.getInt(100) - 50);
                break;
            case 5:
                buff.append(" IS NULL");
                break;
            case 6:
                buff.append(" IS NOT NULL");
                break;
            default:
                buff.append("=");
                buff.append(random.getInt(100) - 50);
            }
        }
        String where = buff.toString();
        String r1 = getResult("SELECT DATA FROM TEST_M " + where + " ORDER BY DATA");
        String r2 = getResult("SELECT DATA FROM TEST_D " + where + " ORDER BY DATA");
        String r3 = getResult("SELECT DATA FROM TEST_MI " + where + " ORDER BY DATA");
        String r4 = getResult("SELECT DATA FROM TEST_DI " + where + " ORDER BY DATA");
        assertEquals(r1, r2);
        assertEquals(r1, r3);
        assertEquals(r1, r4);
    }

    private String getResult(String sql) throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        StringBuilder buff = new StringBuilder();
        while (rs.next()) {
            buff.append(rs.getString(1));
            buff.append("; ");
        }
        rs.close();
        return buff.toString();
    }

    private String randomValue() {
        return random.getInt(10) == 0 ? "NULL" : "" + (random.getInt(100) - 50);
    }

    private void execute(String sql) throws SQLException {
        try {
            println(sql + ";");
            stat.execute(sql);
            println("> update count: 1");
        } catch (SQLException e) {
            println("> exception");
            throw e;
        }
    }

}
