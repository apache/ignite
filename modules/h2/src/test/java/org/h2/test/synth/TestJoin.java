/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.test.TestBase;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * A test that runs random join statements against two databases and compares
 * the results.
 */
public class TestJoin extends TestBase {

    private final ArrayList<Connection> connections = New.arrayList();
    private Random random;
    private int paramCount;
    private StringBuilder buff;

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
        testJoin();
    }

    private void testJoin() throws Exception {
        deleteDb("join");
        String shortestFailed = null;

        Connection c1 = getConnection("join");
        connections.add(c1);

        Class.forName("org.postgresql.Driver");
        Connection c2 = DriverManager.getConnection("jdbc:postgresql:test", "sa", "sa");
        connections.add(c2);

        // Class.forName("com.mysql.jdbc.Driver");
        // Connection c2 =
        // DriverManager.getConnection("jdbc:mysql://localhost/test", "sa",
        // "sa");
        // connections.add(c2);

        // Class.forName("org.hsqldb.jdbcDriver");
        // Connection c2 = DriverManager.getConnection("jdbc:hsqldb:join", "sa",
        // "");
        // connections.add(c2);

        /*
        DROP TABLE ONE;
        DROP TABLE TWO;
        CREATE TABLE ONE(A INT PRIMARY KEY, B INT);
        INSERT INTO ONE VALUES(0, NULL);
        INSERT INTO ONE VALUES(1, 0);
        INSERT INTO ONE VALUES(2, 1);
        INSERT INTO ONE VALUES(3, 4);
        CREATE TABLE TWO(A INT PRIMARY KEY, B INT);
        INSERT INTO TWO VALUES(0, NULL);
        INSERT INTO TWO VALUES(1, 0);
        INSERT INTO TWO VALUES(2, 2);
        INSERT INTO TWO VALUES(3, 3);
        INSERT INTO TWO VALUES(4, NULL);
        */

        execute("DROP TABLE ONE", null, true);
        execute("DROP TABLE TWO", null, true);
        execute("CREATE TABLE ONE(A INT PRIMARY KEY, B INT)", null);
        execute("INSERT INTO ONE VALUES(0, NULL)", null);
        execute("INSERT INTO ONE VALUES(1, 0)", null);
        execute("INSERT INTO ONE VALUES(2, 1)", null);
        execute("INSERT INTO ONE VALUES(3, 4)", null);
        execute("CREATE TABLE TWO(A INT PRIMARY KEY, B INT)", null);
        execute("INSERT INTO TWO VALUES(0, NULL)", null);
        execute("INSERT INTO TWO VALUES(1, 0)", null);
        execute("INSERT INTO TWO VALUES(2, 2)", null);
        execute("INSERT INTO TWO VALUES(3, 3)", null);
        execute("INSERT INTO TWO VALUES(4, NULL)", null);
        random = new Random();
        long startTime = System.nanoTime();
        for (int i = 0;; i++) {
            paramCount = 0;
            buff = new StringBuilder();
            long time = System.nanoTime();
            if (time - startTime > TimeUnit.SECONDS.toNanos(5)) {
                printTime("i:" + i);
                startTime = time;
            }
            buff.append("SELECT ");
            int tables = 1 + random.nextInt(5);
            for (int j = 0; j < tables; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                buff.append("T" + (char) ('0' + j) + ".A");
            }
            buff.append(" FROM ");
            appendRandomTable();
            buff.append(" T0 ");
            for (int j = 1; j < tables; j++) {
                if (random.nextBoolean()) {
                    buff.append("INNER");
                } else {
                    // if(random.nextInt(4)==1) {
                    // buff.append("RIGHT");
                    // } else {
                    buff.append("LEFT");
                    // }
                }
                buff.append(" JOIN ");
                appendRandomTable();
                buff.append(" T");
                buff.append((char) ('0' + j));
                buff.append(" ON ");
                appendRandomCondition(j);
            }
            if (random.nextBoolean()) {
                buff.append("WHERE ");
                appendRandomCondition(tables - 1);
            }
            String sql = buff.toString();
            Object[] params = new Object[paramCount];
            for (int j = 0; j < paramCount; j++) {
                params[j] = random.nextInt(4) == 1 ? null : random.nextInt(10) - 3;
            }
            try {
                execute(sql, params);
            } catch (Exception e) {
                if (shortestFailed == null || shortestFailed.length() > sql.length()) {
                    TestBase.logError("/*SHORT*/ " + sql, null);
                    shortestFailed = sql;
                }
            }
        }
        // c1.close();
        // c2.close();
    }

    private void appendRandomTable() {
        if (random.nextBoolean()) {
            buff.append("ONE");
        } else {
            buff.append("TWO");
        }
    }

    private void appendRandomCondition(int j) {
        if (random.nextInt(10) == 1) {
            buff.append("NOT ");
            appendRandomCondition(j);
        } else if (random.nextInt(5) == 1) {
            buff.append("(");
            appendRandomCondition(j);
            if (random.nextBoolean()) {
                buff.append(") OR (");
            } else {
                buff.append(") AND (");
            }
            appendRandomCondition(j);
            buff.append(")");
        } else {
            if (j > 0 && random.nextBoolean()) {
                buff.append("T" + (char) ('0' + j - 1) + ".A=T" + (char) ('0' + j) + ".A ");
            } else {
                appendRandomConditionPart(j);
            }
        }
    }

    private void appendRandomConditionPart(int j) {
        int t1 = j <= 1 ? 0 : random.nextInt(j + 1);
        int t2 = j <= 1 ? 0 : random.nextInt(j + 1);
        String c1 = random.nextBoolean() ? "A" : "B";
        String c2 = random.nextBoolean() ? "A" : "B";
        buff.append("T" + (char) ('0' + t1));
        buff.append("." + c1);
        if (random.nextInt(4) == 1) {
            if (random.nextInt(5) == 1) {
                buff.append(" IS NOT NULL");
            } else {
                buff.append(" IS NULL");
            }
        } else {
            if (random.nextInt(5) == 1) {
                switch (random.nextInt(5)) {
                case 0:
                    buff.append(">");
                    break;
                case 1:
                    buff.append("<");
                    break;
                case 2:
                    buff.append("<=");
                    break;
                case 3:
                    buff.append(">=");
                    break;
                case 4:
                    buff.append("<>");
                    break;
                default:
                }
            } else {
                buff.append("=");
            }
            if (random.nextBoolean()) {
                buff.append("T" + (char) ('0' + t2));
                buff.append("." + c2);
            } else {
                buff.append(random.nextInt(5) - 1);
            }
        }
        buff.append(" ");
    }

    private void execute(String sql, Object[] params) {
        execute(sql, params, false);
    }

    private void execute(String sql, Object[] params, boolean ignoreDifference) {
        String first = null;
        for (int i = 0; i < connections.size(); i++) {
            Connection conn = connections.get(i);
            String s;
            try {
                Statement stat;
                boolean result;
                if (params == null || params.length == 0) {
                    stat = conn.createStatement();
                    result = stat.execute(sql);
                } else {
                    PreparedStatement prep = conn.prepareStatement(sql);
                    stat = prep;
                    for (int j = 0; j < params.length; j++) {
                        prep.setObject(j + 1, params[j]);
                    }
                    result = prep.execute();
                }
                if (result) {
                    ResultSet rs = stat.getResultSet();
                    s = "rs: " + readResult(rs);
                } else {
                    s = "updateCount: " + stat.getUpdateCount();
                }
            } catch (SQLException e) {
                s = "exception";
            }
            if (i == 0) {
                first = s;
            } else {
                if (!ignoreDifference && !s.equals(first)) {
                    fail("FAIL s:" + s + " first:" + first + " sql:" + sql);
                }
            }
        }
    }

    private static String readResult(ResultSet rs) throws SQLException {
        StringBuilder b = new StringBuilder();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                b.append(",");
            }
            b.append(StringUtils.toUpperEnglish(meta.getColumnLabel(i + 1)));
        }
        b.append(":\n");
        String result = b.toString();
        ArrayList<String> list = New.arrayList();
        while (rs.next()) {
            b = new StringBuilder();
            for (int i = 0; i < columnCount; i++) {
                if (i > 0) {
                    b.append(",");
                }
                b.append(rs.getString(i + 1));
            }
            list.add(b.toString());
        }
        Collections.sort(list);
        for (int i = 0; i < list.size(); i++) {
            result += list.get(i) + "\n";
        }
        return result;
    }

}
