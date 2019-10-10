/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Tests random compare operations.
 */
public class TestRandomCompare extends TestBase {

    private final ArrayList<Statement> dbs = New.arrayList();
    private int aliasId;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("randomCompare");
        testCases();
        testRandom();
        deleteDb("randomCompare");
    }

    private void testRandom() throws Exception {
        Connection conn = getConnection("randomCompare");
        dbs.add(conn.createStatement());

        try {
            Class.forName("org.postgresql.Driver");
            Connection c2 = DriverManager.getConnection(
                    "jdbc:postgresql:test?loggerLevel=OFF", "sa", "sa");
            dbs.add(c2.createStatement());
        } catch (Exception e) {
            // database not installed - ok
        }

        String shortest = null;
        Throwable shortestEx = null;
        /*
        drop table test;
        create table test(x0 int, x1 int);
        create index idx_test_x0 on test(x0);
        insert into test values(null, null);
        insert into test values(null, 1);
        insert into test values(null, 2);
        insert into test values(1, null);
        insert into test values(1, 1);
        insert into test values(1, 2);
        insert into test values(2, null);
        insert into test values(2, 1);
        insert into test values(2, 2);
        */
        try {
            execute("drop table test");
        } catch (Exception e) {
            // ignore
        }
        try {
            execute("drop table test cascade");
        } catch (Exception e) {
            // ignore
        }
        String sql = "create table test(x0 int, x1 int)";
        trace(sql + ";");
        execute(sql);
        sql = "create index idx_test_x0 on test(x0)";
        trace(sql + ";");
        execute(sql);
        for (int x0 = 0; x0 < 3; x0++) {
            for (int x1 = 0; x1 < 3; x1++) {
                sql = "insert into test values(" + (x0 == 0 ? "null" : x0) +
                        ", " + (x1 == 0 ? "null" : x1) + ")";
                trace(sql + ";");
                execute(sql);
            }
        }
        Random random = new Random(1);
        for (int i = 0; i < 1000; i++) {
            StringBuilder buff = new StringBuilder();
            appendRandomCompare(random, buff);
            sql = buff.toString();
            try {
                execute(sql);
            } catch (Throwable e) {
                if (e instanceof SQLException) {
                    trace(sql);
                    fail(sql);
                    // SQLException se = (SQLException) e;
                    // System.out.println(se);
                    // System.out.println("  " + sql);
                }
                if (shortest == null || sql.length() < shortest.length()) {
                    shortest = sql;
                    shortestEx = e;
                }
            }
        }
        if (shortest != null) {
            shortestEx.printStackTrace();
            fail(shortest + " " + shortestEx);
        }
        for (int i = 0; i < 10; i++) {
            try {
                execute("drop table t" + i);
            } catch (Exception e) {
                // ignore
            }
        }
        for (Statement s : dbs) {
            s.getConnection().close();
        }
        deleteDb("randomCompare");
    }

    private void appendRandomCompare(Random random, StringBuilder buff) {
        buff.append("select * from ");
        int alias = aliasId++;
        if (random.nextBoolean()) {
            buff.append("(");
            appendRandomCompare(random, buff);
            buff.append(")");
        } else {
            buff.append("test");
        }
        buff.append(" as t").append(alias);
        if (random.nextInt(10) == 0) {
            return;
        }
        buff.append(" where ");
        int count = 1 + random.nextInt(3);
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                buff.append(random.nextBoolean() ? " or " : " and ");
            }
            if (random.nextInt(10) == 0) {
                buff.append("not ");
            }
            appendRandomValue(random, buff);
            switch (random.nextInt(8)) {
            case 0:
                buff.append("=");
                appendRandomValue(random, buff);
                break;
            case 1:
                buff.append("<");
                appendRandomValue(random, buff);
                break;
            case 2:
                buff.append(">");
                appendRandomValue(random, buff);
                break;
            case 3:
                buff.append("<=");
                appendRandomValue(random, buff);
                break;
            case 4:
                buff.append(">=");
                appendRandomValue(random, buff);
                break;
            case 5:
                buff.append("<>");
                appendRandomValue(random, buff);
                break;
            case 6:
                buff.append(" is distinct from ");
                appendRandomValue(random, buff);
                break;
            case 7:
                buff.append(" is not distinct from ");
                appendRandomValue(random, buff);
                break;
            }
        }
    }

    private static void appendRandomValue(Random random, StringBuilder buff) {
        switch (random.nextInt(7)) {
        case 0:
            buff.append("null");
            break;
        case 1:
            buff.append(1);
            break;
        case 2:
            buff.append(2);
            break;
        case 3:
            buff.append(3);
            break;
        case 4:
            buff.append(-1);
            break;
        case 5:
            buff.append("x0");
            break;
        case 6:
            buff.append("x1");
            break;
        }
    }

    private void execute(String sql) throws SQLException {
        String expected = null;
        SQLException e = null;
        for (Statement s : dbs) {
            try {
                boolean result = s.execute(sql);
                if (result) {
                    String data = getResult(s.getResultSet());
                    if (expected == null) {
                        expected = data;
                    } else {
                        assertEquals(sql, expected, data);
                    }
                }
            } catch (SQLException e2) {
                // ignore now, throw at the end
                e = e2;
            }
        }
        if (e != null) {
            throw e;
        }
    }

    private static String getResult(ResultSet rs) throws SQLException {
        ArrayList<String> list = New.arrayList();
        while (rs.next()) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append(rs.getString(i + 1));
            }
            list.add(buff.toString());
        }
        Collections.sort(list);
        return list.toString();
    }

    private void testCases() throws Exception {

        Connection conn = getConnection("randomCompare");
        Statement stat = conn.createStatement();
        ResultSet rs;

        /*
        create table test(x int);
        insert into test values(null);
        select * from (select x from test
        union all select x from test) where x is null;
        select * from (select x from test) where x is null;
         */
        stat.execute("create table test(x int)");
        stat.execute("insert into test values(null)");
        rs = stat.executeQuery("select * from (select x from test " +
                "union all select x from test) where x is null");
        assertTrue(rs.next());
        rs = stat.executeQuery(
                "select * from (select x from test) where x is null");
        assertTrue(rs.next());
        rs = stat.executeQuery("select * from (select x from test " +
                "union all select x from test) where x is null");
        assertTrue(rs.next());
        assertTrue(rs.next());

        Connection conn2 = DriverManager.getConnection("jdbc:h2:mem:temp");
        conn2.createStatement().execute("create table test(x int) as select null");
        stat.execute("drop table test");
        stat.execute("create linked table test" +
                "(null, 'jdbc:h2:mem:temp', null, null, 'TEST')");
        rs = stat.executeQuery("select * from (select x from test) where x is null");
        assertTrue(rs.next());
        rs = stat.executeQuery("select * from (select x from test " +
                "union all select x from test) where x is null");
        assertTrue(rs.next());
        assertTrue(rs.next());
        conn2.close();

        conn.close();
        deleteDb("randomCompare");
    }

}
