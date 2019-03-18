/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.File;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.New;
import org.h2.util.ScriptReader;

/**
 * Tests nested joins and right outer joins.
 */
public class TestOuterJoins extends TestBase {

    private final ArrayList<Statement> dbs = New.arrayList();

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
        deleteDb("outerJoins");
        testCases();
        testRandom();
        deleteDb("outerJoins");
    }

    private void testRandom() throws Exception {
        Connection conn = getConnection("outerJoins");
        dbs.add(conn.createStatement());

        try {
            Class.forName("org.postgresql.Driver");
            Connection c2 = DriverManager.getConnection(
                    "jdbc:postgresql:test?loggerLevel=OFF", "sa", "sa");
            dbs.add(c2.createStatement());
        } catch (Exception e) {
            // database not installed - ok
        }
        deleteDerby();
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            Connection c2 = DriverManager.getConnection(
                    "jdbc:derby:" + getBaseDir() +
                    "/derby/test;create=true", "sa", "sa");
            dbs.add(c2.createStatement());
        } catch (Exception e) {
            // database not installed - ok
        }
        String shortest = null;
        Throwable shortestEx = null;
        for (int i = 0; i < 4; i++) {
            try {
                executeAndLog("drop table t" + i);
            } catch (Exception e) {
                // ignore
            }
        }
        executeAndLog("create table t0(x int primary key)");
        executeAndLog("create table t1(x int)");
        // for H2, this will ensure it's not using a clustered index
        executeAndLog("create table t2(x real primary key)");
        executeAndLog("create table t3(x int)");
        executeAndLog("create index idx_t3_x on t3(x)");
        for (int i = 0; i < 16; i++) {
            for (int j = 0; j < 4; j++) {
                if ((i & (1 << j)) != 0) {
                    executeAndLog("insert into t" + j + " values(" + i + ")");
                }
            }
        }
        Random random = new Random();
        int len = getSize(500, 5000);
        for (int i = 0; i < len; i++) {
            StringBuilder buff = new StringBuilder();
            int t = 1 + random.nextInt(3);
            buff.append("select ");
            for (int j = 0; j < t; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                buff.append("t" + j + ".x ");
            }
            buff.append("from ");
            appendRandomJoin(random, buff, 0, t - 1);
            appendRandomCondition(random, buff, t);
            String sql = buff.toString();
            try {
                execute(sql);
            } catch (Throwable e) {
                if (e instanceof SQLException) {
                    trace(sql);
                    fail(sql);
                    // SQLException se = (SQLException) e;
                    // System.out.println(se);
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
        for (int i = 0; i < 4; i++) {
            try {
                execute("drop table t" + i);
            } catch (Exception e) {
                // ignore
            }
        }
        for (Statement s : dbs) {
            s.getConnection().close();
        }
        deleteDerby();
        deleteDb("outerJoins");
    }

    private void deleteDerby() {
        try {
            new File("derby.log").delete();
            try {
                DriverManager.getConnection("jdbc:derby:" +
                        getBaseDir() + "/derby/test;shutdown=true", "sa", "sa");
            } catch (Exception e) {
                // ignore
            }
            FileUtils.deleteRecursive(getBaseDir() + "/derby", false);
        } catch (Exception e) {
            e.printStackTrace();
            // database not installed - ok
        }
    }

    private void appendRandomJoin(Random random, StringBuilder buff, int min,
            int max) {
        if (min == max) {
            buff.append("t" + min);
            return;
        }
        buff.append("(");
        int m = min + random.nextInt(max - min);
        int left = min + (m == min ? 0 : random.nextInt(m - min));
        appendRandomJoin(random, buff, min, m);
        switch (random.nextInt(3)) {
        case 0:
            buff.append(" inner join ");
            break;
        case 1:
            buff.append(" left outer join ");
            break;
        case 2:
            buff.append(" right outer join ");
            break;
        }
        m++;
        int right = m + (m == max ? 0 : random.nextInt(max - m));
        appendRandomJoin(random, buff, m, max);
        buff.append(" on t" + left + ".x = t" + right + ".x ");
        buff.append(")");
    }

    private static void appendRandomCondition(Random random,
            StringBuilder buff, int max) {
        if (max > 0 && random.nextInt(4) == 0) {
            return;
        }
        buff.append(" where ");
        int count = 1 + random.nextInt(3);
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                buff.append(random.nextBoolean() ? " and " : " or ");
            }
            buff.append("t" + random.nextInt(max) + ".x");
            switch (random.nextInt(8)) {
            case 0:
                buff.append("=");
                appendRandomValueOrColumn(random, buff, max);
                break;
            case 1:
                buff.append(">=");
                appendRandomValueOrColumn(random, buff, max);
                break;
            case 2:
                buff.append("<=");
                appendRandomValueOrColumn(random, buff, max);
                break;
            case 3:
                buff.append("<");
                appendRandomValueOrColumn(random, buff, max);
                break;
            case 4:
                buff.append(">");
                appendRandomValueOrColumn(random, buff, max);
                break;
            case 5:
                buff.append("<>");
                appendRandomValueOrColumn(random, buff, max);
                break;
            case 6:
                buff.append(" is not null");
                break;
            case 7:
                buff.append(" is null");
                break;
            }
        }
    }

    private static void appendRandomValueOrColumn(Random random,
            StringBuilder buff, int max) {
        if (random.nextBoolean()) {
            buff.append(random.nextInt(8) - 2);
        } else {
            buff.append("t" + random.nextInt(max) + ".x");
        }
    }

    private void executeAndLog(String sql) throws SQLException {
        trace(sql + ";");
        execute(sql);
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
            } catch (AssertionError e2) {
                e = new SQLException(e2.getMessage());
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
                int x = rs.getInt(i + 1);
                buff.append(rs.wasNull() ? "null" : x);
            }
            list.add(buff.toString());
        }
        Collections.sort(list);
        return list.toString();
    }

    private void testCases() throws Exception {

        Connection conn = getConnection("outerJoins");
        Statement stat = conn.createStatement();
        ResultSet rs;
        String sql;

        /*
        create table test(id int primary key);
        explain select * from test a left outer join (test c) on a.id = c.id;
        -- expected: uses the primary key index
        */
        stat.execute("create table test(id int primary key)");
        rs = stat.executeQuery("explain select * from test a " +
                "left outer join (test c) on a.id = c.id");
        assertTrue(rs.next());
        sql = rs.getString(1);
        assertContains(sql, "PRIMARY_KEY");
        stat.execute("drop table test");

        /*
        create table t1(a int, b int);
        create table t2(a int, b int);
        create table t3(a int, b int);
        create table t4(a int, b int);
        insert into t1 values(1,1), (2,2), (3,3);
        insert into t2 values(1,1), (2,2);
        insert into t3 values(1,1), (3,3);
        insert into t4 values(1,1), (2,2), (3,3), (4,4);
        select distinct t1.a, t2.a, t3.a from t1
        right outer join t3 on t1.b=t3.a right outer join t2 on t2.b=t1.a;
        drop table t1, t2, t3, t4;
         */
        stat.execute("create table t1(a int, b int)");
        stat.execute("create table t2(a int, b int)");
        stat.execute("create table t3(a int, b int)");
        stat.execute("create table t4(a int, b int)");
        stat.execute("insert into t1 values(1,1), (2,2), (3,3)");
        stat.execute("insert into t2 values(1,1), (2,2)");
        stat.execute("insert into t3 values(1,1), (3,3)");
        stat.execute("insert into t4 values(1,1), (2,2), (3,3), (4,4)");
        rs = stat.executeQuery("explain select distinct t1.a, t2.a, t3.a from t1 " +
                "right outer join t3 on t1.b=t3.a right outer join t2 on t2.b=t1.a");
        assertTrue(rs.next());
        sql = cleanRemarks(rs.getString(1));
        assertEquals("SELECT DISTINCT T1.A, T2.A, T3.A FROM PUBLIC.T2 " +
                "LEFT OUTER JOIN ( PUBLIC.T3 " +
                "LEFT OUTER JOIN PUBLIC.T1 ON T1.B = T3.A ) " +
                "ON T2.B = T1.A", sql);
        rs = stat.executeQuery("select distinct t1.a, t2.a, t3.a from t1 " +
                "right outer join t3 on t1.b=t3.a right outer join t2 on t2.b=t1.a");
        // expected: 1  1       1; null    2       null
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        stat.execute("drop table t1, t2, t3, t4");

        /*
        create table a(x int);
        create table b(x int);
        create table c(x int);
        insert into a values(1);
        insert into b values(1);
        insert into c values(1), (2);
        select a.x, b.x, c.x from a inner join b on a.x = b.x
        right outer join c on c.x = a.x;
        drop table a, b, c;
        */
        stat.execute("create table a(x int)");
        stat.execute("create table b(x int)");
        stat.execute("create table c(x int)");
        stat.execute("insert into a values(1)");
        stat.execute("insert into b values(1)");
        stat.execute("insert into c values(1), (2)");
        rs = stat.executeQuery("explain select a.x, b.x, c.x from a " +
                "inner join b on a.x = b.x right outer join c on c.x = a.x");
        assertTrue(rs.next());
        sql = cleanRemarks(rs.getString(1));
        assertEquals("SELECT A.X, B.X, C.X FROM PUBLIC.C LEFT OUTER JOIN " +
                "( PUBLIC.A INNER JOIN PUBLIC.B ON A.X = B.X ) ON C.X = A.X", sql);
        rs = stat.executeQuery("select a.x, b.x, c.x from a " +
                "inner join b on a.x = b.x " +
                "right outer join c on c.x = a.x");
        // expected result: 1   1       1; null    null    2
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals("2", rs.getString(3));
        assertFalse(rs.next());
        stat.execute("drop table a, b, c");

        /*
        drop table a, b, c;
        create table a(x int);
        create table b(x int);
        create table c(x int, y int);
        insert into a values(1), (2);
        insert into b values(3);
        insert into c values(1, 3);
        insert into c values(4, 5);
        explain select * from a left outer join
        (b left outer join c on b.x = c.y) on a.x = c.x;
        select * from a left outer join
        (b left outer join c on b.x = c.y) on a.x = c.x;
         */
        stat.execute("create table a(x int)");
        stat.execute("create table b(x int)");
        stat.execute("create table c(x int, y int)");
        stat.execute("insert into a values(1), (2)");
        stat.execute("insert into b values(3)");
        stat.execute("insert into c values(1, 3)");
        stat.execute("insert into c values(4, 5)");
        rs = stat.executeQuery("explain select * from a " +
                "left outer join (b " +
                "left outer join c " +
                "on b.x = c.y) " +
                "on a.x = c.x");
        assertTrue(rs.next());
        sql = cleanRemarks(rs.getString(1));
        assertEquals("SELECT A.X, B.X, C.X, C.Y FROM PUBLIC.A " +
                "LEFT OUTER JOIN ( PUBLIC.B " +
                "LEFT OUTER JOIN PUBLIC.C " +
                "ON B.X = C.Y ) " +
                "ON A.X = C.X", sql);
        rs = stat.executeQuery("select * from a " +
                "left outer join (b " +
                "left outer join c " +
                "on b.x = c.y) " +
                "on a.x = c.x");
        // expected result: 1   3       1       3;  2       null    null    null
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("3", rs.getString(2));
        assertEquals("1", rs.getString(3));
        assertEquals("3", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertEquals(null, rs.getString(4));
        assertFalse(rs.next());
        stat.execute("drop table a, b, c");

        stat.execute("create table a(x int primary key)");
        stat.execute("insert into a values(0), (1)");
        stat.execute("create table b(x int primary key)");
        stat.execute("insert into b values(0)");
        stat.execute("create table c(x int primary key)");
        rs = stat.executeQuery("select a.*, b.*, c.* from a " +
                "left outer join (b " +
                "inner join c " +
                "on b.x = c.x) " +
                "on a.x = b.x");
        // expected result: 0, null, null; 1, null, null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        rs = stat.executeQuery("select * from a " +
                "left outer join b on a.x = b.x " +
                "inner join c on b.x = c.x");
        // expected result: -
        assertFalse(rs.next());
        rs = stat.executeQuery("select * from a " +
                "left outer join b on a.x = b.x " +
                "left outer join c on b.x = c.x");
        // expected result: 0   0       null; 1       null    null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals("0", rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());

        rs = stat.executeQuery("select * from a " +
                "left outer join (b " +
                "inner join c on b.x = c.x) on a.x = b.x");
        // expected result: 0   null    null; 1       null    null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        rs = stat.executeQuery("explain select * from a " +
                "left outer join (b " +
                "inner join c on c.x = 1) on a.x = b.x");
        assertTrue(rs.next());
        sql = cleanRemarks(rs.getString(1));
        assertEquals("SELECT A.X, B.X, C.X FROM PUBLIC.A " +
                "LEFT OUTER JOIN ( PUBLIC.B " +
                "INNER JOIN PUBLIC.C ON C.X = 1 ) ON A.X = B.X", sql);
        stat.execute("drop table a, b, c");

        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test values(0), (1), (2)");
        rs = stat.executeQuery("select * from test a " +
                "left outer join (test b " +
                "inner join test c on b.id = c.id - 2) on a.id = b.id + 1");
        // drop table test;
        // create table test(id int primary key);
        // insert into test values(0), (1), (2);
        // select * from test a left outer join
        // (test b inner join test c on b.id = c.id - 2) on a.id = b.id + 1;
        // expected result: 0 null null; 1 0 2; 2 null null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("0", rs.getString(2));
        assertEquals("2", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        stat.execute("drop table test");

        stat.execute("create table a(pk int, val varchar(255))");
        stat.execute("create table b(pk int, val varchar(255))");
        stat.execute("create table base(pk int, deleted int)");
        stat.execute("insert into base values(1, 0)");
        stat.execute("insert into base values(2, 1)");
        stat.execute("insert into base values(3, 0)");
        stat.execute("insert into a values(1, 'a')");
        stat.execute("insert into b values(2, 'a')");
        stat.execute("insert into b values(3, 'a')");
        rs = stat.executeQuery("explain select a.pk, a_base.pk, b.pk, b_base.pk " +
                "from a " +
                "inner join base a_base on a.pk = a_base.pk " +
                "left outer join (b inner join base b_base " +
                "on b.pk = b_base.pk and b_base.deleted = 0) on 1=1");
        assertTrue(rs.next());
        sql = cleanRemarks(rs.getString(1));
        assertEquals("SELECT A.PK, A_BASE.PK, B.PK, B_BASE.PK " +
                "FROM PUBLIC.BASE A_BASE " +
                "LEFT OUTER JOIN ( PUBLIC.B " +
                "INNER JOIN PUBLIC.BASE B_BASE " +
                "ON (B_BASE.DELETED = 0) AND (B.PK = B_BASE.PK) ) " +
                "ON TRUE INNER JOIN PUBLIC.A ON 1=1 WHERE A.PK = A_BASE.PK", sql);
        rs = stat.executeQuery("select a.pk, a_base.pk, b.pk, b_base.pk from a " +
                "inner join base a_base on a.pk = a_base.pk " +
                "left outer join (b inner join base b_base " +
                "on b.pk = b_base.pk and b_base.deleted = 0) on 1=1");
        // expected: 1    1   3   3
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("3", rs.getString(3));
        assertEquals("3", rs.getString(3));
        assertFalse(rs.next());
        stat.execute("drop table a, b, base");

        // while (rs.next()) {
        //     for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
        //         System.out.print(rs.getString(i + 1) + " ");
        //     }
        //     System.out.println();
        // }

        conn.close();
        deleteDb("outerJoins");
    }

    private static String cleanRemarks(String sql) {
        ScriptReader r = new ScriptReader(new StringReader(sql));
        r.setSkipRemarks(true);
        sql = r.readStatement();
        sql = sql.replaceAll("\\n", " ");
        while (sql.contains("  ")) {
            sql = sql.replaceAll("  ", " ");
        }
        return sql;
    }

}
