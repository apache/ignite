/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.test.db.Db;
import org.h2.test.db.Db.Prepared;
import org.h2.util.New;

/**
 * This test executes random SQL statements to test if optimizations are working
 * correctly.
 */
public class TestFuzzOptimizations extends TestBase {

    private Connection conn;

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
        deleteDb(getTestName());
        conn = getConnection(getTestName());
        if (!config.diskResult) {
            testIn();
        }
        testGroupSorted();
        testInSelect();
        conn.close();
        deleteDb(getTestName());
    }

    /*
        drop table test0;
        drop table test1;
        create table test0(a int, b int, c int);
        create index idx_1 on test0(a);
        create index idx_2 on test0(b, a);
        create table test1(a int, b int, c int);
        insert into test0 select x / 100,
            mod(x / 10, 10), mod(x, 10)
            from system_range(0, 999);
        update test0 set a = null where a = 9;
        update test0 set b = null where b = 9;
        update test0 set c = null where c = 9;
        insert into test1 select * from test0;

        select * from test0 where
        b in(null, 0) and a in(2, null, null)
        order by 1, 2, 3;

        select * from test1 where
        b in(null, 0) and a in(2, null, null)
        order by 1, 2, 3;
     */
    private void testIn() throws SQLException {
        Db db = new Db(conn);
        db.execute("create table test0(a int, b int, c int)");
        db.execute("create index idx_1 on test0(a)");
        db.execute("create index idx_2 on test0(b, a)");
        db.execute("create table test1(a int, b int, c int)");
        db.execute("insert into test0 select x / 100, " +
                "mod(x / 10, 10), mod(x, 10) from system_range(0, 999)");
        db.execute("update test0 set a = null where a = 9");
        db.execute("update test0 set b = null where b = 9");
        db.execute("update test0 set c = null where c = 9");
        db.execute("insert into test1 select * from test0");

        // this failed at some point
        Prepared p = db.prepare("select * from test0 where b in(" +
                "select a from test1 where a <? and a not in(" +
                "select c from test1 where b <=10 and a in(" +
                "select a from test1 where b =1 or b =2 and b not in(2))) or c <>a) " +
                "and c in(0, 10) and c in(10, 0, 0) order by 1, 2, 3");
        p.set(1);
        p.execute();

        Random seedGenerator = new Random();
        String[] columns = new String[] { "a", "b", "c" };
        String[] values = new String[] { null, "0", "0", "1", "2", "10", "a", "?" };
        String[] compares = new String[] { "in(", "not in(", "=", "=", ">",
                "<", ">=", "<=", "<>", "in(select", "not in(select" };
        int size = getSize(100, 1000);
        for (int i = 0; i < size; i++) {
            long seed = seedGenerator.nextLong();
            println("seed: " + seed);
            Random random = new Random(seed);
            ArrayList<String> params = New.arrayList();
            String condition = getRandomCondition(random, params, columns,
                    compares, values);
            String message = "seed: " + seed + " " + condition;
            executeAndCompare(condition, params, message);
            if (params.size() > 0) {
                for (int j = 0; j < params.size(); j++) {
                    String value = values[random.nextInt(values.length - 2)];
                    params.set(j, value);
                }
                executeAndCompare(condition, params, message);
            }
        }
        executeAndCompare("a >=0 and b in(?, 2) and a in(1, ?, null)", Arrays.asList("10", "2"),
                "seed=-6191135606105920350L");
        db.execute("drop table test0, test1");
    }

    private void executeAndCompare(String condition, List<String> params, String message) throws SQLException {
        PreparedStatement prep0 = conn.prepareStatement(
                "select * from test0 where " + condition
                + " order by 1, 2, 3");
        PreparedStatement prep1 = conn.prepareStatement(
                "select * from test1 where " + condition
                + " order by 1, 2, 3");
        for (int j = 0; j < params.size(); j++) {
            prep0.setString(j + 1, params.get(j));
            prep1.setString(j + 1, params.get(j));
        }
        ResultSet rs0 = prep0.executeQuery();
        ResultSet rs1 = prep1.executeQuery();
        assertEquals(message, rs0, rs1);
    }

    private String getRandomCondition(Random random, ArrayList<String> params,
            String[] columns, String[] compares, String[] values) {
        int comp = 1 + random.nextInt(4);
        StringBuilder buff = new StringBuilder();
        for (int j = 0; j < comp; j++) {
            if (j > 0) {
                buff.append(random.nextBoolean() ? " and " : " or ");
            }
            String column = columns[random.nextInt(columns.length)];
            String compare = compares[random.nextInt(compares.length)];
            buff.append(column).append(' ').append(compare);
            if (compare.endsWith("in(")) {
                int len = 1+random.nextInt(3);
                for (int k = 0; k < len; k++) {
                    if (k > 0) {
                        buff.append(", ");
                    }
                    String value = values[random.nextInt(values.length)];
                    buff.append(value);
                    if ("?".equals(value)) {
                        value = values[random.nextInt(values.length - 2)];
                        params.add(value);
                    }
                }
                buff.append(")");
            } else if (compare.endsWith("(select")) {
                String col = columns[random.nextInt(columns.length)];
                buff.append(" ").append(col).append(" from test1 where ");
                String condition = getRandomCondition(
                        random, params, columns, compares, values);
                buff.append(condition);
                buff.append(")");
            } else {
                String value = values[random.nextInt(values.length)];
                buff.append(value);
                if ("?".equals(value)) {
                    value = values[random.nextInt(values.length - 2)];
                    params.add(value);
                }
            }
        }
        return buff.toString();
    }

    private void testInSelect() {
        Db db = new Db(conn);
        db.execute("CREATE TABLE TEST(A INT, B INT)");
        db.execute("CREATE INDEX IDX ON TEST(A)");
        db.execute("INSERT INTO TEST SELECT X/4, MOD(X, 4) " +
                "FROM SYSTEM_RANGE(1, 16)");
        db.execute("UPDATE TEST SET A = NULL WHERE A = 0");
        db.execute("UPDATE TEST SET B = NULL WHERE B = 0");
        Random random = new Random();
        long seed = random.nextLong();
        println("seed: " + seed);
        for (int i = 0; i < 100; i++) {
            String column = random.nextBoolean() ? "A" : "B";
            String value = new String[] { "NULL", "0", "A", "B" }[random.nextInt(4)];
            String compare = random.nextBoolean() ? "A" : "B";
            int x = random.nextInt(3);
            String sql1 = "SELECT * FROM TEST T WHERE " + column + "+0 " +
                "IN(SELECT " + value +
                " FROM TEST I WHERE I." + compare + "=?) ORDER BY 1, 2";
            String sql2 = "SELECT * FROM TEST T WHERE " + column + " " +
                "IN(SELECT " + value +
                " FROM TEST I WHERE I." + compare + "=?) ORDER BY 1, 2";
            List<Map<String, Object>> a = db.prepare(sql1).set(x).query();
            List<Map<String, Object>> b = db.prepare(sql2).set(x).query();
            assertTrue("seed: " + seed + " sql: " + sql1 +
                    " a: " + a + " b: " + b, a.equals(b));
        }
        db.execute("DROP TABLE TEST");
    }

    private void testGroupSorted() {
        Db db = new Db(conn);
        db.execute("CREATE TABLE TEST(A INT, B INT, C INT)");
        Random random = new Random();
        long seed = random.nextLong();
        println("seed: " + seed);
        for (int i = 0; i < 100; i++) {
            Prepared p = db.prepare("INSERT INTO TEST VALUES(?, ?, ?)");
            p.set(new String[] { null, "0", "1", "2" }[random.nextInt(4)]);
            p.set(new String[] { null, "0", "1", "2" }[random.nextInt(4)]);
            p.set(new String[] { null, "0", "1", "2" }[random.nextInt(4)]);
            p.execute();
        }
        int len = getSize(1000, 3000);
        for (int i = 0; i < len / 10; i++) {
            db.execute("CREATE TABLE TEST_INDEXED AS SELECT * FROM TEST");
            int jLen = 1 + random.nextInt(2);
            for (int j = 0; j < jLen; j++) {
                String x = "CREATE INDEX IDX" + j + " ON TEST_INDEXED(";
                int kLen = 1 + random.nextInt(2);
                for (int k = 0; k < kLen; k++) {
                    if (k > 0) {
                        x += ",";
                    }
                    x += new String[] { "A", "B", "C" }[random.nextInt(3)];
                }
                db.execute(x + ")");
            }
            for (int j = 0; j < 10; j++) {
                String x = "SELECT ";
                for (int k = 0; k < 3; k++) {
                    if (k > 0) {
                        x += ",";
                    }
                    x += new String[] { "SUM(A)", "MAX(B)", "AVG(C)",
                            "COUNT(B)" }[random.nextInt(4)];
                    x += " S" + k;
                }
                x += " FROM ";
                String group = " GROUP BY ";
                int kLen = 1 + random.nextInt(2);
                for (int k = 0; k < kLen; k++) {
                    if (k > 0) {
                        group += ",";
                    }
                    group += new String[] { "A", "B", "C" }[random.nextInt(3)];
                }
                group += " ORDER BY 1, 2, 3";
                List<Map<String, Object>> a = db.query(x + "TEST" + group);
                List<Map<String, Object>> b = db.query(x + "TEST_INDEXED" + group);
                assertEquals(a.toString(), b.toString());
                assertTrue(a.equals(b));
            }
            db.execute("DROP TABLE TEST_INDEXED");
        }
        db.execute("DROP TABLE TEST");
    }

}
