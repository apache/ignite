/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.h2.test.TestBase;
import org.h2.util.StatementBuilder;

/**
 * Test for optimizer hint SET FORCE_JOIN_ORDER.
 *
 * @author Sergi Vladykin
 */
public class TestOptimizerHints extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("testOptimizerHints");
        Connection conn = getConnection("testOptimizerHints;FORCE_JOIN_ORDER=1");
        Statement s = conn.createStatement();

        s.execute("create table t1(id int unique)");
        s.execute("create table t2(id int unique, t1_id int)");
        s.execute("create table t3(id int unique)");
        s.execute("create table t4(id int unique, t2_id int, t3_id int)");

        String plan;

        plan = plan(s, "select * from t1, t2 where t1.id = t2.t1_id");
        assertContains(plan, "INNER JOIN PUBLIC.T2");

        plan = plan(s, "select * from t2, t1 where t1.id = t2.t1_id");
        assertContains(plan, "INNER JOIN PUBLIC.T1");

        plan = plan(s, "select * from t2, t1 where t1.id = 1");
        assertContains(plan, "INNER JOIN PUBLIC.T1");

        plan = plan(s, "select * from t2, t1 where t1.id = t2.t1_id and t2.id = 1");
        assertContains(plan, "INNER JOIN PUBLIC.T1");

        plan = plan(s, "select * from t1, t2 where t1.id = t2.t1_id and t2.id = 1");
        assertContains(plan, "INNER JOIN PUBLIC.T2");

        checkPlanComma(s, "t1", "t2", "t3", "t4");
        checkPlanComma(s, "t4", "t2", "t3", "t1");
        checkPlanComma(s, "t2", "t1", "t3", "t4");
        checkPlanComma(s, "t1", "t4", "t3", "t2");
        checkPlanComma(s, "t2", "t1", "t4", "t3");
        checkPlanComma(s, "t4", "t3", "t2", "t1");

        boolean on = false;
        boolean left = false;

        checkPlanJoin(s, on, left, "t1", "t2", "t3", "t4");
        checkPlanJoin(s, on, left, "t4", "t2", "t3", "t1");
        checkPlanJoin(s, on, left, "t2", "t1", "t3", "t4");
        checkPlanJoin(s, on, left, "t1", "t4", "t3", "t2");
        checkPlanJoin(s, on, left, "t2", "t1", "t4", "t3");
        checkPlanJoin(s, on, left, "t4", "t3", "t2", "t1");

        on = false;
        left = true;

        checkPlanJoin(s, on, left, "t1", "t2", "t3", "t4");
        checkPlanJoin(s, on, left, "t4", "t2", "t3", "t1");
        checkPlanJoin(s, on, left, "t2", "t1", "t3", "t4");
        checkPlanJoin(s, on, left, "t1", "t4", "t3", "t2");
        checkPlanJoin(s, on, left, "t2", "t1", "t4", "t3");
        checkPlanJoin(s, on, left, "t4", "t3", "t2", "t1");

        on = true;
        left = false;

        checkPlanJoin(s, on, left, "t1", "t2", "t3", "t4");
        checkPlanJoin(s, on, left, "t4", "t2", "t3", "t1");
        checkPlanJoin(s, on, left, "t2", "t1", "t3", "t4");
        checkPlanJoin(s, on, left, "t1", "t4", "t3", "t2");
        checkPlanJoin(s, on, left, "t2", "t1", "t4", "t3");
        checkPlanJoin(s, on, left, "t4", "t3", "t2", "t1");

        on = true;
        left = true;

        checkPlanJoin(s, on, left, "t1", "t2", "t3", "t4");
        checkPlanJoin(s, on, left, "t4", "t2", "t3", "t1");
        checkPlanJoin(s, on, left, "t2", "t1", "t3", "t4");
        checkPlanJoin(s, on, left, "t1", "t4", "t3", "t2");
        checkPlanJoin(s, on, left, "t2", "t1", "t4", "t3");
        checkPlanJoin(s, on, left, "t4", "t3", "t2", "t1");

        s.close();
        conn.close();
        deleteDb("testOptimizerHints");
    }

    private void checkPlanComma(Statement s, String ... t) throws SQLException {
        StatementBuilder from = new StatementBuilder();
        for (String table : t) {
            from.appendExceptFirst(", ");
            from.append(table);
        }
        String plan = plan(s, "select 1 from " + from.toString() + " where t1.id = t2.t1_id "
                + "and t2.id = t4.t2_id and t3.id = t4.t3_id");
        int prev = plan.indexOf("FROM PUBLIC." + t[0].toUpperCase());
        for (int i = 1; i < t.length; i++) {
            int next = plan.indexOf("INNER JOIN PUBLIC." + t[i].toUpperCase());
            assertTrue("Wrong plan for : " + Arrays.toString(t) + "\n" + plan, next > prev);
            prev = next;
        }
    }

    private void checkPlanJoin(Statement s, boolean on, boolean left,
            String... t) throws SQLException {
        StatementBuilder from = new StatementBuilder();
        for (int i = 0; i < t.length; i++) {
            if (i != 0) {
                if (left) {
                    from.append(" left join ");
                } else {
                    from.append(" inner join ");
                }
            }
            from.append(t[i]);
            if (on && i != 0) {
                from.append(" on 1=1 ");
            }
        }
        String plan = plan(s, "select 1 from " + from.toString() + " where t1.id = t2.t1_id "
                + "and t2.id = t4.t2_id and t3.id = t4.t3_id");
        int prev = plan.indexOf("FROM PUBLIC." + t[0].toUpperCase());
        for (int i = 1; i < t.length; i++) {
            int next = plan.indexOf(
                    (!left ? "INNER JOIN PUBLIC." : on ? "LEFT OUTER JOIN PUBLIC." : "PUBLIC.") +
                    t[i].toUpperCase());
            if (prev > next) {
                System.err.println(plan);
                fail("Wrong plan for : " + Arrays.toString(t) + "\n" + plan);
            }
            prev = next;
        }
    }

    /**
     * @param s Statement.
     * @param query Query.
     * @return Plan.
     * @throws SQLException If failed.
     */
    private String plan(Statement s, String query) throws SQLException {
        ResultSet rs = s.executeQuery("explain " + query);
        assertTrue(rs.next());
        String plan = rs.getString(1);
        rs.close();
        return plan;
    }
}
