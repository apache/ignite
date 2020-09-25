/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class GridSubqueryJoinOptimizerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static IgniteEx ignite;

    /** */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = (IgniteEx)startGridsMultiThreaded(2);
        cache = ignite.getOrCreateCache(new CacheConfiguration<>(CACHE_NAME));

        prepare();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Very simple case: all tables have an alias, all columns referred
     * through tables' alias.
     */
    @Test
    public void testSelectExpression1() {
        String outerSqlTemplate = "select e.name, (%s) from emp e order by 1";
        String subSql = "select d.name from dep d where d.id = e.dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case when tables has no aliases, but they are different.
     */
    @Test
    public void testSelectExpression2() {
        String outerSqlTemplate = "select name, (%s) dname from emp e order by 1";
        String subSql = "select name from dep where id = dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case when tables are the same, inner table has an alias.
     */
    @Test
    public void testSelectExpression3() {
        String outerSqlTemplate = "select name, (%s) ename from emp order by 1";
        String subSql = "select name from emp e where e.id = emp.id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case when tables are the same, outer table has an alias.
     */
    @Test
    public void testSelectExpression4() {
        String outerSqlTemplate = "select name, (%s) ename from emp e order by 1";
        String subSql = "select name from emp where e.id = id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case when tables are the same, the outer table has an alias, the inner has an additional filter.
     */
    @Test
    public void testSelectExpression5() {
        String outerSqlTemplate = "select name, (%s) ename from emp e order by 1";
        String subSql = "select name from emp where e.id = id and name = 'emp1'";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case like {@link #testSelectExpression1()}, but PK of the inner table is compound.
     */
    @Test
    public void testSelectExpressionCompoundPk() {
        String outerSqlTemplate = "select e.name, (%s) from emp e order by 1";
        String subSql = "select d.name from dep2 d where d.id = e.dep_id and d.id2 = e.id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case where several subqueries used as several indeoendent columns.
     */
    @Test
    public void testSelectExpressionMultiple1() {
        String outerSqlTemplate = "select (%s), (%s), (%s) from emp e1 order by 2, 3";

        String subSql1 = "select 42";
        String subSql2 = "select e2.name from emp e2 where e2.id = e1.id";
        String subSql3 = "select d.name from dep d where d.id = dep_id";

        String resSql = String.format(outerSqlTemplate, subSql1, subSql2, subSql3);

        check(resSql, 2);
    }

    /**
     * Case where several subqueries used within a function.
     */
    @Test
    public void testSelectExpressionMultiple2() {
        String outerSqlTemplate = "select name, (%s) as lbl from emp e1 order by 1";
        String subSql = "(select 'prefix ' as pref_val) " +
            "|| (select name from dep2 dn where dn.id = dep_id and dn.id2 = dep_id)";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 2);
    }

    /**
     * Simple case to ensure subqueries correctly pulled out from table list.
     */
    @Test
    public void testTableList1() {
        String outerSqlTemplate = "select e.name, d.name from emp e, (%s) d where e.dep_id = d.id order by 1, 2";
        String subSql = "select dd.name, dd.id from dep dd where dd.id < 100";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure subqueries correctly pulled out from table list, but tables the same.
     */
    @Test
    public void testTableList2() {
        String outerSqlTemplate = "select d1.name, d2.name from dep d1, (%s) d2 where d1.id = d2.id order by 1, 2";
        String subSql = "select d.name, d.id from dep d where d.id < 100";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * The same as {@link #testTableList2()}, but inner table has no alias.
     */
    @Test
    public void testTableList3() {
        String outerSqlTemplate = "select d1.name, d2.name from dep d1, (%s) d2 where d1.id = d2.id order by 1, 2";
        String subSql = "select name, id from dep where id < 100";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * The same as {@link #testTableList2()}, but both tables have no aliases.
     */
    @Test
    public void testTableList4() {
        String outerSqlTemplate = "select name from dep, (%s) d where dep.id = d.id order by 1";
        String subSql = "select id from dep where id < 100";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure subqueries correctly pulled out from table list, but inner table has no filters.
     */
    @Test
    public void testTableList5() {
        String outerSqlTemplate = "select name from dep, (%s) d where dep.id = (MOD(d.id, 7) + 1) order by 1";
        String subSql = "select id from dep";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure simple hierarchical subqueries properly pulled out from table list.
     */
    @Test
    public void testTableList6() {
        String outerSqlTemplate = "select name from (%s) d where d.id < 100 order by 1";
        String subSql = "select * from (select id, name from dep)";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure explicitly declared inner join with explicit join condition
     * properly pulled out from table list.
     */
    @Test
    public void testTableList7() {
        String outerSqlTemplate = "select e.name from emp e inner join (%s) d on e.id = d.id where d.id < 100 order by 1";
        String subSql = "select id from dep";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure explicitly declared inner join with implicit join condition
     * properly pulled out from table list.
     */
    @Test
    public void testTableList8() {
        String outerSqlTemplate = "select e.name from emp e inner join (%s) d where e.id = d.id and d.id < 100 order by 1";
        String subSql = "select id from dep";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure explicitly declared left join with explicit join condition
     * properly pulled out from table list.
     */
    @Test
    public void testTableList9() {
        String outerSqlTemplate = "select e.name from emp e left join (%s) d on e.id = d.id where d.id < 100 order by 1";
        String subSql = "select id from dep";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure explicitly declared left join without join condition
     * properly pulled out from table list.
     */
    @Test
    public void testTableList10() {
        String outerSqlTemplate = "select e.name from emp e left join (%s) d where e.id = d.id and d.id < 100 order by 1";
        String subSql = "select id from dep";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * The same as {@link #testTableList2()}, but both tables have no aliases.
     */
    @Test
    public void testTableListMultiple() {
        String outerSqlTemplate = "select name from dep, (%s) d, (%s) e where dep.id = d.id order by 1";
        String subSql1 = "select id from dep where id < 100";
        String subSql2 = "select id from emp where id < 100";

        String resSql = String.format(outerSqlTemplate, subSql1, subSql2);

        check(resSql, 1);
    }

    /**
     * Simple case where all tables have an alias, all columns referred
     * through tables' alias.
     */
    @Test
    public void testExistsClause1() {
        String outerSqlTemplate = "select e.id, e.name from emp e where exists (%s) order by 1";
        String subSql = "select 1 from dep d where d.id = e.dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but inner table has no alias.
     */
    @Test
    public void testExistsClause2() {
        String outerSqlTemplate = "select e.id, e.name from emp e where exists (%s) order by 1";
        String subSql = "select 1 from dep where id = e.dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but outer table has no alias.
     */
    @Test
    public void testExistsClause3() {
        String outerSqlTemplate = "select id, name from emp where exists (%s) order by 1";
        String subSql = "select 1 from dep d where d.id = dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but both tables have no aliases.
     */
    @Test
    public void testExistsClause4() {
        String outerSqlTemplate = "select id, name from emp where exists (%s) order by 1";
        String subSql = "select 1 from dep where id = dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but tables are the same.
     */
    @Test
    public void testExistsClause5() {
        String outerSqlTemplate = "select id, name from emp e where exists (%s) order by 1";
        String subSql = "select 1 from emp where id = e.id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but inner table has coumpound PK.
     */
    @Test
    public void testExistsClauseCompoundPk() {
        String outerSqlTemplate = "select id, name from emp e where exists (%s) order by 1";
        String subSql = "select 1 from dep2 where id = e.id and id2 = 12";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case with multiple EXISTS clauses.
     */
    @Test
    public void testExistsClauseMultiple() {
        String outerSqlTemplate = "select id, name from dep2 d where exists (%s) and exists (%s) order by 1";
        String subSql1 = "select 1 from emp where d.id = dep_id and id = 3";
        String subSql2 = "select 1 from emp where d.id2 = dep_id and id = 3";

        String resSql = String.format(outerSqlTemplate, subSql1, subSql2);

        check(resSql, 1);
    }

    /**
     * Simple case where all tables have an alias, all columns referred
     * through tables' alias.
     */
    @Test
    public void testInClause1() {
        String outerSqlTemplate = "select e.id, e.name from emp e where e.dep_id in (%s) order by 1";
        String subSql = "select d.id from dep d";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but inner table has no alias.
     */
    @Test
    public void testInClause2() {
        String outerSqlTemplate = "select e.id, e.name from emp e where e.dep_id in (%s) order by 1";
        String subSql = "select id from dep";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but outer table has no alias.
     */
    @Test
    public void testInClause3() {
        String outerSqlTemplate = "select id, name from emp where dep_id in (%s) order by 1";
        String subSql = "select d.id from dep d";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but both tables have no aliases.
     */
    @Test
    public void testInClause4() {
        String outerSqlTemplate = "select id, name from emp where dep_id in (%s) order by 1";
        String subSql = "select id from dep";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but tables are the same.
     */
    @Test
    public void testInClause5() {
        String outerSqlTemplate = "select id, name from emp e where id in (%s) order by 1";
        String subSql = "select id from emp";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure that array on the left of the IN clause is handled properly.
     */
    @Test
    public void testInClause6() {
        String outerSqlTemplate = "select id, name from emp e where (id, name) in (%s) order by 1";
        String subSql = "select (id, name) from emp";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case to ensure that expression on the left of the IN clause is handled properly.
     */
    @Test
    public void testInClause7() {
        String outerSqlTemplate = "select id, name from emp e where (2 + abs(id)) in (%s) order by 1";
        String subSql = "select id from emp";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Simple case, but inner table has coumpound PK.
     */
    @Test
    public void testInClauseCompoundPk() {
        String outerSqlTemplate = "select id, name from emp e where id in (%s) order by 1";
        String subSql = "select id from dep2 where id2 = 12";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 1);
    }

    /**
     * Case with multiple IN clauses.
     */
    @Test
    public void testInClauseMultiple() {
        String outerSqlTemplate = "select id, name from dep2 d where id in (%s) and id2 in (%s) order by 1";
        String subSql = "select id from emp";

        String resSql = String.format(outerSqlTemplate, subSql, subSql);

        check(resSql, 1);
    }

    /**
     * Test should verify cases with UNION ALL.
     */
    @Test
    public void testOptimizationAppliedToUnion() {
        String sql = "" +
            "select emp.name, d.id from emp, (select * from dep) d " +
            "union all " +
            "select (select name from dep2 where id = 1 and id2 = 1) as dep_name, d.id " +
            "from dep d where exists (select 1 from dep where id = d.id)";

        check(sql, 1);
    }

    /**
     * Test should verify all cases where subquery should not be rewrited.
     */
    @Test
    public void testOptimizationShouldNotBeApplied1() {
        String sql = "" +
            // follow should not be rewrited beacuse of aggregates
            "select (select max(id) from emp) f1," +
            "       (select sum(id) from emp) f2," +
            "       (select distinct id from emp where id = 1) f3," +
            "       (select distinct(id) from emp where id = 1) f4," +
            "       (select id from emp where id = 1 group by id) f5," +
            "       (select id from emp where id = 1 group by 1) f6," +
            "       (select id from emp limit 1) f7," +
            "       (select id from emp where id = 2 offset 2) f8," +
            // and this one because dep2 has compound pk (id, id2),
            // so predicate over 'id' could not guarantee uniqueness
            "       (select id from dep2 where id = 2) f9" +
            " from dep";

        check(sql, 10);
    }

    /**
     * Test should verify that optimization won't be applied when columns of the compound index
     * connect by OR.
     */
    @Test
    public void testOptimizationShouldNotBeApplied2() {
        String outerSqlTemplate = "select e.id, e.name from emp e where exists (%s) order by 1";
        String subSql = "select 1 from dep2 d2 where d2.id = e.id or d2.id2 = e.id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 2);
    }

    /**
     * Case to ensure subquery with join won't be pulled out from table list.
     */
    @Test
    public void testOptimizationShouldNotBeApplied3() {
        String outerSqlTemplate = "select e.name from emp e, (%s) d order by 1 limit 10";
        String subSql = "select d1.id, d2.name from dep d1, dep d2";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 2 + 1/* explain reports one additional SELECT in comments */);
    }

    /**
     * Case to ensure subquery with join won't be pulled out from EXISTS.
     */
    @Test
    public void testOptimizationShouldNotBeApplied4() {
        String outerSqlTemplate = "select e.name from emp e where exists (%s) order by 1 limit 10";
        String subSql = "select 1 from dep d1, dep d2 where d1.id = d2.id and d1.id = e.id";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 2);
    }

    /**
     * Test should verify that optimization won't be applied when columns of the compound index
     * connect by OR (ver 2).
     */
    @Test
    public void testOptimizationShouldNotBeApplied5() {
        String outerSqlTemplate = "select e.id, e.name from emp e where exists (%s) order by 1";
        String subSql = "select 1 from dep2 d2 where 1 = 1 and (d2.id = e.id or d2.id2 = e.id)";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 2);
    }

    /**
     * Test should verify that optimization won't be applied when columns of the compound index
     * connect by OR (ver 3).
     */
    @Test
    public void testOptimizationShouldNotBeApplied6() {
        String outerSqlTemplate = "select e.id, e.name from emp e where exists (%s) order by 1";
        String subSql = "select 1 from dep2 d2 where 1 = 1 and (d2.id = e.id and (d2.id2 = 1 or d2.id2 = 2))";

        String resSql = String.format(outerSqlTemplate, subSql);

        check(resSql, 2);
    }

    /**
     * @param sql Sql.
     * @param expSelectClauses Expected select clauses.
     */
    private void check(String sql, int expSelectClauses) {
        optimizationEnabled(false);

        List<List<?>> exp = cache.query(new SqlFieldsQuery(sql)).getAll();

        optimizationEnabled(true);

        List<List<?>> act = cache.query(new SqlFieldsQuery(sql).setEnforceJoinOrder(true)).getAll();

        Assert.assertEquals("Result set mismatch", exp, act);

        String plan = cache.query(new SqlFieldsQuery("explain " + sql)).getAll().get(0).get(0).toString();

        System.out.println(plan);

        int actCnt = countEntries(plan, "SELECT");

        Assert.assertEquals(String.format("SELECT-clause count mismatch: exp=%d, act=%d, plan=[%s]",
            expSelectClauses, actCnt, plan), expSelectClauses, actCnt);
    }

    /**
     * Creates all neccessary tables and inserts data.
     */
    private void prepare() {
        Random rnd = new Random();

        cache.query(new SqlFieldsQuery("CREATE TABLE dep (id LONG PRIMARY KEY, name VARCHAR, dep_name VARCHAR)"));
        cache.query(new SqlFieldsQuery("CREATE TABLE dep2 (id LONG, id2 LONG, name VARCHAR, PRIMARY KEY(id, id2))"));
        cache.query(new SqlFieldsQuery("CREATE TABLE emp (id LONG PRIMARY KEY, name VARCHAR, dep_id LONG)"));

        for (int i = 0; i < 20; i++) {
            cache.query(new SqlFieldsQuery("insert into dep (id, name, dep_name) values(?, ?, ?)")
                .setArgs(i, "dep" + i, "dep" + i));

            cache.query(new SqlFieldsQuery("insert into dep2 (id, id2, name) values(?, ?, ?)")
                .setArgs(i, i, "dep" + i));

            cache.query(new SqlFieldsQuery("insert into emp (id, name, dep_id) values(?, ?, ?)")
                .setArgs(i, "emp" + i, i < 10 ? rnd.nextInt(10) : null));
        }
    }

    /**
     * Count of entries of substring in string.
     *
     * @param where Where to search.
     * @param what What to search.
     * @return Count of entries or -1 if non is found.
     */
    private int countEntries(String where, String what) {
        return where.split(what).length - 1;
    }

    /** */
    private void optimizationEnabled(boolean enabled) {
        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_SUBQUERY_REWRITE_OPTIMIZATION, String.valueOf(enabled));

        GridTestUtils.setFieldValue(GridSubqueryJoinOptimizer.class, "optimizationEnabled", null);
    }
}
