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

package org.apache.ignite.internal.processors.query;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for correct distributed partitioned queries.
 */
public class IgniteSqlSplitterLeftJoinWithSubqueryTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** */
    private static final int CLIENT = NODES_CNT;

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT, false);

        Ignition.setClientMode(true);
        try {
            startGrid(CLIENT);
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /**
     *
     */
    public void testTwoJoinWithSubquery() {
        try {
            sql("CREATE TABLE client (id int PRIMARY KEY, name varchar)");
            sql("CREATE TABLE good (id int PRIMARY KEY, name varchar)");
            sql("CREATE TABLE orders (id int PRIMARY KEY, cliId int, goodId int, price int, comment varchar)");

            // Spit subquery
            String qry =
                "SELECT cli.name, good.name " +
                    "FROM " +
                    "(SELECT DISTINCT cliId, goodId FROM orders) as ord " +
                    "LEFT JOIN client cli ON cli.id = ord.cliId " +
                    "LEFT JOIN good good ON good.id = ord.goodId WHERE cli.id = 1";

            assertTrue(F.isEmpty(sql(qry)));

            List<List<?>> plan = sql("EXPLAIN " + qry);

            // Plan must contains 3 map queries (for each join relation) + 1 reduce query.
            assertEquals("Invalid plan: " + plan, 4, plan.size());

            printPlan(plan);

        // Trivial split test
        qry =
            "SELECT cli.name, good.name \n" +
            "FROM " +
            "(SELECT cliId, goodId FROM orders) as ord \n" +
            "LEFT JOIN client cli ON cli.id = ord.cliId \n" +
            "LEFT JOIN good good ON good.id = ord.goodId";

            assertTrue(F.isEmpty(sql(qry)));

            plan = sql("EXPLAIN " + qry);

            // Trivial two-step plan is expected.
            assertEquals("Invalid plan: " + plan, 2, plan.size());
        }
        finally {
            sql("DROP TABLE client");
            sql("DROP TABLE good");
            sql("DROP TABLE orders");
        }
    }

    /**
     *
     */
    public void testSplitSubqueryWithChildrenNeedSplit() {
        try {
            sql("CREATE TABLE client (id int PRIMARY KEY, name varchar)");
            sql("CREATE TABLE orders (id int PRIMARY KEY, cliId int, goodId int, price int, comment varchar)");
            sql("CREATE TABLE special_offer (id int PRIMARY KEY, cliId int, detail varchar)");

            String qry =
                "SELECT allCli.name, special_offer.detail " +
                    "FROM " +
                    "client as allCli " +
                    "LEFT JOIN (SELECT cli.id as cliId, cli.name as cliName " +
                        "FROM " +
                        "client as cli " +
                        "INNER JOIN (SELECT DISTINCT cliId as cliId, sum(price) as totalSpent FROM orders group by cliId) as ordTotal " +
                            "ON cli.id = ordTotal.cliId " +
                        "WHERE totalSpent > 100) as best_cli ON best_cli.cliId = allCli.id " +
                    "LEFT JOIN special_offer on best_cli.cliId = special_offer.cliId ";

            assertTrue(F.isEmpty(sql(qry)));

            List<List<?>> plan = sql("EXPLAIN " + qry);

            printPlan(plan);
        }
        finally {
            sql("DROP TABLE client");
            sql("DROP TABLE orders");
            sql("DROP TABLE special_offer");
        }
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public void testSplitSubqueryWithChildrenNeedSplitWithData() {
        try {
            final int VALS = 100;

            sql("CREATE TABLE A (id int PRIMARY KEY, name varchar)");
            sql("CREATE TABLE VAL (id int, a_id int, val int, primary key (id, a_id)) with \"affinity_key=a_id\"");

            int val_k = 0;

            List<List<?>> expRes = new ArrayList<>();

            for (int i = 0; i < VALS; ++i) {
                sql(String.format("INSERT INTO A VALUES (%d, 'name_%d')", i, i));

                for (int val = i; val < i + VALS; ++val, ++val_k)
                    sql(String.format("INSERT INTO VAL VALUES (%d, %d, %d)", val_k, i, val));

                expRes.add(Arrays.asList(
                    i, i, i + VALS - 1, BigDecimal.valueOf((long)(2 * i + (VALS - 1)) * VALS / 2)));
            }

            String qry =
                "SELECT allA.id, min_max_sum_T.min_val, min_max_sum_T.max_val, min_max_sum_T.sum_val " +
                    "FROM " +
                    "A as allA " +
                    "LEFT JOIN (SELECT A.id as a_id, max_T0.max_val, min_sum_T.min_val, min_sum_T.sum_val " +
                        "FROM A " +
                        "LEFT JOIN (SELECT DISTINCT a_id, max(val) as max_val FROM VAL GROUP BY a_id) as max_T0 " +
                                "ON A.id = max_T0.a_id " +
                        "LEFT JOIN (SELECT A.id as a_id, min_T0.min_val as min_val, sum_T.sum_val as sum_val " +
                            "FROM A " +
                            "LEFT JOIN (SELECT DISTINCT a_id, min(val) as min_val FROM VAL GROUP BY a_id) as min_T0 " +
                                    "ON A.id = min_T0.a_id " +
                            "LEFT JOIN (SELECT A.id as a_id, sum_T0.sum_val as sum_val " +
                                "FROM A " +
                                "LEFT JOIN (SELECT DISTINCT a_id, SUM(val) as sum_val FROM VAL GROUP BY a_id) as sum_T0 " +
                                        "ON A.id = sum_T0.a_id " +
                                ") as sum_T ON A.id = sum_T.a_id " +
                            ") as min_sum_T ON A.id = min_sum_T.a_id " +
                        ") as min_max_sum_T ON allA.id = min_max_sum_T.a_id";

            List<List<?>> res = sql(qry);

            Collections.sort(res, Comparator.comparing(o -> ((Integer)(o.get(0)))));

            assertEqualsCollections(expRes, res);
        }
        finally {
            sql("DROP TABLE A");
            sql("DROP TABLE VAL");
        }
    }

    /**
     *
     */
    public void testTreeJoinWithSubquery() {
        try {
            sql("CREATE TABLE product (id int PRIMARY KEY, name varchar)");
            sql("CREATE TABLE version (id int PRIMARY KEY, prodId int, name varchar)");
            sql("CREATE TABLE build (id int PRIMARY KEY, verId int, name varchar, ts timestamp)");

            String qry =
                "SELECT count(1) " +
                    "FROM " +
                    "version ver " +
                    "LEFT JOIN (SELECT DISTINCT id, verId, ts FROM build) as bld ON bld.verId = ver.id " +
                    "WHERE bld.id = 1";

            sql(qry);

            List<List<?>> plan = sql("EXPLAIN " + qry);

            printPlan(plan);

            qry =
                "SELECT prod.name, MAX(bld.ts) " +
                    "FROM " +
                    "product prod " +
                    "LEFT JOIN version ver ON ver.prodId = prod.id " +
                    "LEFT JOIN (SELECT DISTINCT id, verId, ts FROM build) as bld ON bld.verId = ver.id " +
                    "GROUP BY prod.id ";

            assertTrue(F.isEmpty(sql(qry)));

            plan = sql("EXPLAIN " + qry);

            // Plan must contains 3 map queries (for each join relation) + 1 reduce query.
            assertEquals("Invalid plan: " + plan, 4, plan.size());

            // Trivial split test
            qry =
                "SELECT prod.name, MAX(bld.ts) " +
                    "FROM " +
                    "product prod " +
                    "LEFT JOIN version ver ON ver.prodId = prod.id " +
                    "LEFT JOIN (SELECT id, verId, ts FROM build) as bld ON bld.verId = ver.id " +
                    "GROUP BY prod.id ";

            assertTrue(F.isEmpty(sql(qry)));

            plan = sql("EXPLAIN " + qry);

            // Trivial two-step plan is expected.
            assertEquals("Invalid plan: " + plan, 2, plan.size());
        }
        finally {
            sql("DROP TABLE product");
            sql("DROP TABLE version");
            sql("DROP TABLE build");
        }
    }

    /**
     * @param plan Ignite query plan.
     */
    private void printPlan(List<List<?>> plan) {
        for (int i = 0; i < plan.size() - 1; ++i)
            System.out.println("MAP #" + i + ": " + plan.get(i).get(0));

        System.out.println("REDUCE: " + plan.get(plan.size() - 1).get(0));
    }

    /**
     * @param sql Query.
     * @return Result.
     */
    private List<List<?>> sql(String sql) {
        return grid(CLIENT).context().query().querySqlFields(
            new SqlFieldsQuery(sql), false).getAll();
    }
}
