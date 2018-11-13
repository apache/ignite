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

import java.util.List;
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
    private static final int CLIENT = 7;

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
        startGridsMultiThreaded(2, false);
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
        sql("CREATE TABLE client (id int PRIMARY KEY, name varchar)");
        sql("CREATE TABLE good (id int PRIMARY KEY, name varchar)");
        sql("CREATE TABLE orders (id int PRIMARY KEY, cliId int, goodId int, price int, comment varchar)");

        // Spit subquery
        String qry =
            "SELECT cli.name, good.name \n" +
            "FROM " +
            "(SELECT DISTINCT cliId, goodId FROM orders) as ord \n" +
            "LEFT JOIN client cli ON cli.id = ord.cliId \n" +
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

    /**
     *
     */
    public void testTwoJoinWithSubqueryPushDown() {
        sql("CREATE TABLE product (id int PRIMARY KEY, name varchar)");
        sql("CREATE TABLE version (id int PRIMARY KEY, prodId int, name varchar)");
        sql("CREATE TABLE build (id int PRIMARY KEY, verId int, name varchar, ts timestamp)");

        String qry =
            "SELECT prod.name, MAX(bld.ts)\n" +
            "FROM " +
            "product prod \n" +
            "LEFT JOIN version ver ON ver.prodId = prod.id \n" +
            "LEFT JOIN (SELECT DISTINCT id, verId, ts FROM build) as bld ON bld.verId = ver.id \n" +
            "GROUP BY prod.id ";

        assertTrue(F.isEmpty(sql(qry)));

        List<List<?>> plan = sql("EXPLAIN " + qry);

        // Plan must contains 3 map queries (for each join relation) + 1 reduce query.
        assertEquals("Invalid plan: " + plan, 4, plan.size());

        // Trivial split test
        qry =
            "SELECT prod.name, MAX(bld.ts)\n" +
                "FROM " +
                "product prod \n" +
                "LEFT JOIN version ver ON ver.prodId = prod.id \n" +
                "LEFT JOIN (SELECT id, verId, ts FROM build) as bld ON bld.verId = ver.id \n" +
                "GROUP BY prod.id ";

        assertTrue(F.isEmpty(sql(qry)));

        plan = sql("EXPLAIN " + qry);

        // Trivial two-step plan is expected.
        assertEquals("Invalid plan: " + plan, 2, plan.size());
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
        return grid(0).context().query().querySqlFields(
            new SqlFieldsQuery(sql), false).getAll();
    }
}
