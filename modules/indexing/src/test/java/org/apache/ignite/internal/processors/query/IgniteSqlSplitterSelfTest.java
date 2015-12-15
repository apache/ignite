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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for correct distributed partitioned queries.
 */
@SuppressWarnings("unchecked")
public class IgniteSqlSplitterSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setIndexedTypes(idxTypes);
    }

    /**
     * Tests offset and limit clauses for query.
     * @throws Exception If failed.
     */
    public void testOffsetLimit() throws Exception {
        IgniteCache<Integer, Integer> c = ignite(0).getOrCreateCache(cacheConfig("ints", true,
            Integer.class, Integer.class));

        try {
            awaitPartitionMapExchange();

            List<Integer> res = new ArrayList<>();

            Random rnd = new GridRandom();

            for (int i = 0; i < 10; i++) {
                int val = rnd.nextInt(100);

                c.put(i, val);
                res.add(val);
            }

            Collections.sort(res);

            String qry = "select _val from Integer order by _val ";

            assertEqualsCollections(res, columnQuery(c, qry));
            assertEqualsCollections(res.subList(0, 0), columnQuery(c, qry + "limit ?", 0));
            assertEqualsCollections(res.subList(0, 3), columnQuery(c, qry + "limit ?", 3));
            assertEqualsCollections(res.subList(0, 9), columnQuery(c, qry + "limit ? offset ?", 9, 0));
            assertEqualsCollections(res.subList(3, 7), columnQuery(c, qry + "limit ? offset ?", 4, 3));
            assertEqualsCollections(res.subList(7, 9), columnQuery(c, qry + "limit ? offset ?", 2, 7));
            assertEqualsCollections(res.subList(8, 10), columnQuery(c, qry + "limit ? offset ?", 2, 8));
            assertEqualsCollections(res.subList(9, 10), columnQuery(c, qry + "limit ? offset ?", 1, 9));
            assertEqualsCollections(res.subList(10, 10), columnQuery(c, qry + "limit ? offset ?", 1, 10));
            assertEqualsCollections(res.subList(9, 10), columnQuery(c, qry + "limit ? offset abs(-(4 + ?))", 1, 5));
        }
        finally {
            c.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupIndexOperations() throws Exception {
        IgniteCache<Integer, GroupIndexTestValue> c = ignite(0).getOrCreateCache(cacheConfig("grp", false,
            Integer.class, GroupIndexTestValue.class));

        try {
            awaitPartitionMapExchange();

            // Check group index usage.
            String qry = "select 1 from GroupIndexTestValue ";

            String plan = columnQuery(c, "explain " + qry + "where a = 1 and b > 0")
                .get(0).toString();

            info("Plan: " + plan);

            assertTrue(plan.contains("grpIdx"));

            // Sorted list
            List<GroupIndexTestValue> list = F.asList(
                new GroupIndexTestValue(0, 0),
                new GroupIndexTestValue(0, 5),
                new GroupIndexTestValue(1, 1),
                new GroupIndexTestValue(1, 3),
                new GroupIndexTestValue(2, -1),
                new GroupIndexTestValue(2, 2)
            );

            // Fill cache.
            for (int i = 0; i < list.size(); i++)
                c.put(i, list.get(i));

            // Check results.
            assertEquals(1, columnQuery(c, qry + "where a = 1 and b = 1").size());
            assertEquals(0, columnQuery(c, qry + "where a = 1 and b = 2").size());
            assertEquals(1, columnQuery(c, qry + "where a = 1 and b = 3").size());
            assertEquals(2, columnQuery(c, qry + "where a = 1 and b < 4").size());
            assertEquals(2, columnQuery(c, qry + "where a = 1 and b <= 3").size());
            assertEquals(1, columnQuery(c, qry + "where a = 1 and b < 3").size());
            assertEquals(2, columnQuery(c, qry + "where a = 1 and b > 0").size());
            assertEquals(1, columnQuery(c, qry + "where a = 1 and b > 1").size());
            assertEquals(2, columnQuery(c, qry + "where a = 1 and b >= 1").size());

            assertEquals(4, columnQuery(c, qry + "where a > 0").size());
            assertEquals(4, columnQuery(c, qry + "where a >= 1").size());
            assertEquals(4, columnQuery(c, qry + "where b > 0").size());
            assertEquals(4, columnQuery(c, qry + "where b >= 1").size());

            assertEquals(4, columnQuery(c, qry + "where a < 2").size());
            assertEquals(4, columnQuery(c, qry + "where a <= 1").size());
            assertEquals(4, columnQuery(c, qry + "where b < 3").size());
            assertEquals(5, columnQuery(c, qry + "where b <= 3").size());

            assertEquals(3, columnQuery(c, qry + "where a > 0 and b > 0").size());
            assertEquals(2, columnQuery(c, qry + "where a > 0 and b >= 2").size());
            assertEquals(3, columnQuery(c, qry + "where a >= 1 and b > 0").size());
            assertEquals(2, columnQuery(c, qry + "where a >= 1 and b >= 2").size());

            assertEquals(3, columnQuery(c, qry + "where a > 0 and b < 3").size());
            assertEquals(2, columnQuery(c, qry + "where a > 0 and b <= 1").size());
            assertEquals(3, columnQuery(c, qry + "where a >= 1 and b < 3").size());
            assertEquals(2, columnQuery(c, qry + "where a >= 1 and b <= 1").size());

            assertEquals(2, columnQuery(c, qry + "where a < 2 and b < 3").size());
            assertEquals(2, columnQuery(c, qry + "where a < 2 and b <= 1").size());
            assertEquals(2, columnQuery(c, qry + "where a <= 1 and b < 3").size());
            assertEquals(2, columnQuery(c, qry + "where a <= 1 and b <= 1").size());

            assertEquals(3, columnQuery(c, qry + "where a < 2 and b > 0").size());
            assertEquals(2, columnQuery(c, qry + "where a < 2 and b >= 3").size());
            assertEquals(3, columnQuery(c, qry + "where a <= 1 and b > 0").size());
            assertEquals(2, columnQuery(c, qry + "where a <= 1 and b >= 3").size());
        }
        finally {
            c.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoins() throws Exception {
        CacheConfiguration ccfg = cacheConfig("persOrg", true,
            Integer.class, Person.class, Integer.class, Organization.class);

        IgniteCache<Integer, Object> c = ignite(0).getOrCreateCache(ccfg);

        try {
            awaitPartitionMapExchange();

            doTestDistributedJoins(c, 30, 100, 1000, false);
            doTestDistributedJoins(c, 30, 100, 1000, true);

            doTestDistributedJoins(c, 3, 10, 3, false);
            doTestDistributedJoins(c, 3, 10, 3, true);

            doTestDistributedJoins(c, 300, 2000, 5, false);
            doTestDistributedJoins(c, 300, 2000, 5, true);
        }
        finally {
            c.destroy();
        }
    }

    /**
     * @param c Cache.
     * @param orgs Number of organizations.
     * @param persons Number of persons.
     * @param pageSize Page size.
     * @param enforceJoinOrder Enforce join order.
     */
    private void doTestDistributedJoins(IgniteCache<Integer, Object> c, int orgs, int persons, int pageSize,
        boolean enforceJoinOrder) {
        assertEquals(0, c.size(CachePeekMode.ALL));

        int key = 0;

        for (int i = 0; i < orgs; i++) {
            Organization o = new Organization();

            o.name = "Org" + i;

            c.put(key++, o);
        }

        Random rnd = new GridRandom();

        for (int i = 0; i < persons; i++) {
            Person p = new Person();

            p.name = "Person" + i;
            p.orgId = rnd.nextInt(orgs);

            c.put(key++, p);
        }

        String select = "select count(*) from Organization o, Person p where p.orgId = o._key";

        String plan = (String)c.query(new SqlFieldsQuery("explain " + select)
            .setDistributedJoins(true).setEnforceJoinOrder(enforceJoinOrder).setPageSize(pageSize))
            .getAll().get(0).get(0);

        X.println("Plan : " + plan);

        if (enforceJoinOrder)
            assertTrue(plan, plan.contains("batched:broadcast"));
        else
            assertTrue(plan, plan.contains("batched:unicast"));

        assertEquals(Long.valueOf(persons), c.query(new SqlFieldsQuery(select).setDistributedJoins(true)
            .setEnforceJoinOrder(enforceJoinOrder).setPageSize(pageSize)).getAll().get(0).get(0));

        c.clear();

        assertEquals(0, c.size(CachePeekMode.ALL));
        assertEquals(0L, c.query(new SqlFieldsQuery(select).setDistributedJoins(true)
            .setEnforceJoinOrder(enforceJoinOrder).setPageSize(pageSize)).getAll().get(0).get(0));
    }

    /**
     * @param c Cache.
     * @param qry Query.
     * @param args Arguments.
     * @return Column as list.
     */
    private static <X> List<X> columnQuery(IgniteCache<?,?> c, String qry, Object... args) {
        return column(0, c.query(new SqlFieldsQuery(qry).setArgs(args)).getAll());
    }

    /**
     * @param idx Column index.
     * @param rows Rows.
     * @return Column as list.
     */
    private static <X> List<X> column(int idx, List<List<?>> rows) {
        List<X> res = new ArrayList<>(rows.size());

        for (List<?> row : rows)
            res.add((X)row.get(idx));

        return res;
    }

    /**
     *
     */
    public void testFunctionNpe() {
        IgniteCache<Integer, User> userCache = ignite(0).createCache(
            cacheConfig("UserCache", true, Integer.class, User.class));
        IgniteCache<Integer, UserOrder> userOrderCache = ignite(0).createCache(
            cacheConfig("UserOrderCache", true, Integer.class, UserOrder.class));
        IgniteCache<Integer, OrderGood> orderGoodCache = ignite(0).createCache(
            cacheConfig("OrderGoodCache", true, Integer.class, OrderGood.class));

        try {
            String sql =
                "SELECT a.* FROM (" +
                    "SELECT CASE WHEN u.id < 100 THEN u.id ELSE ug.id END id " +
                    "FROM \"UserCache\".User u, UserOrder ug " +
                    "WHERE u.id = ug.userId" +
                    ") a, (" +
                    "SELECT CASE WHEN og.goodId < 5 THEN 100 ELSE og.goodId END id " +
                    "FROM UserOrder ug, \"OrderGoodCache\".OrderGood og " +
                    "WHERE ug.id = og.orderId) b " +
                    "WHERE a.id = b.id";

            userOrderCache.query(new SqlFieldsQuery(sql)).getAll();
        }
        finally {
            userCache.destroy();
            userOrderCache.destroy();
            orderGoodCache.destroy();
        }
    }

    /**
     * Test value.
     */
    private static class GroupIndexTestValue implements Serializable {
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "grpIdx", order = 0))
        private int a;

        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "grpIdx", order = 1))
        private int b;

        /**
         * @param a A.
         * @param b B.
         */
        private GroupIndexTestValue(int a, int b) {
            this.a = a;
            this.b = b;
        }
    }

    private static class Person implements Serializable {
        @QuerySqlField
        int orgId;

        @QuerySqlField
        String name;
    }

    private static class Organization implements Serializable {
        @QuerySqlField
        String name;
    }

    private static class User implements Serializable {
        @QuerySqlField
        private int id;
    }

    private static class UserOrder implements Serializable {
        @QuerySqlField
        private int id;

        @QuerySqlField
        private int userId;
    }

    private static class OrderGood implements Serializable {
        @QuerySqlField
        private int orderId;

        @QuerySqlField
        private int goodId;
    }
}