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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testsuites.IgniteIgnore;

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
     * Test HAVING clause.
     */
    public void testHaving() {
        IgniteCache<Integer, Integer> c = ignite(0).getOrCreateCache(cacheConfig("ints", true,
            Integer.class, Integer.class));

        try {
            Random rnd = new GridRandom();

            Map<Integer, AtomicLong> cntMap = new HashMap<>();

            for (int i = 0; i < 1000; i++) {
                int v = (int)(50 * rnd.nextGaussian());

                c.put(i, v);

                AtomicLong cnt = cntMap.get(v);

                if (cnt == null)
                    cntMap.put(v, cnt = new AtomicLong());

                cnt.incrementAndGet();
            }

            assertTrue(cntMap.size() > 10);

            String sqlQry = "select _val, count(*) cnt from Integer group by _val having cnt > ?";

            X.println("Plan: " + c.query(new SqlFieldsQuery("explain " + sqlQry).setArgs(0)).getAll());

            for (int i = -1; i <= 1001; i += 10) {
                List<List<?>> res = c.query(new SqlFieldsQuery(sqlQry).setArgs(i)).getAll();

                for (List<?> row : res) {
                    int v = (Integer)row.get(0);
                    long cnt = (Long)row.get(1);

                    assertTrue(cnt + " > " + i, cnt > i);
                    assertEquals(cntMap.get(v).longValue(), cnt);
                }
            }
        }
        finally {
            c.destroy();
        }
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
    @IgniteIgnore(value = "https://issues.apache.org/jira/browse/IGNITE-1886", forceFailure = true)
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
     *
     */
    public void testImplicitJoinConditionGeneration() {
        IgniteCache<Integer, Person> p = ignite(0).createCache(cacheConfig("P", true, Integer.class, Person.class));
        IgniteCache<Integer, Department> d = ignite(0).createCache(cacheConfig("D", true, Integer.class, Department.class));
        IgniteCache<Integer, Org> o = ignite(0).createCache(cacheConfig("O", true, Integer.class, Org.class));

        try {
            info("Plan: " + p.query(new SqlFieldsQuery(
                "explain select P.Person.*,dep.*,org.* " +
                    "from P.Person inner join D.Department dep ON dep.id=P.Person.depId " +
                    "left join O.Org org ON org.id=dep.orgId"
            )).getAll());

            assertEquals(0, p.query(new SqlFieldsQuery(
                "select P.Person.*,dep.*,org.* " +
                    "from P.Person inner join D.Department dep ON dep.id=P.Person.depId " +
                    "left join O.Org org ON org.id=dep.orgId"
            )).getAll().size());
        }
        finally {
            p.destroy();
            d.destroy();
            o.destroy();
        }
    }

    /** @throws Exception if failed. */
    public void testDistributedAggregates() throws Exception {
        final String cacheName = "ints";

        IgniteCache<Integer, Value> cache = ignite(0).getOrCreateCache(cacheConfig(cacheName, true,
            Integer.class, Value.class));

        AffinityKeyGenerator node0KeyGen = new AffinityKeyGenerator(ignite(0), cacheName);
        AffinityKeyGenerator node1KeyGen = new AffinityKeyGenerator(ignite(1), cacheName);
        AffinityKeyGenerator node2KeyGen = new AffinityKeyGenerator(ignite(2), cacheName);

        try {
            awaitPartitionMapExchange();

            cache.put(node0KeyGen.next(), new Value(1, 3));
            cache.put(node1KeyGen.next(), new Value(1, 3));
            cache.put(node2KeyGen.next(), new Value(1, 3));

            cache.put(node0KeyGen.next(), new Value(2, 1));
            cache.put(node1KeyGen.next(), new Value(2, 2));
            cache.put(node2KeyGen.next(), new Value(2, 3));

            cache.put(node0KeyGen.next(), new Value(3, 1));
            cache.put(node0KeyGen.next(), new Value(3, 1));
            cache.put(node0KeyGen.next(), new Value(3, 2));
            cache.put(node1KeyGen.next(), new Value(3, 1));
            cache.put(node1KeyGen.next(), new Value(3, 2));
            cache.put(node2KeyGen.next(), new Value(3, 2));

            cache.put(node0KeyGen.next(), new Value(4, 2));
            cache.put(node1KeyGen.next(), new Value(5, 2));
            cache.put(node2KeyGen.next(), new Value(6, 2));

            checkSimpleQueryWithAggr(cache);
            checkSimpleQueryWithDistinctAggr(cache);

            checkQueryWithGroupsAndAggrs(cache);
            checkQueryWithGroupsAndDistinctAggr(cache);

            checkSimpleQueryWithAggrMixed(cache);
            checkQueryWithGroupsAndAggrMixed(cache);
        }
        finally {
            cache.destroy();
        }
    }

    /** @throws Exception if failed. */
    public void testCollocatedAggregates() throws Exception {
        final String cacheName = "ints";

        IgniteCache<Integer, Value> cache = ignite(0).getOrCreateCache(cacheConfig(cacheName, true,
            Integer.class, Value.class));

        AffinityKeyGenerator node0KeyGen = new AffinityKeyGenerator(ignite(0), cacheName);
        AffinityKeyGenerator node1KeyGen = new AffinityKeyGenerator(ignite(1), cacheName);
        AffinityKeyGenerator node2KeyGen = new AffinityKeyGenerator(ignite(2), cacheName);

        try {
            awaitPartitionMapExchange();

            cache.put(node0KeyGen.next(), new Value(1, 3));
            cache.put(node0KeyGen.next(), new Value(1, 3));
            cache.put(node0KeyGen.next(), new Value(1, 3));

            cache.put(node1KeyGen.next(), new Value(2, 1));
            cache.put(node1KeyGen.next(), new Value(2, 2));
            cache.put(node1KeyGen.next(), new Value(2, 3));

            cache.put(node2KeyGen.next(), new Value(3, 1));
            cache.put(node2KeyGen.next(), new Value(3, 1));
            cache.put(node2KeyGen.next(), new Value(3, 2));
            cache.put(node2KeyGen.next(), new Value(3, 1));
            cache.put(node2KeyGen.next(), new Value(3, 2));
            cache.put(node2KeyGen.next(), new Value(3, 2));

            cache.put(node0KeyGen.next(), new Value(4, 2));
            cache.put(node1KeyGen.next(), new Value(5, 2));
            cache.put(node2KeyGen.next(), new Value(6, 2));

            checkQueryWithGroupsAndAggrs(cache);
            checkQueryWithGroupsAndDistinctAggr(cache);
            checkQueryWithGroupsAndAggrMixed(cache);
        }
        finally {
            cache.destroy();
        }
    }

    /** Simple query with aggregates */
    private void checkSimpleQueryWithAggr(IgniteCache<Integer, Value> cache) {
        try (QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(
            "SELECT count(fst), sum(snd), avg(snd), min(snd), max(snd) FROM Value"))) {
            List<List<?>> result = qry.getAll();

            assertEquals(1, result.size());

            List<?> row = result.get(0);

            assertEquals("count", 15L, ((Number)row.get(0)).longValue());
            assertEquals("sum", 30L, ((Number)row.get(1)).longValue());
            assertEquals("avg", 2.0d, ((Number)row.get(2)).doubleValue(), 0.001);
            assertEquals("min", 1, ((Integer)row.get(3)).intValue());
            assertEquals("max", 3, ((Integer)row.get(4)).intValue());
        }
    }

    /** Simple query with distinct aggregates */
    private void checkSimpleQueryWithDistinctAggr(IgniteCache<Integer, Value> cache) {
        try (QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(
            "SELECT count(distinct fst), sum(distinct snd), avg(distinct snd), min(distinct snd), max(distinct snd) " +
                "FROM Value"))) {
            List<List<?>> result = qry.getAll();

            assertEquals(1, result.size());

            List<?> row = result.get(0);

            assertEquals("count distinct", 6L, ((Number)row.get(0)).longValue());
            assertEquals("sum distinct", 6L, ((Number)row.get(1)).longValue());
            assertEquals("avg distinct", 2.0d, ((Number)row.get(2)).doubleValue(), 0.001);
            assertEquals("min distinct", 1, ((Integer)row.get(3)).intValue());
            assertEquals("max distinct", 3, ((Integer)row.get(4)).intValue());
        }
    }

    /** Simple query with distinct aggregates */
    private void checkSimpleQueryWithAggrMixed(IgniteCache<Integer, Value> cache) {
        try (QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(
            "SELECT count(fst), sum(snd), avg(snd), min(snd), max(snd)," +
                "count(distinct fst), sum(distinct snd), avg(distinct snd), min(distinct snd), max(distinct snd)  " +
                "FROM Value"))) {
            List<List<?>> result = qry.getAll();

            assertEquals(1, result.size());

            List<?> row = result.get(0);

            assertEquals("count", 15L, ((Number)row.get(0)).longValue());
            assertEquals("sum", 30L, ((Number)row.get(1)).longValue());
            assertEquals("avg", 2.0d, ((Number)row.get(2)).doubleValue(), 0.001);
            assertEquals("min", 1, ((Integer)row.get(3)).intValue());
            assertEquals("max", 3, ((Integer)row.get(4)).intValue());
            assertEquals("count distinct", 6L, ((Number)row.get(5)).longValue());
            assertEquals("sum distinct", 6L, ((Number)row.get(6)).longValue());
            assertEquals("avg distinct", 2.0d, ((Number)row.get(7)).doubleValue(), 0.001);
            assertEquals("min distinct", 1, ((Integer)row.get(8)).intValue());
            assertEquals("max distinct", 3, ((Integer)row.get(9)).intValue());
        }
    }

    /** Query with aggregates and groups */
    private void checkQueryWithGroupsAndAggrs(IgniteCache<Integer, Value> cache) {
        try (QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(
            "SELECT fst, count(snd), sum(snd), avg(snd), min(snd), max(snd) FROM Value GROUP BY fst ORDER BY fst"))) {
            List<List<?>> result = qry.getAll();

            assertEquals(6, result.size());

            List<?> row = result.get(0);
            assertEquals("fst", 1, ((Number)row.get(0)).intValue());
            assertEquals("count", 3L, ((Number)row.get(1)).longValue());
            assertEquals("sum", 9L, ((Number)row.get(2)).longValue());
            assertEquals("avg", 3.0d, ((Number)row.get(3)).doubleValue(), 0.001);
            assertEquals("min", 3, ((Integer)row.get(4)).intValue());
            assertEquals("max", 3, ((Integer)row.get(5)).intValue());

            row = result.get(1);
            assertEquals("fst", 2, ((Number)row.get(0)).intValue());
            assertEquals("count", 3L, ((Number)row.get(1)).longValue());
            assertEquals("sum", 6L, ((Number)row.get(2)).longValue());
            assertEquals("avg", 2.0d, ((Number)row.get(3)).doubleValue(), 0.001);
            assertEquals("min", 1, ((Integer)row.get(4)).intValue());
            assertEquals("max", 3, ((Integer)row.get(5)).intValue());

            row = result.get(2);
            assertEquals("fst", 3, ((Number)row.get(0)).intValue());
            assertEquals("count", 6L, ((Number)row.get(1)).longValue());
            assertEquals("sum", 9L, ((Number)row.get(2)).longValue());
            assertEquals("avg", 1.5d, ((Number)row.get(3)).doubleValue(), 0.001);
            assertEquals("min", 1, ((Integer)row.get(4)).intValue());
            assertEquals("max", 2, ((Integer)row.get(5)).intValue());
        }
    }

    /** Query with distinct aggregates and groups */
    private void checkQueryWithGroupsAndDistinctAggr(IgniteCache<Integer, Value> cache) {
        try (QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(
            "SELECT count(distinct snd), sum(distinct snd), avg(distinct snd), min(distinct snd), max(distinct snd) " +
                "FROM Value GROUP BY fst"))) {
            List<List<?>> result = qry.getAll();

            assertEquals(6, result.size());

            List<?> row = result.get(0);
            assertEquals("count distinct", 1L, ((Number)row.get(0)).longValue());
            assertEquals("sum distinct", 3L, ((Number)row.get(1)).longValue());
            assertEquals("avg distinct", 3.0d, ((Number)row.get(2)).doubleValue(), 0.001);
            assertEquals("min distinct", 3, ((Integer)row.get(3)).intValue());
            assertEquals("max distinct", 3, ((Integer)row.get(4)).intValue());

            row = result.get(1);
            assertEquals("count distinct", 3L, ((Number)row.get(0)).longValue());
            assertEquals("sum distinct", 6L, ((Number)row.get(1)).longValue());
            assertEquals("avg distinct", 2.0d, ((Number)row.get(2)).doubleValue(), 0.001);
            assertEquals("min distinct", 1, ((Integer)row.get(3)).intValue());
            assertEquals("max distinct", 3, ((Integer)row.get(4)).intValue());

            row = result.get(2);
            assertEquals("count distinct", 2L, ((Number)row.get(0)).longValue());
            assertEquals("sum distinct", 3L, ((Number)row.get(1)).longValue());
            assertEquals("avg distinct", 1.5d, ((Number)row.get(2)).doubleValue(), 0.001);
            assertEquals("min distinct", 1, ((Integer)row.get(3)).intValue());
            assertEquals("max distinct", 2, ((Integer)row.get(4)).intValue());
        }
    }

    /** Query with distinct aggregates and groups */
    private void checkQueryWithGroupsAndAggrMixed(IgniteCache<Integer, Value> cache) {
        try (QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(
            "SELECT fst, count(snd), sum(snd), avg(snd), min(snd), max(snd)," +
                "count(distinct snd), sum(distinct snd), avg(distinct snd), min(distinct snd), max(distinct snd) " +
                "FROM Value GROUP BY fst"))) {
            List<List<?>> result = qry.getAll();

            assertEquals(6, result.size());

            List<?> row = result.get(0);
            assertEquals("fst", 1, ((Number)row.get(0)).intValue());
            assertEquals("count", 3L, ((Number)row.get(1)).longValue());
            assertEquals("sum", 9L, ((Number)row.get(2)).longValue());
            assertEquals("avg", 3.0d, ((Number)row.get(3)).doubleValue(), 0.001);
            assertEquals("min", 3, ((Integer)row.get(4)).intValue());
            assertEquals("max", 3, ((Integer)row.get(5)).intValue());
            assertEquals("count distinct", 1L, ((Number)row.get(6)).longValue());
            assertEquals("sum distinct", 3L, ((Number)row.get(7)).longValue());
            assertEquals("avg distinct", 3.0d, ((Number)row.get(8)).doubleValue(), 0.001);
            assertEquals("min distinct", 3, ((Integer)row.get(9)).intValue());
            assertEquals("max distinct", 3, ((Integer)row.get(10)).intValue());

            row = result.get(1);
            assertEquals("fst", 2, ((Number)row.get(0)).intValue());
            assertEquals("count", 3L, ((Number)row.get(1)).longValue());
            assertEquals("sum", 6L, ((Number)row.get(2)).longValue());
            assertEquals("avg", 2.0d, ((Number)row.get(3)).doubleValue(), 0.001);
            assertEquals("min", 1, ((Integer)row.get(4)).intValue());
            assertEquals("max", 3, ((Integer)row.get(5)).intValue());
            assertEquals("count distinct", 3L, ((Number)row.get(6)).longValue());
            assertEquals("sum distinct", 6L, ((Number)row.get(7)).longValue());
            assertEquals("avg distinct", 2.0d, ((Number)row.get(8)).doubleValue(), 0.001);
            assertEquals("min distinct", 1, ((Integer)row.get(9)).intValue());
            assertEquals("max distinct", 3, ((Integer)row.get(10)).intValue());

            row = result.get(2);
            assertEquals("fst", 3, ((Number)row.get(0)).intValue());
            assertEquals("count", 6L, ((Number)row.get(1)).longValue());
            assertEquals("sum", 9L, ((Number)row.get(2)).longValue());
            assertEquals("avg", 1.5d, ((Number)row.get(3)).doubleValue(), 0.001);
            assertEquals("min", 1, ((Integer)row.get(4)).intValue());
            assertEquals("max", 2, ((Integer)row.get(5)).intValue());
            assertEquals("count distinct", 2L, ((Number)row.get(6)).longValue());
            assertEquals("sum distinct", 3L, ((Number)row.get(7)).longValue());
            assertEquals("avg distinct", 1.5d, ((Number)row.get(8)).doubleValue(), 0.001);
            assertEquals("min distinct", 1, ((Integer)row.get(9)).intValue());
            assertEquals("max distinct", 2, ((Integer)row.get(10)).intValue());
        }
    }

    /**  */
    private static class Value {
        /**  */
        @QuerySqlField
        private final Integer fst;

        /**  */
        @QuerySqlField
        private final Integer snd;

        /** Constructor */
        public Value(Integer fst, Integer snd) {
            this.fst = fst;
            this.snd = snd;
        }
    }

    /**
     *
     */
    private static class AffinityKeyGenerator {
        /** */
        private final Affinity<Integer> affinity;

        /** */
        private final ClusterNode node;

        /** */
        private int start = 0;

        /** Constructor */
        AffinityKeyGenerator(Ignite node, String cacheName) {
            this.affinity = node.affinity(cacheName);
            this.node = node.cluster().localNode();
        }

        /**  */
        public Integer next() {
            int key = start;

            while (start < Integer.MAX_VALUE) {
                if (affinity.isPrimary(node, key)) {
                    start = key + 1;

                    return key;
                }

                key++;
            }

            throw new IllegalStateException("Can't find next key");
        }
    }

    /**
     *
     */
    public static class Person {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        private String name;

        /** */
        @QuerySqlField
        private int depId;
    }

    /**
     *
     */
    public static class Org {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private String name;
    }

    /**
     *
     */
    public static class Department {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField(index = true)
        private int orgId;

        /** */
        @QuerySqlField
        private String name;
    }

    /**
     * Test value.
     */
    private static class GroupIndexTestValue implements Serializable {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "grpIdx", order = 0))
        private int a;

        /** */
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

    private static class User implements Serializable {
        /** */
        @QuerySqlField
        private int id;
    }

    /** */
    private static class UserOrder implements Serializable {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        private int userId;
    }

    /**
     *
     */
    private static class OrderGood implements Serializable {
        /** */
        @QuerySqlField
        private int orderId;

        /** */
        @QuerySqlField
        private int goodId;
    }
}