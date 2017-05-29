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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMergeIndex;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testsuites.IgniteIgnore;
import org.springframework.util.StringUtils;

/**
 * Tests for correct distributed partitioned queries.
 */
@SuppressWarnings("unchecked")
public class IgniteSqlSplitterSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CLIENT = 7;

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration(TestKey.class.getName(), "affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);
        Ignition.setClientMode(true);
        try {
            startGrid(CLIENT);
        }
        finally {
            Ignition.setClientMode(false);
        }
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
        return new CacheConfiguration(DEFAULT_CACHE_NAME)
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
     */
    public void testReplicatedTablesUsingPartitionedCache() {
        doTestReplicatedTablesUsingPartitionedCache(1, false, false);
    }

    /**
     */
    public void testReplicatedTablesUsingPartitionedCacheSegmented() {
        doTestReplicatedTablesUsingPartitionedCache(5, false, false);
    }

    /**
     */
    public void testReplicatedTablesUsingPartitionedCacheClient() {
        doTestReplicatedTablesUsingPartitionedCache(1, true, false);
    }

    /**
     */
    public void testReplicatedTablesUsingPartitionedCacheSegmentedClient() {
        doTestReplicatedTablesUsingPartitionedCache(5, true, false);
    }

    /**
     */
    public void testReplicatedTablesUsingPartitionedCacheRO() {
        doTestReplicatedTablesUsingPartitionedCache(1, false, true);
    }

    /**
     */
    public void testReplicatedTablesUsingPartitionedCacheSegmentedRO() {
        doTestReplicatedTablesUsingPartitionedCache(5, false, true);
    }

    /**
     */
    public void testReplicatedTablesUsingPartitionedCacheClientRO() {
        doTestReplicatedTablesUsingPartitionedCache(1, true, true);
    }

    /**
     */
    public void testReplicatedTablesUsingPartitionedCacheSegmentedClientRO() {
        doTestReplicatedTablesUsingPartitionedCache(5, true, true);
    }

    /**
     */
    private SqlFieldsQuery query(String sql, boolean replicatedOnly) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (replicatedOnly)
            qry.setReplicatedOnly(true);

        return qry;
    }

    /**
     */
    private void doTestReplicatedTablesUsingPartitionedCache(int segments, boolean client, boolean replicatedOnlyFlag) {
        IgniteCache<Integer,Value> p = ignite(client ? CLIENT : 0).getOrCreateCache(cacheConfig("p", true,
            Integer.class, Value.class).setQueryParallelism(segments));
        IgniteCache<Integer,Value> r = ignite(client ? CLIENT : 0).getOrCreateCache(cacheConfig("r", false,
            Integer.class, Value.class));

        try {
            int cnt = 1000;

            for (int i = 0; i < cnt; i++)
                r.put(i, new Value(i, -i));

            // Query data from replicated table using partitioned cache.
            assertEquals(cnt, p.query(query("select 1 from \"r\".Value", replicatedOnlyFlag))
                .getAll().size());

            List<List<?>> res = p.query(query("select count(1) from \"r\".Value", replicatedOnlyFlag)).getAll();
            assertEquals(1, res.size());
            assertEquals(cnt, ((Number)res.get(0).get(0)).intValue());
        }
        finally {
            p.destroy();
            r.destroy();
        }
    }

    public void testPartitionedTablesUsingReplicatedCache() {
        doTestPartitionedTablesUsingReplicatedCache(1, false);
    }

    public void testPartitionedTablesUsingReplicatedCacheSegmented() {
        doTestPartitionedTablesUsingReplicatedCache(7, false);
    }

    public void testPartitionedTablesUsingReplicatedCacheClient() {
        doTestPartitionedTablesUsingReplicatedCache(1, true);
    }

    public void testPartitionedTablesUsingReplicatedCacheSegmentedClient() {
        doTestPartitionedTablesUsingReplicatedCache(7, true);
    }

    /**
     */
    private void doTestPartitionedTablesUsingReplicatedCache(int segments, boolean client) {
        IgniteCache<Integer,Value> p = ignite(client ? CLIENT : 0).getOrCreateCache(cacheConfig("p", true,
            Integer.class, Value.class).setQueryParallelism(segments));
        IgniteCache<Integer,Value> r = ignite(client ? CLIENT : 0).getOrCreateCache(cacheConfig("r", false,
            Integer.class, Value.class));

        try {
            int cnt = 1000;

            for (int i = 0; i < cnt; i++)
                p.put(i, new Value(i, -i));

            // Query data from replicated table using partitioned cache.
            assertEquals(cnt, r.query(new SqlFieldsQuery("select 1 from \"p\".Value")).getAll().size());

            List<List<?>> res = r.query(new SqlFieldsQuery("select count(1) from \"p\".Value")).getAll();
            assertEquals(1, res.size());
            assertEquals(cnt, ((Number)res.get(0).get(0)).intValue());
        }
        finally {
            p.destroy();
            r.destroy();
        }
    }

    /**
     */
    public void testSubQueryWithAggregate() {
        CacheConfiguration ccfg1 = cacheConfig("pers", true,
            AffinityKey.class, Person2.class);

        IgniteCache<AffinityKey<Integer>, Person2> c1 = ignite(0).getOrCreateCache(ccfg1);

        try {
            int orgId = 100500;

            c1.put(new AffinityKey<>(1, orgId), new Person2(orgId, "Vasya"));
            c1.put(new AffinityKey<>(2, orgId), new Person2(orgId, "Another Vasya"));

            List<List<?>> rs = c1.query(new SqlFieldsQuery("select name, " +
                "select count(1) from Person2 q where q.orgId = p.orgId " +
                "from Person2 p order by name desc")).getAll();

            assertEquals(2, rs.size());
            assertEquals("Vasya", rs.get(0).get(0));
            assertEquals(2L, rs.get(0).get(1));
            assertEquals("Another Vasya", rs.get(1).get(0));
            assertEquals(2L, rs.get(1).get(1));
        }
        finally {
            c1.destroy();
        }
    }

    /**
     * @throws InterruptedException If failed.
     */
    public void testDistributedJoinFromReplicatedCache() throws InterruptedException {
        CacheConfiguration ccfg1 = cacheConfig("pers", true,
            Integer.class, Person2.class);

        CacheConfiguration ccfg2 = cacheConfig("org", true,
            Integer.class, Organization.class);

        CacheConfiguration ccfg3 = cacheConfig("orgRepl", false,
            Integer.class, Organization.class);

        IgniteCache<Integer, Person2> c1 = ignite(0).getOrCreateCache(ccfg1);
        IgniteCache<Integer, Organization> c2 = ignite(0).getOrCreateCache(ccfg2);
        IgniteCache<Integer, Organization> c3 = ignite(0).getOrCreateCache(ccfg3);

        try {
            awaitPartitionMapExchange();

            doTestDistributedJoins(c3, c1, c2, 300, 2000, 5, false);
            doTestDistributedJoins(c3, c1, c2, 300, 2000, 5, true);
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    public void testExists() {
        IgniteCache<Integer,Person2> x = ignite(0).getOrCreateCache(cacheConfig("x", true,
            Integer.class, Person2.class));
        IgniteCache<Integer,Person2> y = ignite(0).getOrCreateCache(cacheConfig("y", true,
            Integer.class, Person2.class));

        try {
            GridRandom rnd = new GridRandom();

            Set<Integer> intersects = new HashSet<>();

            for (int i = 0; i < 3000; i++) {
                int r = rnd.nextInt(3);

                if (r != 0)
                    x.put(i, new Person2(i, "pers_x_" + i));

                if (r != 1)
                    y.put(i, new Person2(i, "pers_y_" + i));

                if (r == 2)
                    intersects.add(i);
            }

            assertFalse(intersects.isEmpty());

            List<List<?>> res = x.query(new SqlFieldsQuery("select _key from \"x\".Person2 px " +
                "where exists(select 1 from \"y\".Person2 py where px._key = py._key)")).getAll();

            assertEquals(intersects.size(), res.size());

            for (List<?> row : res)
                assertTrue(intersects.contains(row.get(0)));
        }
        finally {
            x.destroy();
            y.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSortedMergeIndex() throws Exception {
        IgniteCache<Integer,Value> c = ignite(0).getOrCreateCache(cacheConfig("v", true,
            Integer.class, Value.class));

        try {
            GridTestUtils.setFieldValue(null, GridMergeIndex.class, "PREFETCH_SIZE", 8);

            Random rnd = new GridRandom();

            int cnt = 1000;

            for (int i = 0; i < cnt; i++) {
                c.put(i, new Value(
                    rnd.nextInt(5) == 0 ? null: rnd.nextInt(100),
                    rnd.nextInt(8) == 0 ? null: rnd.nextInt(2000)));
            }

            List<List<?>> plan = c.query(new SqlFieldsQuery(
                "explain select snd from Value order by fst desc")).getAll();
            String rdcPlan = (String)plan.get(1).get(0);

            assertTrue(rdcPlan.contains("merge_sorted"));
            assertTrue(rdcPlan.contains("/* index sorted */"));

            plan = c.query(new SqlFieldsQuery(
                "explain select snd from Value")).getAll();
            rdcPlan = (String)plan.get(1).get(0);

            assertTrue(rdcPlan.contains("merge_scan"));
            assertFalse(rdcPlan.contains("/* index sorted */"));

            for (int i = 0; i < 10; i++) {
                X.println(" --> " + i);

                List<List<?>> res = c.query(new SqlFieldsQuery(
                    "select fst from Value order by fst").setPageSize(5)
                ).getAll();

                assertEquals(cnt, res.size());

                Integer p = null;

                for (List<?> row : res) {
                    Integer x = (Integer)row.get(0);

                    if (x != null) {
                        if (p != null)
                            assertTrue(x + " >= " + p,  x >= p);

                        p = x;
                    }
                }
            }
        }
        finally {
            GridTestUtils.setFieldValue(null, GridMergeIndex.class, "PREFETCH_SIZE", 1024);

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

            assertTrue("_explain: " + plan, plan.toLowerCase().contains("grpidx"));

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
     */
    public void testUseIndexHints() {
        CacheConfiguration ccfg = cacheConfig("pers", true,
            Integer.class, Person2.class);

        IgniteCache<Integer, Person2> c = ignite(0).getOrCreateCache(ccfg);

        try {
            String select = "select 1 from Person2 use index (\"PERSON2_ORGID_IDX\") where name = '' and orgId = 1";

            String plan = c.query(new SqlFieldsQuery("explain " + select)).getAll().toString();

            X.println("Plan: \n" + plan);

            assertTrue(plan.contains("USE INDEX (PERSON2_ORGID_IDX)"));
            assertTrue(plan.contains("/* \"pers\".PERSON2_ORGID_IDX:"));

            select = "select 1 from Person2 use index (\"PERSON2_NAME_IDX\") where name = '' and orgId = 1";

            plan = c.query(new SqlFieldsQuery("explain " + select)).getAll().toString();

            X.println("Plan: \n" + plan);

            assertTrue(plan.contains("USE INDEX (PERSON2_NAME_IDX)"));
            assertTrue(plan.contains("/* \"pers\".PERSON2_NAME_IDX:"));
        }
        finally {
            c.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoins() throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", true,
            Integer.class, Person2.class);

        CacheConfiguration ccfg2 = cacheConfig("org", true,
            Integer.class, Organization.class);

        IgniteCache<Integer, Person2> c1 = ignite(0).getOrCreateCache(ccfg1);
        IgniteCache<Integer, Organization> c2 = ignite(0).getOrCreateCache(ccfg2);

        try {
            awaitPartitionMapExchange();

            doTestDistributedJoins(c2, c1, c2, 30, 100, 1000, false);
            doTestDistributedJoins(c2, c1, c2, 30, 100, 1000, true);

            doTestDistributedJoins(c2, c1, c2, 3, 10, 3, false);
            doTestDistributedJoins(c2, c1, c2, 3, 10, 3, true);

            doTestDistributedJoins(c2, c1, c2, 300, 2000, 5, false);
            doTestDistributedJoins(c2, c1, c2, 300, 2000, 5, true);
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoinsUnion() throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", true, Integer.class, Person2.class);
        CacheConfiguration ccfg2 = cacheConfig("org", true, Integer.class, Organization.class);

        IgniteCache<Integer, Object> c1 = ignite(0).getOrCreateCache(ccfg1);
        IgniteCache<Integer, Object> c2 = ignite(0).getOrCreateCache(ccfg2);

        try {
            c2.put(1, new Organization("o1"));
            c2.put(2, new Organization("o2"));
            c1.put(3, new Person2(1, "p1"));
            c1.put(4, new Person2(2, "p2"));
            c1.put(5, new Person2(3, "p3"));

            String select = "select o.name n1, p.name n2 from Person2 p, \"org\".Organization o" +
                " where p.orgId = o._key and o._key=1" +
                " union select o.name n1, p.name n2 from Person2 p, \"org\".Organization o" +
                " where p.orgId = o._key and o._key=2";

            String plan = c1.query(new SqlFieldsQuery("explain " + select)
                .setDistributedJoins(true).setEnforceJoinOrder(true))
                .getAll().toString();

            X.println("Plan : " + plan);

            assertEquals(2, StringUtils.countOccurrencesOf(plan, "batched"));
            assertEquals(2, StringUtils.countOccurrencesOf(plan, "batched:unicast"));

            assertEquals(2, c1.query(new SqlFieldsQuery(select).setDistributedJoins(true)
                .setEnforceJoinOrder(false)).getAll().size());

            select = "select * from (" + select + ")";

            plan = c1.query(new SqlFieldsQuery("explain " + select)
                .setDistributedJoins(true).setEnforceJoinOrder(true))
                .getAll().toString();

            X.println("Plan : " + plan);

            assertEquals(2, StringUtils.countOccurrencesOf(plan, "batched"));
            assertEquals(2, StringUtils.countOccurrencesOf(plan, "batched:unicast"));

            assertEquals(2, c1.query(new SqlFieldsQuery(select).setDistributedJoins(true)
                .setEnforceJoinOrder(false)).getAll().size());
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoinsUnionPartitionedReplicated() throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", true,
            Integer.class, Person2.class);
        CacheConfiguration ccfg2 = cacheConfig("org", false,
            Integer.class, Organization.class);

        IgniteCache<Integer, Object> c1 = ignite(0).getOrCreateCache(ccfg1);
        IgniteCache<Integer, Object> c2 = ignite(0).getOrCreateCache(ccfg2);

        try {
            c2.put(1, new Organization("o1"));
            c2.put(2, new Organization("o2"));
            c1.put(3, new Person2(1, "p1"));
            c1.put(4, new Person2(2, "p2"));
            c1.put(5, new Person2(3, "p3"));

            String select0 = "select o.name n1, p.name n2 from \"pers\".Person2 p, \"org\".Organization o where p.orgId = o._key and o._key=1" +
                " union select o.name n1, p.name n2 from \"org\".Organization o, \"pers\".Person2 p where p.orgId = o._key and o._key=2";

            String plan = (String)c1.query(new SqlFieldsQuery("explain " + select0)
                .setDistributedJoins(true))
                .getAll().get(0).get(0);

            X.println("Plan: " + plan);

            assertEquals(0, StringUtils.countOccurrencesOf(plan, "batched"));
            assertEquals(2, c1.query(new SqlFieldsQuery(select0).setDistributedJoins(true)).getAll().size());

            String select = "select * from (" + select0 + ")";

            plan = (String)c1.query(new SqlFieldsQuery("explain " + select)
                .setDistributedJoins(true))
                .getAll().get(0).get(0);

            X.println("Plan : " + plan);

            assertEquals(0, StringUtils.countOccurrencesOf(plan, "batched"));
            assertEquals(2, c1.query(new SqlFieldsQuery(select).setDistributedJoins(true)).getAll().size());

            String select1 = "select o.name n1, p.name n2 from \"pers\".Person2 p, \"org\".Organization o where p.orgId = o._key and o._key=1" +
                " union select * from (select o.name n1, p.name n2 from \"org\".Organization o, \"pers\".Person2 p where p.orgId = o._key and o._key=2)";

            plan = (String)c1.query(new SqlFieldsQuery("explain " + select1)
                .setDistributedJoins(true)).getAll().get(0).get(0);

            X.println("Plan: " + plan);

            assertEquals(0, StringUtils.countOccurrencesOf(plan, "batched"));
            assertEquals(2, c1.query(new SqlFieldsQuery(select).setDistributedJoins(true)).getAll().size());

            select = "select * from (" + select1 + ")";

            plan = (String)c1.query(new SqlFieldsQuery("explain " + select)
                .setDistributedJoins(true)).getAll().get(0).get(0);

            X.println("Plan : " + plan);

            assertEquals(0, StringUtils.countOccurrencesOf(plan, "batched"));
            assertEquals(2, c1.query(new SqlFieldsQuery(select).setDistributedJoins(true)).getAll().size());
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoinsPlan() throws Exception {
        List<IgniteCache<Object, Object>> caches = new ArrayList<>();

        IgniteCache<Object, Object> persPart =
            ignite(0).createCache(cacheConfig("persPart", true, Integer.class, Person2.class));
        caches.add(persPart);

        IgniteCache<Object, Object> persPartAff =
            ignite(0).createCache(cacheConfig("persPartAff", true, TestKey.class, Person2.class));
        caches.add(persPartAff);

        IgniteCache<Object, Object> orgPart =
            ignite(0).createCache(cacheConfig("orgPart", true, Integer.class, Organization.class));
        caches.add(orgPart);

        IgniteCache<Object, Object> orgPartAff =
            ignite(0).createCache(cacheConfig("orgPartAff", true, TestKey.class, Organization.class));
        caches.add(orgPartAff);

        IgniteCache<Object, Object> orgRepl =
            ignite(0).createCache(cacheConfig("orgRepl", false, Integer.class, Organization.class));
        caches.add(orgRepl);

        IgniteCache<Object, Object> orgRepl2 =
            ignite(0).createCache(cacheConfig("orgRepl2", false, Integer.class, Organization.class));
        caches.add(orgRepl2);

        try {
            // Join two partitioned.

            checkQueryPlan(persPart,
                true,
                1,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p, \"orgPart\".Organization o " +
                    "where p.orgId = o._key",
                "batched:unicast");

            checkQueryPlan(persPart,
                false,
                1,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p, \"orgPartAff\".Organization o " +
                    "where p.orgId = o.affKey",
                "batched:unicast");

            checkQueryPlan(persPart,
                false,
                1,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p, \"orgPart\".Organization o " +
                    "where p.orgId = o._key",
                "batched:unicast");

            checkQueryPlan(persPart,
                false,
                1,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p inner join \"orgPart\".Organization o " +
                    "on p.orgId = o._key",
                "batched:unicast");

            checkQueryPlan(persPart,
                false,
                1,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p left outer join \"orgPart\".Organization o " +
                    "on p.orgId = o._key",
                "batched:unicast");

            checkQueryPlan(persPart,
                true,
                1,
                "select p._key k1, o._key k2 " +
                    "from \"orgPart\".Organization o, \"persPart\".Person2 p " +
                    "where p.orgId = o._key",
                "batched:broadcast");

            checkQueryPlan(persPart,
                true,
                1,
                "select p._key k1, o._key k2 " +
                    "from \"orgPartAff\".Organization o, \"persPart\".Person2 p " +
                    "where p.orgId = o.affKey",
                "batched:broadcast");

            // Join partitioned and replicated.

            checkQueryPlan(persPart,
                true,
                0,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p, \"orgRepl\".Organization o " +
                    "where p.orgId = o._key");

            checkQueryPlan(persPart,
                false,
                0,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p, \"orgRepl\".Organization o " +
                    "where p.orgId = o._key");

            checkQueryPlan(persPart,
                false,
                0,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p, (select _key, _val, * from \"orgRepl\".Organization) o " +
                    "where p.orgId = o._key");

            checkQueryPlan(persPart,
                false,
                0,
                "select p._key k1, o._key k2 " +
                    "from (select _key, _val, * from \"orgRepl\".Organization) o, \"persPart\".Person2 p " +
                    "where p.orgId = o._key");

            checkQueryPlan(persPart,
                false,
                0,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p inner join \"orgRepl\".Organization o " +
                    "on p.orgId = o._key");

            checkQueryPlan(persPart,
                false,
                0,
                "select p._key k1, o._key k2 " +
                    "from \"persPart\".Person2 p left outer join \"orgRepl\".Organization o " +
                    "on p.orgId = o._key");

            checkQueryPlan(persPart,
                false,
                0,
                "select p._key k1, o._key k2 " +
                    "from \"orgRepl\".Organization o, \"persPart\".Person2 p " +
                    "where p.orgId = o._key");

            checkQueryPlan(persPart,
                false,
                0,
                "select p._key k1, o._key k2 " +
                    "from \"orgRepl\".Organization o inner join \"persPart\".Person2 p " +
                    "on p.orgId = o._key");

//            checkQueryPlan(persPart,
//                true,
//                1,
//                "select p._key k1, o._key k2 " +
//                    "from \"orgRepl\".Organization o left outer join \"persPart\".Person2 p " +
//                    "on p.orgId = o._key",
//                "batched:broadcast");

            // Join on affinity keys.

            checkNoBatchedJoin(persPart, "select p._key k1, o._key k2 ",
                "\"persPart\".Person2 p",
                "\"orgPart\".Organization o",
                "where p._key = o._key", true);

            checkNoBatchedJoin(persPart, "select p._key k1, o._key k2 ",
                "\"persPart\".Person2 p",
                "\"orgRepl\".Organization o",
                "where p._key = o._key", true);

            checkNoBatchedJoin(persPartAff, "select p._key k1, o._key k2 ",
                "\"persPartAff\".Person2 p",
                "\"orgPart\".Organization o",
                "where p.affKey = o._key", true);

            checkNoBatchedJoin(persPartAff, "select p._key k1, o._key k2 ",
                "\"persPartAff\".Person2 p",
                "\"orgRepl\".Organization o",
                "where p.affKey = o._key", true);

            // TODO Now we can not analyze subqueries to decide if we are collocated or not.
//            checkNoBatchedJoin(persPart, "select p._key k1, o._key k2 ",
//                "(select * from \"persPart\".Person2) p",
//                "\"orgPart\".Organization o",
//                "where p._key = o._key", false);
//            checkNoBatchedJoin(persPart, "select p._key k1, o._key k2 ",
//                "\"persPart\".Person2 p",
//                "(select * from \"orgPart\".Organization) o",
//                "where p._key = o._key", false);

            // Join multiple.

            {
                String sql = "select * from " +
                    "(select o1._key k1, o2._key k2 from \"orgRepl\".Organization o1, \"orgRepl2\".Organization o2 where o1._key > o2._key) o, " +
                    "\"persPart\".Person2 p where p.orgId = o.k1";

                checkQueryPlan(persPart,
                    false,
                    0,
                    sql);

                checkQueryPlan(persPart,
                    true,
                    0,
                    sql);

                sql = "select o.k1, p1._key k2, p2._key k3 from " +
                    "(select o1._key k1, o2._key k2 " +
                    "from \"orgRepl\".Organization o1, \"orgRepl2\".Organization o2 " +
                    "where o1._key > o2._key) o, " +
                    "\"persPartAff\".Person2 p1, \"persPart\".Person2 p2 " +
                    "where p1._key=p2._key and p2.orgId = o.k1";

                checkQueryPlan(persPart,
                    false,
                    0,
                    sql,
                    "persPartAff", "persPart", "orgRepl");

                checkQueryFails(persPart, sql, true);

                sql = "select o.ok, p._key from " +
                    "(select o1._key ok, p1._key pk " +
                    "from \"orgRepl\".Organization o1, \"persPart\".Person2 p1 " +
                    "where o1._key = p1.orgId) o, " +
                    "\"persPartAff\".Person2 p where p._key=o.ok";

                checkQueryPlan(persPart,
                    false,
                    1,
                    sql,
                    "FROM \"persPart\"", "INNER JOIN \"orgRepl\"",
                    "INNER JOIN \"persPartAff\"", "batched:unicast");

                checkQueryFails(persPart, sql, true);
            }

            {
                String sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from \"persPartAff\".Person2 p1, \"persPart\".Person2 p2, \"orgPart\".Organization o " +
                    "where p1.affKey=p2._key and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    true,
                    2,
                    sql,
                    "batched:unicast", "batched:unicast");

                checkQueryPlan(persPart,
                    false,
                    2,
                    sql,
                    "batched:unicast", "batched:unicast");
            }

            {
                String sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from \"persPartAff\".Person2 p1, \"persPart\".Person2 p2, \"orgPart\".Organization o " +
                    "where p1.affKey > p2._key and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    true,
                    2,
                    sql,
                    "batched:broadcast", "batched:unicast");

                checkQueryPlan(persPart,
                    false,
                    2,
                    sql,
                    "batched:broadcast", "batched:unicast");
            }

            {
                // First join is collocated, second is replicated.

                String sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from \"persPartAff\".Person2 p1, \"persPart\".Person2 p2, \"orgRepl\".Organization o " +
                    "where p1.affKey=p2._key and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    true,
                    0,
                    sql);

                checkQueryPlan(persPart,
                    false,
                    0,
                    sql);
            }

            {
                String sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from \"persPartAff\".Person2 p1, \"persPart\".Person2 p2, \"orgRepl\".Organization o " +
                    "where p1._key=p2.name and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    false,
                    1,
                    sql,
                    "batched:unicast");

                sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from \"persPartAff\".Person2 p1, \"persPart\".Person2 p2, \"orgRepl\".Organization o " +
                    "where p1._key=p2._key and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    false,
                    0,
                    sql);

                sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from \"orgRepl\".Organization o, \"persPartAff\".Person2 p1, \"persPart\".Person2 p2 " +
                    "where p1._key=p2.name and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    false,
                    1,
                    sql,
                    "batched:unicast");

                sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from \"orgRepl\".Organization o, \"persPartAff\".Person2 p1, \"persPart\".Person2 p2 " +
                    "where p1._key=p2._key and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    false,
                    0,
                    sql);

                sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from (select _key, _val, * from \"orgRepl\".Organization) o, \"persPartAff\".Person2 p1, \"persPart\".Person2 p2 " +
                    "where p1._key=p2.name and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    false,
                    1,
                    sql,
                    "batched:unicast");

                sql = "select p1._key k1, p2._key k2, o._key k3 " +
                    "from (select _key, _val, * from \"orgRepl\".Organization) o, \"persPartAff\".Person2 p1, \"persPart\".Person2 p2 " +
                    "where p1._key=p2._key and p2.orgId = o._key";

                checkQueryPlan(persPart,
                    false,
                    0,
                    sql);
            }
        }
        finally {
            for (IgniteCache<Object, Object> cache : caches)
                ignite(0).destroyCache(cache.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoinsEnforceReplicatedNotLast() throws Exception {
        List<IgniteCache<Object, Object>> caches = new ArrayList<>();

        IgniteCache<Object, Object> persPart =
            ignite(0).createCache(cacheConfig("persPart", true, Integer.class, Person2.class));
        caches.add(persPart);

        IgniteCache<Object, Object> persPartAff =
            ignite(0).createCache(cacheConfig("persPartAff", true, TestKey.class, Person2.class));
        caches.add(persPartAff);

        IgniteCache<Object, Object> orgRepl =
            ignite(0).createCache(cacheConfig("orgRepl", false, Integer.class, Organization.class));
        caches.add(orgRepl);

        try {
            checkQueryFails(persPart, "select p1._key k1, p2._key k2, o._key k3 " +
                "from \"orgRepl\".Organization o, \"persPartAff\".Person2 p1, \"persPart\".Person2 p2 " +
                "where p1._key=p2._key and p2.orgId = o._key", true);

            checkQueryFails(persPart, "select p1._key k1, p2._key k2, o._key k3 " +
                "from \"persPartAff\".Person2 p1, \"orgRepl\".Organization o, \"persPart\".Person2 p2 " +
                "where p1._key=p2._key and p2.orgId = o._key", true);

            checkQueryFails(persPart, "select p1._key k1, p2._key k2, o._key k3 " +
                "from \"persPartAff\".Person2 p1, (select * from \"orgRepl\".Organization) o, \"persPart\".Person2 p2 " +
                "where p1._key=p2._key and p2.orgId = o._key", true);

            checkQueryPlan(persPart,
                true,
                0,
                "select p._key k1, o._key k2 from \"orgRepl\".Organization o, \"persPart\".Person2 p");

            checkQueryPlan(persPart,
                true,
                0,
                "select p._key k1, o._key k2 from \"orgRepl\".Organization o, \"persPart\".Person2 p union " +
                    "select p._key k1, o._key k2 from \"persPart\".Person2 p, \"orgRepl\".Organization o");
        }
        finally {
            for (IgniteCache<Object, Object> cache : caches)
                ignite(0).destroyCache(cache.getName());
        }
    }

    /**
     */
    public void testSchemaQuoted() {
        assert false; // TODO test hangs
        doTestSchemaName("\"ppAf\"");
    }

    /**
     */
    public void testSchemaQuotedUpper() {
        assert false; // TODO test hangs
        doTestSchemaName("\"PPAF\"");
    }

    /**
     */
    public void testSchemaUnquoted() {
        doTestSchemaName("ppAf");
    }

    /**
     */
    public void testSchemaUnquotedUpper() {
        doTestSchemaName("PPAF");
    }

    /**
     * @param schema Schema name.
     */
    public void doTestSchemaName(String schema) {
        CacheConfiguration ccfg = cacheConfig("persPartAff", true, Integer.class, Person2.class);

        ccfg.setSqlSchema(schema);

        IgniteCache<Integer, Person2> ppAf = ignite(0).createCache(ccfg);

        try {
            ppAf.put(1, new Person2(10, "Petya"));
            ppAf.put(2, new Person2(10, "Kolya"));

            List<List<?>> res = ppAf.query(new SqlFieldsQuery("select name from " +
                schema + ".Person2 order by _key")).getAll();

            assertEquals("Petya", res.get(0).get(0));
            assertEquals("Kolya", res.get(1).get(0));
        }
        finally {
            ppAf.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexSegmentation() throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", true,
            Integer.class, Person2.class).setQueryParallelism(4);
        CacheConfiguration ccfg2 = cacheConfig("org", true,
            Integer.class, Organization.class).setQueryParallelism(4);

        IgniteCache<Object, Object> c1 = ignite(0).getOrCreateCache(ccfg1);
        IgniteCache<Object, Object> c2 = ignite(0).getOrCreateCache(ccfg2);

        try {
            c2.put(1, new Organization("o1"));
            c2.put(2, new Organization("o2"));
            c1.put(3, new Person2(1, "p1"));
            c1.put(4, new Person2(2, "p2"));
            c1.put(5, new Person2(3, "p3"));

            String select0 = "select o.name n1, p.name n2 from \"pers\".Person2 p, \"org\".Organization o where p.orgId = o._key and o._key=1";

            checkQueryPlan(c1, true, 1, new SqlFieldsQuery(select0));

            checkQueryPlan(c1, true, 1, new SqlFieldsQuery(select0).setLocal(true));
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicationCacheIndexSegmentationFailure() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                CacheConfiguration ccfg = cacheConfig("org", false,
                    Integer.class, Organization.class).setQueryParallelism(4);

                IgniteCache<Object, Object> c = ignite(0).createCache(ccfg);

                return null;
            }
        }, CacheException.class, " Segmented indices are supported for PARTITIONED mode only.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexSegmentationPartitionedReplicated() throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", true,
            Integer.class, Person2.class).setQueryParallelism(4);
        CacheConfiguration ccfg2 = cacheConfig("org", false,
            Integer.class, Organization.class);

        final IgniteCache<Object, Object> c1 = ignite(0).getOrCreateCache(ccfg1);
        final IgniteCache<Object, Object> c2 = ignite(0).getOrCreateCache(ccfg2);

        try {
            c2.put(1, new Organization("o1"));
            c2.put(2, new Organization("o2"));
            c1.put(3, new Person2(1, "p1"));
            c1.put(4, new Person2(2, "p2"));
            c1.put(5, new Person2(3, "p3"));

            String select0 = "select o.name n1, p.name n2 from \"pers\".Person2 p, \"org\".Organization o where p.orgId = o._key";

            SqlFieldsQuery qry = new SqlFieldsQuery(select0);

            qry.setDistributedJoins(true);

            List<List<?>> results = c1.query(qry).getAll();

            assertEquals(2, results.size());

            select0 += " order by n2 desc";

            qry = new SqlFieldsQuery(select0);

            qry.setDistributedJoins(true);

            results = c1.query(qry).getAll();

            assertEquals(2, results.size());

            assertEquals("p2", results.get(0).get(1));
            assertEquals("p1", results.get(1).get(1));

            // Test for replicated subquery with aggregate.
            select0 = "select p.name " +
                "from \"pers\".Person2 p, " +
                "(select max(_key) orgId from \"org\".Organization) o " +
                "where p.orgId = o.orgId";

            X.println("Plan: \n" +
                c1.query(new SqlFieldsQuery("explain " + select0).setDistributedJoins(true)).getAll());

            qry = new SqlFieldsQuery(select0);

            qry.setDistributedJoins(true);

            results = c1.query(qry).getAll();

            assertEquals(1, results.size());
            assertEquals("p2", results.get(0).get(0));
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexWithDifferentSegmentationLevelsFailure() throws Exception {
        CacheConfiguration ccfg1 = cacheConfig("pers", true,
            Integer.class, Person2.class).setQueryParallelism(4);
        CacheConfiguration ccfg2 = cacheConfig("org", true,
            Integer.class, Organization.class).setQueryParallelism(3);

        final IgniteCache<Object, Object> c1 = ignite(0).getOrCreateCache(ccfg1);
        final IgniteCache<Object, Object> c2 = ignite(0).getOrCreateCache(ccfg2);

        try {
            c2.put(1, new Organization("o1"));
            c2.put(2, new Organization("o2"));
            c1.put(3, new Person2(1, "p1"));
            c1.put(4, new Person2(2, "p2"));
            c1.put(5, new Person2(3, "p3"));

            String select0 = "select o.name n1, p.name n2 from \"pers\".Person2 p, \"org\".Organization o where p.orgId = o._key and o._key=1";

            final SqlFieldsQuery qry = new SqlFieldsQuery(select0);

            qry.setDistributedJoins(true);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    c1.query(qry);

                    return null;
                }
            }, CacheException.class, "Using indexes with different parallelism levels in same query is forbidden.");
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @param cache Cache.
     * @param sql SQL.
     * @param enforceJoinOrder Enforce join order flag.
     */
    private void checkQueryFails(final IgniteCache<Object, Object> cache,
        String sql,
        boolean enforceJoinOrder) {
        final SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        qry.setDistributedJoins(true);
        qry.setEnforceJoinOrder(enforceJoinOrder);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.query(qry);

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     * @param cache Query cache.
     * @param select Select clause.
     * @param cache1 Cache name1.
     * @param cache2 Cache name2.
     * @param where Where clause.
     * @param testEnforceJoinOrder If {@code true} tests query with enforced join order.
     */
    private void checkNoBatchedJoin(IgniteCache<Object, Object> cache,
        String select,
        String cache1,
        String cache2,
        String where,
        boolean testEnforceJoinOrder) {
        checkQueryPlan(cache,
            false,
            0,
            select +
                "from " + cache1 + "," + cache2 + " " + where);

        checkQueryPlan(cache,
            false,
            0,
            select +
                "from " + cache2 + "," + cache1 + " " + where);

        if (testEnforceJoinOrder) {
            checkQueryPlan(cache,
                true,
                0,
                select +
                    "from " + cache1 + "," + cache2 + " " + where);

            checkQueryPlan(cache,
                true,
                0,
                select +
                    "from " + cache2 + "," + cache1 + " " + where);
        }
    }

    /**
     * @param cache Cache.
     * @param enforceJoinOrder Enforce join order flag.
     * @param expBatchedJoins Expected batched joins count.
     * @param sql Query.
     * @param expText Expected text to find in plan.
     */
    private void checkQueryPlan(IgniteCache<Object, Object> cache,
        boolean enforceJoinOrder,
        int expBatchedJoins,
        String sql,
        String...expText
    ) {
        checkQueryPlan(cache,
            enforceJoinOrder,
            expBatchedJoins,
            new SqlFieldsQuery(sql),
            expText);

        sql = "select * from (" + sql + ")";

        checkQueryPlan(cache,
            enforceJoinOrder,
            expBatchedJoins,
            new SqlFieldsQuery(sql),
            expText);

        sql = "select * from (" + sql + ")";

        checkQueryPlan(cache,
            enforceJoinOrder,
            expBatchedJoins,
            new SqlFieldsQuery(sql),
            expText);
    }

    /**
     * @param cache Cache.
     * @param enforceJoinOrder Enforce join order flag.
     * @param expBatchedJoins Expected batched joins count.
     * @param qry Query.
     * @param expText Expected text to find in plan.
     */
    private void checkQueryPlan(IgniteCache<Object, Object> cache,
        boolean enforceJoinOrder,
        int expBatchedJoins,
        SqlFieldsQuery qry,
        String... expText) {
        qry.setEnforceJoinOrder(enforceJoinOrder);
        qry.setDistributedJoins(true);

        String plan = queryPlan(cache, qry);

        log.info("\n  Plan:\n" + plan);

        assertEquals("Unexpected number of batched joins in plan [plan=" + plan + ", qry=" + qry + ']',
            expBatchedJoins,
            StringUtils.countOccurrencesOf(plan, "batched"));

        int startIdx = 0;

        for (String exp : expText) {
            int idx = plan.indexOf(exp, startIdx);

            if (idx == -1) {
                fail("Plan does not contain expected string [startIdx=" + startIdx +
                    ", plan=" + plan +
                    ", exp=" + exp + ']');
            }

            startIdx = idx + 1;
        }
    }

    /**
     * Test HAVING clause.
     */
    public void testHaving() {
        IgniteCache<Integer, Integer> c = ignite(0).getOrCreateCache(cacheConfig("having", true,
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
     * @param c1 Persons cache.
     * @param c2 Organizations cache.
     * @param orgs Number of organizations.
     * @param persons Number of persons.
     * @param pageSize Page size.
     * @param enforceJoinOrder Enforce join order.
     */
    private void doTestDistributedJoins(
        IgniteCache<?,?> qryCache,
        IgniteCache<Integer, Person2> c1,
        IgniteCache<Integer, Organization> c2,
        int orgs,
        int persons,
        int pageSize,
        boolean enforceJoinOrder
    ) {
        assertEquals(0, c1.size(CachePeekMode.ALL));
        assertEquals(0, c2.size(CachePeekMode.ALL));

        int key = 0;

        for (int i = 0; i < orgs; i++) {
            Organization o = new Organization();

            o.name = "Org" + i;

            c2.put(key++, o);
        }

        Random rnd = new GridRandom();

        for (int i = 0; i < persons; i++) {
            Person2 p = new Person2();

            p.name = "Person" + i;
            p.orgId = rnd.nextInt(orgs);

            c1.put(key++, p);
        }

        String select = "select count(*) from \"org\".Organization o, \"pers\".Person2 p where p.orgId = o._key";

        String plan = (String)qryCache.query(new SqlFieldsQuery("explain " + select)
            .setDistributedJoins(true).setEnforceJoinOrder(enforceJoinOrder).setPageSize(pageSize))
            .getAll().get(0).get(0);

        X.println("Plan : " + plan);

        if (enforceJoinOrder)
            assertTrue(plan, plan.contains("batched:broadcast"));
        else
            assertTrue(plan, plan.contains("batched:unicast"));

        assertEquals((long)persons, qryCache.query(new SqlFieldsQuery(select).setDistributedJoins(true)
            .setEnforceJoinOrder(enforceJoinOrder).setPageSize(pageSize)).getAll().get(0).get(0));

        c1.clear();
        c2.clear();

        assertEquals(0, c1.size(CachePeekMode.ALL));
        assertEquals(0L, c1.query(new SqlFieldsQuery(select).setDistributedJoins(true)
            .setEnforceJoinOrder(enforceJoinOrder).setPageSize(pageSize)).getAll().get(0).get(0));
    }

    /**
     * @param c Cache.
     * @param qry Query.
     * @param args Arguments.
     * @return Column as list.
     */
    private static <X> List<X> columnQuery(IgniteCache<?, ?> c, String qry, Object... args) {
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

    /**
     * @throws Exception If failed.
     */
    public void testJoinWithSubquery() throws Exception {
        IgniteCache<Integer, Contract> c1 = ignite(0).createCache(
            cacheConfig("Contract", true,
                Integer.class, Contract.class));

        IgniteCache<Integer, PromoContract> c2 = ignite(0).createCache(
            cacheConfig("PromoContract", true,
                Integer.class, PromoContract.class));

        for (int i = 0; i < 100; i++) {
            int coId = i % 10;
            int cust = i / 10;
            c1.put( i, new Contract(coId, cust));
        }

        for (int i = 0; i < 10; i++)
            c2.put(i, new PromoContract((i % 5) + 1, i));

        final List<List<?>> res = c2.query(new SqlFieldsQuery("SELECT CO.CO_ID \n" +
            "FROM PromoContract PMC  \n" +
            "INNER JOIN \"Contract\".Contract CO  ON PMC.CO_ID = 5  \n" +
            "AND PMC.CO_ID = CO.CO_ID  \n" +
            "INNER JOIN  (SELECT CO_ID FROM PromoContract EBP WHERE EBP.CO_ID = 5 LIMIT 1) VPMC  \n" +
            "ON PMC.CO_ID = VPMC.CO_ID ")).getAll();

        assertFalse(res.isEmpty());
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

    /**
     *
     */
    private static class Person2 implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int orgId;

        /** */
        @QuerySqlField(index = true)
        String name;

        /**
         *
         */
        public Person2() {
            // No-op.
        }

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person2(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }

    /**
     *
     */
    private static class TestKey implements Serializable {
        /** */
        @QuerySqlField(index = true)
        @AffinityKeyMapped
        int affKey;

        /** */
        @QuerySqlField()
        int id;

        /**
         * @param affKey Affinity key.
         * @param id ID.
         */
        public TestKey(int affKey, int id) {
            this.affKey = affKey;
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey personKey = (TestKey)o;

            return id == personKey.id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    private static class Organization implements Serializable {
        /** */
        @QuerySqlField
        String name;

        /**
         *
         */
        public Organization() {
            // No-op.
        }

        /**
         * @param name Organization name.
         */
        public Organization(String name) {
            this.name = name;
        }
    }

    /**
     *
     */
    private static class User implements Serializable {
        /** */
        @QuerySqlField
        private int id;
    }

    /**
     *
     */
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

    /** */
    private static class Contract implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private final int CO_ID;

        /** */
        @QuerySqlField(index = true)
        private final int CUSTOMER_ID;

        /** */
        public Contract(final int CO_ID, final int CUSTOMER_ID) {
            this.CO_ID = CO_ID;
            this.CUSTOMER_ID = CUSTOMER_ID;
        }

    }

    /** */
    public class PromoContract implements Serializable {
        /** */
        @QuerySqlField(index = true, orderedGroups = {
            @QuerySqlField.Group(name = "myIdx", order = 1)})
        private final int CO_ID;

        /** */
        @QuerySqlField(index = true, orderedGroups = {
            @QuerySqlField.Group(name = "myIdx", order = 0)})
        private final int OFFER_ID;

        /** */
        public PromoContract(final int co_Id, final int offer_Id) {
            this.CO_ID = co_Id;
            this.OFFER_ID = offer_Id;
        }
    }
}
