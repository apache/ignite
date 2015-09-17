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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
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
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

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
}