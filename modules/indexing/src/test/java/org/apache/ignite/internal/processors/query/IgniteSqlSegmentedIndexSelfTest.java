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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for correct distributed queries with index consisted of many segments.
 */
public class IgniteSqlSegmentedIndexSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration("MyCache", "affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSqlQueryParallelismLevel(97);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static <K, V> CacheConfiguration<K, V> cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration<K, V>()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setIndexedTypes(idxTypes);
    }

    /**
     * Run tests on single-node grid
     * @throws Exception If failed.
     */
    public void testSingleNodeIndexSegmentation() throws Exception {
        startGridsMultiThreaded(1, true);

        fillCache();

        checkDistributedQueryWithSegmentedIndex();

        checkLocalQueryWithSegmentedIndex();
    }

    /**
     * Run tests on multi-node grid
     * @throws Exception If failed.
     */
    public void testMultiNodeIndexSegmentation() throws Exception {
        startGridsMultiThreaded(4, true);

        fillCache();

        checkDistributedQueryWithSegmentedIndex();

        checkLocalQueryWithSegmentedIndex();
    }

    /**
     * Check distributed joins.
     * @throws Exception If failed.
     */
    public void checkDistributedQueryWithSegmentedIndex() throws Exception {
        IgniteCache<Integer, Person> c1 = ignite(0).cache("pers");

        int expectedPersons = 0;

        for (Cache.Entry<Integer, Person> e : c1) {
            final Integer orgId = e.getValue().orgId;

            if (10 <= orgId && orgId < 500)
                expectedPersons++;
        }

        String select0 = "select o.name n1, p.name n2 from \"pers\".Person p, \"org\".Organization o where p.orgId = o._key";

        List<List<?>> result = c1.query(new SqlFieldsQuery(select0).setDistributedJoins(true)).getAll();

        assertEquals(expectedPersons, result.size());
    }

    /**
     * Test local query.
     * @throws Exception If failed.
     */
    public void checkLocalQueryWithSegmentedIndex() throws Exception {
        IgniteCache<Integer, Person> c1 = ignite(0).cache("pers");
        IgniteCache<Integer, Organization> c2 = ignite(0).cache("org");

        Set<Integer> localOrgIds = new HashSet<>();

        for(Cache.Entry<Integer, Organization> e : c2.localEntries())
            localOrgIds.add(e.getKey());

        int expectedPersons = 0;

        for (Cache.Entry<Integer, Person> e : c1.localEntries()) {
            final Integer orgId = e.getValue().orgId;

            if (localOrgIds.contains(orgId))
                expectedPersons++;
        }

        String select0 = "select o.name n1, p.name n2 from \"pers\".Person p, \"org\".Organization o where p.orgId = o._key";

        List<List<?>> result = c1.query(new SqlFieldsQuery(select0).setLocal(true)).getAll();

        assertEquals(expectedPersons, result.size());
    }

    /** */
    private void fillCache() {
        CacheConfiguration<Object, Object> ccfg1 = cacheConfig("pers", true,
            Integer.class, Person.class).setIndexSegmentationEnabled(true);

        CacheConfiguration<Object, Object> ccfg2 = cacheConfig("org", true,
            Integer.class, Organization.class).setIndexSegmentationEnabled(true);

        IgniteCache<Object, Object> c1 = ignite(0).getOrCreateCache(ccfg1);

        IgniteCache<Object, Object> c2 = ignite(0).getOrCreateCache(ccfg2);

        final int orgCount = 500;

        for (int i = 0; i < orgCount; i++)
            c2.put(i, new Organization("org-" + i));

        for (int i = 0; i < 1000; i++) {
            int orgID = i % orgCount + 10;

            c1.put(i, new Person(orgID, "pers-" + i));
        }
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField(index = true)
        Integer orgId;

        /** */
        @QuerySqlField
        String name;

        /**
         *
         */
        public Person() {
            // No-op.
        }

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
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
}
