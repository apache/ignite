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
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.h2.H2TwoStepCachedQuery;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for behavior in various cases of local and distributed queries.
 */
public class IgniteCachelessQueriesSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final static String SELECT =
        "select count(*) from \"pers\".Person p, \"org\".Organization o where p.orgId = o._key";

    /** */
    private static final String ORG_CACHE_NAME = "org";

    /** */
    private static final String PERSON_CAHE_NAME = "pers";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration("MyCache", "affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** @return number of nodes to be prestarted. */
    private int nodesCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(nodesCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @param name Cache name.
     * @param mode Cache mode.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfig(String name, TestCacheMode mode, Class<?>... idxTypes) {
        return new CacheConfiguration<K, V>()
            .setName(name)
            .setCacheMode(mode == TestCacheMode.REPLICATED ? CacheMode.REPLICATED : CacheMode.PARTITIONED)
            .setQueryParallelism(mode == TestCacheMode.SEGMENTED ? 5 : 1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setIndexedTypes(idxTypes);
    }

    /**
     *
     */
    public void testDistributedQueryOnPartitionedCaches() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.PARTITIONED, false, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnPartitionedAndReplicatedCache() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.REPLICATED, false, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnReplicatedCaches() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.REPLICATED, false, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnSegmentedCaches() {
        createCachesAndExecuteQuery(TestCacheMode.SEGMENTED, TestCacheMode.SEGMENTED, false, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnReplicatedAndSegmentedCache() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.SEGMENTED, false, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnPartitionedCachesWithReplicatedFlag() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.PARTITIONED, true, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnPartitionedAndReplicatedCacheWithReplicatedFlag() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.REPLICATED, true, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testLocalQueryOnReplicatedCachesWithReplicatedFlag() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.REPLICATED, true, false);

        assertLocalQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnSegmentedCachesWithReplicatedFlag() {
        createCachesAndExecuteQuery(TestCacheMode.SEGMENTED, TestCacheMode.SEGMENTED, true, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testDistributedQueryOnReplicatedAndSegmentedCacheWithReplicatedFlag() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.SEGMENTED, true, false);

        assertDistributedQuery();
    }

    /**
     *
     */
    public void testLocalQueryOnPartitionedCachesWithLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.PARTITIONED, false, true);

        assertLocalQuery();
    }

    /**
     *
     */
    public void testLocalQueryOnPartitionedAndReplicatedCacheWithLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.REPLICATED, false, true);

        assertLocalQuery();
    }

    /**
     *
     */
    public void testLocalQueryOnReplicatedCachesWithLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.REPLICATED, false, true);

        assertLocalQuery();
    }

    /**
     *
     */
    public void testLocalTwoStepQueryOnSegmentedCachesWithLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.SEGMENTED, TestCacheMode.SEGMENTED, false, true);

        assertLocalTwoStepQuery();
    }

    /**
     *
     */
    public void testLocalTwoStepQueryOnReplicatedAndSegmentedCacheWithLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.SEGMENTED, false, true);

        assertLocalTwoStepQuery();
    }

    /**
     *
     */
    public void testLocalQueryOnPartitionedCachesWithReplicatedAndLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.PARTITIONED, false, true);

        assertLocalQuery();
    }

    /**
     *
     */
    public void testLocalQueryOnPartitionedAndReplicatedCacheWithReplicatedAndLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.PARTITIONED, TestCacheMode.REPLICATED, true, true);

        assertLocalQuery();
    }

    /**
     *
     */
    public void testLocalQueryOnReplicatedCachesWithReplicatedAndLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.REPLICATED, true, true);

        assertLocalQuery();
    }

    /**
     *
     */
    public void testLocalTwoStepQueryOnSegmentedCachesWithReplicatedAndLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.SEGMENTED, TestCacheMode.SEGMENTED, true, true);

        assertLocalTwoStepQuery();
    }

    /**
     *
     */
    public void testLocalTwoStepQueryOnReplicatedAndSegmentedCacheWithReplicatedAndLocalFlag() {
        createCachesAndExecuteQuery(TestCacheMode.REPLICATED, TestCacheMode.SEGMENTED, true, true);

        assertLocalTwoStepQuery();
    }

    /**
     * @param firstCacheMode First cache mode.
     * @param secondCacheMode Second cache mode.
     * @param replicatedOnly Replicated only query flag.
     * @param loc Local query flag.
     */
    private void createCachesAndExecuteQuery(TestCacheMode firstCacheMode, TestCacheMode secondCacheMode,
        boolean replicatedOnly, boolean loc) {
        Ignite node = ignite(0);

        node.createCache(cacheConfig(PERSON_CAHE_NAME, firstCacheMode, Integer.class, Person.class));
        node.createCache(cacheConfig(ORG_CACHE_NAME, secondCacheMode, Integer.class, Organization.class));

        IgniteCache<Integer, Person> c = node.cache(PERSON_CAHE_NAME);

        c.query(new SqlFieldsQuery(SELECT).setReplicatedOnly(replicatedOnly).setLocal(loc)).getAll();
    }

    /**
     * @return Cached two-step query, or {@code null} if none occurred.
     */
    private GridCacheTwoStepQuery cachedTwoStepQuery() {
        GridQueryIndexing idx = grid(0).context().query().getIndexing();

        Map<?, H2TwoStepCachedQuery> m = U.field(idx, "twoStepCache");

        if (m.isEmpty())
            return null;

        H2TwoStepCachedQuery q = m.values().iterator().next();

        return q.query();
    }

    /**
     * Check that truly distributed query has happened.
     */
    private void assertDistributedQuery() {
        GridCacheTwoStepQuery q = cachedTwoStepQuery();

        assertNotNull(q);

        assertFalse(q.isLocal());
    }

    /**
     * Check that local two-step query has happened.
     */
    private void assertLocalTwoStepQuery() {
        GridCacheTwoStepQuery q = cachedTwoStepQuery();

        assertNotNull(q);

        assertTrue(q.isLocal());
    }


    /**
     * Check that no distributed query has happened.
     */
    private void assertLocalQuery() {
        GridCacheTwoStepQuery q = cachedTwoStepQuery();

        assertNull(q);
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

    /**
     * Mode for test cache.
     */
    private enum TestCacheMode {
        /** */
        SEGMENTED,

        /** */
        PARTITIONED,

        /** */
        REPLICATED
    }
}
