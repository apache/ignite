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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for correct distributed queries with index consisted of many segments.
 */
@RunWith(JUnit4.class)
public class IgniteSqlSegmentedIndexSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final String ORG_CACHE_NAME = "org";

    /** */
    private static final String PERSON_CAHE_NAME = "pers";

    /** */
    private static final int ORG_CACHE_SIZE = 500;

    /** */
    private static final int PERSON_CACHE_SIZE = 1000;

    /** */
    private static final int ORPHAN_ROWS = 10;

    /** */
    private static int QRY_PARALLELISM_LVL = 97;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration("MyCache", "affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** @return number of nodes to be prestarted. */
    protected int nodesCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(nodesCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).destroyCaches(Arrays.asList(PERSON_CAHE_NAME, ORG_CACHE_NAME));
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration<K, V>()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setQueryParallelism(partitioned ? QRY_PARALLELISM_LVL : 1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setIndexedTypes(idxTypes);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentedIndex() throws Exception {
        ignite(0).createCache(cacheConfig(PERSON_CAHE_NAME, true, Integer.class, Person.class));
        ignite(0).createCache(cacheConfig(ORG_CACHE_NAME, true, Integer.class, Organization.class));

        fillCache();

        checkDistributedQueryWithSegmentedIndex();

        checkLocalQueryWithSegmentedIndex();

        checkLocalSizeQueryWithSegmentedIndex();
    }

    /**
     * Check correct index snapshots with segmented indices.
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentedIndexReproducableResults() throws Exception {
        ignite(0).createCache(cacheConfig(ORG_CACHE_NAME, true, Integer.class, Organization.class));

        IgniteCache<Object, Object> cache = ignite(0).cache(ORG_CACHE_NAME);

        // Unequal entries distribution among partitions.
        int expSize = nodesCount() * QRY_PARALLELISM_LVL *  3 / 2;

        for (int i = 0; i < expSize; i++)
            cache.put(i, new Organization("org-" + i));

        String select0 = "select * from \"org\".Organization o";

        // Check for stable results.
        for(int i = 0; i < 10; i++) {
            List<List<?>> res = cache.query(new SqlFieldsQuery(select0)).getAll();

            assertEquals(expSize, res.size());
        }
    }

    /**
     * Checks correct <code>select count(*)</code> result with segmented indices.
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentedIndexSizeReproducableResults() throws Exception {
        ignite(0).createCache(cacheConfig(ORG_CACHE_NAME, true, Integer.class, Organization.class));

        IgniteCache<Object, Object> cache = ignite(0).cache(ORG_CACHE_NAME);

        // Unequal entries distribution among partitions.
        long expSize = nodesCount() * QRY_PARALLELISM_LVL *  3 / 2;

        for (int i = 0; i < expSize; i++)
            cache.put(i, new Organization("org-" + i));

        String select0 = "select count(*) from \"org\".Organization o";

        // Check for stable results.
        for(int i = 0; i < 10; i++) {
            List<List<?>> res = cache.query(new SqlFieldsQuery(select0)).getAll();

            assertEquals(expSize, res.get(0).get(0));
        }
    }

    /**
     * Run tests on single-node grid
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentedIndexWithEvictionPolicy() throws Exception {
        final IgniteCache<Object, Object> cache = ignite(0).createCache(
            cacheConfig(ORG_CACHE_NAME, true, Integer.class, Organization.class)
                .setEvictionPolicy(new FifoEvictionPolicy(10))
                .setOnheapCacheEnabled(true));

        final long SIZE = 20;

        for (int i = 0; i < SIZE; i++)
            cache.put(i, new Organization("org-" + i));

        String select0 = "select name from \"org\".Organization";

        List<List<?>> res = cache.query(new SqlFieldsQuery(select0)).getAll();

        assertEquals(SIZE, res.size());
    }

    /**
     * Verifies that <code>select count(*)</code> return valid result on a single-node grid.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSizeOnSegmentedIndexWithEvictionPolicy() throws Exception {
        final IgniteCache<Object, Object> cache = ignite(0).createCache(
            cacheConfig(ORG_CACHE_NAME, true, Integer.class, Organization.class)
                .setEvictionPolicy(new FifoEvictionPolicy(10))
                .setOnheapCacheEnabled(true));

        final long SIZE = 20;

        for (int i = 0; i < SIZE; i++)
            cache.put(i, new Organization("org-" + i));

        String select0 = "select count(*) from \"org\".Organization";

        List<List<?>> res = cache.query(new SqlFieldsQuery(select0)).getAll();

        assertEquals(SIZE, res.get(0).get(0));
    }

    /**
     * Run tests on multi-node grid
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentedPartitionedWithReplicated() throws Exception {
        ignite(0).createCache(cacheConfig(PERSON_CAHE_NAME, true, Integer.class, Person.class));
        ignite(0).createCache(cacheConfig(ORG_CACHE_NAME, false, Integer.class, Organization.class));

        fillCache();

        checkDistributedQueryWithSegmentedIndex();

        checkLocalQueryWithSegmentedIndex();

        checkLocalSizeQueryWithSegmentedIndex();
    }

    /**
     * Check distributed joins.
     *
     * @throws Exception If failed.
     */
    public void checkDistributedQueryWithSegmentedIndex() throws Exception {
        for (int i = 0; i < nodesCount(); i++) {
            IgniteCache<Integer, Person> c1 = ignite(i).cache(PERSON_CAHE_NAME);

            long expPersons = 0;

            for (Cache.Entry<Integer, Person> e : c1) {
                final Integer orgId = e.getValue().orgId;

                // We have as orphan ORG rows as orphan PERSON rows.
                if (ORPHAN_ROWS <= orgId && orgId < 500)
                    expPersons++;
            }

            String select0 = "select o.name n1, p.name n2 from \"pers\".Person p, \"org\".Organization o where p.orgId = o._key";

            List<List<?>> res = c1.query(new SqlFieldsQuery(select0).setDistributedJoins(true)).getAll();

            assertEquals(expPersons, res.size());
        }
    }

    /**
     * Test local query.
     *
     * @throws Exception If failed.
     */
    public void checkLocalQueryWithSegmentedIndex() throws Exception {
        for (int i = 0; i < nodesCount(); i++) {
            final Ignite node = ignite(i);

            IgniteCache<Integer, Person> c1 = node.cache(PERSON_CAHE_NAME);
            IgniteCache<Integer, Organization> c2 = node.cache(ORG_CACHE_NAME);

            Set<Integer> locOrgIds = new HashSet<>();

            for (Cache.Entry<Integer, Organization> e : c2.localEntries())
                locOrgIds.add(e.getKey());

            long expPersons = 0;

            for (Cache.Entry<Integer, Person> e : c1.localEntries()) {
                final Integer orgId = e.getValue().orgId;

                if (locOrgIds.contains(orgId))
                    expPersons++;
            }

            String select0 = "select o.name n1, p.name n2 from \"pers\".Person p, \"org\".Organization o where p.orgId = o._key";

            List<List<?>> res = c1.query(new SqlFieldsQuery(select0).setLocal(true)).getAll();

            assertEquals(expPersons, res.size());
        }
    }

    /**
     * Verifies that local <code>select count(*)</code> query returns a correct result.
     *
     * @throws Exception If failed.
     */
    public void checkLocalSizeQueryWithSegmentedIndex() throws Exception {
        for (int i = 0; i < nodesCount(); i++) {
            final Ignite node = ignite(i);

            IgniteCache<Integer, Person> c1 = node.cache(PERSON_CAHE_NAME);
            IgniteCache<Integer, Organization> c2 = node.cache(ORG_CACHE_NAME);

            Set<Integer> locOrgIds = new HashSet<>();

            for (Cache.Entry<Integer, Organization> e : c2.localEntries())
                locOrgIds.add(e.getKey());

            int expPersons = 0;

            for (Cache.Entry<Integer, Person> e : c1.localEntries()) {
                final Integer orgId = e.getValue().orgId;

                if (locOrgIds.contains(orgId))
                    expPersons++;
            }

            String select0 = "select count(*) from \"pers\".Person p, \"org\".Organization o where p.orgId = o._key";

            List<List<?>> res = c1.query(new SqlFieldsQuery(select0).setLocal(true)).getAll();

            assertEquals((long) expPersons, res.get(0).get(0));
        }
    }

    /** */
    private void fillCache() {
        IgniteCache<Object, Object> c1 = ignite(0).cache(PERSON_CAHE_NAME);
        IgniteCache<Object, Object> c2 = ignite(0).cache(ORG_CACHE_NAME);

        for (int i = 0; i < ORG_CACHE_SIZE; i++)
            c2.put(i, new Organization("org-" + i));

        final Random random = new Random();

        for (int i = 0; i < PERSON_CACHE_SIZE; i++) {
            // We have as orphan ORG rows as orphan PERSON rows.
            int orgID = ORPHAN_ROWS + random.nextInt(ORG_CACHE_SIZE + ORPHAN_ROWS);

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
