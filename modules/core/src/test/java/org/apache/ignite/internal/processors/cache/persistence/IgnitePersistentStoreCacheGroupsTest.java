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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgnitePersistentStoreCacheGroupsTest extends GridCommonAbstractTest {
    /** */
    private static final String GROUP1 = "grp1";

    /** */
    private static final String GROUP2 = "grp2";

    /** */
    private CacheConfiguration[] ccfgs;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
            .setPageSize(1024)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** Entries count. */
    protected int entriesCount() {
        return 10;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterRestartStaticCaches1() throws Exception {
        clusterRestart(1, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterRestartStaticCaches2() throws Exception {
        clusterRestart(3, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterRestartDynamicCaches1() throws Exception {
        clusterRestart(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterRestartDynamicCaches2() throws Exception {
        clusterRestart(3, false);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testClusterRestartCachesWithH2Indexes() throws Exception {
        CacheConfiguration[] ccfgs1 = new CacheConfiguration[5];

        // Several caches with the same indexed type (and index names).
        ccfgs1[0] = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1).
            setIndexedTypes(Integer.class, Person.class);
        ccfgs1[1] = cacheConfiguration(GROUP1, "c2", PARTITIONED, TRANSACTIONAL, 1).
            setIndexedTypes(Integer.class, Person.class);
        ccfgs1[2] = cacheConfiguration(GROUP2, "c3", PARTITIONED, ATOMIC, 1).
            setIndexedTypes(Integer.class, Person.class);
        ccfgs1[3] = cacheConfiguration(GROUP2, "c4", PARTITIONED, TRANSACTIONAL, 1).
            setIndexedTypes(Integer.class, Person.class);
        ccfgs1[4] = cacheConfiguration(null, "c5", PARTITIONED, ATOMIC, 1).
            setIndexedTypes(Integer.class, Person.class);

        String[] caches = {"c1", "c2", "c3", "c4", "c5"};

        startGrids(3);

        Ignite node = ignite(0);

        node.active(true);

        node.createCaches(Arrays.asList(ccfgs1));

        putPersons(caches, node);

        checkPersons(caches, node);
        checkPersonsQuery(caches, node);

        stopAllGrids();

        startGrids(3);

        node = ignite(0);

        node.active(true);

        awaitPartitionMapExchange();

        checkPersons(caches, node);
        checkPersonsQuery(caches, node);

        Random rnd = ThreadLocalRandom.current();

        int idx = rnd.nextInt(caches.length);

        String cacheName = caches[idx];
        CacheConfiguration cacheCfg = ccfgs1[idx];

        node.destroyCache(cacheName);

        node.createCache(cacheCfg);

        putPersons(new String[]{cacheName}, node);

        checkPersons(caches, node);
        checkPersonsQuery(caches, node);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpiryPolicy() throws Exception {
        long ttl = 10 * 60000;

        CacheConfiguration[] ccfgs1 = new CacheConfiguration[5];

        ccfgs1[0] = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1);
        ccfgs1[1] = cacheConfiguration(GROUP1, "c2", PARTITIONED, TRANSACTIONAL, 1);
        ccfgs1[2] = cacheConfiguration(GROUP2, "c3", PARTITIONED, ATOMIC, 1);
        ccfgs1[3] = cacheConfiguration(GROUP2, "c4", PARTITIONED, TRANSACTIONAL, 1);
        ccfgs1[4] = cacheConfiguration(null, "c5", PARTITIONED, ATOMIC, 1);

        String[] caches = {"c1", "c2", "c3", "c4", "c5"};

        startGrids(3);

        Ignite node = ignite(0);

        node.cluster().active(true);

        node.createCaches(Arrays.asList(ccfgs1));

        ExpiryPolicy plc = new PlatformExpiryPolicy(ttl, -2, -2);

        Map<String, Map<Integer, Long>> expTimes = new HashMap<>();

        for (String cacheName : caches) {
            Map<Integer, Long> cacheExpTimes = new HashMap<>();
            expTimes.put(cacheName, cacheExpTimes);

            IgniteCache<Object, Object> cache = node.cache(cacheName).withExpiryPolicy(plc);

            for (int i = 0; i < entriesCount(); i++) {
                Integer key = i;

                cache.put(key, cacheName + i);

                IgniteKernal primaryNode = (IgniteKernal)primaryCache(i, cacheName).unwrap(Ignite.class);
                GridCacheEntryEx entry = primaryNode.internalCache(cacheName).entryEx(key);
                entry.unswap();

                assertTrue(entry.expireTime() > 0);
                cacheExpTimes.put(key, entry.expireTime());
            }
        }

        stopAllGrids();

        startGrids(3);

        node = ignite(0);

        node.cluster().active(true);

        for (String cacheName : caches) {
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < entriesCount(); i++) {
                Integer key = i;

                assertEquals(cacheName + i, cache.get(i));

                IgniteKernal primaryNode = (IgniteKernal)primaryCache(i, cacheName).unwrap(Ignite.class);
                GridCacheEntryEx entry = primaryNode.internalCache(cacheName).entryEx(key);
                entry.unswap();

                assertEquals(expTimes.get(cacheName).get(key), (Long)entry.expireTime());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDropCache() throws Exception {
        ccfgs = new CacheConfiguration[]{cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1)
            .setIndexedTypes(Integer.class, Person.class)};

        Ignite ignite = startGrid();

        ignite.active(true);

        ignite.cache("c1").destroy();

        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDropCache1() throws Exception {
        CacheConfiguration ccfg1 = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1);

        CacheConfiguration ccfg2 = cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 1);

        Ignite ignite = startGrid();

        ignite.active(true);

        ignite.createCaches(Arrays.asList(ccfg1, ccfg2));

        ignite.cache("c1").destroy();

        ignite.cache("c2").destroy();

        ignite.createCache(ccfg1);
        ignite.createCache(ccfg2);

        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDropCache2() throws Exception {
        CacheConfiguration ccfg1 = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1)
            .setIndexedTypes(Integer.class, Person.class);

        CacheConfiguration ccfg2 = cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 1)
            .setIndexedTypes(Integer.class, Person.class);

        Ignite ignite = startGrid();

        ignite.active(true);

        ignite.createCaches(Arrays.asList(ccfg1, ccfg2));

        ignite.cache("c1").destroy();

        ignite.createCache(ccfg1);

        stopGrid();
    }

    /**
     * @param caches Cache names to put data into.
     * @param node Ignite node.
     */
    private void putPersons(String[] caches, Ignite node) {
        for (String cacheName : caches) {
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < entriesCount(); i++)
                cache.put(i, new Person("" + i, cacheName));
        }
    }

    /**
     * @param caches Cache names to invoke a query against to.
     * @param node Ignite node.
     */
    private void checkPersons(String[] caches, Ignite node) {
        for (String cacheName : caches) {
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < entriesCount(); i++)
                assertEquals(new Person("" + i, cacheName), cache.get(i));

            assertEquals(entriesCount(), cache.size());
        }
    }

    /**
     * @param caches Cache names to invoke a query against to.
     * @param node Ignite node.
     */
    private void checkPersonsQuery(String[] caches, Ignite node) {
        SqlQuery<Integer, Person> qry = new SqlQuery<>(
            Person.class, "SELECT p.* FROM Person p WHERE p.lname=? ORDER BY p.fname");

        for (String cacheName : caches) {
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            List<Cache.Entry<Integer, Person>> persons = cache.query(qry.setArgs(cacheName)).getAll();

            for (int i = 0; i < entriesCount(); i++)
                assertEquals(new Person("" + i, cacheName), persons.get(i).getValue());

            assertEquals(entriesCount(), persons.size());
        }
    }

    /**
     * @param nodes Nodes number.
     * @param staticCaches {@code True} if caches should be statically configured.
     * @throws Exception If failed.
     */
    private void clusterRestart(int nodes, boolean staticCaches) throws Exception {
        CacheConfiguration[] ccfgs = new CacheConfiguration[5];

        ccfgs[0] = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1);
        ccfgs[1] = cacheConfiguration(GROUP1, "c2", PARTITIONED, TRANSACTIONAL, 1);
        ccfgs[2] = cacheConfiguration(GROUP2, "c3", PARTITIONED, ATOMIC, 1);
        ccfgs[3] = cacheConfiguration(GROUP2, "c4", PARTITIONED, TRANSACTIONAL, 1);
        ccfgs[4] = cacheConfiguration(null, "c5", PARTITIONED, ATOMIC, 1);

        String[] caches = {"c1", "c2", "c3", "c4", "c5"};

        for (int i = 0; i < nodes; i++) {
            if (staticCaches)
                this.ccfgs = ccfgs;

            startGrid(i);
        }

        Ignite node = ignite(0);

        node.active(true);

        if (!staticCaches)
            node.createCaches(Arrays.asList(ccfgs));

        for (String cacheName : caches) {
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < entriesCount(); i++) {
                cache.put(i, cacheName + i);

                assertEquals(cacheName + i, cache.get(i));
            }

            assertEquals(entriesCount(), cache.size());
        }

        stopAllGrids();

        node = startGrids(nodes);

        node.active(true);

        awaitPartitionMapExchange();

        for (String cacheName : caches) {
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < entriesCount(); i++)
                assertEquals(cacheName + i, cache.get(i));

            assertEquals(entriesCount(), cache.size());
        }
    }

    /**
     * @param grpName Cache group name.
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Backups number.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(
        String grpName,
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     *
     */
    static class Person implements Serializable {
        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        String fName;

        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        String lName;

        /**
         * @param fName First name.
         * @param lName Last name.
         */
        public Person(String fName, String lName) {
            this.fName = fName;
            this.lName = lName;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Person person = (Person)o;
            return Objects.equals(fName, person.fName) &&
                Objects.equals(lName, person.lName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(fName, lName);
        }
    }
}
