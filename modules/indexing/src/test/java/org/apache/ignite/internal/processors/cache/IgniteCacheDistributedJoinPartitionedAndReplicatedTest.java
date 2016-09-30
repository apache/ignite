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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheDistributedJoinPartitionedAndReplicatedTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private static final String ORG_CACHE = "org";

    /** */
    private static final String ACCOUNT_CACHE = "acc";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = ((TcpDiscoverySpi)cfg.getDiscoverySpi());

        spi.setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @return Cache configuration.
     */
    private CacheConfiguration configuration(String name, CacheMode cacheMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @param idx Use index flag.
     * @param persCacheMode Person cache mode.
     * @param orgCacheMode Organization cache mode.
     * @param accCacheMode Account cache mode.
     * @return Configurations.
     */
    private List<CacheConfiguration> caches(boolean idx,
        CacheMode persCacheMode,
        CacheMode orgCacheMode,
        CacheMode accCacheMode) {
        List<CacheConfiguration> ccfgs = new ArrayList<>();

        {
            CacheConfiguration ccfg = configuration(PERSON_CACHE, persCacheMode);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Person.class.getName());
            entity.addQueryField("orgId", Integer.class.getName(), null);
            entity.addQueryField("name", String.class.getName(), null);

            if (idx)
                entity.setIndexes(F.asList(new QueryIndex("orgId"), new QueryIndex("name")));

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ORG_CACHE, orgCacheMode);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Organization.class.getName());
            entity.addQueryField("name", String.class.getName(), null);

            if (idx)
                entity.setIndexes(F.asList(new QueryIndex("name")));

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ACCOUNT_CACHE, accCacheMode);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Account.class.getName());
            entity.addQueryField("orgId", Integer.class.getName(), null);
            entity.addQueryField("personId", Integer.class.getName(), null);
            entity.addQueryField("name", String.class.getName(), null);

            if (idx) {
                entity.setIndexes(F.asList(new QueryIndex("orgId"),
                    new QueryIndex("personId"),
                    new QueryIndex("name")));
            }

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        return ccfgs;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);

        client = true;

        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoin1() throws Exception {
        join(true, REPLICATED, PARTITIONED, PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoin2() throws Exception {
        join(true, PARTITIONED, REPLICATED, PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoin3() throws Exception {
        join(true, PARTITIONED, PARTITIONED, REPLICATED);
    }

    /**
     * @param idx Use index flag.
     * @param persCacheMode Person cache mode.
     * @param orgCacheMode Organization cache mode.
     * @param accCacheMode Account cache mode.
     * @throws Exception If failed.
     */
    private void join(boolean idx, CacheMode persCacheMode, CacheMode orgCacheMode, CacheMode accCacheMode)
        throws Exception {
        Ignite client = grid(2);

        for (CacheConfiguration ccfg : caches(idx, persCacheMode, orgCacheMode, accCacheMode))
            client.createCache(ccfg);

        try {
            IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);
            IgniteCache<Object, Object> orgCache = client.cache(ORG_CACHE);
            IgniteCache<Object, Object> accCache = client.cache(ACCOUNT_CACHE);

            Affinity<Object> aff = client.affinity(PERSON_CACHE);

            AtomicInteger pKey = new AtomicInteger(100_000);
            AtomicInteger orgKey = new AtomicInteger();
            AtomicInteger accKey = new AtomicInteger();

            ClusterNode node0 = ignite(0).cluster().localNode();
            ClusterNode node1 = ignite(1).cluster().localNode();

            /**
             * One organization, one person, two accounts.
             */

            {
                int orgId1 = keyForNode(aff, orgKey, node0);

                orgCache.put(orgId1, new Organization("obj-" + orgId1));

                int pid1 = keyForNode(aff, pKey, node0);
                personCache.put(pid1, new Person(orgId1, "o1-p1"));

                accCache.put(keyForNode(aff, accKey, node0), new Account(pid1, orgId1, "a0"));
                accCache.put(keyForNode(aff, accKey, node1), new Account(pid1, orgId1, "a1"));
            }

            IgniteCache<Object, Object> qryCache = replicated(orgCache) ? personCache : orgCache;

            checkQuery("select p._key, p.name, a.name " +
                "from \"person\".Person p, \"acc\".Account a " +
                "where p._key = a.personId", qryCache, false, 2);

            checkQuery("select o.name, p._key, p.name, a.name " +
                "from \"org\".Organization o, \"person\".Person p, \"acc\".Account a " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key", qryCache, false, 2);

            checkQuery("select o.name, p._key, p.name, a.name " +
                "from \"org\".Organization o, \"acc\".Account a, \"person\".Person p " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key", qryCache, false, 2);

            checkQuery("select o.name, p._key, p.name, a.name " +
                "from \"person\".Person p, \"org\".Organization o, \"acc\".Account a " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key", qryCache, false, 2);

            checkQuery("select * from (select o.name n1, p._key, p.name n2, a.name n3 " +
                "from \"acc\".Account a, \"person\".Person p, \"org\".Organization o " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key)", qryCache, false, 2);

            checkQuery("select * from (select o.name n1, p._key, p.name n2, a.name n3 " +
                "from \"person\".Person p, \"acc\".Account a, \"org\".Organization o " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key)", qryCache, false, 2);

            List<List<?>> res = checkQuery("select count(*) " +
                "from \"org\".Organization o, \"person\".Person p, \"acc\".Account a " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key", qryCache, false, 1);

            assertEquals(2L, res.get(0).get(0));

            checkQueries(qryCache, 2);

            {
                int orgId2 = keyForNode(aff, orgKey, node1);

                orgCache.put(orgId2, new Organization("obj-" + orgId2));

                int pid2 = keyForNode(aff, pKey, node0);
                personCache.put(pid2, new Person(orgId2, "o2-p1"));

                accCache.put(keyForNode(aff, accKey, node0), new Account(pid2, orgId2, "a3"));
                accCache.put(keyForNode(aff, accKey, node1), new Account(pid2, orgId2, "a4"));
            }

            checkQuery("select o.name, p._key, p.name, a.name " +
                "from \"org\".Organization o, \"person\".Person p, \"acc\".Account a " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key", qryCache, false, 4);

            checkQuery("select o.name, p._key, p.name, a.name " +
                "from \"org\".Organization o inner join \"person\".Person p on p.orgId = o._key " +
                "inner join \"acc\".Account a on p._key = a.personId and a.orgId=o._key", qryCache, false, 4);

            res = checkQuery("select count(*) " +
                "from \"org\".Organization o, \"person\".Person p, \"acc\".Account a " +
                "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key", qryCache, false, 1);

            assertEquals(4L, res.get(0).get(0));

            checkQuery("select o.name, p._key, p.name, a.name " +
                "from \"org\".Organization o, \"person\".Person p, \"acc\".Account a " +
                "where p.orgId = o._key and a.orgId = o._key and a.orgId=o._key", qryCache, false, 4);

            res = checkQuery("select count(*) " +
                "from \"org\".Organization o, \"person\".Person p, \"acc\".Account a " +
                "where p.orgId = o._key and a.orgId = o._key and a.orgId=o._key", qryCache, false, 1);

            assertEquals(4L, res.get(0).get(0));

            checkQueries(qryCache, 4);
        }
        finally {
            client.destroyCache(PERSON_CACHE);
            client.destroyCache(ORG_CACHE);
            client.destroyCache(ACCOUNT_CACHE);
        }
    }

    /**
     * @param qryCache Query cache.
     * @param expSize Expected results size.
     */
    private void checkQueries(IgniteCache<Object, Object> qryCache, int expSize) {
        String[] cacheNames = {"\"org\".Organization o", "\"person\".Person p", "\"acc\".Account a"};

        for (int c1 = 0; c1 < cacheNames.length; c1++) {
            for (int c2 = 0; c2 < cacheNames.length; c2++) {
                if (c2 == c1)
                    continue;

                for (int c3 = 0; c3 < cacheNames.length; c3++) {
                    if (c3 == c1 || c3 == c2)
                        continue;

                    String cache1 = cacheNames[c1];
                    String cache2 = cacheNames[c2];
                    String cache3 = cacheNames[c3];

                    String qry = "select o.name, p._key, p.name, a.name from " +
                        cache1 + ", " +
                        cache2 + ", " +
                        cache3 + " " +
                        "where p.orgId = o._key and p._key = a.personId and a.orgId=o._key";

                    checkQuery(qry, qryCache, false, expSize);

                    qry = "select o.name, p._key, p.name, a.name from " +
                        cache1 + ", " +
                        cache2 + ", " +
                        cache3 + " " +
                        "where p.orgId = o._key and a.orgId = o._key and a.orgId=o._key";

                    checkQuery(qry, qryCache, false, expSize);
                }
            }
        }
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param enforceJoinOrder Enforce join order flag.
     * @param expSize Expected results size.
     * @param args Arguments.
     * @return Results.
     */
    private List<List<?>> checkQuery(String sql,
        IgniteCache<Object, Object> cache,
        boolean enforceJoinOrder,
        int expSize,
        Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        qry.setDistributedJoins(true);
        qry.setEnforceJoinOrder(enforceJoinOrder);
        qry.setArgs(args);

        log.info("Plan: " + queryPlan(cache, qry));

        QueryCursor<List<?>> cur = cache.query(qry);

        List<List<?>> res = cur.getAll();

        if (expSize != res.size())
            log.info("Results: " + res);

        assertEquals(expSize, res.size());

        return res;
    }

    /**
     * @param cache Cache.
     * @return {@code True} if cache is replicated.
     */
    private boolean replicated(IgniteCache<?, ?> cache) {
        return cache.getConfiguration(CacheConfiguration.class).getCacheMode() == REPLICATED;
    }

    /**
     *
     */
    private static class Account implements Serializable {
        /** */
        int personId;

        /** */
        int orgId;

        /** */
        String name;

        /**
         * @param personId Person ID.
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Account(int personId, int orgId, String name) {
            this.personId = personId;
            this.orgId = orgId;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        int orgId;

        /** */
        String name;

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }

    /**
     *
     */
    private static class Organization implements Serializable {
        /** */
        String name;

        /**
         * @param name Name.
         */
        public Organization(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Organization.class, this);
        }
    }
}
