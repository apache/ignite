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
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheDistributedJoinQueryConditionsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private static final String ORG_CACHE = "org";

    /** */
    private boolean client;

    /** */
    private int total;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = ((TcpDiscoverySpi) cfg.getDiscoverySpi());

        spi.setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
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
    public void testJoinQuery1() throws Exception {
        joinQuery1(true);
    }

    /**
     * @param idx Use index flag.
     * @throws Exception If failed.
     */
    private void joinQuery1(boolean idx) throws Exception {
        Ignite client = grid(2);

        try {
            CacheConfiguration ccfg1 =
                cacheConfiguration(PERSON_CACHE).setQueryEntities(F.asList(personEntity(idx, idx)));
            CacheConfiguration ccfg2 =
                cacheConfiguration(ORG_CACHE).setQueryEntities(F.asList(organizationEntity(idx)));

            IgniteCache<Object, Object> pCache = client.createCache(ccfg1);
            client.createCache(ccfg2);

            List<Integer> orgIds = putData1();

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key", pCache, total);

            checkQuery("select * from (select o._key, o.name, p._key pKey, p.name pName " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key)", pCache, total);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o inner join Person p " +
                "on p.orgId = o._key", pCache, total);

            checkQuery("select * from (select o._key o_key, o.name o_name, p._key p_key, p.name p_name " +
                "from \"org\".Organization o inner join Person p " +
                "on p.orgId = o._key)", pCache, total);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and o._key=" + orgIds.get(3), pCache, 3);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and o._key IN (" + orgIds.get(2) + "," + orgIds.get(3) + ")", pCache, 5);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and o._key IN (" + orgIds.get(2) + "," + orgIds.get(3) + ")", pCache, 5);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and o._key > " + orgIds.get(2), pCache, total - 3);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and o._key > " + orgIds.get(1) + " and o._key < " + orgIds.get(4), pCache, 5);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.name = o.name", pCache, total);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.name = o.name and o._key=" + orgIds.get(0), pCache, 0);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.name = o.name and o._key=" + orgIds.get(3), pCache, 3);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.name = o.name and o._key IN (" + orgIds.get(2) + "," + orgIds.get(3) + ")", pCache, 5);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.name = o.name and o.name='obj-" + orgIds.get(3) + "'", pCache, 3);
        }
        finally {
            client.destroyCache(PERSON_CACHE);
            client.destroyCache(ORG_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQuery2() throws Exception {
        Ignite client = grid(2);

        try {
            CacheConfiguration ccfg1 = cacheConfiguration(PERSON_CACHE).setQueryEntities(F.asList(personEntity(false, true)));
            CacheConfiguration ccfg2 = cacheConfiguration(ORG_CACHE).setQueryEntities(F.asList(organizationEntity(false)));

            IgniteCache<Object, Object> pCache = client.createCache(ccfg1);
            IgniteCache<Object, Object> orgCache = client.createCache(ccfg2);

            ClusterNode node0 = ignite(0).cluster().localNode();
            ClusterNode node1 = ignite(1).cluster().localNode();

            Affinity<Object> aff = client.affinity(PERSON_CACHE);

            AtomicInteger orgKey = new AtomicInteger();
            AtomicInteger pKey = new AtomicInteger();

            List<Integer> pIds = new ArrayList<>();

            for (int i = 0; i < 3; i++) {
                Integer orgId = keyForNode(aff, orgKey, node0);

                orgCache.put(orgId, new Organization("org-" + orgId));

                Integer pId = keyForNode(aff, pKey, node1);

                pCache.put(pId, new Person(orgId, "p-" + orgId));

                pIds.add(pId);
            }

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and p._key >= 0", pCache, 3);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and p._key=" + pIds.get(0), pCache, 1);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId = o._key and p._key in (" + pIds.get(0) + ", " + pIds.get(1) + ")", pCache, 2);
        }
        finally {
            client.destroyCache(PERSON_CACHE);
            client.destroyCache(ORG_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testJoinQuery3() throws Exception {
        Ignite client = grid(2);

        try {
            CacheConfiguration ccfg1 = cacheConfiguration(PERSON_CACHE).setQueryEntities(F.asList(personEntity(false, true)));
            CacheConfiguration ccfg2 = cacheConfiguration(ORG_CACHE).setQueryEntities(F.asList(organizationEntity(false)));

            IgniteCache<Object, Object> pCache = client.createCache(ccfg1);
            IgniteCache<Object, Object> orgCache = client.createCache(ccfg2);

            ClusterNode node0 = ignite(0).cluster().localNode();
            ClusterNode node1 = ignite(1).cluster().localNode();

            Affinity<Object> aff = client.affinity(PERSON_CACHE);

            AtomicInteger orgKey = new AtomicInteger();
            AtomicInteger pKey = new AtomicInteger();

            List<Integer> pIds = new ArrayList<>();

            for (int i = 0; i < 3; i++) {
                Integer orgId = keyForNode(aff, orgKey, node0);

                orgCache.put(orgId, new Organization("org-" + orgId));

                Integer pId = keyForNode(aff, pKey, node1);

                pCache.put(pId, new Person(orgId + 100_000, "p-" + orgId));

                pIds.add(pId);
            }

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId != o._key", pCache, 9);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId != o._key and p._key=" + pIds.get(0), pCache, 3);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId != o._key and p._key in (" + pIds.get(0) + ", " + pIds.get(1) + ")", pCache, 6);

            checkQuery("select o._key, o.name, p._key, p.name " +
                "from \"org\".Organization o, Person p " +
                "where p.orgId != o._key and p._key >=" + pIds.get(0) + "and p._key <= " + pIds.get(2), pCache, 9);
        }
        finally {
            client.destroyCache(PERSON_CACHE);
            client.destroyCache(ORG_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQuery4() throws Exception {
        Ignite client = grid(2);

        try {
            CacheConfiguration ccfg1 =
                cacheConfiguration(PERSON_CACHE).setQueryEntities(F.asList(personEntity(true, false)));

            IgniteCache<Object, Object> pCache = client.createCache(ccfg1);

            ClusterNode node0 = ignite(0).cluster().localNode();
            ClusterNode node1 = ignite(1).cluster().localNode();

            Affinity<Object> aff = client.affinity(PERSON_CACHE);

            AtomicInteger pKey = new AtomicInteger();

            Integer pId0 = keyForNode(aff, pKey, node0);

            pCache.put(pId0, new Person(0, "p0"));

            for (int i = 0; i < 3; i++) {
                Integer pId = keyForNode(aff, pKey, node1);

                pCache.put(pId, new Person(0, "p"));
            }

            checkQuery("select p1._key, p1.name, p2._key, p2.name " +
                "from Person p1, Person p2 " +
                "where p2._key > p1._key", pCache, 6);

            checkQuery("select p1._key, p1.name, p2._key, p2.name " +
                "from Person p1, Person p2 " +
                "where p2._key > p1._key and p1._key=" + pId0, pCache, 3);

            checkQuery("select p1._key, p1.name, p2._key, p2.name " +
                "from Person p1, Person p2 " +
                "where p2._key > p1._key and p1.name='p0'", pCache, 3);

            checkQuery("select p1._key, p1.name, p2._key, p2.name " +
                "from Person p1, Person p2 " +
                "where p1.name > p2.name", pCache, 3);
        }
        finally {
            client.destroyCache(PERSON_CACHE);
            client.destroyCache(ORG_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQuery5() throws Exception {
        Ignite client = grid(2);

        try {
            CacheConfiguration ccfg1 = cacheConfiguration(PERSON_CACHE).setQueryEntities(F.asList(personEntity(false, true)));
            CacheConfiguration ccfg2 = cacheConfiguration(ORG_CACHE).setQueryEntities(F.asList(organizationEntity(false)));

            IgniteCache<Object, Object> pCache = client.createCache(ccfg1);
            IgniteCache<Object, Object> orgCache = client.createCache(ccfg2);

            ClusterNode node0 = ignite(0).cluster().localNode();
            ClusterNode node1 = ignite(1).cluster().localNode();

            Affinity<Object> aff = client.affinity(PERSON_CACHE);

            AtomicInteger orgKey = new AtomicInteger();
            AtomicInteger pKey = new AtomicInteger();

            Integer orgId = keyForNode(aff, orgKey, node0);

            orgCache.put(orgId, new Organization("org-" + orgId));

            Integer pId = keyForNode(aff, pKey, node1);

            pCache.put(pId, new Person(orgId, "p-" + orgId));

            checkQuery("select o._key from \"org\".Organization o, Person p where p.orgId = o._key", pCache, 1);

            // Distributed join is not enabled for expressions, just check query does not fail.
            checkQuery("select o.name from \"org\".Organization o where o._key in " +
                "(select o._key from \"org\".Organization o, Person p where p.orgId = o._key)", pCache, 0);
        }
        finally {
            client.destroyCache(PERSON_CACHE);
            client.destroyCache(ORG_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQuery6() throws Exception {
        Ignite client = grid(2);

        try {
            CacheConfiguration ccfg1 =
                cacheConfiguration(PERSON_CACHE).setQueryEntities(F.asList(personEntity(true, true)));
            CacheConfiguration ccfg2 =
                cacheConfiguration(ORG_CACHE).setQueryEntities(F.asList(organizationEntity(true)));

            IgniteCache<Object, Object> pCache = client.createCache(ccfg1);

            client.createCache(ccfg2);

            putData1();

            checkQuery("select _key, name from \"org\".Organization o " +
                "inner join (select orgId from Person) p on p.orgId = o._key", pCache, total);

            checkQuery("select o._key, o.name from (select _key, name from \"org\".Organization) o " +
                "inner join Person p on p.orgId = o._key", pCache, total);

            checkQuery("select o._key, o.name from (select _key, name from \"org\".Organization) o " +
                "inner join (select orgId from Person) p on p.orgId = o._key", pCache, total);

            checkQuery("select * from " +
                "(select _key, name from \"org\".Organization) o " +
                "inner join " +
                "(select orgId from Person) p " +
                "on p.orgId = o._key", pCache, total);
        }
        finally {
            client.destroyCache(PERSON_CACHE);
            client.destroyCache(ORG_CACHE);
        }
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param expSize Expected results size.
     * @param args Arguments.
     */
    private void checkQuery(String sql, IgniteCache<Object, Object> cache, int expSize, Object... args) {
        log.info("Execute query: " + sql);

        checkQuery(sql, cache, false, expSize, args);

        checkQuery(sql, cache, true, expSize, args);
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param enforceJoinOrder Enforce join order flag.
     * @param expSize Expected results size.
     * @param args Arguments.
     */
    private void checkQuery(String sql,
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
    }

    /**
     * @param idxName Name index flag.
     * @param idxOrgId Org ID index flag.
     * @return Entity.
     */
    private QueryEntity personEntity(boolean idxName, boolean idxOrgId) {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        entity.addQueryField("orgId", Integer.class.getName(), null);
        entity.addQueryField("name", String.class.getName(), null);

        List<QueryIndex> idxs = new ArrayList<>();

        if (idxName) {
            QueryIndex idx = new QueryIndex("name");

            idxs.add(idx);
        }

        if (idxOrgId) {
            QueryIndex idx = new QueryIndex("orgId");

            idxs.add(idx);
        }

        entity.setIndexes(idxs);

        return entity;
    }

    /**
     * @param idxName Name index flag.
     * @return Entity.
     */
    private QueryEntity organizationEntity(boolean idxName) {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Organization.class.getName());

        entity.addQueryField("name", String.class.getName(), null);

        if (idxName) {
            QueryIndex idx = new QueryIndex("name");

            entity.setIndexes(F.asList(idx));
        }

        return entity;
    }

    /**
     * @return Organization ids.
     */
    private List<Integer> putData1() {
        total = 0;

        Ignite client = grid(2);

        Affinity<Object> aff = client.affinity(PERSON_CACHE);

        IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);
        IgniteCache<Object, Object> orgCache = client.cache(ORG_CACHE);

        AtomicInteger pKey = new AtomicInteger();
        AtomicInteger orgKey = new AtomicInteger();

        ClusterNode node0 = ignite(0).cluster().localNode();
        ClusterNode node1 = ignite(1).cluster().localNode();

        List<Integer> data = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            int orgId = keyForNode(aff, orgKey, node0);

            orgCache.put(orgId, new Organization("obj-" + orgId));

            for (int j = 0; j < i; j++) {
                personCache.put(keyForNode(aff, pKey, node1), new Person(orgId, "obj-" + orgId));

                total++;
            }

            data.add(orgId);
        }

        return data;
    }

    /**
     * @param name Cache name.
     * @return Configuration.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(0);

        return ccfg;
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
    }
}
