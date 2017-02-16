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
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
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
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheDistributedJoinCollocatedAndNotTest extends GridCommonAbstractTest {
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

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration(PersonKey.class.getName(), "affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

        TcpDiscoverySpi spi = ((TcpDiscoverySpi)cfg.getDiscoverySpi());

        spi.setIpFinder(IP_FINDER);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        {
            CacheConfiguration ccfg = configuration(PERSON_CACHE);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(PersonKey.class.getName());
            entity.setValueType(Person.class.getName());
            entity.addQueryField("id", Integer.class.getName(), null);
            entity.addQueryField("affKey", Integer.class.getName(), null);
            entity.addQueryField("name", String.class.getName(), null);

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ORG_CACHE);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Organization.class.getName());
            entity.addQueryField("name", String.class.getName(), null);
            entity.setIndexes(F.asList(new QueryIndex("name")));

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ACCOUNT_CACHE);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Account.class.getName());
            entity.addQueryField("personId", Integer.class.getName(), null);
            entity.addQueryField("name", String.class.getName(), null);
            entity.setIndexes(F.asList(new QueryIndex("personId"), new QueryIndex("name")));

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        cfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

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
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration configuration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoin() throws Exception {
        Ignite client = grid(2);

        IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);
        IgniteCache<Object, Object> orgCache = client.cache(ORG_CACHE);
        IgniteCache<Object, Object> accCache = client.cache(ACCOUNT_CACHE);

        Affinity<Object> aff = client.affinity(PERSON_CACHE);

        AtomicInteger orgKey = new AtomicInteger();
        AtomicInteger accKey = new AtomicInteger();

        ClusterNode node0 = ignite(0).cluster().localNode();
        ClusterNode node1 = ignite(1).cluster().localNode();

        /**
         * One organization, one person, two accounts.
         */

        int orgId1 = keyForNode(aff, orgKey, node0);

        orgCache.put(orgId1, new Organization("obj-" + orgId1));

        personCache.put(new PersonKey(1, orgId1), new Person(1, "o1-p1"));
        personCache.put(new PersonKey(2, orgId1), new Person(2, "o1-p2"));

        accCache.put(keyForNode(aff, accKey, node0), new Account(1, "a0"));
        accCache.put(keyForNode(aff, accKey, node1), new Account(1, "a1"));

        // Join on affinity keys equals condition should not be distributed.
        String qry = "select o.name, p._key, p.name " +
            "from \"org\".Organization o, \"person\".Person p " +
            "where p.affKey = o._key";

        assertFalse(plan(qry, orgCache, false).contains("batched"));

        checkQuery(qry, orgCache, false, 2);

        checkQuery("select o.name, p._key, p.name, a.name " +
            "from \"org\".Organization o, \"person\".Person p, \"acc\".Account a " +
            "where p.affKey = o._key and p.id = a.personId", orgCache, true, 2);
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param enforceJoinOrder Enforce join order flag.
     * @return Query plan.
     */
    private String plan(String sql,
        IgniteCache<?, ?> cache,
        boolean enforceJoinOrder) {
        return (String)cache.query(new SqlFieldsQuery("explain " + sql)
            .setDistributedJoins(true)
            .setEnforceJoinOrder(enforceJoinOrder))
            .getAll().get(0).get(0);
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param enforceJoinOrder Enforce join order flag.
     * @param expSize Expected results size.
     */
    private void checkQuery(String sql,
        IgniteCache<Object, Object> cache,
        boolean enforceJoinOrder,
        int expSize) {
        String plan = (String)cache.query(new SqlFieldsQuery("explain " + sql)
            .setDistributedJoins(true)
            .setEnforceJoinOrder(enforceJoinOrder))
            .getAll().get(0).get(0);

        log.info("Plan: " + plan);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        qry.setDistributedJoins(true);
        qry.setEnforceJoinOrder(enforceJoinOrder);

        QueryCursor<List<?>> cur = cache.query(qry);

        List<List<?>> res = cur.getAll();

        if (expSize != res.size())
            log.info("Results: " + res);

        assertEquals(expSize, res.size());
    }
    /**
     *
     */
    public static class PersonKey {
        /** */
        private int id;

        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param id Key.
         * @param affKey Affinity key.
         */
        public PersonKey(int id, int affKey) {
            this.id = id;
            this.affKey = affKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PersonKey other = (PersonKey)o;

            return id == other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    private static class Account implements Serializable {
        /** */
        int personId;

        /** */
        String name;

        /**
         * @param personId Person ID.
         * @param name Name.
         */
        public Account(int personId, String name) {
            this.personId = personId;
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
        int id;

        /** */
        String name;

        /**
         * @param id Person ID.
         * @param name Name.
         */
        public Person(int id, String name) {
            this.id = id;
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
