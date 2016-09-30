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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.AbstractH2CompareQueryTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheJoinPartitionedAndReplicatedCollocationTest extends AbstractH2CompareQueryTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private static final String ACCOUNT_CACHE = "acc";

    /** */
    private boolean client;

    /** */
    private boolean h2DataInserted;

    /** {@inheritDoc} */
    @Override protected void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void initCacheAndDbData() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = ((TcpDiscoverySpi)cfg.getDiscoverySpi());

        spi.setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration personCache() {
        CacheConfiguration ccfg = configuration(PERSON_CACHE, 0);

        // Person cache is replicated.
        ccfg.setCacheMode(REPLICATED);

        QueryEntity entity = new QueryEntity();
        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());
        entity.addQueryField("name", String.class.getName(), null);

        ccfg.setQueryEntities(F.asList(entity));

        return ccfg;
    }

    /**
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration accountCache(int backups) {
        CacheConfiguration ccfg = configuration(ACCOUNT_CACHE, backups);

        QueryEntity entity = new QueryEntity();
        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Account.class.getName());
        entity.addQueryField("personId", Integer.class.getName(), null);
        entity.addQueryField("name", String.class.getName(), null);
        entity.setIndexes(F.asList(new QueryIndex("personId")));

        ccfg.setQueryEntities(F.asList(entity));

        return ccfg;
    }

    /**
     * @param name Cache name.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration configuration(String name, int backups) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(backups);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = true;

        startGrid(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = super.initializeH2Schema();

        st.execute("CREATE SCHEMA \"person\"");
        st.execute("CREATE SCHEMA \"acc\"");

        st.execute("create table \"person\".PERSON" +
            "  (_key int not null," +
            "  _val other not null," +
            "  name varchar(255))");

        st.execute("create table \"acc\".ACCOUNT" +
            "  (_key int not null," +
            "  _val other not null," +
            "  personId int," +
            "  name varchar(255))");

        return st;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoin() throws Exception {
        Ignite client = grid(SRVS);

        client.createCache(personCache());

        checkJoin(0);

        h2DataInserted = true;

        checkJoin(1);

        checkJoin(2);
    }

    /**
     * @param accBackups Account cache backups.
     * @throws Exception If failed.
     */
    private void checkJoin(int accBackups) throws Exception {
        Ignite client = grid(SRVS);

        IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);

        Affinity<Object> aff = client.affinity(PERSON_CACHE);

        AtomicInteger pKey = new AtomicInteger(100_000);
        AtomicInteger accKey = new AtomicInteger();

        ClusterNode node0 = ignite(0).cluster().localNode();
        ClusterNode node1 = ignite(1).cluster().localNode();

        try {
            IgniteCache<Object, Object> accCache = client.createCache(accountCache(accBackups));

            Integer pKey1 = keyForNode(aff, pKey, node0); // No accounts.
            insert(personCache, pKey1, new Person("p1"));

            Integer pKey2 = keyForNode(aff, pKey, node0); // 1 collocated account.
            insert(personCache, pKey2, new Person("p2"));
            insert(accCache, keyForNode(aff, accKey, node0), new Account(pKey2, "a-p2"));

            Integer pKey3 = keyForNode(aff, pKey, node0); // 1 non-collocated account.
            insert(personCache, pKey3, new Person("p3"));
            insert(accCache, keyForNode(aff, accKey, node1), new Account(pKey3, "a-p3"));

            Integer pKey4 = keyForNode(aff, pKey, node0); // 1 collocated, 1 non-collocated account.
            insert(personCache, pKey4, new Person("p4"));
            insert(accCache, keyForNode(aff, accKey, node0), new Account(pKey4, "a-p4-1"));
            insert(accCache, keyForNode(aff, accKey, node1), new Account(pKey4, "a-p4-2"));

            Integer pKey5 = keyForNode(aff, pKey, node0); // 2 collocated accounts.
            insert(personCache, pKey5, new Person("p5"));
            insert(accCache, keyForNode(aff, accKey, node0), new Account(pKey5, "a-p5-1"));
            insert(accCache, keyForNode(aff, accKey, node0), new Account(pKey5, "a-p5-1"));

            Integer pKey6 = keyForNode(aff, pKey, node0); // 2 non-collocated accounts.
            insert(personCache, pKey6, new Person("p6"));
            insert(accCache, keyForNode(aff, accKey, node1), new Account(pKey6, "a-p5-1"));
            insert(accCache, keyForNode(aff, accKey, node1), new Account(pKey6, "a-p5-1"));

            Integer[] keys = {pKey1, pKey2, pKey3, pKey4, pKey5, pKey6};

            for (int i = 0; i < keys.length; i++) {
                log.info("Test key: " + i);

                Integer key = keys[i];

                checkQuery("select p._key, p.name, a.name " +
                    "from \"person\".Person p, \"acc\".Account a " +
                    "where p._key = a.personId and p._key=?", accCache, true, key);

                checkQuery("select p._key, p.name, a.name " +
                    "from \"acc\".Account a, \"person\".Person p " +
                    "where p._key = a.personId and p._key=?", accCache, true, key);

                checkQuery("select p._key, p.name, a.name " +
                    "from \"person\".Person p right outer join \"acc\".Account a " +
                    "on (p._key = a.personId) and p._key=?", accCache, true, key);

                checkQuery("select p._key, p.name, a.name " +
                    "from \"acc\".Account a left outer join \"person\".Person p " +
                    "on (p._key = a.personId) and p._key=?", accCache, true, key);

//                checkQuery("select p._key, p.name, a.name " +
//                    "from \"acc\".Account a right outer join \"person\".Person p " +
//                    "on (p._key = a.personId) and p._key=?", accCache, true, key);
//
//                checkQuery("select p._key, p.name, a.name " +
//                    "from \"person\".Person p left outer join \"acc\".Account a " +
//                    "on (p._key = a.personId) and p._key=?", accCache, true, key);
            }
        }
        finally {
            client.destroyCache(ACCOUNT_CACHE);

            personCache.removeAll();
        }
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param p Person.
     * @throws Exception If failed.
     */
    private void insert(IgniteCache<Object, Object> cache, int key, Person p) throws Exception {
        cache.put(key, p);

        if (h2DataInserted)
            return;

        try(PreparedStatement st = conn.prepareStatement("insert into \"person\".PERSON " +
            "(_key, _val, name) values(?, ?, ?)")) {
            st.setObject(1, key);
            st.setObject(2, p);
            st.setObject(3, p.name);

            st.executeUpdate();
        }
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param  a Account.
     * @throws Exception If failed.
     */
    private void insert(IgniteCache<Object, Object> cache, int key, Account a) throws Exception {
        cache.put(key, a);

        if (h2DataInserted)
            return;

        try(PreparedStatement st = conn.prepareStatement("insert into \"acc\".ACCOUNT " +
            "(_key, _val, personId, name) values(?, ?, ?, ?)")) {
            st.setObject(1, key);
            st.setObject(2, a);
            st.setObject(3, a.personId);
            st.setObject(4, a.name);

            st.executeUpdate();
        }
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param enforceJoinOrder Enforce join order flag.
     * @param args Arguments.
     * @throws Exception If failed.
     */
    private void checkQuery(String sql,
        IgniteCache<Object, Object> cache,
        boolean enforceJoinOrder,
        Object... args) throws Exception {
        String plan = (String)cache.query(new SqlFieldsQuery("explain " + sql)
            .setArgs(args)
            .setDistributedJoins(true)
            .setEnforceJoinOrder(enforceJoinOrder))
            .getAll().get(0).get(0);

        log.info("Plan: " + plan);

        compareQueryRes0(cache, sql, true, enforceJoinOrder, args, Ordering.RANDOM);
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
        String name;

        /**
         * @param name Name.
         */
        public Person(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
