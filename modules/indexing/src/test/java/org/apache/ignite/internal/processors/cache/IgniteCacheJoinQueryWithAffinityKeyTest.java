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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheJoinQueryWithAffinityKeyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private boolean client;

    /** */
    private boolean escape;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration();

        keyCfg.setTypeName(TestKeyWithAffinity.class.getName());
        keyCfg.setAffinityKeyFieldName("affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQuery() throws Exception {
        testJoinQuery(PARTITIONED, 0, false, true);

        testJoinQuery(PARTITIONED, 1, false, true);

        testJoinQuery(REPLICATED, 0, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQueryEscapeAll() throws Exception {
        escape = true;

        testJoinQuery();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQueryWithAffinityKey() throws Exception {
        testJoinQuery(PARTITIONED, 0, true, true);

        testJoinQuery(PARTITIONED, 1, true, true);

        testJoinQuery(REPLICATED, 0, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQueryWithAffinityKeyEscapeAll() throws Exception {
        escape = true;

        testJoinQueryWithAffinityKey();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQueryWithAffinityKeyNotQueryField() throws Exception {
        testJoinQuery(PARTITIONED, 0, true, false);

        testJoinQuery(PARTITIONED, 1, true, false);

        testJoinQuery(REPLICATED, 0, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQueryWithAffinityKeyNotQueryFieldEscapeAll() throws Exception {
        escape = true;

        testJoinQueryWithAffinityKeyNotQueryField();
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param affKey If {@code true} uses key with affinity key field.
     * @param includeAffKey If {@code true} includes affinity key field in query fields.
     */
    private void testJoinQuery(CacheMode cacheMode, int backups, final boolean affKey, boolean includeAffKey) {
        CacheConfiguration ccfg = cacheConfiguration(cacheMode, backups, affKey, includeAffKey);

        log.info("Test cache [mode=" + cacheMode + ", backups=" + backups + ']');

        IgniteCache cache = ignite(0).createCache(ccfg);

        try {
            final PutData putData = putData(cache, affKey);

            for (int i = 0; i < NODES; i++) {
                log.info("Test node: " + i);

                final IgniteCache cache0 = ignite(i).cache(ccfg.getName());

                if (cacheMode == REPLICATED && !ignite(i).configuration().isClientMode()) {
                    GridTestUtils.assertThrows(log, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            checkPersonAccountsJoin(cache0, putData.personAccounts, affKey);

                            return null;
                        }
                    }, CacheException.class, "Queries using distributed JOINs have to be run on partitioned cache");
                }
                else {
                    checkPersonAccountsJoin(cache0, putData.personAccounts, affKey);

                    checkOrganizationPersonsJoin(cache0, putData.orgPersons);
                }
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache.
     * @param cnts Organizations per person counts.
     */
    private void checkOrganizationPersonsJoin(IgniteCache cache, Map<Integer, Integer> cnts) {
        SqlFieldsQuery qry;

        if (escape) {
            qry = new SqlFieldsQuery("select o.\"name\", p.\"name\" " +
                "from \"Organization\" o, \"Person\" p " +
                "where p.\"orgId\" = o._key and o._key=?");
        }
        else {
            qry = new SqlFieldsQuery("select o.name, p.name " +
                "from Organization o, Person p " +
                "where p.orgId = o._key and o._key=?");
        }

        qry.setDistributedJoins(true);

        long total = 0;

        for (int i = 0; i < cnts.size(); i++) {
            qry.setArgs(i);

            List<List<Object>> res = cache.query(qry).getAll();

            assertEquals((int)cnts.get(i), res.size());

            total += res.size();
        }

        SqlFieldsQuery qry2;

        if (escape) {
            qry2 = new SqlFieldsQuery("select count(*) " +
                "from \"Organization\" o, \"Person\" p where p.\"orgId\" = o._key");
        }
        else {
            qry2 = new SqlFieldsQuery("select count(*) " +
                "from Organization o, Person p where p.orgId = o._key");
        }

        qry2.setDistributedJoins(true);

        List<List<Object>> res = cache.query(qry2).getAll();

        assertEquals(1, res.size());
        assertEquals(total, res.get(0).get(0));
    }

    /**
     * @param cache Cache.
     * @param cnts Accounts per person counts.
     * @param affKey If {@code true} uses key with affinity key field.
     */
    private void checkPersonAccountsJoin(IgniteCache cache, Map<Object, Integer> cnts, boolean affKey) {
        String sql1;

        if (escape) {
            sql1 = "select p.\"name\" from \"Person\" p, \"" + (affKey ? "AccountKeyWithAffinity" : "Account") + "\" a " +
                "where p._key = a.\"personKey\" and p._key=?";
        }
        else {
            sql1 = "select p.name from Person p, " + (affKey ? "AccountKeyWithAffinity" : "Account") + " a " +
                "where p._key = a.personKey and p._key=?";
        }

        SqlFieldsQuery qry1 = new SqlFieldsQuery(sql1);

        qry1.setDistributedJoins(true);

        String sql2;

        if (escape) {
            sql2 = "select p.\"name\" from \"Person\" p, \"" + (affKey ? "AccountKeyWithAffinity" : "Account") + "\" a " +
                "where p.\"id\" = a.\"personId\" and p.\"id\"=?";
        }
        else {
            sql2 = "select p.name from Person p, " + (affKey ? "AccountKeyWithAffinity" : "Account") + " a " +
                "where p.id = a.personId and p.id=?";
        }

        SqlFieldsQuery qry2 = new SqlFieldsQuery(sql2);

        qry2.setDistributedJoins(true);

        Ignite ignite = (Ignite)cache.unwrap(Ignite.class);

        boolean binary = ignite.configuration().getMarshaller() instanceof BinaryMarshaller;

        long total = 0;

        for (Map.Entry<Object, Integer> e : cnts.entrySet()) {
            Object arg = binary ? ignite.binary().toBinary(e.getKey()) : e.getKey();

            qry1.setArgs(arg);

            List<List<Object>> res = cache.query(qry1).getAll();

            assertEquals((int)e.getValue(), res.size());

            total += res.size();

            qry2.setArgs(((Id)e.getKey()).id());

            res = cache.query(qry2).getAll();

            assertEquals((int)e.getValue(), res.size());
        }

        SqlFieldsQuery[] qrys = new SqlFieldsQuery[2];


        if (escape) {
            qrys[0] = new SqlFieldsQuery("select count(*) " +
                "from \"Person\" p, \"" + (affKey ? "AccountKeyWithAffinity" : "Account") + "\" a " +
                "where p.\"id\" = a.\"personId\"");

            qrys[1] = new SqlFieldsQuery("select count(*) " +
                "from \"Person\" p, \"" + (affKey ? "AccountKeyWithAffinity" : "Account") + "\" a " +
                "where p._key = a.\"personKey\"");
        }
        else {
            qrys[0] = new SqlFieldsQuery("select count(*) " +
                "from Person p, " + (affKey ? "AccountKeyWithAffinity" : "Account") + " a " +
                "where p.id = a.personId");

            qrys[1] = new SqlFieldsQuery("select count(*) " +
                "from Person p, " + (affKey ? "AccountKeyWithAffinity" : "Account") + " a " +
                "where p._key = a.personKey");
        }

        for (SqlFieldsQuery qry : qrys) {
            qry.setDistributedJoins(true);

            List<List<Object>> res = cache.query(qry).getAll();

            assertEquals(1, res.size());
            assertEquals(total, res.get(0).get(0));
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param affKey If {@code true} uses key with affinity key field.
     * @param includeAffKey If {@code true} includes affinity key field in query fields.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheMode cacheMode,
        int backups,
        boolean affKey,
        boolean includeAffKey) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        String personKeyType = affKey ? TestKeyWithAffinity.class.getName() : TestKey.class.getName();

        QueryEntity account = new QueryEntity();
        account.setKeyType(Integer.class.getName());
        account.setValueType(affKey ? AccountKeyWithAffinity.class.getName() : Account.class.getName());
        account.addQueryField("personKey", personKeyType, null);
        account.addQueryField("personId", Integer.class.getName(), null);
        account.setIndexes(F.asList(new QueryIndex("personKey"), new QueryIndex("personId")));

        QueryEntity person = new QueryEntity();
        person.setKeyType(personKeyType);
        person.setValueType(Person.class.getName());
        person.addQueryField("orgId", Integer.class.getName(), null);
        person.addQueryField("id", Integer.class.getName(), null);
        person.addQueryField("name", String.class.getName(), null);
        person.setIndexes(F.asList(new QueryIndex("orgId"), new QueryIndex("id"), new QueryIndex("name")));

        if (affKey && includeAffKey)
            person.addQueryField("affKey", Integer.class.getName(), null);

        QueryEntity org = new QueryEntity();
        org.setKeyType(Integer.class.getName());
        org.setValueType(Organization.class.getName());
        org.addQueryField("name", String.class.getName(), null);
        org.setIndexes(F.asList(new QueryIndex("name")));

        ccfg.setQueryEntities(F.asList(account, person, org));

        ccfg.setSqlEscapeAll(escape);

        return ccfg;
    }

    /**
     * @param cache Cache.
     * @param affKey If {@code true} uses key with affinity key field.
     * @return Put data counts.
     */
    private PutData putData(IgniteCache cache, boolean affKey) {
        Map<Integer, Integer> orgPersons = new HashMap<>();
        Map<Object, Integer> personAccounts = new HashMap<>();

        final int ORG_CNT = 10;

        for (int i = 0; i < ORG_CNT; i++)
            cache.put(i, new Organization("org-" + i));

        Set<Integer> personIds = new HashSet<>();
        Set<Integer> accountIds = new HashSet<>();

        for (int i = 0; i < ORG_CNT; i++) {
            int persons = ThreadLocalRandom.current().nextInt(100);

            for (int p = 0; p < persons; p++) {
                int personId = ThreadLocalRandom.current().nextInt();

                while (!personIds.add(personId))
                    personId = ThreadLocalRandom.current().nextInt();

                Object personKey = affKey ? new TestKeyWithAffinity(personId) : new TestKey(personId);

                String name = "person-" + personId;

                cache.put(personKey, new Person(i, name));

                int accounts = ThreadLocalRandom.current().nextInt(10);

                for (int a = 0; a < accounts; a++) {
                    int accountId = ThreadLocalRandom.current().nextInt();

                    while (!accountIds.add(accountId))
                        accountId = ThreadLocalRandom.current().nextInt();

                    cache.put(accountId, affKey ? new AccountKeyWithAffinity(personKey) : new Account(personKey));
                }

                personAccounts.put(personKey, accounts);
            }

            orgPersons.put(i, persons);
        }

        return new PutData(orgPersons, personAccounts);
    }

    /**
     *
     */
    private static class PutData {
        /** */
        final Map<Integer, Integer> orgPersons;

        /** */
        final Map<Object, Integer> personAccounts;

        /**
         * @param orgPersons Organizations per person counts.
         * @param personAccounts Accounts per person counts.
         */
        public PutData(Map<Integer, Integer> orgPersons, Map<Object, Integer> personAccounts) {
            this.orgPersons = orgPersons;
            this.personAccounts = personAccounts;
        }
    }

    /**
     *
     */
    public interface Id {
        /**
         * @return ID.
         */
        public int id();
    }

    /**
     *
     */
    public static class TestKey implements Id {
        /** */
        private int id;

        /**
         * @param id Key.
         */
        public TestKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey other = (TestKey)o;

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
    public static class TestKeyWithAffinity implements Id {
        /** */
        private int id;

        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param id Key.
         */
        public TestKeyWithAffinity(int id) {
            this.id = id;

            affKey = id + 1;
        }

        /** {@inheritDoc} */
        @Override public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKeyWithAffinity other = (TestKeyWithAffinity)o;

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
        @QuerySqlField
        private TestKey personKey;

        /** */
        @QuerySqlField
        private int personId;

        /**
         * @param personKey Person key.
         */
        public Account(Object personKey) {
            this.personKey = (TestKey)personKey;
            personId = this.personKey.id;
        }
    }

    /**
     *
     */
    private static class AccountKeyWithAffinity implements Serializable {
        /** */
        @QuerySqlField
        private TestKeyWithAffinity personKey;

        /** */
        @QuerySqlField
        private int personId;

        /**
         * @param personKey Person key.
         */
        public AccountKeyWithAffinity(Object personKey) {
            this.personKey = (TestKeyWithAffinity)personKey;
            personId = this.personKey.id;
        }
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField
        int orgId;

        /** */
        @QuerySqlField
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
        @QuerySqlField
        String name;

        /**
         * @param name Name.
         */
        public Organization(String name) {
            this.name = name;
        }
    }
}
