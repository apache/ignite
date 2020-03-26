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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.AbstractH2CompareQueryTest;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings({"unchecked", "PackageVisibleField", "serial"})
public class IgniteCrossCachesJoinsQueryTest extends AbstractH2CompareQueryTest {
    /** */
    private static final String PERSON_CACHE_NAME = "person";

    /** */
    private static final String ORG_CACHE_NAME = "org";

    /** */
    private static final String ACC_CACHE_NAME = "acc";

    /** */
    private static final int NODES = 5;

    /** */
    private Data data;

    /** Tested qry. */
    private String qry;

    /** Tested cache. */
    private IgniteCache cache;

    /** */
    private boolean distributedJoins;

    /** */
    private static Random rnd;

    /** {@inheritDoc} */
    @Override protected void createCaches() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        log.info("Random seed: " + seed);

        startGridsMultiThreaded(NODES - 1);

        startClientGrid(NODES - 1);

        awaitPartitionMapExchange();

        conn = openH2Connection(false);

        initializeH2Schema();
    }

    /** {@inheritDoc} */
    @Override protected void initCacheAndDbData() throws SQLException {
        Statement st = conn.createStatement();

        final String keyType = useCollocatedData() ? "other" : "int";

        st.execute("create table \"" + ACC_CACHE_NAME + "\".Account" +
            "  (" +
            "  _key " + keyType + " not null," +
            "  _val other not null," +
            "  id int unique," +
            "  personId int," +
            "  personDateId TIMESTAMP," +
            "  personStrId varchar(255)" +
            "  )");

        st.execute("create table \"" + PERSON_CACHE_NAME + "\".Person" +
            "  (" +
            "  _key " + keyType + " not null," +
            "  _val other not null," +
            "  id int unique," +
            "  strId varchar(255) ," +
            "  dateId TIMESTAMP ," +
            "  orgId int," +
            "  orgDateId TIMESTAMP," +
            "  orgStrId varchar(255), " +
            "  name varchar(255), " +
            "  salary int" +
            "  )");

        st.execute("create table \"" + ORG_CACHE_NAME + "\".Organization" +
            "  (" +
            "  _key int not null," +
            "  _val other not null," +
            "  id int unique," +
            "  strId varchar(255) ," +
            "  dateId TIMESTAMP ," +
            "  name varchar(255) " +
            "  )");

        conn.commit();

        st.close();

        for (Account account : data.accounts)
            insertInDb(account);

        for (Person person : data.persons)
            insertInDb(person);

        for (Organization org : data.orgs)
            insertInDb(org);
    }

    /**
     *
     */
    private void initCachesData() {
        IgniteCache accCache = ignite(0).cache(ACC_CACHE_NAME);

        for (Account account : data.accounts)
            accCache.put(account.key(useCollocatedData()), account);

        IgniteCache personCache = ignite(0).cache(PERSON_CACHE_NAME);

        for (Person person : data.persons)
            personCache.put(person.key(useCollocatedData()), person);

        IgniteCache orgCache = ignite(0).cache(ORG_CACHE_NAME);

        for (Organization org : data.orgs)
            orgCache.put(org.id, org);
    }

    /**
     * @param acc Account.
     * @throws SQLException If failed.
     */
    private void insertInDb(Account acc) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement(
            "insert into \"" + ACC_CACHE_NAME + "\".Account (_key, _val, id, personId, personDateId, personStrId) " +
                "values(?, ?, ?, ?, ?, ?)")) {
            int i = 0;

            st.setObject(++i, acc.key(useCollocatedData()));
            st.setObject(++i, acc);
            st.setObject(++i, acc.id);
            st.setObject(++i, acc.personId);
            st.setObject(++i, acc.personDateId);
            st.setObject(++i, acc.personStrId);

            st.executeUpdate();
        }
    }

    /**
     * @param p Person.
     * @throws SQLException If failed.
     */
    private void insertInDb(Person p) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement(
            "insert into \"" + PERSON_CACHE_NAME + "\".Person (_key, _val, id, strId, dateId, name, orgId, orgDateId, " +
                "orgStrId, salary) " +
                "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            int i = 0;

            st.setObject(++i, p.key(useCollocatedData()));
            st.setObject(++i, p);
            st.setObject(++i, p.id);
            st.setObject(++i, p.strId);
            st.setObject(++i, p.dateId);
            st.setObject(++i, p.name);
            st.setObject(++i, p.orgId);
            st.setObject(++i, p.orgDateId);
            st.setObject(++i, p.orgStrId);
            st.setObject(++i, p.salary);

            st.executeUpdate();
        }
    }

    /**
     * @param o Organization.
     * @throws SQLException If failed.
     */
    private void insertInDb(Organization o) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement(
            "insert into \"" + ORG_CACHE_NAME + "\".Organization (_key, _val, id, strId, dateId, name) " +
                "values(?, ?, ?, ?, ?, ?)")) {
            int i = 0;

            st.setObject(++i, o.id);
            st.setObject(++i, o);
            st.setObject(++i, o.id);
            st.setObject(++i, o.strId);
            st.setObject(++i, o.dateId);
            st.setObject(++i, o.name);

            st.executeUpdate();
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {
        compareQueryRes0(ignite(0).cache(ACC_CACHE_NAME), "select _key, _val, id, personId, personDateId, personStrId " +
            "from \"" + ACC_CACHE_NAME + "\".Account");

        compareQueryRes0(ignite(0).cache(PERSON_CACHE_NAME), "select _key, _val, id, strId, dateId, name, orgId, " +
            "orgDateId, orgStrId, salary from \"" + PERSON_CACHE_NAME + "\".Person");

        compareQueryRes0(ignite(0).cache(ORG_CACHE_NAME), "select _key, _val, id, strId, dateId, name " +
            "from \"" + ORG_CACHE_NAME + "\".Organization");
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = conn.createStatement();

        for (String cacheName : new String[]{"person", "acc", "org"})
            st.execute("CREATE SCHEMA \"" + cacheName + "\"");

        return st;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 60_000;
    }

    /**
     * @return Distributed joins flag.
     */
    protected boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Use collocated data.
     */
    private boolean useCollocatedData() {
        return !distributedJoins();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoins1() throws Exception {
        distributedJoins = true;

        checkAllCacheCombinationsSet1(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoins2() throws Exception {
        distributedJoins = true;

        checkAllCacheCombinationsSet2(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoins3() throws Exception {
        distributedJoins = true;

        checkAllCacheCombinationsSet3(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCollocatedJoins1() throws Exception {
        distributedJoins = false;

        checkAllCacheCombinationsSet1(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCollocatedJoins2() throws Exception {
        distributedJoins = false;

        checkAllCacheCombinationsSet2(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCollocatedJoins3() throws Exception {
        distributedJoins = false;

        checkAllCacheCombinationsSet3(true);
    }

    /**
     * @param idx Index flag.
     * @throws Exception If failed.
     */
    private void checkAllCacheCombinationsSet1(boolean idx) throws Exception {
        List<List<TestCache>> types = cacheCombinations(TestCacheType.REPLICATED);

        checkAllCacheCombinations(idx, types);
    }

    /**
     * @param idx Index flag.
     * @throws Exception If failed.
     */
    private void checkAllCacheCombinationsSet2(boolean idx) throws Exception {
        List<List<TestCache>> types = cacheCombinations(TestCacheType.PARTITIONED_b0);

        checkAllCacheCombinations(idx, types);
    }

    /**
     * @param idx Index flag.
     * @throws Exception If failed.
     */
    private void checkAllCacheCombinationsSet3(boolean idx) throws Exception {
        List<List<TestCache>> types = cacheCombinations(TestCacheType.PARTITIONED_b1);

        checkAllCacheCombinations(idx, types);
    }

    /**
     * @param personType Person cache type.
     * @return Cache combinations.
     */
    private List<List<TestCache>> cacheCombinations(TestCacheType personType) {
        List<List<TestCache>> res = new ArrayList<>();

        for (TestCacheType accCacheType : TestCacheType.values()) {
            for (TestCacheType orgCacheType : TestCacheType.values()) {
                List<TestCache> cacheTypes = new ArrayList<>(3);

                cacheTypes.add(new TestCache("person", personType));
                cacheTypes.add(new TestCache("acc", accCacheType));
                cacheTypes.add(new TestCache("org", orgCacheType));

                res.add(cacheTypes);
            }
        }

        return res;
    }

    /**
     * @param idx Index flag.
     * @param cacheList Caches.
     * @throws Exception If failed.
     */
    private void checkAllCacheCombinations(
        boolean idx,
        List<List<TestCache>> cacheList) throws Exception {
        data = prepareData();

        initCacheAndDbData();

        try {
            Map<TestConfig, Throwable> errors = new LinkedHashMap<>();
            List<TestConfig> success = new ArrayList<>();

            int cfgIdx = 0;

            for (List<TestCache> caches : cacheList) {
                assert caches.size() == 3 : caches;

                TestCache personCache = caches.get(0);
                TestCache accCache = caches.get(1);
                TestCache orgCache = caches.get(2);

                try {
                    check(idx, personCache, accCache, orgCache);

                    success.add(new TestConfig(cfgIdx, cache, personCache, accCache, orgCache, ""));
                }
                catch (Throwable e) {
                    error("", e);

                    errors.put(new TestConfig(cfgIdx, cache, personCache, accCache, orgCache, qry), e);
                }

                cfgIdx++;
            }

            if (!errors.isEmpty()) {
                int total = cacheList.size();

                SB sb = new SB("Test failed for the following " + errors.size() + " combination(s) (" + total + " total):\n");

                for (Map.Entry<TestConfig, Throwable> e : errors.entrySet())
                    sb.a(e.getKey()).a(", error=").a(e.getValue()).a("\n");

                sb.a("Successfully finished combinations:\n");

                for (TestConfig t : success)
                    sb.a(t).a("\n");

                sb.a("The following data has beed used for test:\n " + data);

                fail(sb.toString());
            }
        }
        finally {
            for (String cacheName : new String[]{PERSON_CACHE_NAME, ACC_CACHE_NAME, ORG_CACHE_NAME})
                ignite(0).destroyCache(cacheName);

            Statement st = conn.createStatement();

            st.execute("drop table \"" + ACC_CACHE_NAME + "\".Account");
            st.execute("drop table \"" + PERSON_CACHE_NAME + "\".Person");
            st.execute("drop table \"" + ORG_CACHE_NAME + "\".Organization");

            conn.commit();

            st.close();
        }
    }

    /**
     * @param idx Index flag.
     * @param personCacheType Person cache personCacheType.
     * @param accCacheType Account cache personCacheType.
     * @param orgCacheType Organization cache personCacheType.
     * @throws Exception If failed.
     */
    private void check(
        boolean idx,
        final TestCache personCacheType,
        final TestCache accCacheType,
        final TestCache orgCacheType) throws Exception {
        info("Checking cross cache joins [accCache=" + accCacheType +
            ", personCache=" + personCacheType +
            ", orgCache=" + orgCacheType + "]");

        Collection<TestCache> cacheTypes = F.asList(personCacheType, accCacheType, orgCacheType);

        for (TestCache cache : cacheTypes) {
            CacheConfiguration cc = cacheConfiguration(cache.cacheName,
                cache.type.cacheMode,
                cache.type.backups,
                idx,
                cache == accCacheType,
                cache == personCacheType,
                cache == orgCacheType
            );

            ignite(0).getOrCreateCache(cc);

            info("Created cache [name=" + cache.cacheName + ", mode=" + cache.type + "]");
        }

        initCachesData();

        // checkAllDataEquals();

        List<String> cacheNames = new ArrayList<>();

        cacheNames.add(personCacheType.cacheName);
        cacheNames.add(orgCacheType.cacheName);
        cacheNames.add(accCacheType.cacheName);

        for (int i = 0; i < NODES; i++) {
            Ignite testNode = ignite(i);

            log.info("Test node [idx=" + i + ", isClient=" + testNode.configuration().isClientMode() + "]");

            for (String cacheName : cacheNames) {
                cache = testNode.cache(cacheName);

                log.info("Use cache: " + cache.getName());

                boolean distributeJoins0 = distributedJoins;

                if (replicated(cache)) {
//                    if (!testNode.configuration().isClientMode())
//                        assertProperException(cache);

                    boolean all3CachesAreReplicated =
                        replicated(ignite(0).cache(ACC_CACHE_NAME)) &&
                        replicated(ignite(0).cache(PERSON_CACHE_NAME)) &&
                        replicated(ignite(0).cache(ORG_CACHE_NAME));

                    // Queries running on replicated cache should not contain JOINs with partitioned tables.
                    if (distributeJoins0 && !all3CachesAreReplicated)
                        continue;
                    else
                        distributedJoins = false;
                }

                if (!cache.getName().equals(orgCacheType.cacheName))
                    checkPersonAccountsJoin(cache, data.accountsPerPerson);

                if (!cache.getName().equals(accCacheType.cacheName))
                    checkOrganizationPersonsJoin(cache);

                checkOrganizationPersonAccountJoin(cache);

                checkUnion();
                checkUnionAll();

                if (!cache.getName().equals(orgCacheType.cacheName))
                    checkPersonAccountCrossJoin(cache);

                if (!cache.getName().equals(accCacheType.cacheName))
                    checkPersonOrganizationGroupBy(cache);

                if (!cache.getName().equals(orgCacheType.cacheName))
                    checkPersonAccountGroupBy(cache);

                checkGroupBy();

                distributedJoins = distributeJoins0;
            }
        }
    }

    /**
     * @param cache Cache.
     * @return {@code True} if cache is replicated.
     */
    private boolean replicated(IgniteCache<?, ?> cache) {
        return cache.getConfiguration(CacheConfiguration.class).getCacheMode() == REPLICATED;
    }

    /**
     * Organization ids: [0, 9]. Person ids: randoms at [10, 9999]. Accounts ids: randoms at [10000, 999_999]
     *
     * @return Data.
     */
    private Data prepareData() {
        Map<Integer, Integer> personsPerOrg = new HashMap<>();
        Map<Integer, Integer> accountsPerPerson = new HashMap<>();
        Map<Integer, Integer> accountsPerOrg = new HashMap<>();
        Map<Integer, Integer> maxSalaryPerOrg = new HashMap<>();

        List<Organization> orgs = new ArrayList<>();
        List<Person> persons = new ArrayList<>();
        List<Account> accounts = new ArrayList<>();

        final int ORG_CNT = 10;
        final int MAX_PERSONS_PER_ORG = 20;
        final int MAX_ACCOUNTS_PER_PERSON = 10;

        for (int id = 0; id < ORG_CNT; id++)
            orgs.add(new Organization(id, "org-" + id));

        Set<Integer> personIds = new HashSet<>();
        Set<Integer> accountIds = new HashSet<>();

        for (int orgId = 0; orgId < ORG_CNT; orgId++) {
            int personsCnt = rnd.nextInt(MAX_PERSONS_PER_ORG);

            int accsPerOrg = 0;
            int maxSalary = -1;

            for (int p = 0; p < personsCnt; p++) {
                int personId = rnd.nextInt(10_000) + 10;

                while (!personIds.add(personId))
                    personId = rnd.nextInt(10_000) + 10;

                String name = "person-" + personId;

                int salary = (rnd.nextInt(10) + 1) * 1000;

                if (salary > maxSalary)
                    maxSalary = salary;

                persons.add(new Person(personId, orgId, name, salary));

                int accountsCnt = rnd.nextInt(MAX_ACCOUNTS_PER_PERSON);

                for (int a = 0; a < accountsCnt; a++) {
                    int accountId = rnd.nextInt(100_000) + 10_000;

                    while (!accountIds.add(accountId))
                        accountId = rnd.nextInt(100_000) + 10_000;

                    accounts.add(new Account(accountId, personId, orgId));
                }

                accountsPerPerson.put(personId, accountsCnt);

                accsPerOrg += accountsCnt;
            }

            personsPerOrg.put(orgId, personsCnt);
            accountsPerOrg.put(orgId, accsPerOrg);
            maxSalaryPerOrg.put(orgId, maxSalary);
        }

        return new Data(orgs, persons, accounts, personsPerOrg, accountsPerPerson, accountsPerOrg, maxSalaryPerOrg);
    }

    /**
     * @param cacheName Cache name.
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param idx Index flag.
     * @param accountCache Account cache flag.
     * @param personCache Person cache flag.
     * @param orgCache Organization cache flag.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName,
        CacheMode cacheMode,
        int backups,
        boolean idx,
        boolean accountCache,
        boolean personCache,
        boolean orgCache) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(cacheName);
        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        List<QueryEntity> entities = new ArrayList<>();

        if (accountCache) {
            QueryEntity account = new QueryEntity();
            account.setKeyType(useCollocatedData() ? AffinityKey.class.getName() : Integer.class.getName());
            account.setValueType(Account.class.getName());
            account.addQueryField("id", Integer.class.getName(), null);
            account.addQueryField("personId", Integer.class.getName(), null);
            account.addQueryField("personDateId", Date.class.getName(), null);
            account.addQueryField("personStrId", String.class.getName(), null);

            if (idx) {
                account.setIndexes(F.asList(new QueryIndex("id"),
                    new QueryIndex("personId"),
                    new QueryIndex("personDateId"),
                    new QueryIndex("personStrId")));
            }

            entities.add(account);
        }

        if (personCache) {
            QueryEntity person = new QueryEntity();
            person.setKeyType(useCollocatedData() ? AffinityKey.class.getName() : Integer.class.getName());
            person.setValueType(Person.class.getName());
            person.addQueryField("id", Integer.class.getName(), null);
            person.addQueryField("dateId", Date.class.getName(), null);
            person.addQueryField("strId", String.class.getName(), null);
            person.addQueryField("orgId", Integer.class.getName(), null);
            person.addQueryField("orgDateId", Date.class.getName(), null);
            person.addQueryField("orgStrId", String.class.getName(), null);
            person.addQueryField("name", String.class.getName(), null);
            person.addQueryField("salary", Integer.class.getName(), null);

            if (idx) {
                person.setIndexes(F.asList(new QueryIndex("id"),
                    new QueryIndex("dateId"),
                    new QueryIndex("strId"),
                    new QueryIndex("orgId"),
                    new QueryIndex("orgDateId"),
                    new QueryIndex("orgStrId"),
                    new QueryIndex("name"),
                    new QueryIndex("salary")));
            }

            entities.add(person);
        }

        if (orgCache) {
            QueryEntity org = new QueryEntity();
            org.setKeyType(Integer.class.getName());
            org.setValueType(Organization.class.getName());
            org.addQueryField("id", Integer.class.getName(), null);
            org.addQueryField("dateId", Date.class.getName(), null);
            org.addQueryField("strId", String.class.getName(), null);
            org.addQueryField("name", String.class.getName(), null);

            if (idx) {
                org.setIndexes(F.asList(new QueryIndex("id"),
                    new QueryIndex("dateId"),
                    new QueryIndex("strId"),
                    new QueryIndex("name")));
            }

            entities.add(org);
        }

        ccfg.setQueryEntities(entities);

        return ccfg;
    }

    /**
     * @param cache Cache.
     */
    private void checkOrganizationPersonsJoin(IgniteCache cache) {
        if (skipQuery(cache, PERSON_CACHE_NAME, ORG_CACHE_NAME))
            return;

        qry = "checkOrganizationPersonsJoin";

        SqlFieldsQuery qry = new SqlFieldsQuery("select o.name, p.name " +
            "from \"" + ORG_CACHE_NAME + "\".Organization o, \"" + PERSON_CACHE_NAME + "\".Person p " +
            "where p.orgId = o._key and o._key=?");

        qry.setDistributedJoins(distributedJoins());

        SqlQuery qry2 = null;

        if (PERSON_CACHE_NAME.equals(cache.getName())) {
            qry2 = new SqlQuery(Person.class,
                "from \"" + ORG_CACHE_NAME + "\".Organization, \"" + PERSON_CACHE_NAME + "\".Person " +
                    "where Person.orgId = Organization._key and Organization._key=?"
            );

            qry2.setDistributedJoins(distributedJoins());
        }

        long total = 0;

        for (int i = 0; i < data.personsPerOrg.size(); i++) {
            qry.setArgs(i);

            if (qry2 != null)
                qry2.setArgs(i);

            List<List<Object>> res = cache.query(qry).getAll();

            assertEquals((int)data.personsPerOrg.get(i), res.size());

            if (qry2 != null) {
                List<List<Object>> res2 = cache.query(qry2).getAll();

                assertEquals((int)data.personsPerOrg.get(i), res2.size());
            }

            total += res.size();
        }

        SqlFieldsQuery qry3 = new SqlFieldsQuery("select count(*) " +
            "from \"" + ORG_CACHE_NAME + "\".Organization o, \"" + PERSON_CACHE_NAME + "\".Person p where p.orgId = o._key");

        qry3.setDistributedJoins(distributedJoins());

        List<List<Object>> res = cache.query(qry3).getAll();

        assertEquals(1, res.size());
        assertEquals(total, res.get(0).get(0));
    }

    /**
     * @param cache Cache.
     * @param cnts Accounts per person counts.
     */
    private void checkPersonAccountsJoin(IgniteCache cache, Map<Integer, Integer> cnts) {
        if (skipQuery(cache, PERSON_CACHE_NAME, ACC_CACHE_NAME))
            return;

        qry = "checkPersonAccountsJoin";

        List<Query> qrys = new ArrayList<>();

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + PERSON_CACHE_NAME + "\".Person p, " +
                "\"" + ACC_CACHE_NAME + "\".Account a " +
                "where p.id = a.personId and p.id=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + PERSON_CACHE_NAME + "\".Person p, " +
                "\"" + ACC_CACHE_NAME + "\".Account a " +
                "where p.dateId = a.personDateId and p.id=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + PERSON_CACHE_NAME + "\".Person p, " +
                "\"" + ACC_CACHE_NAME + "\".Account a " +
                "where p.strId = a.personStrId and p.id=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + PERSON_CACHE_NAME + "\".Person p, " +
                "\"" + ACC_CACHE_NAME + "\".Account a " +
                "where p.id = a.personId and p.id=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + PERSON_CACHE_NAME + "\".Person p, " +
                "\"" + ACC_CACHE_NAME + "\".Account a " +
                "where p.dateId = a.personDateId and p.id=?")
        );

        qrys.add(new SqlFieldsQuery("select p.name from " +
                "\"" + PERSON_CACHE_NAME + "\".Person p, " +
                "\"" + ACC_CACHE_NAME + "\".Account a " +
                "where p.strId = a.personStrId and p.id=?")
        );

        if (PERSON_CACHE_NAME.equals(cache.getName())) {
            qrys.add(new SqlQuery(Person.class,
                    "from \"" + PERSON_CACHE_NAME + "\".Person , \"" + ACC_CACHE_NAME + "\".Account  " +
                        "where Person.id = Account.personId and Person.id=?")
            );

            qrys.add(new SqlQuery(Person.class,
                    "from \"" + PERSON_CACHE_NAME + "\".Person , \"" + ACC_CACHE_NAME + "\".Account  " +
                        "where Person.id = Account.personId and Person.id=?")
            );
        }

        List<Integer> keys = new ArrayList<>(cnts.keySet());

        for (int i = 0; i < 10; i++) {
            Integer key = keys.get(rnd.nextInt(keys.size()));

            List<List<Object>> res;

            for (Query q : qrys) {
                if (q instanceof SqlFieldsQuery) {
                    ((SqlFieldsQuery)q).setDistributedJoins(distributedJoins());

                    ((SqlFieldsQuery)q).setArgs(key);
                }
                else {
                    ((SqlQuery)q).setDistributedJoins(distributedJoins());

                    ((SqlQuery)q).setArgs(key);
                }

                res = cache.query(q).getAll();

                assertEquals((int)cnts.get(key), res.size());
            }
        }

        qrys.clear();

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + PERSON_CACHE_NAME + "\".Person p, \"" + ACC_CACHE_NAME + "\".Account" + " a " +
            "where p.id = a.personId"));

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + PERSON_CACHE_NAME + "\".Person p, \"" + ACC_CACHE_NAME + "\".Account" + " a " +
            "where p.Dateid = a.personDateId"));

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + PERSON_CACHE_NAME + "\".Person p, \"" + ACC_CACHE_NAME + "\".Account" + " a " +
            "where p.strId = a.personStrId"));

        qrys.add(new SqlFieldsQuery("select count(*) " +
            "from \"" + PERSON_CACHE_NAME + "\".Person p, \"" + ACC_CACHE_NAME + "\".Account" + " a " +
            "where p.id = a.personId"));

        long total = 0;

        for (Integer cnt : data.accountsPerPerson.values())
            total += cnt;

        for (Query q : qrys) {
            ((SqlFieldsQuery)q).setDistributedJoins(distributedJoins());

            List<List<Object>> res = cache.query(q).getAll();

            assertEquals(1, res.size());
            assertEquals(total, res.get(0).get(0));
        }
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkOrganizationPersonAccountJoin(IgniteCache cache) throws Exception {
        if (skipQuery(cache, PERSON_CACHE_NAME, ORG_CACHE_NAME, ACC_CACHE_NAME))
            return;

        qry = "checkOrganizationPersonAccountJoin";

        List<String> sqlFields = new ArrayList<>();

        sqlFields.add("select o.name, p.name, a._key " +
            "from " +
            "\"" + ORG_CACHE_NAME + "\".Organization o, " +
            "\"" + PERSON_CACHE_NAME + "\".Person p, " +
            "\"" + ACC_CACHE_NAME + "\".Account a " +
            "where p.orgId = o.id and p.id = a.personId and o.id = ?");

        sqlFields.add("select o.name, p.name, a._key " +
            "from " +
            "\"" + ORG_CACHE_NAME + "\".Organization o, " +
            "\"" + PERSON_CACHE_NAME + "\".Person p, " +
            "\"" + ACC_CACHE_NAME + "\".Account a " +
            "where p.orgDateId = o.dateId and p.strId = a.personStrId and o.id = ?");

        sqlFields.add("select o.name, p.name, a._key " +
            "from " +
            "\"" + ORG_CACHE_NAME + "\".Organization o, " +
            "\"" + PERSON_CACHE_NAME + "\".Person p, " +
            "\"" + ACC_CACHE_NAME + "\".Account a " +
            "where p.orgStrId = o.strId and p.id = a.personId and o.id = ?");

        for (Organization org : data.orgs) {
            for (String sql : sqlFields)
                compareQueryRes0(cache, sql, distributedJoins(), new Object[] {org.id}, Ordering.RANDOM);
        }

        if (ACC_CACHE_NAME.equals(cache.getName())) {
            for (int orgId = 0; orgId < data.accountsPerOrg.size(); orgId++) {
                SqlQuery q = new SqlQuery(Account.class, "from " +
                    "\"" + ORG_CACHE_NAME + "\".Organization , " +
                    "\"" + PERSON_CACHE_NAME + "\".Person , " +
                    "\"" + ACC_CACHE_NAME + "\".Account  " +
                    "where Person.orgId = Organization.id and Person.id = Account.personId and Organization.id = ?");

                q.setDistributedJoins(distributedJoins());

                q.setArgs(orgId);

                List<List<Object>> res = cache.query(q).getAll();

                assertEquals((int)data.accountsPerOrg.get(orgId), res.size());
            }
        }

        String sql = "select count(*) " +
            "from " +
            "\"" + ORG_CACHE_NAME + "\".Organization o, " +
            "\"" + PERSON_CACHE_NAME + "\".Person p, " +
            "\"" + ACC_CACHE_NAME + "\".Account a " +
            "where p.orgId = o.id and p.id = a.personId";

        compareQueryRes0(cache, sql, distributedJoins(), new Object[0], Ordering.RANDOM);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkUnionAll() throws Exception {
        if (skipQuery(cache, PERSON_CACHE_NAME, ACC_CACHE_NAME, ORG_CACHE_NAME))
            return;

        qry = "checkUnionAll";

        String sql = "select a.id, p.name from " +
            "\"" + PERSON_CACHE_NAME + "\".Person p, " +
            "\"" + ACC_CACHE_NAME + "\".Account a " +
            "where p.id = a.personId and p.id = ? " +
            "union all " +
            "select p.id, o.name from " +
            "\"" + ORG_CACHE_NAME + "\".Organization o, " +
            "\"" + PERSON_CACHE_NAME + "\".Person p " +
            "where p.orgStrId = o.strId and o.id = ?";

        for (int i = 0; i < 10; i++) {
            Person person = data.persons.get(rnd.nextInt(data.persons.size()));

            for (Organization org : data.orgs)
                compareQueryRes0(cache, sql, distributedJoins(), new Object[] {person.id, org.id}, Ordering.RANDOM);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkUnion() throws Exception {
        if (skipQuery(cache, PERSON_CACHE_NAME, ACC_CACHE_NAME, ORG_CACHE_NAME))
            return;

        qry = "checkUnion";

        String sql = "select a.id, p.name from " +
            "\"" + PERSON_CACHE_NAME + "\".Person p, " +
            "\"" + ACC_CACHE_NAME + "\".Account a " +
            "where p.id = a.personId and p.id = ? " +
            "union " +
            "select p.id, o.name from " +
            "\"" + ORG_CACHE_NAME + "\".Organization o, " +
            "\"" + PERSON_CACHE_NAME + "\".Person p " +
            "where p.orgStrId = o.strId and o.id = ?";

        for (int i = 0; i < 10; i++) {
            Person person = data.persons.get(rnd.nextInt(data.persons.size()));

            for (Organization org : data.orgs)
                compareQueryRes0(cache, sql, distributedJoins(), new Object[] {person.id, org.id}, Ordering.RANDOM);
        }
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkPersonAccountCrossJoin(IgniteCache cache) throws Exception {
        if (skipQuery(cache, PERSON_CACHE_NAME, ACC_CACHE_NAME))
            return;

        qry = "checkPersonAccountCrossJoin";

        String sql = "select p.name " +
            "from \"" + PERSON_CACHE_NAME + "\".Person p " +
            "cross join \"" + ACC_CACHE_NAME + "\".Account a";

        compareQueryRes0(cache, sql, distributedJoins(), new Object[0], Ordering.RANDOM);
    }

    /**
     * @param cache Cache.
     */
    private void checkPersonOrganizationGroupBy(IgniteCache cache) {
        if (skipQuery(cache, PERSON_CACHE_NAME, ORG_CACHE_NAME))
            return;

        qry = "checkPersonOrganizationGroupBy";

        // Max salary per organization.
        SqlFieldsQuery q = new SqlFieldsQuery("select max(p.salary) " +
            "from \"" + PERSON_CACHE_NAME + "\".Person p join \"" + ORG_CACHE_NAME + "\".Organization o " +
            "on p.orgId = o.id " +
            "group by o.name " +
            "having o.id = ?");

        q.setDistributedJoins(distributedJoins());

        for (Map.Entry<Integer, Integer> e : data.maxSalaryPerOrg.entrySet()) {
            Integer orgId = e.getKey();
            Integer maxSalary = e.getValue();

            q.setArgs(orgId);

            List<List<?>> res = cache.query(q).getAll();

            String errMsg = "Expected data [orgId=" + orgId + ", maxSalary=" + maxSalary + ", data=" + data + "]";

            // MaxSalary == -1 means that there are no persons at organization.
            if (maxSalary > 0) {
                assertEquals(errMsg, 1, res.size());
                assertEquals(errMsg, 1, res.get(0).size());
                assertEquals(errMsg, maxSalary, res.get(0).get(0));
            }
            else
                assertEquals(errMsg, 0, res.size());
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkPersonAccountGroupBy(IgniteCache cache) {
        if (skipQuery(cache, PERSON_CACHE_NAME, ACC_CACHE_NAME))
            return;

        qry = "checkPersonAccountGroupBy";

        // Count accounts per person.
        SqlFieldsQuery q = new SqlFieldsQuery("select count(a.id) " +
            "from \"" + PERSON_CACHE_NAME + "\".Person p join \"" + ACC_CACHE_NAME + "\".Account a " +
            "on p.strId = a.personStrId " +
            "group by p.name " +
            "having p.id = ?");

        q.setDistributedJoins(distributedJoins());

        List<Integer> keys = new ArrayList<>(data.accountsPerPerson.keySet());

        for (int i = 0; i < 10; i++) {
            Integer personId = keys.get(rnd.nextInt(keys.size()));
            Integer cnt = data.accountsPerPerson.get(personId);

            q.setArgs(personId);

            List<List<?>> res = cache.query(q).getAll();

            String errMsg = "Expected data [personId=" + personId + ", cnt=" + cnt + ", data=" + data + "]";

            // Cnt == 0 means that there are no accounts for the person.
            if (cnt > 0) {
                assertEquals(errMsg, 1, res.size());
                assertEquals(errMsg, 1, res.get(0).size());
                assertEquals(errMsg, (long)cnt, res.get(0).get(0));
            }
            else
                assertEquals(errMsg, 0, res.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGroupBy() throws Exception {
        if (skipQuery(cache, PERSON_CACHE_NAME, ACC_CACHE_NAME, ORG_CACHE_NAME))
            return;

        qry = "checkGroupBy";

        // Select persons with count of accounts of person at organization.
        String sql = "select p.id, count(a.id) " +
            "from " +
            "\"" + PERSON_CACHE_NAME + "\".Person p, " +
            "\"" + ORG_CACHE_NAME + "\".Organization o, " +
            "\"" + ACC_CACHE_NAME + "\".Account a " +
            "where p.id = a.personId and p.orgStrId = o.strId " +
            "group by p.id " +
            "having o.id = ?";

        for (Organization org : data.orgs)
            compareQueryRes0(cache, sql, distributedJoins(), new Object[] {org.id}, Ordering.RANDOM);
    }

    /**
     * @param cache Cache used to run query.
     * @param caches Cache names.
     * @return {@code True} if skip query execution.
     */
    private boolean skipQuery(IgniteCache cache, String... caches) {
        // Skip join replicated/partitioned caches executed on replicated cache.
        Ignite node = (Ignite)cache.unwrap(Ignite.class);

        if (!distributedJoins() && replicated(cache)) {
            for (String cacheName : caches) {
                if (!replicated(node.cache(cacheName)))
                    return true;
            }
        }

        return false;
    }

    /**
     *
     */
    private enum TestCacheType {
        /** */
        REPLICATED(CacheMode.REPLICATED, 0),

        /** */
        PARTITIONED_b0(CacheMode.PARTITIONED, 0),

        /** */
        PARTITIONED_b1(CacheMode.PARTITIONED, 1);

        /** */
        final CacheMode cacheMode;

        /** */
        final int backups;

        /**
         * @param mode Cache mode.
         * @param backups Backups.
         */
        TestCacheType(CacheMode mode, int backups) {
            cacheMode = mode;
            this.backups = backups;
        }
    }

    /**
     *
     */
    private static class TestCache {
        /** */
        @GridToStringInclude
        final String cacheName;

        /** */
        @GridToStringInclude
        final TestCacheType type;

        /**
         * @param cacheName Cache name.
         * @param type Cache type.
         */
        public TestCache(String cacheName, TestCacheType type) {
            this.cacheName = cacheName;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestCache.class, this);
        }
    }

    /**
     *
     */
    private static class Data {
        /** */
        final List<Organization> orgs;

        /** */
        final List<Person> persons;

        /** */
        final List<Account> accounts;

        /** */
        final Map<Integer, Integer> personsPerOrg;

        /** PersonId to count of accounts that person has. */
        final Map<Integer, Integer> accountsPerPerson;

        /** */
        final Map<Integer, Integer> accountsPerOrg;

        /** */
        final Map<Integer, Integer> maxSalaryPerOrg;

        /**
         * @param orgs Organizations.
         * @param persons Persons.
         * @param accounts Accounts.
         * @param personsPerOrg Count of persons per organization.
         * @param accountsPerPerson Count of accounts per person.
         * @param accountsPerOrg Count of accounts per organization.
         * @param maxSalaryPerOrg Maximum salary per organization.
         */
        Data(List<Organization> orgs,
            List<Person> persons,
            List<Account> accounts,
            Map<Integer, Integer> personsPerOrg,
            Map<Integer, Integer> accountsPerPerson,
            Map<Integer, Integer> accountsPerOrg,
             Map<Integer, Integer> maxSalaryPerOrg) {
            this.orgs = orgs;
            this.persons = persons;
            this.accounts = accounts;
            this.personsPerOrg = personsPerOrg;
            this.accountsPerPerson = accountsPerPerson;
            this.accountsPerOrg = accountsPerOrg;
            this.maxSalaryPerOrg = maxSalaryPerOrg;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Data [" +
                "orgs=" + orgs +
                ", persons=" + persons +
                ", accounts=" + accounts +
                ", personsPerOrg=" + personsPerOrg +
                ", accountsPerPerson=" + accountsPerPerson +
                ", accountsPerOrg=" + accountsPerOrg +
                ", maxSalaryPerOrg=" + maxSalaryPerOrg +
                ']';
        }
    }

    /**
     *
     */
    private static class Account implements Serializable {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        private int personId;

        /** */
        @QuerySqlField
        private Date personDateId;

        /** */
        @QuerySqlField
        private String personStrId;

        /** */
        @QuerySqlField
        private int orgId;

        /**
         * @param id ID.
         * @param personId Person ID.
         * @param orgId Organization ID.
         */
        Account(int id, int personId, int orgId) {
            this.id = id;
            this.personId = personId;
            this.orgId = orgId;
            personDateId = new Date(personId);
            personStrId = "personId" + personId;
        }

        /**
         * @param useCollocatedData Use colocated data.
         * @return Key.
         */
        public Object key(boolean useCollocatedData) {
            return useCollocatedData ? new AffinityKey<>(id, orgId) : id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Account [" +
                "id=" + id +
                ", personId=" + personId +
                ']';
        }
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField
        int id;

        /** Date as ID. */
        @QuerySqlField
        Date dateId;

        /** String as ID */
        @QuerySqlField
        String strId;

        /** */
        @QuerySqlField
        int orgId;

        /** */
        @QuerySqlField
        Date orgDateId;

        /** */
        @QuerySqlField
        String orgStrId;

        /** */
        @QuerySqlField
        String name;

        /** */
        @QuerySqlField
        int salary;

        /**
         * @param id ID.
         * @param orgId Organization ID.
         * @param name Name.
         * @param salary Salary.
         */
        Person(int id, int orgId, String name, int salary) {
            this.id = id;
            dateId = new Date(id);
            strId = "personId" + id;
            this.orgId = orgId;
            orgDateId = new Date(orgId);
            orgStrId = "orgId" + orgId;
            this.name = name;
            this.salary = salary;
        }

        /**
         * @param useCollocatedData Use collocated data.
         * @return Key.
         */
        public Object key(boolean useCollocatedData) {
            return useCollocatedData ? new AffinityKey<>(id, orgId) : id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [" +
                "id=" + id +
                ", orgId=" + orgId +
                ", name='" + name + '\'' +
                ", salary=" + salary +
                ']';
        }
    }

    /**
     *
     */
    private static class Organization implements Serializable {
        /** */
        @QuerySqlField
        int id;

        /** */
        @QuerySqlField
        Date dateId;

        /** */
        @QuerySqlField
        String strId;

        /** */
        @QuerySqlField
        String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        Organization(int id, String name) {
            this.id = id;
            dateId = new Date(id);
            strId = "orgId" + id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [" +
                "name='" + name + '\'' +
                ", id=" + id +
                ']';
        }
    }

    /**
     *
     */
    private static class TestConfig {
        /** */
        private final int idx;

        /** */
        private final IgniteCache testedCache;

        /** */
        private final TestCache personCache;

        /** */
        private final TestCache accCache;

        /** */
        private final TestCache orgCache;

        /** */
        private final String qry;

        /**
         * @param cfgIdx Tested configuration index.
         * @param testedCache Tested testedCache.
         * @param personCacheType Person testedCache personCacheType.
         * @param accCacheType Account testedCache personCacheType.
         * @param orgCacheType Organization testedCache personCacheType.
         * @param testedQry Query.
         */
        TestConfig(int cfgIdx,
            IgniteCache testedCache,
            TestCache personCacheType,
            TestCache accCacheType,
            TestCache orgCacheType,
            String testedQry) {
            idx = cfgIdx;
            this.testedCache = testedCache;
            this.personCache = personCacheType;
            this.accCache = accCacheType;
            this.orgCache = orgCacheType;
            qry = testedQry;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestConfig [" +
                "idx=" + idx +
                ", testedCache=" + testedCache.getName() +
                ", personCache=" + personCache +
                ", accCache=" + accCache +
                ", orgCache=" + orgCache +
                ", qry=" + qry +
                ']';
        }
    }
}
