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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheJoinPartitionedAndReplicatedTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private static final String ORG_CACHE = "org";

    /** */
    private static final String ORG_CACHE_REPLICATED = "orgRepl";

    /** */
    private static final int NUMBER_OF_PARTITIONS = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        {
            CacheConfiguration ccfg = configuration(PERSON_CACHE);

            ccfg.setCacheMode(REPLICATED);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Person.class.getName());
            entity.addQueryField("orgId", Integer.class.getName(), null);
            entity.addQueryField("name", String.class.getName(), null);

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ORG_CACHE);

            ccfg.setCacheMode(PARTITIONED);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Organization.class.getName());
            entity.addQueryField("id", Integer.class.getName(), null);
            entity.addQueryField("name", String.class.getName(), null);

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ORG_CACHE_REPLICATED);

            ccfg.setCacheMode(REPLICATED);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Organization.class.getName());
            entity.addQueryField("id", Integer.class.getName(), null);
            entity.addQueryField("name", String.class.getName(), null);

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        cfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration configuration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(name);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, NUMBER_OF_PARTITIONS));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);

        startClientGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Ignite client = grid(2);

        IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);
        IgniteCache<Object, Object> orgCache = client.cache(ORG_CACHE);
        IgniteCache<Object, Object> orgCacheRepl = client.cache(ORG_CACHE_REPLICATED);

        personCache.clear();
        orgCache.clear();
        orgCacheRepl.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoin() {
        Ignite client = grid(2);

        IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);
        IgniteCache<Object, Object> orgCache = client.cache(ORG_CACHE);
        IgniteCache<Object, Object> orgCacheRepl = client.cache(ORG_CACHE_REPLICATED);

        List<Integer> keys = primaryKeys(ignite(0).cache(PERSON_CACHE), 3, 200_000);

        orgCache.put(keys.get(0), new Organization(0, "org1"));
        orgCacheRepl.put(keys.get(0), new Organization(0, "org1"));
        personCache.put(keys.get(1), new Person(0, "p1"));
        personCache.put(keys.get(2), new Person(0, "p2"));

        checkQuery("select o.name, p._key, p.name " +
            "from \"person\".Person p join \"org\".Organization o " +
            "on (p.orgId = o.id)", orgCache, 2);

        checkQuery("select o.name, p._key, p.name " +
            "from \"org\".Organization o join \"person\".Person p " +
            "on (p.orgId = o.id)", orgCache, 2);

        checkQuery("select o.name, p._key, p.name " +
            "from \"person\".Person p join \"orgRepl\".Organization o " +
            "on (p.orgId = o.id)", orgCacheRepl, 2);

        checkQuery("select o.name, p._key, p.name " +
            "from \"orgRepl\".Organization o join \"person\".Person p " +
            "on (p.orgId = o.id)", orgCacheRepl, 2);

        checkQuery("select p.name from \"person\".Person p", ignite(0).cache(PERSON_CACHE), 2);
        checkQuery("select p.name from \"person\".Person p", ignite(1).cache(PERSON_CACHE), 2);

        for (int i = 0; i < 10; i++)
            checkQuery("select p.name from \"person\".Person p", personCache, 2);

        checkQuery("select o.name, p._key, p.name " +
            "from \"org\".Organization o left join \"person\".Person p " +
            "on (p.orgId = o.id)", orgCache, 2);

        checkQuery("select o.name, p._key, p.name " +
            "from \"person\".Person p left join \"orgRepl\".Organization o " +
            "on (p.orgId = o.id)", orgCacheRepl, 2);

        checkQuery("select o.name, p._key, p.name " +
            "from \"orgRepl\".Organization o left join \"person\".Person p " +
            "on (p.orgId = o.id)", orgCacheRepl, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSubquery() {
        Ignite client = grid(2);

        IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);
        IgniteCache<Object, Object> orgCache = client.cache(ORG_CACHE);
        IgniteCache<Object, Object> orgCacheRepl = client.cache(ORG_CACHE_REPLICATED);

        List<Integer> keys = primaryKeys(ignite(0).cache(PERSON_CACHE), 4, 200_000);

        orgCache.put(keys.get(0), new Organization(0, "org1"));
        orgCacheRepl.put(keys.get(0), new Organization(0, "org1"));

        personCache.put(keys.get(1), new Person(0, "p1"));
        personCache.put(keys.get(2), new Person(0, "p2"));
        personCache.put(keys.get(3), new Person(0, "p3"));

        // Subquery in `WHERE` clause
        checkQuery("select p._key, p.name " +
            "from \"person\".Person p where " +
            "p.orgId in (select o.id from \"org\".Organization o)", orgCache, 3);

        checkQuery("select o.name " +
            "from \"org\".Organization o where " +
            "o.id in (select p.orgId from \"person\".Person p)", orgCache, 1);

        checkQuery("select p._key, p.name " +
            "from \"person\".Person p where " +
            "p.orgId in (select o.id from \"org\".Organization o)", orgCacheRepl, 3);

        checkQuery("select o.name " +
            "from \"org\".Organization o where " +
            "o.id in (select p.orgId from \"person\".Person p)", orgCacheRepl, 1);

        // Prevent `IN` optimization.
        checkQuery("select p._key, p.name " +
            "from \"person\".Person p where " +
            "p.orgId < 10 or p.orgId in (select o.id from \"org\".Organization o)", orgCache, 3);

        checkQuery("select o.name " +
            "from \"org\".Organization o where " +
            "o.id < 10 or o.id in (select p.orgId from \"person\".Person p)", orgCache, 1);

        checkQuery("select p._key, p.name " +
            "from \"person\".Person p where " +
            "p.orgId < 10 or p.orgId in (select o.id from \"org\".Organization o)", orgCacheRepl, 3);

        checkQuery("select o.name " +
            "from \"org\".Organization o where " +
            "o.id < 10 or o.id in (select p.orgId from \"person\".Person p)", orgCacheRepl, 1);

        // Subquery in `FROM` clause
        checkQuery("select p1._key, p1.name " +
            "from (select p._key, p.name, p.orgId from \"person\".Person p " +
            "    where p.orgId < 10 or p.orgId in (select o.id from \"org\".Organization o)) p1 " +
            "where p1.orgId > -1", orgCache, 3);

        checkQuery("select o1.name " +
            "from (select o.id, o.name from \"org\".Organization o " +
            "    where o.id < 10 or o.id in (select p.orgId from \"person\".Person p)) o1 " +
            "where o1.id > -1", orgCache, 1);

        checkQuery("select p1._key, p1.name " +
            "from (select p._key, p.name, p.orgId from \"person\".Person p " +
            "    where p.orgId < 10 or p.orgId in (select o.id from \"org\".Organization o)) p1 " +
            "where p1.orgId > -1", orgCacheRepl, 3);

        checkQuery("select o1.name " +
            "from (select o.id, o.name from \"org\".Organization o " +
            "    where o.id < 10 or o.id in (select p.orgId from \"person\".Person p)) o1 " +
            "where o1.id > -1", orgCacheRepl, 1);

        // Join with subquery
        checkQuery("select o1.name, p._key, p.name " +
            "from \"person\".Person p " +
            "join (select o.id, o.name from \"org\".Organization o " +
            "    where o.id < 10 or o.id in (select p.orgId from \"person\".Person p)) o1 " +
            "on (p.orgId = o1.id)", orgCache, 3);

        checkQuery("select o.name, p1._key, p1.name " +
            "from \"org\".Organization o " +
            "join  (select p._key, p.name, p.orgId from \"person\".Person p " +
            "    where p.orgId < 10 or p.orgId in (select o.id from \"org\".Organization o)) p1 " +
            "on (p1.orgId = o.id)", orgCache, 3);

        checkQuery("select o1.name, p._key, p.name " +
            "from \"person\".Person p " +
            "join (select o.id, o.name from \"org\".Organization o " +
            "    where o.id < 10 or o.id in (select p.orgId from \"person\".Person p)) o1 " +
            "on (p.orgId = o1.id)", orgCacheRepl, 3);

        checkQuery("select o.name, p1._key, p1.name " +
            "from \"org\".Organization o " +
            "join (select p._key, p.name, p.orgId from \"person\".Person p " +
            "    where p.orgId < 10 or p.orgId in (select o.id from \"org\".Organization o)) p1 " +
            "on (p1.orgId = o.id)", orgCacheRepl, 3);
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param expSize Expected results size.
     * @param args Arguments.
     */
    private void checkQuery(String sql,
        IgniteCache<Object, Object> cache,
        int expSize,
        Object... args) {
        String plan = (String)cache.query(new SqlFieldsQuery("explain " + sql))
            .getAll().get(0).get(0);

        log.info("Plan: " + plan);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        qry.setArgs(args);

        QueryCursor<List<?>> cur = cache.query(qry);

        List<List<?>> res = cur.getAll();

        if (expSize != res.size())
            log.info("Results: " + res);

        assertEquals(expSize, res.size());
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

        /** */
        int id;

        /**
         * @param id ID.
         * @param name Name.
         */
        public Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Organization.class, this);
        }
    }
}
