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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.CacheException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheJoinPartitionedAndReplicatedTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private static final String ORG_CACHE = "org";

    /** */
    private static final String ORG_CACHE_REPLICATED = "orgRepl";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = ((TcpDiscoverySpi)cfg.getDiscoverySpi());

        spi.setIpFinder(IP_FINDER);

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

        cfg.setClientMode(client);

        return cfg;
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
    public void testJoin() throws Exception {
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

        checkQueryFails("select o.name, p._key, p.name " +
                "from \"person\".Person p left join \"org\".Organization o " +
                "on (p.orgId = o.id)", personCache);

        checkQueryFails("select o.name, p._key, p.name " +
                "from \"org\".Organization o right join \"person\".Person p " +
                "on (p.orgId = o.id)", personCache);
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     */
    private void checkQueryFails(final String sql, final IgniteCache<Object, Object> cache) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                SqlFieldsQuery qry = new SqlFieldsQuery(sql);

                cache.query(qry).getAll();

                return null;
            }
        }, CacheException.class, null);
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
