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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheDistributedJoinNoIndexTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private static final String ORG_CACHE = "org";

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

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Person.class.getName());
            entity.addQueryField("orgId", Integer.class.getName(), null);
            entity.addQueryField("orgName", String.class.getName(), null);

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ORG_CACHE);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Organization.class.getName());
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
        ccfg.setBackups(0);

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

        Affinity<Object> aff = client.affinity(PERSON_CACHE);

        final IgniteCache<Object, Object> personCache = client.cache(PERSON_CACHE);
        IgniteCache<Object, Object> orgCache = client.cache(ORG_CACHE);

        AtomicInteger pKey = new AtomicInteger(100_000);
        AtomicInteger orgKey = new AtomicInteger();

        ClusterNode node0 = ignite(0).cluster().localNode();
        ClusterNode node1 = ignite(1).cluster().localNode();

        for (int i = 0; i < 3; i++) {
            int orgId = keyForNode(aff, orgKey, node0);

            orgCache.put(orgId, new Organization("org-" + i));

            for (int j = 0; j < i; j++)
                personCache.put(keyForNode(aff, pKey, node1), new Person(orgId, "org-" + i));
        }

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from \"org\".Organization o, \"person\".Person p " +
            "where p.orgName = o.name");

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from \"org\".Organization o inner join \"person\".Person p " +
            "on p.orgName = o.name");

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from \"org\".Organization o, \"person\".Person p " +
            "where p.orgName > o.name");

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from (select * from \"org\".Organization) o, \"person\".Person p " +
            "where p.orgName = o.name");

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from \"org\".Organization o, (select * from \"person\".Person) p " +
            "where p.orgName = o.name");

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from (select * from \"org\".Organization) o, (select * from \"person\".Person) p " +
            "where p.orgName = o.name");

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from \"org\".Organization o, \"person\".Person p");

        checkNoIndexError(personCache, "select o.name, p._key, p.orgName " +
            "from \"org\".Organization o, \"person\".Person p where o._key != p._key");

        checkQuery("select o.name, p._key, p.orgName " +
            "from \"org\".Organization o, \"person\".Person p " +
            "where p._key = o._key and o.name=?", personCache, 0, "aaa");
    }

    /**
     * @param cache Cache.
     * @param sql SQL.
     */
    private void checkNoIndexError(final IgniteCache<Object, Object> cache, final String sql) {
        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                SqlFieldsQuery qry = new SqlFieldsQuery(sql);

                qry.setDistributedJoins(true);

                cache.query(qry).getAll();

                return null;
            }
        }, CacheException.class, null);

        log.info("Error: " + err.getMessage());

        assertTrue("Unexpected error message: " + err.getMessage(),
            err.getMessage().contains("join condition does not use index"));
    }

    /**
     * @param sql SQL.
     * @param cache Cache.
     * @param expSize Expected results size.
     * @param args Arguments.
     * @return Results.
     */
    private List<List<?>> checkQuery(String sql,
        IgniteCache<Object, Object> cache,
        int expSize,
        Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        qry.setDistributedJoins(true);
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
     *
     */
    private static class Person implements Serializable {
        /** */
        int orgId;

        /** */
        String orgName;

        /**
         * @param orgId Organization ID.
         * @param orgName Organization name.
         */
        public Person(int orgId, String orgName) {
            this.orgId = orgId;
            this.orgName = orgName;
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
