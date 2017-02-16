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
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
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

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheDistributedJoinCustomAffinityMapper extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private static final String PERSON_CACHE_CUSTOM_AFF = "personCustomAff";

    /** */
    private static final String ORG_CACHE = "org";

    /** */
    private static final String ORG_CACHE_REPL_CUSTOM = "orgReplCustomAff";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        {
            CacheConfiguration ccfg = configuration(PERSON_CACHE);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Person.class.getName());
            entity.addQueryField("orgId", Integer.class.getName(), null);
            entity.setIndexes(F.asList(new QueryIndex("orgId")));

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(PERSON_CACHE_CUSTOM_AFF);

            ccfg.setAffinityMapper(new TestMapper());

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Person.class.getName());
            entity.addQueryField("orgId", Integer.class.getName(), null);
            entity.setIndexes(F.asList(new QueryIndex("orgId")));

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ORG_CACHE);

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Organization.class.getName());

            ccfg.setQueryEntities(F.asList(entity));

            ccfgs.add(ccfg);
        }

        {
            CacheConfiguration ccfg = configuration(ORG_CACHE_REPL_CUSTOM);

            ccfg.setCacheMode(REPLICATED);
            ccfg.setAffinityMapper(new TestMapper());

            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());
            entity.setValueType(Organization.class.getName());

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

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinCustomAffinityMapper() throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Object, Object> cache = ignite.cache(PERSON_CACHE);

        checkQueryFails(cache, "select o._key k1, p._key k2 " +
            "from \"org\".Organization o, \"personCustomAff\".Person p where o._key=p.orgId", false);

        checkQueryFails(cache, "select o._key k1, p._key k2 " +
            "from \"personCustomAff\".Person p, \"org\".Organization o where o._key=p.orgId", false);

        {
            // Check regular query does not fail.
            SqlFieldsQuery qry = new SqlFieldsQuery("select o._key k1, p._key k2 " +
                "from \"org\".Organization o, \"personCustomAff\".Person p where o._key=p.orgId");

            cache.query(qry).getAll();
        }

        {
            // Should not check affinity for replicated cache.
            SqlFieldsQuery qry = new SqlFieldsQuery("select o1._key k1, p._key k2, o2._key k3 " +
                "from \"org\".Organization o1, \"person\".Person p, \"orgReplCustomAff\".Organization o2 where " +
                "o1._key=p.orgId and o2._key=p.orgId");

            cache.query(qry).getAll();
        }
    }

    /**
     * @param cache Cache.
     * @param sql SQL.
     * @param enforceJoinOrder Enforce join order flag.
     */
    private void checkQueryFails(final IgniteCache<Object, Object> cache,
        String sql,
        boolean enforceJoinOrder) {
        final SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        qry.setDistributedJoins(true);
        qry.setEnforceJoinOrder(enforceJoinOrder);

        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.query(qry);

                return null;
            }
        }, CacheException.class, null);

        assertTrue("Unexpected error message: " + err.getMessage(),
            err.getMessage().contains("can not use distributed joins for cache with custom AffinityKeyMapper configured."));
    }

    /**
     *
     */
    static class TestMapper implements AffinityKeyMapper {
        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }
    }
    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        int orgId;

        /**
         * @param orgId Organization ID.
         */
        public Person(int orgId) {
            this.orgId = orgId;
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
        // No-op.
    }
}
