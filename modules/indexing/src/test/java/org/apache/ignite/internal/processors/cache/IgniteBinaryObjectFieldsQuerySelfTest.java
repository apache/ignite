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

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests that server nodes do not need class definitions to execute queries.
 */
public class IgniteBinaryObjectFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** */
    public static final String PERSON_KEY_CLS_NAME = "org.apache.ignite.tests.p2p.cache.PersonKey";

    /** */
    public static final String PERSON_CLS_NAME = "org.apache.ignite.tests.p2p.cache.Person";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static ClassLoader extClassLoader;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(null);

        if (getTestGridName(3).equals(gridName)) {
            cfg.setClientMode(true);
            cfg.setClassLoader(extClassLoader);
        }

        return cfg;
    }

    /**
     * @return Cache.
     */
    protected CacheConfiguration cache(CacheMode cacheMode, CacheAtomicityMode atomicity) throws Exception {
        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(null);
        cache.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cache.setRebalanceMode(CacheRebalanceMode.SYNC);
        cache.setCacheMode(cacheMode);
        cache.setAtomicityMode(atomicity);

        cache.setIndexedTypes(extClassLoader.loadClass(PERSON_KEY_CLS_NAME), extClassLoader.loadClass(PERSON_CLS_NAME));

        return cache;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        extClassLoader = getExternalClassLoader();

        startGrids(4);

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        extClassLoader = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryPartitionedAtomic() throws Exception {
        checkQuery(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReplicatedAtomic() throws Exception {
        checkQuery(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryPartitionedTransactional() throws Exception {
        checkQuery(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReplicatedTransactional() throws Exception {
        checkQuery(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryPartitionedAtomic() throws Exception {
        checkFieldsQuery(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryReplicatedAtomic() throws Exception {
        checkFieldsQuery(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryPartitionedTransactional() throws Exception {
        checkFieldsQuery(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryReplicatedTransactional() throws Exception {
        checkFieldsQuery(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkFieldsQuery(CacheMode cacheMode, CacheAtomicityMode atomicity) throws Exception {
        IgniteCache<Object, Object>cache = grid(3).getOrCreateCache(cache(cacheMode, atomicity));

        try {
            populate(cache);

            QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select id, name, lastName, salary from " +
                "Person order by id asc"));

            List<List<?>> all = cur.getAll();

            assertEquals(100, all.size());

            for (int i = 0; i < 100; i++) {
                List<?> row = all.get(i);

                assertEquals(i, row.get(0));
                assertEquals("person-" + i, row.get(1));
                assertEquals("person-last-" + i, row.get(2));
                assertEquals((double)(i * 25), row.get(3));
            }
        }
        finally {
            grid(3).destroyCache(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkQuery(CacheMode cacheMode, CacheAtomicityMode atomicity) throws Exception {
        IgniteCache<Object, Object> cache = grid(3).getOrCreateCache(cache(cacheMode, atomicity));

        try {
            populate(cache);

            QueryCursor<Cache.Entry<Object, Object>> cur = cache.query(new SqlQuery("Person", "order " +
                "by id asc"));

            List<Cache.Entry<Object, Object>> all = cur.getAll();

            assertEquals(100, all.size());

            for (int i = 0; i < 100; i++) {
                Object person = all.get(i).getValue();

                assertEquals(i, U.field(person, "id"));
                assertEquals("person-" + i, U.field(person, "name"));
                assertEquals("person-last-" + i, U.field(person, "lastName"));
                assertEquals((double)(i * 25), U.field(person, "salary"));
            }
        }
        finally {
            grid(3).destroyCache(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void populate(IgniteCache<Object, Object> cache) throws Exception {
        Class<?> keyCls = extClassLoader.loadClass(PERSON_KEY_CLS_NAME);
        Class<?> cls = extClassLoader.loadClass(PERSON_CLS_NAME);

        for (int i = 0; i < 100; i++) {
            Object key = keyCls.newInstance();

            GridTestUtils.setFieldValue(key, "id", i);

            Object person = cls.newInstance();

            GridTestUtils.setFieldValue(person, "id", i);
            GridTestUtils.setFieldValue(person, "name", "person-" + i);
            GridTestUtils.setFieldValue(person, "lastName", "person-last-" + i);
            GridTestUtils.setFieldValue(person, "salary", (double)(i * 25));

            cache.put(key, person);
        }
    }
}
