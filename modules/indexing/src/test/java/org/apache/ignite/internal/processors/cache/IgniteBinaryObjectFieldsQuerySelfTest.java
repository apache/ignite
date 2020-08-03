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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that server nodes do not need class definitions to execute queries.
 */
public class IgniteBinaryObjectFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** */
    public static final String PERSON_KEY_CLS_NAME = "org.apache.ignite.tests.p2p.cache.PersonKey";

    /** Grid count. */
    public static final int GRID_CNT = 4;

    /** */
    private static ClassLoader extClassLoader;

    /**
     * Gets Person class name.
     * @return class name.
     */
    protected String getPersonClassName() {
        return "org.apache.ignite.tests.p2p.cache.Person";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setMarshaller(null);

        if (getTestIgniteInstanceName(3).equals(igniteInstanceName))
            cfg.setClassLoader(extClassLoader);

        return cfg;
    }

    /**
     * @return Cache.
     */
    protected CacheConfiguration cache(CacheMode cacheMode, CacheAtomicityMode atomicity) throws Exception {
        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cache.setRebalanceMode(CacheRebalanceMode.SYNC);
        cache.setCacheMode(cacheMode);
        cache.setAtomicityMode(atomicity);

        cache.setIndexedTypes(extClassLoader.loadClass(PERSON_KEY_CLS_NAME),
            extClassLoader.loadClass(getPersonClassName()));

        return cache;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        initExtClassLoader();

        startGrids(GRID_CNT - 1);
        startClientGrid(GRID_CNT - 1);
    }

    /** */
    protected void initExtClassLoader() {
        extClassLoader = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        extClassLoader = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryPartitionedAtomic() throws Exception {
        checkQuery(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryReplicatedAtomic() throws Exception {
        checkQuery(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryPartitionedTransactional() throws Exception {
        checkQuery(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryReplicatedTransactional() throws Exception {
        checkQuery(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFieldsQueryPartitionedAtomic() throws Exception {
        checkFieldsQuery(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFieldsQueryReplicatedAtomic() throws Exception {
        checkFieldsQuery(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFieldsQueryPartitionedTransactional() throws Exception {
        checkFieldsQuery(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFieldsQueryReplicatedTransactional() throws Exception {
        checkFieldsQuery(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkFieldsQuery(CacheMode cacheMode, CacheAtomicityMode atomicity) throws Exception {
        IgniteCache<Object, Object> cache = grid(GRID_CNT - 1).getOrCreateCache(cache(cacheMode, atomicity));

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
            grid(3).destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkQuery(CacheMode cacheMode, CacheAtomicityMode atomicity) throws Exception {
        IgniteCache<Object, Object> cache = grid(GRID_CNT - 1).getOrCreateCache(cache(cacheMode, atomicity));

        try {
            populate(cache);

            QueryCursor<Cache.Entry<Object, Object>> cur = cache.query(new SqlQuery("Person", "order " +
                "by id asc"));

            List<Cache.Entry<Object, Object>> all = cur.getAll();

            assertEquals(100, all.size());

            for (int i = 0; i < 100; i++) {
                Object person = all.get(i).getValue();

                assertEquals(Integer.valueOf(i), U.field(person, "id"));
                assertEquals("person-" + i, U.field(person, "name"));
                assertEquals("person-last-" + i, U.field(person, "lastName"));
                assertEquals((double)(i * 25), U.field(person, "salary"));
            }

            int max = 49;

            // Check local scan query with keepBinary flag set.
            ScanQuery<BinaryObject, BinaryObject> scanQry = new ScanQuery<>(new PersonKeyFilter(max));

            QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> curs = grid(GRID_CNT - 1)
                .cache(DEFAULT_CACHE_NAME).withKeepBinary().query(scanQry);

            List<Cache.Entry<BinaryObject, BinaryObject>> records = curs.getAll();

            assertEquals(50, records.size());

            for (Cache.Entry<BinaryObject, BinaryObject> entry : records) {
                BinaryObject key = entry.getKey();

                assertTrue(key.<Integer>field("id") <= max);

                assertEquals(PERSON_KEY_CLS_NAME, key.deserialize().getClass().getName());
            }
        }
        finally {
            grid(GRID_CNT - 1).cache(DEFAULT_CACHE_NAME).removeAll();
            grid(GRID_CNT - 1).destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void populate(IgniteCache<Object, Object> cache) throws Exception {
        Class<?> keyCls = extClassLoader.loadClass(PERSON_KEY_CLS_NAME);
        Class<?> cls = extClassLoader.loadClass(getPersonClassName());

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

    /**
     *
     */
    private static class PersonKeyFilter implements IgniteBiPredicate<BinaryObject, BinaryObject> {
        /** Max ID allowed. */
        private int maxId;

        /**
         * @param maxId Max ID allowed.
         */
        public PersonKeyFilter(int maxId) {
            this.maxId = maxId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BinaryObject key, BinaryObject val) {
            return key.<Integer>field("id") <= maxId;
        }
    }
}
