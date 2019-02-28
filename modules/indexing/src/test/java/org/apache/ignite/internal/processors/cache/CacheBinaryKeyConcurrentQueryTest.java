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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheBinaryKeyConcurrentQueryTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 3;

    /** */
    private static final int KEYS = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAndQueries() throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache cache1 = ignite.createCache(cacheConfiguration("cache1", ATOMIC));
        IgniteCache cache2 = ignite.createCache(cacheConfiguration("cache2", TRANSACTIONAL));

        insertData(ignite, cache1.getName());
        insertData(ignite, cache2.getName());

        IgniteInternalFuture<?> fut1 = startUpdate(cache1.getName());
        IgniteInternalFuture<?> fut2 = startUpdate(cache2.getName());

        fut1.get();
        fut2.get();
    }

    /**
     * @param cacheName Cache name.
     * @return Future.
     */
    private IgniteInternalFuture<?> startUpdate(final String cacheName) {
        final long stopTime = System.currentTimeMillis() + 30_000;

        final AtomicInteger idx = new AtomicInteger();

        return GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Void call() {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                IgniteCache cache = ignite(idx.getAndIncrement() % NODES).cache(cacheName).withKeepBinary();

                while (System.currentTimeMillis() < stopTime) {
                    switch (rnd.nextInt(5)) {
                        case 0: {
                            TestKey key = new TestKey(rnd.nextInt(KEYS));

                            CacheEntry e = cache.getEntry(key);

                            assertNotNull(e);
                            assertTrue(e.getKey() instanceof BinaryObject);

                            cache.put(e.getKey(), new TestValue(rnd.nextInt(KEYS)));

                            break;
                        }

                        case 1: {
                            Iterator<Cache.Entry> it = cache.iterator();

                            for (int i = 0; i < 100 && it.hasNext(); i++) {
                                Cache.Entry e = it.next();

                                assertTrue(e.getKey() instanceof BinaryObject);

                                cache.put(e.getKey(), new TestValue(rnd.nextInt(KEYS)));
                            }

                            break;
                        }

                        case 2: {
                            SqlFieldsQuery qry = new SqlFieldsQuery("select _key " +
                                "from \"" + cache.getName() + "\".TestValue where id=?");

                            qry.setArgs(rnd.nextInt(KEYS));

                            List<List> res = cache.query(qry).getAll();

                            assertEquals(1, res.size());

                            BinaryObject key = (BinaryObject)res.get(0).get(0);

                            cache.put(key, new TestValue(rnd.nextInt(KEYS)));

                            break;
                        }

                        case 3: {
                            SqlQuery qry = new SqlQuery("TestValue", "id=?");

                            qry.setArgs(rnd.nextInt(KEYS));

                            List<Cache.Entry> res = cache.query(qry).getAll();

                            assertEquals(1, res.size());

                            break;
                        }

                        case 4: {
                            SqlQuery qry = new SqlQuery("TestValue", "order by id");

                            int cnt = 0;

                            for (Cache.Entry e : (Iterable<Cache.Entry>)cache.query(qry)) {
                                assertNotNull(cache.get(e.getKey()));

                                cnt++;
                            }

                            assertTrue(cnt > 0);

                            break;
                        }

                        default:
                            fail();
                    }
                }

                return null;
            }
        }, NODES * 2, "test-thread");
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     */
    private void insertData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < KEYS; i++)
                streamer.addData(new TestKey(i), new TestValue(i));
        }
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(name);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(TestKey.class.getName());
        qryEntity.setValueType(TestValue.class.getName());

        qryEntity.addQueryField("id", Integer.class.getName(), null);
        qryEntity.addQueryField("val", Integer.class.getName(), null);

        qryEntity.setKeyFields(Collections.singleton("id"));

        qryEntity.setIndexes(F.asList(new QueryIndex("id"), new QueryIndex("val")));

        ccfg.setQueryEntities(F.asList(qryEntity));

        return ccfg;
    }

    /**
     *
     */
    static class TestKey {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /**
         * @param id ID.
         */
        public TestKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey testKey = (TestKey)o;

            return id == testKey.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        @QuerySqlField(index = true)
        private int val;

        /**
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }
    }
}
