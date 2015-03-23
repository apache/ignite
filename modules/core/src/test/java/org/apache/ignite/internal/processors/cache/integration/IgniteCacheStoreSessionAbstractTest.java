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

package org.apache.ignite.internal.processors.cache.integration;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 *
 */
public abstract class IgniteCacheStoreSessionAbstractTest extends IgniteCacheAbstractTest {
    /** */
    protected static volatile List<ExpectedData> expData;

    /** */
    protected static final String CACHE_NAME1 = "cache1";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestStore store = new TestStore(); // Use the same store instance for both caches.

        assert cfg.getCacheConfiguration().length == 1;

        CacheConfiguration ccfg0 = cfg.getCacheConfiguration()[0];

        ccfg0.setReadThrough(true);
        ccfg0.setWriteThrough(true);

        ccfg0.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));

        CacheConfiguration ccfg1 = cacheConfiguration(gridName);

        ccfg1.setReadThrough(true);
        ccfg1.setWriteThrough(true);

        ccfg1.setName(CACHE_NAME1);

        ccfg1.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));

        cfg.setCacheConfiguration(ccfg0, ccfg1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        expData = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        expData = Collections.synchronizedList(new ArrayList<ExpectedData>());

        super.beforeTestsStarted();
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Keys.
     * @throws Exception If failed.
     */
    protected List<Integer> testKeys(IgniteCache cache, int cnt) throws Exception {
        return primaryKeys(cache, cnt, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreSession() throws Exception {
        assertNull(jcache(0).getName());

        assertEquals(CACHE_NAME1, ignite(0).jcache(CACHE_NAME1).getName());

        testStoreSession(jcache(0));

        testStoreSession(ignite(0).jcache(CACHE_NAME1));
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void testStoreSession(IgniteCache<Object, Object> cache) throws Exception {
        Set<Integer> keys = new HashSet<>(primaryKeys(cache, 3, 100_000));

        Integer key = keys.iterator().next();

        boolean tx = atomicityMode() == TRANSACTIONAL;

        expData.add(new ExpectedData(false, "load", new HashMap<>(), cache.getName()));

        assertEquals(key, cache.get(key));

        assertTrue(expData.isEmpty());

        expData.add(new ExpectedData(false, "loadAll", new HashMap<>(), cache.getName()));

        assertEquals(3, cache.getAll(keys).size());

        assertTrue(expData.isEmpty());

        expectedData(tx, "write", cache.getName());

        cache.put(key, key);

        assertTrue(expData.isEmpty());

        expectedData(tx, "write", cache.getName());

        cache.invoke(key, new EntryProcessor<Object, Object, Object>() {
            @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
                e.setValue("val1");

                return null;
            }
        });

        assertTrue(expData.isEmpty());

        expectedData(tx, "delete", cache.getName());

        cache.remove(key);

        assertTrue(expData.isEmpty());

        Map<Object, Object> vals = new HashMap<>();

        for (Object key0 : keys)
            vals.put(key0, key0);

        expectedData(tx, "writeAll", cache.getName());

        cache.putAll(vals);

        assertTrue(expData.isEmpty());

        expectedData(tx, "deleteAll", cache.getName());

        cache.removeAll(keys);

        assertTrue(expData.isEmpty());

        expectedData(false, "loadCache", cache.getName());

        cache.localLoadCache(null);

        assertTrue(expData.isEmpty());
    }

    /**
     * @param tx {@code True} is transaction is expected.
     * @param expMtd Expected method.
     * @param expCacheName Expected cache name.
     */
    private void expectedData(boolean tx, String expMtd, String expCacheName) {
        expData.add(new ExpectedData(tx, expMtd, new HashMap<>(), expCacheName));

        if (tx)
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, expMtd), expCacheName));
    }

    /**
     *
     */
    static class ExpectedData {
        /** */
        private final boolean tx;

        /** */
        private final String expMtd;

        /** */
        private  final Map<Object, Object> expProps;

        /** */
        private final String expCacheName;

        /**
         * @param tx {@code True} if transaction is enabled.
         * @param expMtd Expected method.
         * @param expProps Expected properties.
         * @param expCacheName Expected cache name.
         */
        public ExpectedData(boolean tx, String expMtd, Map<Object, Object> expProps, String expCacheName) {
            this.tx = tx;
            this.expMtd = expMtd;
            this.expProps = expProps;
            this.expCacheName = expCacheName;
        }
    }

    /**
     *
     */
    private class TestStore implements CacheStore<Object, Object> {
        /** Auto-injected store session. */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, @Nullable Object... args) {
            log.info("Load cache [tx=" + session().transaction() + ']');

            checkSession("loadCache");
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            log.info("Tx end [commit=" + commit + ", tx=" + session().transaction() + ']');

            checkSession("sessionEnd");
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            log.info("Load [key=" + key + ", tx=" + session().transaction() + ']');

            checkSession("load");

            return key;
        }

        /** {@inheritDoc} */
        @Override public Map<Object, Object> loadAll(Iterable<?> keys) throws CacheLoaderException {
            log.info("LoadAll [keys=" + keys + ", tx=" + session().transaction() + ']');

            checkSession("loadAll");

            Map<Object, Object> loaded = new HashMap<>();

            for (Object key : keys)
                loaded.put(key, key);

            return loaded;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            log.info("Write [write=" + entry + ", tx=" + session().transaction() + ']');

            checkSession("write");
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) throws CacheWriterException {
            log.info("WriteAll: [writeAll=" + entries + ", tx=" + session().transaction() + ']');

            checkSession("writeAll");
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            log.info("Delete [key=" + key + ", tx=" + session().transaction() + ']');

            checkSession("delete");
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            log.info("DeleteAll [keys=" + keys + ", tx=" + session().transaction() + ']');

            checkSession("deleteAll");
        }

        /**
         * @return Store session.
         */
        private CacheStoreSession session() {
            return ses;
        }

        /**
         * @param mtd Called stored method.
         */
        private void checkSession(String mtd) {
            assertNotNull(ignite);

            assertFalse(expData.isEmpty());

            ExpectedData exp = expData.remove(0);

            assertEquals(exp.expMtd, mtd);

            CacheStoreSession ses = session();

            assertNotNull(ses);

            if (exp.tx)
                assertNotNull(ses.transaction());
            else
                assertNull(ses.transaction());

            Map<Object, Object> props = ses.properties();

            assertNotNull(props);

            assertEquals(exp.expProps, props);

            props.put(props.size(), mtd);

            assertEquals(exp.expCacheName, ses.cacheName());
        }
    }
}
