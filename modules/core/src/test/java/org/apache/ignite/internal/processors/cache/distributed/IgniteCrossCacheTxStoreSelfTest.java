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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteCrossCacheTxStoreSelfTest extends GridCommonAbstractTest {
    /** */
    private static Map<String, CacheStore> firstStores = new ConcurrentHashMap<>();

    /** */
    private static Map<String, CacheStore> secondStores = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cfg1 = cacheConfiguration("cacheA", new FirstStoreFactory());
        CacheConfiguration cfg2 = cacheConfiguration("cacheB", new FirstStoreFactory());

        CacheConfiguration cfg3 = cacheConfiguration("cacheC", new SecondStoreFactory());
        CacheConfiguration cfg4 = cacheConfiguration("cacheD", null);

        cfg.setCacheConfiguration(cfg1, cfg2, cfg3, cfg4);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @param factory Factory to use.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName, Factory<CacheStore> factory) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setNearConfiguration(null);
        cfg.setName(cacheName);

        cfg.setBackups(1);

        if (factory != null) {
            cfg.setCacheStoreFactory(factory);

            cfg.setWriteThrough(true);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        firstStores.clear();
        secondStores.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache("cacheA").removeAll();
        grid(0).cache("cacheB").removeAll();
        grid(0).cache("cacheC").removeAll();

        for (CacheStore store : firstStores.values())
            ((TestStore)store).clear();

        for (CacheStore store : secondStores.values())
            ((TestStore)store).clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameStore() throws Exception {
        IgniteEx grid = grid(0);

        TestStore firstStore = (TestStore)firstStores.get(grid.name());
        TestStore secondStore = (TestStore)secondStores.get(grid.name());

        assertNotNull(firstStore);
        assertNotNull(secondStore);

        Collection<String> firstStoreEvts = firstStore.events();
        Collection<String> secondStoreEvts = secondStore.events();

        try (Transaction tx = grid.transactions().txStart()) {
            IgniteCache<Object, Object> cacheA = grid.cache("cacheA");
            IgniteCache<Object, Object> cacheB = grid.cache("cacheB");

            cacheA.put("1", "1");
            cacheA.put("2", "2");
            cacheB.put("1", "1");
            cacheB.put("2", "2");

            cacheA.remove("3");
            cacheA.remove("4");
            cacheB.remove("3");
            cacheB.remove("4");

            cacheA.put("5", "5");
            cacheA.remove("6");

            cacheB.put("7", "7");

            tx.commit();
        }

        assertEqualsCollections(F.asList(
            "writeAll cacheA 2",
            "writeAll cacheB 2",
            "deleteAll cacheA 2",
            "deleteAll cacheB 2",
            "write cacheA",
            "delete cacheA",
            "write cacheB",
            "sessionEnd true"
        ),
        firstStoreEvts);

        assertEquals(0, secondStoreEvts.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentStores() throws Exception {
        IgniteEx grid = grid(0);

        TestStore firstStore = (TestStore)firstStores.get(grid.name());
        TestStore secondStore = (TestStore)secondStores.get(grid.name());

        assertNotNull(firstStore);
        assertNotNull(secondStore);

        Collection<String> firstStoreEvts = firstStore.events();
        Collection<String> secondStoreEvts = secondStore.events();

        try (Transaction tx = grid.transactions().txStart()) {
            IgniteCache<Object, Object> cacheA = grid.cache("cacheA");
            IgniteCache<Object, Object> cacheC = grid.cache("cacheC");

            cacheA.put("1", "1");
            cacheA.put("2", "2");
            cacheC.put("1", "1");
            cacheC.put("2", "2");

            cacheA.remove("3");
            cacheA.remove("4");
            cacheC.remove("3");
            cacheC.remove("4");

            cacheA.put("5", "5");
            cacheA.remove("6");

            cacheC.put("7", "7");

            tx.commit();
        }

        assertEqualsCollections(F.asList(
            "writeAll cacheA 2",
            "deleteAll cacheA 2",
            "write cacheA",
            "delete cacheA",
            "sessionEnd true"
        ),
        firstStoreEvts);

        assertEqualsCollections(F.asList(
            "writeAll cacheC 2",
            "deleteAll cacheC 2",
            "write cacheC",
            "sessionEnd true"
        ),
        secondStoreEvts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonPersistentCache() throws Exception {
        IgniteEx grid = grid(0);

        TestStore firstStore = (TestStore)firstStores.get(grid.name());
        TestStore secondStore = (TestStore)secondStores.get(grid.name());

        assertNotNull(firstStore);
        assertNotNull(secondStore);

        Collection<String> firstStoreEvts = firstStore.events();
        Collection<String> secondStoreEvts = secondStore.events();

        try (Transaction tx = grid.transactions().txStart()) {
            IgniteCache<Object, Object> cacheA = grid.cache("cacheA");
            IgniteCache<Object, Object> cacheD = grid.cache("cacheD");

            cacheA.put("1", "1");
            cacheA.put("2", "2");
            cacheD.put("1", "1");
            cacheD.put("2", "2");

            cacheA.remove("3");
            cacheA.remove("4");
            cacheD.remove("3");
            cacheD.remove("4");

            cacheA.put("5", "5");
            cacheA.remove("6");

            cacheD.put("7", "7");

            tx.commit();
        }

        assertEqualsCollections(F.asList(
            "writeAll cacheA 2",
            "deleteAll cacheA 2",
            "write cacheA",
            "delete cacheA",
            "sessionEnd true"
        ),
        firstStoreEvts);

        assertEquals(0, secondStoreEvts.size());
    }

    /**
     *
     */
    private static class TestStore implements CacheStore<Object, Object> {
        /** */
        private Queue<String> evts = new ConcurrentLinkedDeque<>();

        /** Auto-injected store session. */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /**
         *
         */
        public void clear() {
            evts.clear();
        }

        /**
         * @return Collection of recorded events.
         */
        public Collection<String> events() {
            return evts;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, @Nullable Object... args)
            throws CacheLoaderException {
        }

        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            evts.offer("sessionEnd " + commit);
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<Object, Object> loadAll(Iterable<?> keys) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            CacheStoreSession ses = session();

            String cacheName = ses.cacheName();

            evts.add("write " + cacheName);
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) throws CacheWriterException {
            String cacheName = session().cacheName();

            evts.add("writeAll " + cacheName + " " + entries.size());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            String cacheName = session().cacheName();

            evts.add("delete " + cacheName);
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            String cacheName = session().cacheName();

            evts.add("deleteAll " + cacheName + " " + keys.size());
        }

        /**
         * @return Store session.
         */
        private CacheStoreSession session() {
            return ses;
        }
    }

    /**
     *
     */
    private static class FirstStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            String gridName = startingGrid.get();

            CacheStore store = firstStores.get(gridName);

            if (store == null)
                store = F.addIfAbsent(firstStores, gridName, new TestStore());

            return store;
        }
    }

    /**
     *
     */
    private static class SecondStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            String gridName = startingGrid.get();

            CacheStore store = secondStores.get(gridName);

            if (store == null)
                store = F.addIfAbsent(secondStores, gridName, new TestStore());

            return store;
        }
    }
}