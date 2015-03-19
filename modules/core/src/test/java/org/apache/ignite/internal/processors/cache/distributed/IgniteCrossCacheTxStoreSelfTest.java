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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
public class IgniteCrossCacheTxStoreSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        TestStore firstStore = new TestStore();
        TestStore secondStore = new TestStore();

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cfg1 = cacheConfiguration("cacheA", firstStore);
        CacheConfiguration cfg2 = cacheConfiguration("cacheB", firstStore);

        CacheConfiguration cfg3 = cacheConfiguration("cacheC", secondStore);
        CacheConfiguration cfg4 = cacheConfiguration("cacheD", null);

        cfg.setCacheConfiguration(cfg1, cfg2, cfg3, cfg4);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @param store Cache store.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName, CacheStore<Object, Object> store) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        cfg.setName(cacheName);

        cfg.setBackups(1);

        if (store != null) {
            cfg.setCacheStoreFactory(
                new FactoryBuilder.SingletonFactory<CacheStore<? super Object, ? super Object>>(store));

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
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).jcache("cacheA").removeAll();
        grid(0).jcache("cacheB").removeAll();
        grid(0).jcache("cacheC").removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteThrough() throws Exception {
        IgniteEx grid = grid(0);

        TestStore firstStore = null;

        for (CacheConfiguration ccfg : grid(0).configuration().getCacheConfiguration()) {
            if (ccfg.getCacheStoreFactory() != null) {
                firstStore = (TestStore)ccfg.getCacheStoreFactory().create();

                break;
            }
        }

        assertNotNull(firstStore);

        Collection<String> evts = firstStore.events();

        try (Transaction tx = grid.transactions().txStart()) {
            IgniteCache<Object, Object> cacheA = grid.jcache("cacheA");
            IgniteCache<Object, Object> cacheB = grid.jcache("cacheB");

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
                "txEnd true"
            ),
            evts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncompatibleCaches1() throws Exception {
        IgniteEx grid = grid(0);

        try (Transaction ignored = grid.transactions().txStart()) {
            IgniteCache<Object, Object> cacheA = grid.jcache("cacheA");
            IgniteCache<Object, Object> cacheC = grid.jcache("cacheC");

            cacheA.put("1", "2");

            cacheC.put("1", "2");

            fail("Must not allow to enlist caches with different stores to one transaction");
        }
        catch (CacheException e) {
            assertTrue(e.getMessage().contains("Failed to enlist new cache to existing transaction"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncompatibleCaches2() throws Exception {
        IgniteEx grid = grid(0);

        try (Transaction ignored = grid.transactions().txStart()) {
            IgniteCache<Object, Object> cacheA = grid.jcache("cacheA");
            IgniteCache<Object, Object> cacheC = grid.jcache("cacheD");

            cacheA.put("1", "2");

            cacheC.put("1", "2");

            fail("Must not allow to enlist caches with different stores to one transaction");
        }
        catch (CacheException e) {
            assertTrue(e.getMessage().contains("Failed to enlist new cache to existing transaction"));
        }
    }

    /**
     * @param col1 Collection 1.
     * @param col2 Collection 2.
     */
    private static void assertEqualsCollections(Collection<?> col1, Collection<?> col2) {
        if (col1.size() != col2.size())
            fail("Collections are not equal:\nExpected:\t" + col1 + "\nActual:\t" + col2);

        Iterator<?> it1 = col1.iterator();
        Iterator<?> it2 = col2.iterator();

        int idx = 0;

        while (it1.hasNext()) {
            Object item1 = it1.next();
            Object item2 = it2.next();

            if (!F.eq(item1, item2))
                fail("Collections are not equal (position " + idx + "):\nExpected: " + col1 + "\nActual:   " + col2);

            idx++;
        }
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

        @Override public void txEnd(boolean commit) throws CacheWriterException {
            evts.offer("txEnd " + commit);
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
}
