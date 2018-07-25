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

import java.io.Closeable;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/** */
public class CacheCloseableResourcesCleanupTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** */
    private static final String DFLT_CACHE = "cache1";

    /**
     * List of resources created by factories.
     */
    private static final Collection<CloseableResource> rsrcs = new ConcurrentLinkedDeque<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (grid(0).cache(DFLT_CACHE) != null)
            grid(0).cache(DFLT_CACHE).destroy();

        rsrcs.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheWriterCleanup() throws Exception {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DFLT_CACHE);

        ccfg.setCacheWriterFactory(factoryOf(new CloseableCacheWriter<>()));
        ccfg.setWriteThrough(true);

        checkResourcesCleanup(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheLoaderCleanup() throws Exception {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DFLT_CACHE);

        ccfg.setCacheLoaderFactory(factoryOf(new CloseableCacheLoader<>()));

        checkResourcesCleanup(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheStoreCleanup() throws Exception {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DFLT_CACHE);

        ccfg.setCacheStoreFactory(factoryOf(new CloseableCacheStore<>()));

        checkResourcesCleanup(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheStoreSessionListenerCleanup() throws Exception {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DFLT_CACHE);

        CloseableCacheStoreSessionListener lsnr1 = new CloseableCacheStoreSessionListener();
        CloseableCacheStoreSessionListener lsnr2 = new CloseableCacheStoreSessionListener();

        ccfg.setCacheStoreSessionListenerFactories(factoryOf(lsnr1), factoryOf(lsnr2));

        checkResourcesCleanup(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictPolicyCleanup() throws Exception {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DFLT_CACHE);

        ccfg.setEvictionPolicyFactory(factoryOf(new CloseableEvictionPolicy<>()));
        ccfg.setOnheapCacheEnabled(true);

        checkResourcesCleanup(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJCacheQueryListenerCleanup() throws Exception {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DFLT_CACHE);

        MutableCacheEntryListenerConfiguration<Integer, String> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            factoryOf(new CloseableCacheEntryListener<>()), factoryOf(new CloseableRemoteFilter<>()), true, true);

        ccfg.addCacheEntryListenerConfiguration(lsnrCfg);

        checkResourcesCleanup(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousQueryRemoteFilterCleanup() throws Exception {
        checkResourcesCleanup(new CacheConfiguration<>(DFLT_CACHE), cache -> {
            ContinuousQueryWithTransformer<Integer, String, ?> qry = new ContinuousQueryWithTransformer<>();

            qry.setLocalListener(evts -> {});
            qry.setRemoteFilterFactory(factoryOf(new CloseableRemoteFilter<>()));
            qry.setRemoteTransformerFactory(factoryOf(new CloseableTransformer<>()));

            assertEquals(0, rsrcs.size());

            cache.query(qry);
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousQueryRemoteFilterCleanupClient() throws Exception {
        Ignition.setClientMode(true);

        startGrid(NODES_CNT);

        Ignition.setClientMode(false);

        try {
            testContinuousQueryRemoteFilterCleanup();
        } finally {
            stopGrid(NODES_CNT);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testContinuousQueryRemoteFilterCleanupWithCursor() throws Exception {
        checkResourcesCleanup(new CacheConfiguration<>(DFLT_CACHE), cache -> {
            ContinuousQueryWithTransformer<Integer, String, ?> qry = new ContinuousQueryWithTransformer<>();

            qry.setLocalListener(evts -> {});
            qry.setRemoteFilterFactory(factoryOf(new CloseableRemoteFilter<>()));
            qry.setRemoteTransformerFactory(factoryOf(new CloseableTransformer<>()));

            assertEquals(0, rsrcs.size());

            try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                // No-op.
            }

            for (CloseableResource obj : rsrcs)
                assertTrue("Not closed resource: " + obj, obj.closed());
        });
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkResourcesCleanup(CacheConfiguration<Integer, String> ccfg) throws Exception {
        checkResourcesCleanup(ccfg, cache -> {
            cache.put(1, "1");
            cache.put(2, "2");
        });
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private <K, V> void checkResourcesCleanup(CacheConfiguration<K, V> ccfg, CI1<IgniteCache<K, V>> c)
        throws Exception {
        assertEquals(0, rsrcs.size());

        int idx = G.allGrids().size() > NODES_CNT ? NODES_CNT : 0;

        Ignite node = grid(idx);

        assertTrue(idx == 0 || node.cluster().localNode().isClient());

        IgniteCache<K, V> cache = node.createCache(ccfg);

        c.apply(cache);

        cache.destroy();

        awaitPartitionMapExchange();

        assertTrue("No objects was created.", rsrcs.size() > 0);

        for (CloseableResource obj : rsrcs)
            assertTrue("Not closed resource: " + obj, obj.closed());
    }

    /** */
    private <T extends CloseableResource> Factory<T> factoryOf(T obj) {
        return new SingletonFactory<>(obj);
    }

    /** */
    private static class SingletonFactory<T extends CloseableResource> implements Factory<T> {
        /** */
        private final T instance;

        /** */
        private SingletonFactory(T instance) {
            this.instance = instance;
        }

        /** {@inheritDoc} */
        @Override public T create() {
            rsrcs.add(instance);

            return instance;
        }
    }

    /** */
    private static abstract class CloseableResource implements Closeable, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private volatile boolean closed;

        /** {@inheritDoc} */
        @Override public void close() {
            assertFalse("Resource was already closed!", closed);

            closed = true;
        }

        /**
         * @return Close counter.
         */
        boolean closed() {
            return closed;
        }
    }

    /** */
    private static class CloseableCacheWriter<K, V> extends CloseableResource implements CacheWriter<K, V> {
        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection keys) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection coll) {
            // No-op.
        }
    }

    /** */
    private static class CloseableCacheLoader<k, V> extends CloseableResource implements CacheLoader<k, V> {
        /** {@inheritDoc} */
        @Override public V load(k key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<k, V> loadAll(Iterable<? extends k> keys) {
            return null;
        }
    }

    /** */
    private static class CloseableEvictionPolicy<K, V> extends CloseableResource implements EvictionPolicy<K, V> {
        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
            // No-op.
        }
    }

    /** */
    private static class CloseableCacheStore<k, V> extends CloseableResource implements CacheStore<k, V> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<k, V> clo, @Nullable Object... args) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public V load(k key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<k, V> loadAll(Iterable<? extends k> keys) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends k, ? extends V> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(
            Collection<Cache.Entry<? extends k, ? extends V>> entries) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            // No-op.
        }
    }

    /** */
    private static class CloseableCacheStoreSessionListener extends CloseableResource implements CacheStoreSessionListener {
        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
            // No-op.
        }
    }

    /** */
    private static class CloseableRemoteFilter<K, V> extends CloseableResource implements CacheEntryEventFilter<K, V> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent ignore) {
            return true;
        }
    }

    /** */
    private static class CloseableTransformer<K, V, T> extends CloseableResource
        implements IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, T> {
        /** {@inheritDoc} */
        @Override public T apply(CacheEntryEvent<? extends K, ? extends V> evt) {
            return null;
        }
    }

    /** */
    public static class CloseableCacheEntryListener<K, V> extends CloseableResource
        implements CacheEntryCreatedListener<K, V> {

        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> evts) {
            // No-op.
        }
    }
}
