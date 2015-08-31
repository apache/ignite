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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CompletionListenerFuture;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test that entries are indexed on load/reload methods.
 */
public class IgniteCacheQueryLoadSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Puts count. */
    private static final int PUT_CNT = 10;

    /** Store map. */
    private static final Map<Integer, ValueObject> STORE_MAP = new HashMap<>();

    /** */
    public IgniteCacheQueryLoadSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(REPLICATED);
        ccfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setLoadPreviousValue(true);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setIndexedTypes(
            Integer.class, ValueObject.class
        );

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        jcache().removeAll();

        assert jcache().localSize() == 0;
        assert size(ValueObject.class) == 0;

        STORE_MAP.clear();
    }

    /**
     * Number of objects of given type in index.
     *
     * @param cls Value type.
     * @return Objects number.
     * @throws IgniteCheckedException If failed.
     */
    private long size(Class<?> cls) throws IgniteCheckedException {
        GridCacheQueryManager<Object, Object> qryMgr = ((IgniteKernal)grid()).internalCache().context().queries();

        assert qryMgr != null;

        return qryMgr.size(cls);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        IgniteCache<Integer, ValueObject> cache = grid().cache(null);

        cache.loadCache(null);

        assertEquals(PUT_CNT, cache.size());

        Collection<Cache.Entry<Integer, ValueObject>> res =
            cache.query(new SqlQuery(ValueObject.class, "val >= 0")).getAll();

        assertNotNull(res);
        assertEquals(PUT_CNT, res.size());
        assertEquals(PUT_CNT, size(ValueObject.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheAsync() throws Exception {
        IgniteCache<Integer, ValueObject> cache = grid().cache(null);

        IgniteCache<Integer, ValueObject> asyncCache = cache.withAsync();

        asyncCache.loadCache(null, 0);

        asyncCache.future().get();

        assert cache.size() == PUT_CNT;

        Collection<Cache.Entry<Integer, ValueObject>> res =
            cache.query(new SqlQuery(ValueObject.class, "val >= 0")).getAll();

        assert res != null;
        assert res.size() == PUT_CNT;
        assert size(ValueObject.class) == PUT_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheFiltered() throws Exception {
        IgniteCache<Integer, ValueObject> cache = grid().cache(null);

        cache.loadCache(new P2<Integer,ValueObject>() {
            @Override
            public boolean apply(Integer key, ValueObject val) {
                return key >= 5;
            }
        });

        assert cache.size() == PUT_CNT - 5;

        Collection<Cache.Entry<Integer, ValueObject>> res =
            cache.query(new SqlQuery(ValueObject.class, "val >= 0")).getAll();

        assert res != null;
        assert res.size() == PUT_CNT - 5;
        assert size(ValueObject.class) == PUT_CNT - 5;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheAsyncFiltered() throws Exception {
        IgniteCache<Integer, ValueObject> cache = grid().cache(null);

        IgniteCache<Integer, ValueObject> asyncCache = cache.withAsync();

        asyncCache.loadCache(new P2<Integer, ValueObject>() {
            @Override
            public boolean apply(Integer key, ValueObject val) {
                return key >= 5;
            }
        }, 0);

        asyncCache.future().get();

        assert cache.localSize() == PUT_CNT - 5;

        Collection<Cache.Entry<Integer, ValueObject>> res =
            cache.query(new SqlQuery(ValueObject.class, "val >= 0")).getAll();

        assert res != null;
        assert res.size() == PUT_CNT - 5;
        assert size(ValueObject.class) == PUT_CNT - 5;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReloadAsync() throws Exception {
        STORE_MAP.put(1, new ValueObject(1));

        IgniteCache<Integer, ValueObject> cache = jcache();

        IgniteCache<Integer, ValueObject> asyncCache = cache.withAsync();

        asyncCache.get(1);

        assert ((ValueObject)asyncCache.future().get()).value() == 1;

        assert cache.size() == 1;

        Collection<Cache.Entry<Integer, ValueObject>> res =
            cache.query(new SqlQuery(ValueObject.class, "val >= 0")).getAll();

        assert res != null;
        assert res.size() == 1;
        assert size(ValueObject.class) == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReloadAll() throws Exception {
        for (int i = 0; i < PUT_CNT; i++)
            STORE_MAP.put(i, new ValueObject(i));

        IgniteCache<Integer, ValueObject> cache = jcache();

        Integer[] keys = new Integer[PUT_CNT - 5];

        for (int i = 0; i < PUT_CNT - 5; i++)
            keys[i] = i + 5;

        CompletionListenerFuture fut = new CompletionListenerFuture();

        grid().<Integer, Integer>cache(null).loadAll(F.asSet(keys), true, fut);

        fut.get();

        assert cache.size() == PUT_CNT - 5;

        Collection<Cache.Entry<Integer, ValueObject>> res =
            cache.query(new SqlQuery(ValueObject.class, "val >= 0")).getAll();

        assert res != null;
        assert res.size() == PUT_CNT - 5;
        assert size(ValueObject.class) == PUT_CNT - 5;

        cache.clear();

        assert cache.size() == 0;
        assertEquals(0, cache.size());

        fut = new CompletionListenerFuture();

        grid().<Integer, Integer>cache(null).loadAll(F.asSet(keys), true, fut);

        fut.get();

        assertEquals(PUT_CNT - 5, cache.size());

        res = cache.query(new SqlQuery(ValueObject.class, "val >= 0")).getAll();

        assert res != null;
        assert res.size() == PUT_CNT - 5;
        assert size(ValueObject.class) == PUT_CNT - 5;
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter<Integer, ValueObject> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, ValueObject> clo, @Nullable Object... args) {
            assert clo != null;

            for (int i = 0; i < PUT_CNT; i++)
                clo.apply(i, new ValueObject(i));
        }

        /** {@inheritDoc} */
        @Override public ValueObject load(Integer key) {
            assert key != null;

            return STORE_MAP.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends ValueObject> e) {
            assert e != null;
            assert e.getKey() != null;
            assert e.getValue() != null;

            STORE_MAP.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            assert key != null;

            STORE_MAP.remove(key);
        }
    }

    /**
     * Value object class.
     */
    private static class ValueObject implements Serializable {
        /** Value. */
        @QuerySqlField
        private final int val;

        /**
         * @param val Value.
         */
        ValueObject(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ValueObject.class, this);
        }
    }
}