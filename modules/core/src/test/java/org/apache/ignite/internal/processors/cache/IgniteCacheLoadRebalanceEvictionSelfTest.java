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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheLoadRebalanceEvictionSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int LRU_MAX_SIZE = 10;

    /** */
    private static final int ENTRIES_CNT = 10000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        LruEvictionPolicy evictionPolicy = new LruEvictionPolicy<>();
        evictionPolicy.setMaxSize(LRU_MAX_SIZE);

        CacheConfiguration<String, byte[]> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setReadFromBackup(true);
        cacheCfg.setEvictionPolicy(evictionPolicy);
        cacheCfg.setOnheapCacheEnabled(true);
        cacheCfg.setStatisticsEnabled(true);

        cacheCfg.setWriteThrough(false);
        cacheCfg.setReadThrough(false);

        cacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(new Storage()));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartRebalancing() throws Exception {
        List<IgniteInternalFuture<Object>> futs = new ArrayList<>();

        int gridCnt = 4;

        for (int i = 0; i < gridCnt; i++) {
            final IgniteEx ig = startGrid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ig.cache(DEFAULT_CACHE_NAME).localLoadCache(null);

                    return null;
                }
            }));
        }

        try {
            for (IgniteInternalFuture<Object> fut : futs)
                fut.get();

            for (int i = 0; i < gridCnt; i++) {
                IgniteEx grid = grid(i);

                final IgniteCache<Object, Object> cache = grid.cache(DEFAULT_CACHE_NAME);

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        return cache.localSize(CachePeekMode.ONHEAP) <= 10;
                    }
                }, getTestTimeout());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class Storage extends CacheStoreAdapter<Integer, byte[]> implements Serializable {
        /** */
        private static final byte[] data = new byte[1024];

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends byte[]> e) throws CacheWriterException {
            throw new UnsupportedOperationException("Unsupported");
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends byte[]>> entries)
            throws CacheWriterException {
            throw new UnsupportedOperationException("Unsupported");
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            throw new UnsupportedOperationException("Unsupported");
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            throw new UnsupportedOperationException("Unsupported");
        }

        /** {@inheritDoc} */
        @Override public byte[] load(Integer key) throws CacheLoaderException {
            return data;
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, byte[]> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, byte[]> res = new HashMap<>();

            for (Integer key : keys)
                res.put(key, data);

            return res;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, byte[]> clo,
            @Nullable Object... args) throws CacheLoaderException {

            for (int i = 0; i < ENTRIES_CNT; i++)
                clo.apply(i, data);
        }
    }
}
