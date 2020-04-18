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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Query cursor spliterator called multiple times without triggering IgniteException("Iterator is already fetched or query was cancelled.")
 */
public class CacheQueryMultipleSpliteratorCallsTest extends GridCommonAbstractTest
    implements Serializable {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    @NotNull
    protected CacheConfiguration<Integer, String> cacheConfiguration(
        CacheAtomicityMode cacheAtomicityMode,
        CacheMode cacheMode) {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(1);
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testCacheScanQuerySpliteratorMultipleCalls() throws IgniteException {
        Ignite client = grid(0);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        ScanQuery<Object, Object> qry = new ScanQuery<>(new IgniteBiPredicate<Object, Object>() {
            /** {@inheritDoc} */
            @Override public boolean apply(Object key, Object val) {
                return key != null;
            }
        });

        try (QueryCursor<?> cur = cache.query(qry)) {
            cur.iterator();
            cur.spliterator();

            GridTestUtils.assertThrows(log, IgniteException.class, "Iterator is already fetched or query was cancelled.", cur, "iterator");
        }
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testCacheContinuousQuerySpliteratorMultipleCalls() throws IgniteException {
        Ignite client = grid(0);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>()
                .setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Object, Object>() {
                    /** {@inheritDoc} */
                    @Override public boolean apply(Object key, Object val) {
                        return key != null;
                    }
                }))
                .setAutoUnsubscribe(true)
                .setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                    /** {@inheritDoc} */
                    @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> iterable) throws CacheEntryListenerException {
                    }
                });

        try(QueryCursor<?> cur = cache.query(qry)) {
            cur.iterator();
            cur.spliterator();

            GridTestUtils.assertThrows(log, IgniteException.class, "Iterator is already fetched or query was cancelled.", cur, "iterator");
        }
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testCacheSqlQuerySpliteratorMultipleCalls() throws IgniteException {
        Ignite client = grid(0);

        IgniteCache<Object, String> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        try(QueryCursor<?> cur = cache.query(new SqlQuery<>("String", "from String"))) {
            cur.iterator();
            cur.spliterator();

            GridTestUtils.assertThrows(log, IgniteException.class, "Iterator is already fetched or query was cancelled.", cur, "iterator");
        }
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testCacheSqlFieldQuerySpliteratorMultipleCalls() throws IgniteException {
        Ignite client = grid(0);

        IgniteCache<Object, String> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        try(QueryCursor<?> cur = cache.query(new SqlFieldsQuery("select _key from String"))) {
            cur.iterator();
            cur.spliterator();

            GridTestUtils.assertThrows(log, IgniteException.class, "Iterator is already fetched or query was cancelled.", cur, "iterator");
        }
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testCacheTextQuerySpliteratorMultipleCalls() throws IgniteException {
        Ignite client = grid(0);

        IgniteCache<Object, String> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        try(QueryCursor<?> cur = cache.query(new TextQuery<>("String", "1"))) {
            cur.iterator();
            cur.spliterator();

            GridTestUtils.assertThrows(log, IgniteException.class, "Iterator is already fetched or query was cancelled.", cur, "iterator");
        }
    }


}
