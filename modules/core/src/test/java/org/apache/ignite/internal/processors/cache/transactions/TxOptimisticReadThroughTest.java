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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests optimistic tx with read/through cache.
 */
public class TxOptimisticReadThroughTest extends GridCommonAbstractTest {
    /** Test nodes count. */
    protected static final int NODE_CNT = 2;

    /** Shared read/write-through store. */
    private static final Map<Object, Object> storeMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Different adresses not to pickup only first node when searching value wihit transaction.
        cfg.setUserAttributes(Collections.singletonMap(
            IgniteNodeAttributes.ATTR_MACS_OVERRIDE, UUID.randomUUID().toString()));

        return cfg;
    }

    /**
     *  Checks optimistic serializable transaction asks primary node for actual value version when read-through is
     *  enabled for replicated transactional cache.
     */
    @Test
    public void testReplicated() throws Exception {
        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);

        checkOptimisticSerializableTransaction(cacheCfg, false);
    }

    /**
     *  Checks optimistic serializable transaction asks primary node for actual value version when read-through is
     *  enabled for partitioned transactional cache.
     */
    @Test
    public void testPartitioned() throws Exception {
        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);

        checkOptimisticSerializableTransaction(cacheCfg, false);
    }

    /**
     *  Checks optimistic serializable transaction asks primary node for actual value version when read-through is
     *  enabled for near partitioned transactional cache.
     */
    @Test
    public void testNearPartitioned() throws Exception {
        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);

        checkOptimisticSerializableTransaction(cacheCfg, true);
    }

    /**
     *  Checks optimistic serializable transaction asks primary node for actual value version when read-through is
     *  enabled for near replicated transactional cache.
     */
    @Test
    public void testNearReplicated() throws Exception {
        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);

        checkOptimisticSerializableTransaction(cacheCfg, true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /**
     *  Checks optimistic serializable transaction asks primary node for actual value version when read-throug is
     *  enabled for a transactional cache.
     *
     * @param cacheCfg Transactional cache configuration for the testing cache.
     * @param near {@code True} to check transaction with near cache from a client node. {@code False} for server node.
     */
    private void checkOptimisticSerializableTransaction(CacheConfiguration<Object, Object> cacheCfg, boolean near)
        throws Exception {
        startGrids(NODE_CNT);

        final IgniteCache<Object, Object> cache0 = grid(0).getOrCreateCache(cacheCfg);

        final IgniteCache<Object, Object> txCache;
        final IgniteEx txGrid;

        if (near) {
            txGrid = startClientGrid();

            txCache = txGrid.createNearCache(cacheCfg.getName(), new NearCacheConfiguration<>());
        }
        else {
            txGrid = grid(1);

            txCache = grid(1).cache(cacheCfg.getName());
        }

        List<Integer> primaryKeys = primaryKeys(cache0, 3);

        primaryKeys.forEach(k -> cache0.put(k, k));

        primaryKeys.forEach(k -> cache0.localClear(k));

        primaryKeys.forEach(k -> assertEquals(k, cache0.get(k)));

        try (Transaction tx = txGrid.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
            // Force requesting value from primary node by one key.
            txCache.get(primaryKeys.get(0));

            // Force requesting value from primary node by keys batch.
            txCache.getAll(new HashSet<>(primaryKeys.subList(1, primaryKeys.size())));

            primaryKeys.forEach(k -> txCache.put(k, k + 1));

            tx.commit();
        }

        for (int i = 0; i < NODE_CNT; ++i) {
            IgniteCache<Object, Object> cache = grid(i).cache(cacheCfg.getName());

            primaryKeys.forEach(k -> assertEquals(k + 1, cache.get(k)));
        }
    }

    /** @return Default configuration of a transactional cache cache. */
    private static CacheConfiguration<Object, Object> cacheConfiguration() {
        return new CacheConfiguration<>("tx")
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheStoreFactory(new TestStoreFactory())
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(1)
            .setReadThrough(true)
            .setWriteThrough(true);
    }

    /** Shared read/write-through store factory. */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter<Object, Object>() {
                /** {@inheritDoc} */
                @Override public Object load(Object key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                /** {@inheritDoc} */
                @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
                    storeMap.put(entry.getKey(), entry.getValue());
                }

                /** {@inheritDoc} */
                @Override public void delete(Object key) throws CacheWriterException {
                    storeMap.remove(key);
                }
            };
        }
    }
}
