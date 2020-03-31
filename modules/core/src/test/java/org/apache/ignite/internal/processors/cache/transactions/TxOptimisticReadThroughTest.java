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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Test for optimistic tx with read/through cache.
 */
public class TxOptimisticReadThroughTest extends GridCommonAbstractTest {
    /** Test nodes count. */
    protected static final int NODE_CNT = 5;

    /** Shared read/write-through store. */
    private static final Map<Object, Object> storeMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>("tx")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheStoreFactory(new TestStoreFactory())
            .setReadThrough(true)
            .setWriteThrough(true)
        );

        cfg.setUserAttributes(Collections.singletonMap(
            IgniteNodeAttributes.ATTR_MACS_OVERRIDE, UUID.randomUUID().toString()));

        return cfg;
    }

    /** Check optimistic transaction synchronizes value version. */
    @Test
    public void testReplicatedOptimistic() throws Exception {
        startGrids(NODE_CNT);

        final IgniteCache<Object, Object> cache0 = grid(0).cache("tx");

        final IgniteCache<Object, Object> cache1 = grid(1).cache("tx");

        final int key = primaryKey(cache0);

        cache0.put(key, key);

        cache0.put(key+1, key + 1);

        cache0.localClear(key);

        assertEquals(1, cache0.get(key));

        try (Transaction tx = grid(1).transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
            cache1.getAll(Stream.of(key, key + 1, key + 2).collect(Collectors.toSet()));

            cache1.put(key, key + 1);

            tx.commit();
        }

        for (int i = 0; i < NODE_CNT; ++i)
            assertEquals(key + 1, grid(i).cache("tx").get(key));
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
