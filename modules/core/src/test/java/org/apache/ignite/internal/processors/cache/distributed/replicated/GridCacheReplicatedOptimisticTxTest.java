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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
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
public class GridCacheReplicatedOptimisticTxTest extends GridCommonAbstractTest {
    /** Shared read/write-through store. */
    private static Map<Object, Object> storeMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>("tx")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
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
        startGrids(5);

        int key = primaryKey(grid(0).cache("tx"));

        {
            IgniteCache<Object, Object> cache = grid(0).cache("tx");

            cache.put(key, 1);

            cache.localClear(key);

            assertEquals(1, cache.get(key));
        }

        {
            IgniteCache<Object, Object> cache = grid(1).cache("tx");

            try (Transaction tx = grid(1).transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.get(key);

                cache.put(key, 2);

                tx.commit();
            }

            assertEquals(2, grid(1).cache("tx").get(key));

            assertEquals(2, grid(0).cache("tx").get(key));
        }
    }

    /** Shared cache read/write-through store factory. */
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