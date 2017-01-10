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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Check that near cache is updated when entry loaded from store.
 */
public class GridNearCacheStoreUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private Ignite srv;

    /** */
    private Ignite client;

    /** */
    private IgniteCache<String, String> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("client"))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        srv = startGrid("server");
        client = startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If fail.
     */
    public void testAtomicUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration(), new NearCacheConfiguration<String, String>());

        checkNear(null, null);
    }

    /**
     * @throws Exception If fail.
     */
    public void testTransactionAtomicUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration(), new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws Exception If fail.
     */
    public void testPessimisticRepeatableReadUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws Exception If fail.
     */
    public void testPessimisticReadCommittedUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /**
     * @throws Exception If fail.
     */
    public void testOptimisticSerializableUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNear(TransactionConcurrency txConc, TransactionIsolation txIsolation) throws Exception {
        checkNearSingle(txConc, txIsolation);
        checkNearBatch(txConc, txIsolation);
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearSingle(TransactionConcurrency txConc, TransactionIsolation txIsolation) throws Exception {
        final String key = "key";

        boolean tx = txConc != null && txIsolation != null;

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Read from store.
                    assertEquals(key, cache.get(key));

                    return null;
                }
            });
        }
        else
            assertEquals(key, cache.get(key));


        final String updatedVal = "key_updated";

        if (tx) {
            doInTransaction(srv, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Update value.
                    srv.cache(CACHE_NAME).put(key, updatedVal);

                    return null;
                }
            });
        }
        else
            srv.cache(CACHE_NAME).put(key, updatedVal);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(updatedVal, cache.get(key));

                    return null;
                }
            });
        }
        else
            assertEquals(updatedVal, cache.get(key));

    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearBatch(TransactionConcurrency txConc, TransactionIsolation txIsolation) throws Exception {
        final Map<String, String> data1 = new HashMap<>();
        final Map<String, String> data2 = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            data1.put(String.valueOf(i), String.valueOf(i));
            data2.put(String.valueOf(i), "other");
        }

        boolean tx = txConc != null && txIsolation != null;

        // Read from store.
        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(data1, cache.getAll(data1.keySet()));

                    return null;
                }
            });
        }
        else
            assertEquals(data1, cache.getAll(data1.keySet()));

        // Update value.
        if (tx) {
            doInTransaction(srv, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    srv.cache(CACHE_NAME).putAll(data2);

                    return null;
                }
            });
        }
        else
            srv.cache(CACHE_NAME).putAll(data2);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(data2, cache.getAll(data2.keySet()));

                    return null;
                }
            });
        }
        else
            assertEquals(data2, cache.getAll(data2.keySet()));

    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<String, String> cacheConfiguration() {
        CacheConfiguration<String, String> cfg = new CacheConfiguration<>(CACHE_NAME);

        cfg.setCacheStoreFactory(new StoreFactory());

        cfg.setReadThrough(true);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return cfg;
    }

    /**
     *
     */
    private static class StoreFactory implements Factory<CacheStore<? super String, ? super String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public CacheStore<? super String, ? super String> create() {
            return new TestStore();
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<String, String> implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public String load(String key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends String, ? extends String> entry) throws CacheWriterException {
            System.out.println(entry);
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            System.out.println(key);
        }
    }
}
