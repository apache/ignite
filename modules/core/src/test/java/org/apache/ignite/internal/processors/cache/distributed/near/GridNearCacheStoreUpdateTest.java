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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
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

        checkNear(null, null, false);
    }

    /**
     * @throws Exception If fail.
     */
    public void testTransactionAtomicUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration(), new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If fail.
     */
    public void testPessimisticRepeatableReadUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If fail.
     */
    public void testPessimisticReadCommittedUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, false);
    }

    /**
     * @throws Exception If fail.
     */
    public void testOptimisticSerializableUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, false);
    }

    /**
     * @throws Exception If fail.
     */
    public void testAsyncAtomicUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration(), new NearCacheConfiguration<String, String>());

        checkNear(null, null, true);
    }

    /**
     * @throws Exception If fail.
     */
    public void testAsyncTransactionAtomicUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration(), new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If fail.
     */
    public void testAsyncPessimisticRepeatableReadUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If fail.
     */
    public void testAsyncPessimisticReadCommittedUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, true);
    }

    /**
     * @throws Exception If fail.
     */
    public void testAsyncOptimisticSerializableUpdateNear() throws Exception {
        cache = client.createCache(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new NearCacheConfiguration<String, String>());

        checkNear(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, true);
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNear(TransactionConcurrency txConc, TransactionIsolation txIsolation, boolean async) throws Exception {
        checkNearSingle(txConc, txIsolation, async);
        checkNearSingleConcurrent(txConc, txIsolation, async);
        checkNearBatch(txConc, txIsolation, async);
        checkNearBatchConcurrent(txConc, txIsolation, async);
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearSingle(TransactionConcurrency txConc, TransactionIsolation txIsolation, final boolean async) throws Exception {
        final String key = "key";

        boolean tx = txConc != null && txIsolation != null;

        final IgniteCache<String, String> clientCache = async ? this.cache.withAsync() : this.cache;
        final IgniteCache<String, String> srvCache = async ? srv.<String, String>cache(CACHE_NAME).withAsync()
            : srv.<String, String>cache(CACHE_NAME);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Read from store.
                    assertEquals(key, getValue(clientCache, key, async));

                    return null;
                }
            });
        }
        else
            assertEquals(key, getValue(clientCache, key, async));

        final String updatedVal = "key_updated";

        if (tx) {
            doInTransaction(srv, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Update value.
                    putValue(srvCache, key, updatedVal, async);

                    return null;
                }
            });
        }
        else
            putValue(srvCache, key, updatedVal, async);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(updatedVal, getValue(clientCache, key, async));

                    return null;
                }
            });
        }
        else
            assertEquals(updatedVal, getValue(clientCache, key, async));
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearSingleConcurrent(final TransactionConcurrency txConc, final TransactionIsolation txIsolation,
        final boolean async) throws Exception {
        for (int i = 0; i < 10; i++) {
            final String key = String.valueOf(new Random().nextInt());

            boolean tx = txConc != null && txIsolation != null;

            final IgniteCache<String, String> clientCache = async ? this.cache.withAsync() : this.cache;
            final IgniteCache<String, String> srvCache = async ? srv.<String, String>cache(CACHE_NAME).withAsync()
                : srv.<String, String>cache(CACHE_NAME);

            final CountDownLatch storeLatch = new CountDownLatch(1);

            final AtomicReference<String> val = new AtomicReference<>();

            final IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    storeLatch.await();

                    val.set(getValue(clientCache, key, async));

                    System.out.println("== get store");

                    return null;
                }
            });


            IgniteInternalFuture<Object> fut2 = null;

            if (!tx) {
                // Doesn't work on transactional cache.
                fut2 = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        storeLatch.await();

                        removeValue(async, key, srvCache);

                        return null;
                    }
                });
            }

            final IgniteInternalFuture<Object> fut3 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    storeLatch.await();

                    putValue(srvCache, key, "other", async);

                    return null;
                }
            });

            storeLatch.countDown();

            fut1.get();

            if (!tx)
                fut2.get();

            fut3.get();

            final String srvVal = getValue(srvCache, key, async);
            final String clientVal = getValue(clientCache, key, async);

            assertEquals(srvVal, clientVal);
        }
    }

    private static void removeValue(final boolean async, final String key, final IgniteCache<String, String> cache) {
        cache.remove(key);

        if (async)
            cache.future().get();
    }

    private void putValue(final IgniteCache<String, String> cache, final String key, final String val,
        final boolean async) {
        cache.put(key, val);

        if (async)
            cache.future().get();
    }

    private static String getValue(final IgniteCache<String, String> cache, final String key, final boolean async) {
        String res = cache.get(key);

        if (async)
            res = cache.<String>future().get();
        return res;
    }

    private static Map<String, String> getValues(final IgniteCache<String, String> cache, final Set<String> keys, final boolean async) {
        Map<String, String> res = cache.getAll(keys);

        if (async)
            res = cache.<Map<String, String>>future().get();

        return res;
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearBatch(TransactionConcurrency txConc, TransactionIsolation txIsolation, final boolean async) throws Exception {
        final Map<String, String> data1 = new HashMap<>();
        final Map<String, String> data2 = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            data1.put(String.valueOf(i), String.valueOf(i));
            data2.put(String.valueOf(i), "other");
        }

        final IgniteCache<String, String> clientCache = async ? this.cache.withAsync() : this.cache;
        final IgniteCache<String, String> srvCache = async ? srv.<String, String>cache(CACHE_NAME).withAsync()
            : srv.<String, String>cache(CACHE_NAME);

        boolean tx = txConc != null && txIsolation != null;

        // Read from store.
        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(data1, getValues(clientCache, data1.keySet(), async));

                    return null;
                }
            });
        }
        else
            assertEquals(data1, getValues(clientCache, data1.keySet(), async));

        // Update value.
        if (tx) {
            doInTransaction(srv, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    putValues(srvCache, data2, async);

                    return null;
                }
            });
        }
        else
            putValues(srvCache, data2, async);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(data2, getValues(clientCache, data2.keySet(), async));

                    return null;
                }
            });
        }
        else
            assertEquals(data2, getValues(clientCache, data2.keySet(), async));
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearBatchConcurrent(TransactionConcurrency txConc, TransactionIsolation txIsolation,
        final boolean async) throws Exception {
        final Map<String, String> data1 = new HashMap<>();
        final Map<String, String> data2 = new HashMap<>();

        for (int j = 0; j < 10; j++) {
            data1.clear();
            data2.clear();

            for (int i = j * 10; i < j * 10 + 10; i++) {
                data1.put(String.valueOf(i), String.valueOf(i));
                data2.put(String.valueOf(i), "other");
            }

            final IgniteCache<String, String> clientCache = async ? this.cache.withAsync() : this.cache;
            final IgniteCache<String, String> srvCache = async ? srv.<String, String>cache(CACHE_NAME).withAsync()
                : srv.<String, String>cache(CACHE_NAME);

            boolean tx = txConc != null && txIsolation != null;

            CountDownLatch latch = new CountDownLatch(1);

            final IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    latch.await();

                    getValues(clientCache, data1.keySet(), async);

                    return null;
                }
            });

            IgniteInternalFuture<Object> fut2 =  GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    latch.await();

                    putValues(srvCache, data2, async);

                    return null;
                }
            });

            IgniteInternalFuture<Object> fut3 = null;

            if (!tx) {
                // Doesn't work on transactional cache.
                fut3 = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        latch.await();

                        removeValues(srvCache, data1.keySet(), async);

                        return null;
                    }
                });
            }

            latch.countDown();

            if (!tx)
                fut3.get();

            fut1.get();
            fut2.get();

            final Map<String, String> srvVals = getValues(srvCache, data1.keySet(), async);
            final Map<String, String> clientVals = getValues(clientCache, data1.keySet(), async);

            assertEquals(srvVals, clientVals);
        }
    }

    private static void putValues(final IgniteCache<String, String> cache, final Map<String, String> data,
        final boolean async) {
        cache.putAll(data);

        if (async)
            cache.future().get();
    }

    private static void removeValues(IgniteCache<String, String> cache, Set<String> keys, boolean async) {
        cache.removeAll(keys);

        if (async)
            cache.future().get();
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
