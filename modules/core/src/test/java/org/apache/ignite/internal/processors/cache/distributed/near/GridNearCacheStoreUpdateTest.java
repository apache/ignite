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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
        checkNearSingleConcurrent(txConc, txIsolation);
        checkNearBatch(txConc, txIsolation);
        checkNearBatchConcurrent(txConc, txIsolation);
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearSingle(TransactionConcurrency txConc, TransactionIsolation txIsolation) throws Exception {
        final String key = "key";

        boolean tx = txConc != null && txIsolation != null;

        final IgniteCache<String, String> clientCache = this.cache;
        final IgniteCache<String, String> srvCache = srv.<String, String>cache(CACHE_NAME);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Read from store.
                    assertEquals(key, clientCache.get(key));

                    return null;
                }
            });
        }
        else
            assertEquals(key, clientCache.get(key));

        final String updatedVal = "key_updated";

        if (tx) {
            doInTransaction(srv, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Update value.
                    srvCache.put(key, updatedVal);

                    return null;
                }
            });
        }
        else
            srvCache.put(key, updatedVal);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(updatedVal, clientCache.get(key));

                    return null;
                }
            });
        }
        else
            assertEquals(updatedVal, clientCache.get(key));
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearSingleConcurrent(final TransactionConcurrency txConc, final TransactionIsolation txIsolation) throws Exception {
        for (int i = 0; i < 10; i++) {
            final String key = String.valueOf(-((new Random().nextInt(99) + 1)));

            boolean tx = txConc != null && txIsolation != null;

            final IgniteCache<String, String> clientCache = this.cache;
            final IgniteCache<String, String> srvCache = srv.cache(CACHE_NAME);

            final CountDownLatch storeLatch = new CountDownLatch(1);

            final IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    storeLatch.await();

                    clientCache.get(key);

                    return null;
                }
            });


//            IgniteInternalFuture<Object> fut2 = null;

            // TODO Sometimes Near cache becomes inconsistent
//            if (!tx) {
//                // TODO: IGNITE-3498
//                // TODO: Doesn't work on transactional cache.
//                fut2 = GridTestUtils.runAsync(new Callable<Object>() {
//                    @Override public Object call() throws Exception {
//                        storeLatch.await();
//
//                        srvCache.remove(key);
//
//                        return null;
//                    }
//                });
//            }

            final IgniteInternalFuture<Object> fut3 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    storeLatch.await();

                    srvCache.put(key, "other");

                    return null;
                }
            });

            storeLatch.countDown();

            fut1.get();

//            if (!tx)
//                fut2.get();

            fut3.get();

            final String srvVal = srvCache.get(key);
            final String clientVal = clientCache.get(key);

            assertEquals(srvVal, clientVal);
        }
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

        final IgniteCache<String, String> clientCache = this.cache;
        final IgniteCache<String, String> srvCache = srv.cache(CACHE_NAME);

        boolean tx = txConc != null && txIsolation != null;

        // Read from store.
        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(data1, clientCache.getAll(data1.keySet()));

                    return null;
                }
            });
        }
        else
            assertEquals(data1, clientCache.getAll(data1.keySet()));

        // Update value.
        if (tx) {
            doInTransaction(srv, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    srvCache.putAll(data2);

                    return null;
                }
            });
        }
        else
            srvCache.putAll(data2);

        if (tx) {
            doInTransaction(client, txConc, txIsolation, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertEquals(data2, clientCache.getAll(data2.keySet()));

                    return null;
                }
            });
        }
        else
            assertEquals(data2, clientCache.getAll(data2.keySet()));
    }

    /**
     * @param txConc Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @throws Exception If fail.
     */
    private void checkNearBatchConcurrent(TransactionConcurrency txConc, TransactionIsolation txIsolation)
        throws Exception {
        final Map<String, String> data1 = new HashMap<>();
        final Map<String, String> data2 = new HashMap<>();

        for (int j = 0; j < 10; j++) {
            data1.clear();
            data2.clear();

            for (int i = j * 10; i < j * 10 + 10; i++) {
                data1.put(String.valueOf(i), String.valueOf(i));
                data2.put(String.valueOf(i), "other");
            }

            final IgniteCache<String, String> clientCache = this.cache;
            final IgniteCache<String, String> srvCache = srv.cache(CACHE_NAME);

            boolean tx = txConc != null && txIsolation != null;

            final CountDownLatch latch = new CountDownLatch(1);

            final IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    latch.await();

                    clientCache.getAll(data1.keySet());

                    return null;
                }
            });

            IgniteInternalFuture<Object> fut2 =  GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    latch.await();

                    srvCache.putAll(data2);

                    return null;
                }
            });

//            IgniteInternalFuture<Object> fut3 = null;
//
//            // TODO Sometimes Near cache becomes inconsistent
//            if (!tx) {
//                // TODO: IGNITE-3498
//                // TODO: Doesn't work on transactional cache.
//                fut3 = GridTestUtils.runAsync(new Callable<Object>() {
//                    @Override public Object call() throws Exception {
//                        latch.await();
//
//                        srvCache.removeAll(data1.keySet());
//
//                        return null;
//                    }
//                });
//            }

            latch.countDown();

//            if (!tx)
//                fut3.get();

            fut1.get();
            fut2.get();

            final Map<String, String> srvVals = srvCache.getAll(data1.keySet());
            final Map<String, String> clientVals = clientCache.getAll(data1.keySet());

            assertEquals(srvVals, clientVals);
        }
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration<String, String> cacheConfiguration() {
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
        private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public TestStore() {
            for (int i = -100; i < 1000; i++)
                map.put(String.valueOf(i), String.valueOf(i));

            map.put("key", "key");
        }

        /** {@inheritDoc} */
        @Override public String load(String key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends String, ? extends String> entry) throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SuspiciousMethodCalls")
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }
    }
}
