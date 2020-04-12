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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheTxRecoveryRollbackTest extends GridCommonAbstractTest {
    /** */
    private static ConcurrentHashMap<Object, Object> storeMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            for (Ignite node : G.allGrids()) {
                Collection<IgniteInternalTx> txs = ((IgniteKernal)node).context().cache().context().tm().activeTransactions();

                assertTrue("Unfinished txs [node=" + node.name() + ", txs=" + txs + ']', txs.isEmpty());
            }
        }
        finally {
            stopAllGrids();

            storeMap.clear();

            super.afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearTx1Implicit() throws Exception {
        nearTx1(null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearTx1Optimistic() throws Exception {
        nearTx1(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearTx1Pessimistic() throws Exception {
        nearTx1(PESSIMISTIC);
    }

    /**
     * Stop tx near node (client2), near cache tx on client1 is either committed
     * by primary or invalidated.
     *
     * @param concurrency Tx concurrency or {@code null} for implicit transaction.
     * @throws Exception If failed.
     */
    private void nearTx1(final TransactionConcurrency concurrency) throws Exception {
        startGrids(4);

        Ignite srv0 = grid(0);

        final IgniteCache<Integer, Integer> srvCache = srv0.createCache(cacheConfiguration(2, false, false));

        awaitPartitionMapExchange();

        final Ignite client1 = startClientGrid(4);
        final Ignite client2 = startClientGrid(5);

        final Integer key = primaryKey(srv0.cache(DEFAULT_CACHE_NAME));

        final IgniteCache<Integer, Integer> cache1 =
            client1.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        final IgniteCache<Integer, Integer> cache2 =
            client2.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        cache1.put(key, 1);

        final Integer newVal = 2;

        testSpi(client2).blockMessages(GridNearTxFinishRequest.class, srv0.name());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start put, concurrency: " + concurrency);

                if (concurrency != null) {
                    try (Transaction tx = client2.transactions().txStart(concurrency, REPEATABLE_READ)) {
                        cache2.put(key, newVal);

                        tx.commit();
                    }
                }
                else
                    cache2.put(key, newVal);

                return null;
            }
        });

        U.sleep(500);

        assertFalse(fut.isDone());

        testSpi(client2).waitForBlocked(GridNearTxFinishRequest.class, srv0.name());

        stopGrid(client2.name());

        try {
            fut.get();
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return newVal.equals(srvCache.get(key)) && newVal.equals(cache1.get(key));
            }
        }, 5000);

        checkData(F.asMap(key, newVal));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearTx2Implicit() throws Exception {
        nearTx2(null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearTx2Optimistic() throws Exception {
        nearTx2(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearTx2Pessimistic() throws Exception {
        nearTx2(PESSIMISTIC);
    }

    /**
     * Stop both tx near node (client2) and primary node, near cache tx on client1 is invalidated.
     *
     * @param concurrency Tx concurrency or {@code null} for implicit transaction.
     * @throws Exception If failed.
     */
    private void nearTx2(final TransactionConcurrency concurrency) throws Exception {
        startGrids(4);

        Ignite srv0 = grid(0);

        srv0.createCache(cacheConfiguration(2, false, false));

        awaitPartitionMapExchange();

        final Ignite client1 = startClientGrid(4);
        final Ignite client2 = startClientGrid(5);

        final Integer key = primaryKey(srv0.cache(DEFAULT_CACHE_NAME));

        final IgniteCache<Integer, Integer> cache1 =
            client1.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        final IgniteCache<Integer, Integer> cache2 =
            client2.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        cache1.put(key, 1);

        final Integer newVal = 2;

        testSpi(client2).blockMessages(GridNearTxFinishRequest.class, srv0.name());

        testSpi(srv0).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg instanceof GridDhtTxFinishRequest;
            }
        });

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start put, concurrency: " + concurrency);

                if (concurrency != null) {
                    try (Transaction tx = client2.transactions().txStart(concurrency, REPEATABLE_READ)) {
                        cache2.put(key, newVal);

                        tx.commit();
                    }
                }
                else
                    cache2.put(key, newVal);

                return null;
            }
        });

        U.sleep(500);

        assertFalse(fut.isDone());

        testSpi(client2).waitForBlocked(GridNearTxFinishRequest.class, srv0.name());

        stopGrid(client2.name());
        stopGrid(srv0.name());

        try {
            fut.get();
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }

        final IgniteCache<Integer, Integer> srvCache = grid(1).cache(DEFAULT_CACHE_NAME);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return newVal.equals(srvCache.get(key)) && newVal.equals(cache1.get(key));
            }
        }, 5000);

        checkData(F.asMap(key, newVal));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxWithStoreImplicit() throws Exception {
        txWithStore(null, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxWithStoreOptimistic() throws Exception {
        txWithStore(OPTIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxWithStorePessimistic() throws Exception {
        txWithStore(PESSIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxWithStoreNoWriteThroughImplicit() throws Exception {
        txWithStore(null, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxWithStoreNoWriteThroughOptimistic() throws Exception {
        txWithStore(OPTIMISTIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxWithStoreNoWriteThroughPessimistic() throws Exception {
        txWithStore(PESSIMISTIC, false);
    }

    /**
     * @param concurrency Tx concurrency or {@code null} for implicit transaction.
     * @param writeThrough Store write through flag.
     * @throws Exception If failed.
     */
    private void txWithStore(final TransactionConcurrency concurrency, boolean writeThrough) throws Exception {
        startGrids(4);

        Ignite srv0 = grid(0);

        IgniteCache<Integer, Integer> srv0Cache = srv0.createCache(cacheConfiguration(1, true, writeThrough));

        awaitPartitionMapExchange();

        final Integer key = primaryKey(srv0Cache);

        srv0Cache.put(key, 1);

        Ignite client = startClientGrid(4);

        testSpi(srv0).blockMessages(GridNearTxPrepareResponse.class, client.name());

        final IgniteCache<Integer, Integer> clientCache = client.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start put");

                clientCache.put(key, 2);

                return null;
            }
        });

        U.sleep(500);

        assertFalse(fut.isDone());

        testSpi(srv0).waitForBlocked(GridNearTxPrepareResponse.class, client.name());

        stopGrid(client.name());

        try {
            fut.get();
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }

        U.sleep(1000);

        if (writeThrough)
            checkData(F.asMap(key, 1));
        else
            checkData(F.asMap(key, 2));
    }

    /**
     * @param node Node.
     * @return Node communication SPI.
     */
    private TestRecordingCommunicationSpi testSpi(Ignite node) {
        return (TestRecordingCommunicationSpi)node.configuration().getCommunicationSpi();
    }

    /**
     * @param backups Number of backups.
     * @param store Cache store flag.
     * @param writeThrough Store write through flag.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(int backups, boolean store, boolean writeThrough) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);
        ccfg.setRebalanceMode(ASYNC);

        if (store) {
            ccfg.setWriteThrough(writeThrough);

            ccfg.setCacheStoreFactory(new TestStoreFactory());
        }

        return ccfg;
    }

    /**
     * @param expData Expected cache data.
     */
    private void checkData(Map<Integer, Integer> expData) {
        assert !expData.isEmpty();

        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

            for (Map.Entry<Integer, Integer> e : expData.entrySet()) {
                assertEquals("Invalid value [key=" + e.getKey() + ", node=" + node.name() + ']',
                    e.getValue(),
                    cache.get(e.getKey()));
            }
        }
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    log.info("Store write [key=" + entry.getKey() + ", val=" + entry.getValue() + ']');

                    storeMap.put(entry.getKey(), entry.getValue());
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    log.info("Store delete [key=" + key + ']');

                    storeMap.remove(key);
                }
            };
        }
    }
}
