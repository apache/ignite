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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.*;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_TX_COLLISIONS_INTERVAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** Tests tx key contention detection functional. */
public class TxWithKeyContentionSelfTest extends GridCommonAbstractTest {
    /** Client flag. */
    private boolean client;

    /** Near cache flag. */
    private boolean nearCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("NODE_" + name.substring(name.length() - 1));

        if (client)
            cfg.setClientMode(true);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(20 * 1024 * 1024)
                )
        );

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setCacheConfiguration(getCacheConfiguration(DEFAULT_CACHE_NAME));

        if (client) {
            cfg.setConsistentId("Client");

            cfg.setClientMode(client);
        }

        return cfg;
    }

    /** */
    protected CacheConfiguration<?, ?> getCacheConfiguration(String name) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(name)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setBackups(2)
            .setStatisticsEnabled(true);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests transactional payload.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticRepeatableReadCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * Tests transactional payload with near cache enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticRepeatableReadCheckContentionTxMetricNear() throws Exception {
        nearCache = true;

        runKeyCollisionsMetric(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticReadCommitedCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticReadCommitedCheckContentionTxMetricNear() throws Exception {
        nearCache = true;

        runKeyCollisionsMetric(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticReadCommittedCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticReadCommittedCheckContentionTxMetricNear() throws Exception {
        nearCache = true;

        runKeyCollisionsMetric(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticRepeatableReadCheckContentionTxMetric() throws Exception {
        runKeyCollisionsMetric(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticRepeatableReadCheckContentionTxMetricNear() throws Exception {
        nearCache = true;

        runKeyCollisionsMetric(OPTIMISTIC, REPEATABLE_READ);
    }

    private AtomicInteger startTransactions(Ignite clientIgnite, IgniteCache<Integer, Integer> cache, Integer key, int txCount,
                                            TransactionConcurrency concurrency, TransactionIsolation isolation) throws InterruptedException {
        // Latch to synchronize the start of transactions
        CountDownLatch startLatch = new CountDownLatch(1);

        // Latch to wait for all transactions to complete
        CountDownLatch endLatch = new CountDownLatch(txCount);

        // Executor service to run transactions in parallel
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        // Atomic integer to count transaction collisions
        AtomicInteger collisionCount = new AtomicInteger(0);

        for (int i = 0; i < txCount; i++) {
            executor.submit(() -> {
                try {
                    // Wait until all threads are ready
                    startLatch.await();

                    // Start a transaction
                    try (Transaction tx = clientIgnite.transactions().txStart(concurrency, isolation)) {
                        // Read the value to acquire the lock
                        cache.get(key);

                        // Simulate some processing time
                        Thread.sleep(50);

                        // Commit the transaction
                        tx.commit();
                    }
                } catch (TransactionTimeoutException | TransactionDeadlockException e) {
                    // Increment collision count if a collision occurs
                    collisionCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // Signal that this transaction is complete
                    endLatch.countDown();
                }
            });
        }

        // Release all threads to start transactions simultaneously
        startLatch.countDown();

        // Wait for all transactions to complete
        endLatch.await();

        // Shutdown the executor service
        executor.shutdown();

        return collisionCount;
    }


    /** Tests metric correct results while tx collisions occured.
     *
     * @param concurrency Concurrency level.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    private void runKeyCollisionsMetric(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        // Start 3 server nodes
        Ignite ignite = startGridsMultiThreaded(3);

        // Activate the cluster
        ignite.cluster().state(ClusterState.ACTIVE);

        // Start a client node
        client = true;
        Ignite clientIgnite = startGrid("client");

        // Get the cache from the client node
        IgniteCache<Integer, Integer> cache = clientIgnite.cache(DEFAULT_CACHE_NAME);

        // Select a primary key for the cache
        final Integer key = primaryKey(ignite.cache(DEFAULT_CACHE_NAME));

        // Define the number of transactions to simulate contention
        int txCount = 100;

//        // Start transactions to create contention
        AtomicInteger collisionCount = startTransactions(clientIgnite, cache, key, txCount, concurrency, isolation);
//
//        // Retrieve the transaction metrics
//        TransactionMetrics metrics = ignite.context().cache().context().tm().metrics();
//
//        // Get the number of key collisions from the metrics
//        long keyCollisions = metrics.getTxKeyCollisions();
//        //Assert that the collisions recorded match the expected count
//        assertTrue("Expected key collisions to be greater than zero", keyCollisions > 0);
//
//        // Optionally, compare with the local collision count
//        assertEquals("Mismatch in collision count", collisionCount.get(), keyCollisions);


        IgniteTxManager srvTxMgr = grid(0).context().cache().context().tm();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    U.invoke(IgniteTxManager.class, srvTxMgr, "collectTxCollisionsInfo");
                }
                catch (IgniteCheckedException e) {
                    fail(e.toString());
                }

                CacheMetrics metrics = grid(0).cache(DEFAULT_CACHE_NAME).localMetrics();

                String coll1 = metrics.getTxKeyCollisions();

                System.out.println("COL!!!!!!!!! = " + coll1);

                if (!coll1.isEmpty()) {
                    String coll2 = metrics.getTxKeyCollisions();

                    // check idempotent
                    assertEquals(coll1, coll2);

                    assertTrue(coll1.contains("queueSize"));

                    return true;
                }
                else
                    return false;
            }
        }, 10_000));

//        // Получаем метрику
//        CacheMetrics metrics = cache.localMetrics();
//
//        String keyCollisions = metrics.getTxKeyCollisions();

//        System.out.println("Количество коллизий ключей: " + keyCollisions);

        //    Проверяем, что коллизии произошли
        //assertTrue("Ожидались коллизии ключей", keyCollisions > 0);


//        Ignite ig = startGridsMultiThreaded(3);
//
//        int contCnt = (int)U.staticField(IgniteTxManager.class, "COLLISIONS_QUEUE_THRESHOLD") * 20;
//
//        CountDownLatch txLatch = new CountDownLatch(contCnt);
//
//        ig.cluster().state(ClusterState.ACTIVE);
//
//        client = true;
//
//        Ignite cl = startGrid();
//
//        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);
//
//        final Integer keyId = primaryKey(cache);
//
//        GridCompoundFuture<?, ?> finishFut = new GridCompoundFuture<>();
//
//        for (int i = 0; i < contCnt; ++i) {
//            IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
//                try (Transaction tx = cl.transactions().txStart(concurrency, isolation)) {
//                    cache.put(keyId, 0);
//
//                    tx.commit();
//
//                    txLatch.countDown();
//                }
//            });
//
//            finishFut.add(f0);
//        }
//
//        finishFut.markInitialized();
//
//        IgniteTxManager srvTxMgr = ((IgniteEx)ig).context().cache().context().tm();
//
//        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
//            @Override public boolean apply() {
//                try {
//                    U.invoke(IgniteTxManager.class, srvTxMgr, "collectTxCollisionsInfo");
//                }
//                catch (IgniteCheckedException e) {
//                    fail(e.toString());
//                }
//
//                CacheMetrics metrics = ig.cache(DEFAULT_CACHE_NAME).localMetrics();
//
//                String coll1 = metrics.getTxKeyCollisions();
//
//                if (!coll1.isEmpty()) {
//                    String coll2 = metrics.getTxKeyCollisions();
//
//                    // check idempotent
//                    assertEquals(coll1, coll2);
//
//                    assertTrue(coll1.contains("queueSize"));
//
//                    return true;
//                }
//                else
//                    return false;
//            }
//        }, 10_000));
//
//        finishFut.get();
//
//        txLatch.await();
    }
}
