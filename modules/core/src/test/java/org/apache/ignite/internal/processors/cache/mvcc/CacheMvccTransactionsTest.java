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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.TestCacheNodeExcludingFilter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * TODO IGNITE-3478: extend tests to use single/mutiple nodes, all tx types.
 * TODO IGNITE-3478: test with cache groups.
 */
@SuppressWarnings("unchecked")
public class CacheMvccTransactionsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int DFLT_PARTITION_COUNT = RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

    /** */
    private static final long DFLT_TEST_TIME = 30_000;

    /** */
    private static final int SRVS = 4;

    /** */
    private boolean client;

    /** */
    private boolean testSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMvccEnabled(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DFLT_TEST_TIME + 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            verifyCoordinatorInternalState();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTx1() throws Exception {
        checkPessimisticTx(new CI1<IgniteCache<Integer, Integer>>() {
            @Override public void apply(IgniteCache<Integer, Integer> cache) {
                try {
                    IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                    List<Integer> keys = testKeys(cache);

                    for (Integer key : keys) {
                        log.info("Test key: " + key);

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Integer val = cache.get(key);

                            assertNull(val);

                            cache.put(key, key);

                            val = cache.get(key);

                            assertEquals(key, val);

                            tx.commit();
                        }

                        Integer val = cache.get(key);

                        assertEquals(key, val);
                    }
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTx2() throws Exception {
        checkPessimisticTx(new CI1<IgniteCache<Integer, Integer>>() {
            @Override public void apply(IgniteCache<Integer, Integer> cache) {
                try {
                    IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                    List<Integer> keys = testKeys(cache);

                    for (Integer key : keys) {
                        log.info("Test key: " + key);

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache.put(key, key);
                            cache.put(key + 1, key + 1);

                            assertEquals(key, cache.get(key));
                            assertEquals(key + 1, (Object)cache.get(key + 1));

                            tx.commit();
                        }

                        assertEquals(key, cache.get(key));
                        assertEquals(key + 1, (Object)cache.get(key + 1));
                    }
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     * @param c Closure to run.
     * @throws Exception If failed.
     */
    private void checkPessimisticTx(IgniteInClosure<IgniteCache<Integer, Integer>> c) throws Exception {
        startGridsMultiThreaded(SRVS);

        try {
            for (CacheConfiguration<Object, Object> ccfg : cacheConfigurations()) {
                logCacheInfo(ccfg);

                ignite(0).createCache(ccfg);

                try {
                    Ignite node = ignite(0);

                    IgniteCache<Integer, Integer> cache = node.cache(ccfg.getName());

                    c.apply(cache);
                }
                finally {
                    ignite(0).destroyCache(ccfg.getName());
                }
            }

            verifyCoordinatorInternalState();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll1() throws Exception {
        startGridsMultiThreaded(SRVS);

        try {
            client = true;

            Ignite ignite = startGrid(SRVS);

            CacheConfiguration ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, 512);

            IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

            Set<Integer> keys = new HashSet<>();

            keys.addAll(primaryKeys(ignite(0).cache(ccfg.getName()), 2));

            Map<Integer, Integer> res = cache.getAll(keys);

            verifyCoordinatorInternalState();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimplePutGetAll() throws Exception {
        Ignite node = startGrid(0);

        IgniteTransactions txs = node.transactions();

        final IgniteCache<Object, Object> cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        final int KEYS = 10_000;

        Set<Integer> keys = new HashSet<>();

        for (int k = 0; k < KEYS; k++)
            keys.add(k);

        Map<Object, Object> map = cache.getAll(keys);

        assertTrue(map.isEmpty());

        for (int v = 0; v < 3; v++) {
            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int k = 0; k < KEYS; k++) {
                    if (k % 2 == 0)
                        cache.put(k, v);
                }

                tx.commit();
            }

            map = cache.getAll(keys);

            for (int k = 0; k < KEYS; k++) {
                if (k % 2 == 0)
                    assertEquals(v, map.get(k));
                else
                    assertNull(map.get(k));
            }

            assertEquals(KEYS / 2, map.size());

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                map = cache.getAll(keys);

                for (int k = 0; k < KEYS; k++) {
                    if (k % 2 == 0)
                        assertEquals(v, map.get(k));
                    else
                        assertNull(map.get(k));
                }

                assertEquals(KEYS / 2, map.size());

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreadUpdatesAreVisibleForThisThread() throws Exception {
        final Ignite ignite = startGrid(0);

        final IgniteCache<Object, Object> cache = ignite.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        final int THREADS = Runtime.getRuntime().availableProcessors() * 2;

        final int KEYS = 10;

        final CyclicBarrier b = new CyclicBarrier(THREADS);

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                try {
                    int min = idx * KEYS;
                    int max = min + KEYS;

                    Set<Integer> keys = new HashSet<>();

                    for (int k = min; k < max; k++)
                        keys.add(k);

                    b.await();

                    for (int i = 0; i < 100; i++) {
                        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            for (int k = min; k < max; k++)
                                cache.put(k, i);

                            tx.commit();
                        }

                        Map<Object, Object> res = cache.getAll(keys);

                        for (Integer key : keys)
                            assertEquals(i, res.get(key));

                        assertEquals(KEYS, res.size());
                    }
                }
                catch (Exception e) {
                    error("Unexpected error: " + e, e);

                    fail("Unexpected error: " + e);
                }
            }
        }, THREADS, "test-thread");
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitPreviousTxAck() throws Exception {
        testSpi = true;

        startGrid(0);

        client = true;

        final Ignite ignite = startGrid(1);

        final IgniteCache<Object, Object> cache =
            ignite.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 16));

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 1);
            cache.put(2, 1);
            cache.put(3, 1);

            tx.commit();
        }

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(ignite);

        clientSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            /** */
            boolean block = true;

            @Override public boolean apply(ClusterNode node, Message msg) {
                if (block && msg instanceof CoordinatorTxAckRequest) {
                    block = false;

                    return true;
                }

                return false;
            }
        });

        IgniteInternalFuture<?> txFut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(2, 2);
                    cache.put(3, 2);

                    tx.commit();
                }

                return null;
            }
        });

        IgniteInternalFuture<?> txFut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(1, 3);
                    cache.put(2, 3);

                    tx.commit();
                }

                // Should see changes mady by both tx1 and tx2.
                Map<Object, Object> res = cache.getAll(F.asSet(1, 2, 3));

                assertEquals(3, res.get(1));
                assertEquals(3, res.get(2));
                assertEquals(2, res.get(3));

                return null;
            }
        });

        clientSpi.waitForBlocked();

        Thread.sleep(1000);

        clientSpi.stopBlock(true);

        txFut1.get();
        txFut2.get();

        Map<Object, Object> res = cache.getAll(F.asSet(1, 2, 3));

        assertEquals(3, res.get(1));
        assertEquals(3, res.get(2));
        assertEquals(2, res.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartialCommitResultNoVisible() throws Exception {
        testSpi = true;

        startGrids(2);

        client = true;

        final Ignite ignite = startGrid(3);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache =
            ignite.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 16));

        final Integer key1 = primaryKey(ignite(0).cache(cache.getName()));
        final Integer key2 = primaryKey(ignite(1).cache(cache.getName()));

        info("Test keys [key1=" + key1 + ", key2=" + key2 + ']');

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key1, 1);
            cache.put(key2, 1);

            tx.commit();
        }

        Integer val = 1;

        // Allow finish update for key1 and block update for key2.

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(ignite);
        TestRecordingCommunicationSpi srvSpi = TestRecordingCommunicationSpi.spi(ignite(0));

        for (int i = 0; i < 10; i++) {
            info("Iteration: " + i);

            clientSpi.blockMessages(GridNearTxFinishRequest.class, getTestIgniteInstanceName(1));

            srvSpi.record(GridNearTxFinishResponse.class);

            final Integer newVal = val + 1;

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.put(key1, newVal);
                        cache.put(key2, newVal);

                        tx.commit();
                    }

                    return null;
                }
            });

            try {
                srvSpi.waitForRecorded();

                srvSpi.recordedMessages(true);

                assertFalse(fut.isDone());

                if (i % 2 == 1) {
                    // Execute one more update to increase counter.
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.put(1000_0000, 1);

                        tx.commit();
                    }
                }

                Set<Integer> keys = new HashSet<>();

                keys.add(key1);
                keys.add(key2);

                Map<Object, Object> res;

                res = cache.getAll(keys);

                assertEquals(val, res.get(key1));
                assertEquals(val, res.get(key2));

                res = new HashMap<>();

                for (IgniteCache.Entry<Object, Object> e : cache) {
                    if (key1.equals(e.getKey()) || key2.equals(e.getKey())) {
                        Object old = res.put(e.getKey(), e.getValue());

                        assertNull(old);
                    }
                }

                assertEquals(val, res.get(key1));
                assertEquals(val, res.get(key2));

                clientSpi.stopBlock(true);

                fut.get();

                res = cache.getAll(keys);

                assertEquals(newVal, res.get(key1));
                assertEquals(newVal, res.get(key2));

                res = new HashMap<>();

                for (IgniteCache.Entry<Object, Object> e : cache) {
                    if (key1.equals(e.getKey()) || key2.equals(e.getKey())) {
                        Object old = res.put(e.getKey(), e.getValue());

                        assertNull(old);
                    }
                }

                assertEquals(newVal, res.get(key1));
                assertEquals(newVal, res.get(key2));

                val = newVal;
            }
            finally {
                clientSpi.stopBlock(true);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCleanupWaitsForGet1() throws Exception {
        boolean vals[] = {true, false};

        for (boolean otherPuts : vals) {
            for (boolean putOnStart : vals) {
                cleanupWaitsForGet1(otherPuts, putOnStart);

                afterTest();
            }
        }
    }

    /**
     * @param otherPuts {@code True} to update unrelated keys to increment mvcc counter.
     * @param putOnStart {@code True} to put data in cache before getAll.
     * @throws Exception If failed.
     */
    private void cleanupWaitsForGet1(boolean otherPuts, final boolean putOnStart) throws Exception {
        info("cleanupWaitsForGet [otherPuts=" + otherPuts + ", putOnStart=" + putOnStart + "]");

        testSpi = true;

        client = false;

        final Ignite srv = startGrid(0);

        client = true;

        final Ignite client = startGrid(1);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> srvCache =
            srv.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 16));

        final Integer key1 = 1;
        final Integer key2 = 2;

        if (putOnStart) {
            try (Transaction tx = srv.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                srvCache.put(key1, 0);
                srvCache.put(key2, 0);

                tx.commit();
            }
        }

        if (otherPuts) {
            for (int i = 0; i < 3; i++) {
                try (Transaction tx = srv.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    srvCache.put(1_000_000 + i, 99);

                    tx.commit();
                }
            }
        }

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);

        clientSpi.blockMessages(GridNearGetRequest.class, getTestIgniteInstanceName(0));

        IgniteInternalFuture<?> getFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache<Integer, Integer> cache = client.cache(srvCache.getName());

                Map<Integer, Integer> vals = cache.getAll(F.asSet(key1, key2));

                if (putOnStart) {
                    assertEquals(2, vals.size());
                    assertEquals(0, (Object)vals.get(key1));
                    assertEquals(0, (Object)vals.get(key2));
                }
                else
                    assertEquals(0, vals.size());

                return null;
            }
        }, "get-thread");

        clientSpi.waitForBlocked();

        for (int i = 0; i < 5; i++) {
            try (Transaction tx = srv.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                srvCache.put(key1, i + 1);
                srvCache.put(key2, i + 1);

                tx.commit();
            }
        }

        clientSpi.stopBlock(true);

        getFut.get();

        IgniteCache<Integer, Integer> cache = client.cache(srvCache.getName());

        Map<Integer, Integer> vals = cache.getAll(F.asSet(key1, key2));

        assertEquals(2, vals.size());
        assertEquals(5, (Object)vals.get(key1));
        assertEquals(5, (Object)vals.get(key2));
    }



    /**
     * @throws Exception If failed.
     */
    public void testCleanupWaitsForGet2() throws Exception {
        /*
        Simulate case when there are two active transactions modifying the same key
        (it is possible if key lock is released but ack message is delayed), and at this moment
        query is started.
         */
        testSpi = true;

        client = false;

        startGrids(2);

        client = true;

        final Ignite client = startGrid(2);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache = client.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 16).
            setNodeFilter(new TestCacheNodeExcludingFilter(ignite(0).name())));

        final Integer key1 = 1;
        final Integer key2 = 2;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key1, 0);
            cache.put(key2, 0);

            tx.commit();
        }

        TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(grid(0));

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);

        final CountDownLatch getLatch = new CountDownLatch(1);

        clientSpi.closure(new IgniteBiInClosure<ClusterNode, Message>() {
            @Override public void apply(ClusterNode node, Message msg) {
                if (msg instanceof CoordinatorTxAckRequest)
                    doSleep(2000);
            }
        });

        crdSpi.closure(new IgniteBiInClosure<ClusterNode, Message>() {
            /** */
            private AtomicInteger cntr = new AtomicInteger();

            @Override public void apply(ClusterNode node, Message msg) {
                if (msg instanceof MvccCoordinatorVersionResponse) {
                    if (cntr.incrementAndGet() == 2) {
                        getLatch.countDown();

                        doSleep(1000);
                    }
                }
            }
        });

        final IgniteInternalFuture<?> putFut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key1, 1);

                    tx.commit();
                }

                return null;
            }
        }, "put1");

        final IgniteInternalFuture<?> putFut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key1, 2);

                    tx.commit();
                }

                return null;
            }
        }, "put2");

        IgniteInternalFuture<?> getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                U.await(getLatch);

                while (!putFut1.isDone() || !putFut2.isDone()) {
                    Map<Object, Object> vals = cache.getAll(F.asSet(key1, key2));

                    assertEquals(2, vals.size());
                }

                return null;
            }
        }, 4, "get-thread");

        putFut1.get();
        putFut2.get();
        getFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode() throws Exception {
        putAllGetAll(1, 0, 0, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode_SinglePartition() throws Exception {
        putAllGetAll(1, 0, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0() throws Exception {
        putAllGetAll(4, 2, 0, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1() throws Exception {
        putAllGetAll(4, 2, 1, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups2() throws Exception {
        putAllGetAll(4, 2, 2, 64);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @throws Exception If failed.
     */
    private void putAllGetAll(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts
    ) throws Exception
    {
        final int RANGE = 20;

        final int writers = 4;

        final int readers = 4;

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
            @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int min = idx * RANGE;
                int max = min + RANGE;

                info("Thread range [min=" + min + ", max=" + max + ']');

                Map<Integer, Integer> map = new HashMap<>();

                int v = idx * 1_000_000;

                boolean updated = false;

                while (!stop.get()) {
                    while (map.size() < RANGE)
                        map.put(rnd.nextInt(min, max), v);

                    IgniteCache<Integer, Integer> cache = randomCache(caches, rnd);

                    IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        if (updated && rnd.nextBoolean()) {
                            Map<Integer, Integer> res = cache.getAll(map.keySet());

                            for (Integer k : map.keySet())
                                assertEquals(v - 1, (Object)res.get(k));
                        }

                        cache.putAll(map);

                        tx.commit();

                        updated = true;
                    }

                    if (rnd.nextBoolean()) {
                        Map<Integer, Integer> res = cache.getAll(map.keySet());

                        for (Integer k : map.keySet())
                            assertEquals(v, (Object)res.get(k));
                    }

                    map.clear();

                    v++;
                }

                info("Writer done, updates: " + v);
            }
        };

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    Map<Integer, Integer> readVals = new HashMap<>();

                    while (!stop.get()) {
                        int range = rnd.nextInt(0, writers);

                        int min = range * RANGE;
                        int max = min + RANGE;

                        while (keys.size() < RANGE)
                            keys.add(rnd.nextInt(min, max));

                        IgniteCache<Integer, Integer> cache = randomCache(caches, rnd);

                        Map<Integer, Integer> map = cache.getAll(keys);

                        assertTrue("Invalid map size: " + map.size(),
                            map.isEmpty() || map.size() == RANGE);

                        Integer val0 = null;

                        for (Map.Entry<Integer, Integer> e: map.entrySet()) {
                            Integer val = e.getValue();

                            assertNotNull(val);

                            if (val0 == null) {
                                Integer readVal = readVals.get(range);

                                if (readVal != null)
                                    assertTrue(readVal <= val);

                                readVals.put(range, val);

                                val0 = val;
                            }
                            else {
                                if (!F.eq(val0, val)) {
                                    assertEquals("Unexpected value [range=" + range + ", key=" + e.getKey() + ']',
                                        val0,
                                        val);
                                }
                            }
                        }

                        keys.clear();
                    }
                }
            };

        readWriteTest(srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            DFLT_TEST_TIME,
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_SingleNode() throws Exception {
        accountsTxGetAll(1, 0, 0, 64, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_SingleNode_SinglePartition() throws Exception {
        accountsTxGetAll(1, 0, 0, 1, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups0() throws Exception {
        accountsTxGetAll(4, 2, 0, 64, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups1() throws Exception {
        accountsTxGetAll(4, 2, 1, 64, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups2() throws Exception {
        accountsTxGetAll(4, 2, 2, 64, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_SingleNode_SinglePartition() throws Exception {
        accountsTxGetAll(1, 0, 0, 1, ReadMode.SCAN);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param readMode Read mode.
     * @throws Exception If failed.
     */
    private void accountsTxGetAll(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        final ReadMode readMode
    )
        throws Exception
    {
        final int ACCOUNTS = 20;

        final int ACCOUNT_START_VAL = 1000;

        final int writers = 4;

        final int readers = 4;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                Map<Integer, MvccTestAccount> accounts = new HashMap<>();

                for (int i = 0; i < ACCOUNTS; i++)
                    accounts.put(i, new MvccTestAccount(ACCOUNT_START_VAL, 1));

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.putAll(accounts);

                    tx.commit();
                }
            }
        };

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        IgniteCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);
                        IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                        cnt++;

                        Integer id1 = rnd.nextInt(ACCOUNTS);
                        Integer id2 = rnd.nextInt(ACCOUNTS);

                        while (id1.equals(id2))
                            id2 = rnd.nextInt(ACCOUNTS);

                        TreeSet<Integer> keys = new TreeSet<>();

                        keys.add(id1);
                        keys.add(id2);

                        Integer cntr1;
                        Integer cntr2;

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            MvccTestAccount a1;
                            MvccTestAccount a2;

                            Map<Integer, MvccTestAccount> accounts = cache.getAll(keys);

                            a1 = accounts.get(id1);
                            a2 = accounts.get(id2);

                            assertNotNull(a1);
                            assertNotNull(a2);

                            cntr1 = a1.updateCnt + 1;
                            cntr2 = a2.updateCnt + 1;

                            cache.put(id1, new MvccTestAccount(a1.val + 1, cntr1));
                            cache.put(id2, new MvccTestAccount(a2.val - 1, cntr2));

                            tx.commit();
                        }

                        Map<Integer, MvccTestAccount> accounts = cache.getAll(keys);

                        MvccTestAccount a1 = accounts.get(id1);
                        MvccTestAccount a2 = accounts.get(id2);

                        assertNotNull(a1);
                        assertNotNull(a2);

                        assertTrue(a1.updateCnt >= cntr1);
                        assertTrue(a2.updateCnt >= cntr2);
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    Map<Integer, Integer> lastUpdateCntrs = new HashMap<>();

                    while (!stop.get()) {
                        while (keys.size() < ACCOUNTS)
                            keys.add(rnd.nextInt(ACCOUNTS));

                        IgniteCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);

                        Map<Integer, MvccTestAccount> accounts;

                        if (readMode == ReadMode.SCAN) {
                            accounts = new HashMap<>();

                            for (IgniteCache.Entry<Integer, MvccTestAccount> e : cache) {
                                MvccTestAccount old = accounts.put(e.getKey(), e.getValue());

                                assertNull(old);
                            }
                        }
                        else
                            accounts = cache.getAll(keys);

                        assertEquals(ACCOUNTS, accounts.size());

                        int sum = 0;

                        for (int i = 0; i < ACCOUNTS; i++) {
                            MvccTestAccount account = accounts.get(i);

                            assertNotNull(account);

                            sum += account.val;

                            Integer cntr = lastUpdateCntrs.get(i);

                            if (cntr != null)
                                assertTrue(cntr <= account.updateCnt);

                            lastUpdateCntrs.put(i, cntr);
                        }

                        assertEquals(ACCOUNTS * ACCOUNT_START_VAL, sum);
                    }

                    if (idx == 0) {
                        IgniteCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);

                        Map<Integer, MvccTestAccount> accounts = cache.getAll(keys);

                        int sum = 0;

                        for (int i = 0; i < ACCOUNTS; i++) {
                            MvccTestAccount account = accounts.get(i);

                            info("Account [id=" + i + ", val=" + account.val + ']');

                            sum += account.val;
                        }

                        info("Sum: " + sum);
                    }
                }
            };

        readWriteTest(srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            DFLT_TEST_TIME,
            init,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_SingleNode_SinglePartition() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 1, 0, 0, 1, 10_000);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_SingleNode() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 1, 0, 0, 64, 10_000);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceConsistency_SingleNode() throws Exception {
        operationsSequenceConsistency(1, 0, 0, 64);
    }

    /**
     * TODO IGNITE-3478: enable when scan is fully implemented.
     *
     * @throws Exception If failed
     */
//    public void testOperationsSequenceConsistency_ClientServer_Backups0() throws Exception {
//        operationsSequenceConsistency(4, 2, 0, 64);
//    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @throws Exception If failed.
     */
    private void operationsSequenceConsistency(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts
    )
        throws Exception
    {
        final int writers = 4;

        final int readers = 4;

        final long time = 10_000;

        final AtomicInteger keyCntr = new AtomicInteger();

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        IgniteCache<Integer, Value> cache = randomCache(caches, rnd);
                        IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                        Integer key = keyCntr.incrementAndGet();

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache.put(key, new Value(idx, cnt++));

                            tx.commit();
                        }

                        if (key > 1_000_000)
                            break;
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        IgniteCache<Integer, Value> cache = randomCache(caches, rnd);

                        Map<Integer, TreeSet<Integer>> vals = new HashMap<>();

                        for (IgniteCache.Entry<Integer, Value> e : cache) {
                            Value val = e.getValue();

                            assertNotNull(val);

                            TreeSet<Integer> cntrs = vals.get(val.key);

                            if (cntrs == null)
                                vals.put(val.key, cntrs = new TreeSet<>());

                            boolean add = cntrs.add(val.cnt);

                            assertTrue(add);
                        }

                        for (TreeSet<Integer> readCntrs : vals.values()) {
                            for (int i = 0; i < readCntrs.size(); i++)
                                assertTrue(readCntrs.contains(i));
                        }
                    }
                }
            };

        readWriteTest(srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            time,
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActiveQueryCleanupOnNodeFailure() throws Exception {
        testSpi = true;

        final Ignite srv = startGrid(0);

        srv.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1024));

        client = true;

        final Ignite client = startGrid(1);

        TestRecordingCommunicationSpi srvSpi = TestRecordingCommunicationSpi.spi(srv);

        srvSpi.blockMessages(GridNearGetResponse.class, getTestIgniteInstanceName(1));

        TestRecordingCommunicationSpi.spi(client).blockMessages(CoordinatorQueryAckRequest.class,
            getTestIgniteInstanceName(0));

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);

                cache.getAll(F.asSet(1, 2, 3));

                return null;
            }
        });

        srvSpi.waitForBlocked();

        assertFalse(fut.isDone());

        stopGrid(1);

        verifyCoordinatorInternalState();

        try {
            fut.get();
        }
        catch (Exception ignore) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalance1() throws Exception {
        Ignite srv0 = startGrid(0);

        IgniteCache<Integer, Integer> cache =  (IgniteCache)srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT));

        Map<Integer, Integer> map;
        Map<Integer, Integer> resMap;

        try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            map = new HashMap<>();

            for (int i = 0; i < DFLT_PARTITION_COUNT * 3; i++)
                map.put(i, i);

            cache.putAll(map);

            tx.commit();
        }

        startGrid(1);

        awaitPartitionMapExchange();

        resMap = cache.getAll(map.keySet());

        assertEquals(map.size(), resMap.size());

        for (int i = 0; i < map.size(); i++)
            assertEquals(i, (Object)resMap.get(i));

        try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < DFLT_PARTITION_COUNT * 3; i++)
                map.put(i, i + 1);

            cache.putAll(map);

            tx.commit();
        }
        try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < DFLT_PARTITION_COUNT * 3; i++)
                map.put(i, i + 2);

            cache.putAll(map);

            tx.commit();
        }

        startGrid(2);

        awaitPartitionMapExchange();

        resMap = cache.getAll(map.keySet());

        assertEquals(map.size(), map.size());

        for (int i = 0; i < map.size(); i++)
            assertEquals(i + 2, (Object)resMap.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCoordinatorFailurePessimisticTx() throws Exception {
        testSpi = true;

        startGrids(3);

        client = true;

        final Ignite client = startGrid(3);

        final IgniteCache cache = client.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT));

        final Integer key1 = primaryKey(jcache(1));
        final Integer key2 = primaryKey(jcache(2));

        TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(ignite(0));

        crdSpi.blockMessages(MvccCoordinatorVersionResponse.class, client.name());

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                try {
                    try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.put(key1, 1);
                        cache.put(key2, 2);

                        tx.commit();
                    }

                    fail();
                }
                catch (ClusterTopologyException e) {
                    info("Expected exception: " + e);
                }

                return null;
            }
        }, "tx-thread");

        crdSpi.waitForBlocked();

        stopGrid(0);

        fut.get();

        assertNull(cache.get(key1));
        assertNull(cache.get(key2));

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key1, 1);
            cache.put(key2, 2);

            tx.commit();
        }

        assertEquals(1, cache.get(key1));
        assertEquals(2, cache.get(key2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadInProgressCoordinatorFails() throws Exception {
        testSpi = true;

        startGrids(4);

        client = true;

        final Ignite client = startGrid(4);

        final IgniteCache cache = client.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setNodeFilter(new TestCacheNodeExcludingFilter(getTestIgniteInstanceName(0), getTestIgniteInstanceName(1))));

        final Set<Integer> keys = new HashSet<>();

        List<Integer> keys1 = primaryKeys(jcache(2), 10);

        keys.addAll(keys1);
        keys.addAll(primaryKeys(jcache(3), 10));

        Map<Integer, Integer> vals = new HashMap();

        for (Integer key : keys)
            vals.put(key, -1);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.putAll(vals);

            tx.commit();
        }

        final TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);

        clientSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg instanceof GridNearGetRequest;
            }
        });

        IgniteInternalFuture getFut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                Map<Integer, Integer> res = cache.getAll(keys);

                assertEquals(20, res.size());

                Integer val = null;

                for (Integer val0 : res.values()) {
                    assertNotNull(val0);

                    if (val == null)
                        val = val0;
                    else
                        assertEquals(val, val0);
                }

                return null;
            }
        }, "get-thread");

        clientSpi.waitForBlocked();

        final IgniteInternalFuture releaseWaitFut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                Thread.sleep(3000);

                clientSpi.stopBlock(true);

                return null;
            }
        }, "get-thread");

        stopGrid(0);

        for (int i = 0; i < 10; i++) {
            vals = new HashMap();

            for (Integer key : keys)
                vals.put(key, i);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.putAll(vals);

                tx.commit();
            }
        }

        releaseWaitFut.get();
        getFut.get();
    }

    /**
     * @param N Number of object to update in single transaction.
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param time Test time.
     * @throws Exception If failed.
     */
    private void updateNObjectsTest(
        final int N,
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        long time
    )
        throws Exception
    {
        final int TOTAL = 20;

        assert N <= TOTAL;

        info("updateNObjectsTest [n=" + N + ", total=" + TOTAL + ']');

        final int writers = 4;

        final int readers = 4;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                Map<Integer, Integer> vals = new HashMap<>();

                for (int i = 0; i < TOTAL; i++)
                    vals.put(i, N);

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.putAll(vals);

                    tx.commit();
                }
            }
        };

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        IgniteCache<Integer, Integer> cache = randomCache(caches, rnd);
                        IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                        TreeSet<Integer> keys = new TreeSet<>();

                        while (keys.size() < N)
                            keys.add(rnd.nextInt(TOTAL));

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Map<Integer, Integer> curVals = cache.getAll(keys);

                            assertEquals(N, curVals.size());

                            Map<Integer, Integer> newVals = new HashMap<>();

                            for (Map.Entry<Integer, Integer> e : curVals.entrySet())
                                newVals.put(e.getKey(), e.getValue() + 1);

                            cache.putAll(newVals);

                            tx.commit();
                        }

                        cnt++;
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<IgniteCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    while (!stop.get()) {
                        while (keys.size() < TOTAL)
                            keys.add(rnd.nextInt(TOTAL));

                        IgniteCache<Integer, Integer> cache = randomCache(caches, rnd);

                        Map<Integer, Integer> vals = cache.getAll(keys);

                        assertEquals(TOTAL, vals.size());

                        int sum = 0;

                        for (int i = 0; i < TOTAL; i++) {
                            Integer val = vals.get(i);

                            assertNotNull(val);

                            sum += val;
                        }

                        assertTrue(sum % N == 0);
                    }

                    if (idx == 0) {
                        IgniteCache<Integer, Integer> cache = randomCache(caches, rnd);

                        Map<Integer, Integer> vals = cache.getAll(keys);

                        int sum = 0;

                        for (int i = 0; i < TOTAL; i++) {
                            Integer val = vals.get(i);

                            info("Value [id=" + i + ", val=" + val + ']');

                            sum += val;
                        }

                        info("Sum [sum=" + sum + ", mod=" + sum % N + ']');
                    }
                }
            };

        readWriteTest(srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            time,
            init,
            writer,
            reader);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param time Test time.
     * @param writers Number of writers.
     * @param readers Number of readers.
     * @param init Optional init closure.
     * @param writer Writers threads closure.
     * @param reader Readers threads closure.
     * @throws Exception If failed.
     */
    private void readWriteTest(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        final int writers,
        final int readers,
        final long time,
        IgniteInClosure<IgniteCache<Object, Object>> init,
        final GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> writer,
        final GridInClosure3<Integer, List<IgniteCache>, AtomicBoolean> reader) throws Exception {
        Ignite srv0 = startGridsMultiThreaded(srvs);

        if (clients > 0) {
            client = true;

            startGridsMultiThreaded(srvs, clients);
        }

        IgniteCache<Object, Object> cache = srv0.createCache(cacheConfiguration(PARTITIONED,
            FULL_SYNC,
            cacheBackups,
            cacheParts));

        if (init != null)
            init.apply(cache);

        final List<IgniteCache> caches = new ArrayList<>(srvs + clients);

        for (int i = 0; i < srvs + clients; i++) {
            Ignite node = grid(i);

            caches.add(node.cache(cache.getName()));
        }

        final long stopTime = U.currentTimeMillis() + time;

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            final AtomicInteger writerIdx = new AtomicInteger();

            IgniteInternalFuture<?> writeFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        int idx = writerIdx.getAndIncrement();

                        writer.apply(idx, caches, stop);
                    }
                    catch (Throwable e) {
                        error("Unexpected error: " + e, e);

                        stop.set(true);

                        fail("Unexpected error: " + e);
                    }

                    return null;
                }
            }, writers, "writer");

            final AtomicInteger readerIdx = new AtomicInteger();

            IgniteInternalFuture<?> readFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        int idx = readerIdx.getAndIncrement();

                        reader.apply(idx, caches, stop);
                    }
                    catch (Throwable e) {
                        error("Unexpected error: " + e, e);

                        stop.set(true);

                        fail("Unexpected error: " + e);
                    }

                    return null;
                }
            }, readers, "reader");

            while (System.currentTimeMillis() < stopTime && !stop.get())
                Thread.sleep(1000);

            stop.set(true);

            writeFut.get();
            readFut.get();
        }
        finally {
            stop.set(true);
        }
    }

    /**
     * @return Cache configurations.
     */
    private List<CacheConfiguration<Object, Object>> cacheConfigurations() {
        List<CacheConfiguration<Object, Object>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));

        return ccfgs;
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void logCacheInfo(CacheConfiguration<?, ?> ccfg) {
        log.info("Test cache [mode=" + ccfg.getCacheMode() +
            ", sync=" + ccfg.getWriteSynchronizationMode() +
            ", backups=" + ccfg.getBackups() +
            ", near=" + (ccfg.getNearConfiguration() != null) +
            ']');
    }

    /**
     * @param cache Cache.
     * @return Test keys.
     * @throws Exception If failed.
     */
    private List<Integer> testKeys(IgniteCache<Integer, Integer> cache) throws Exception {
        CacheConfiguration ccfg = cache.getConfiguration(CacheConfiguration.class);

        List<Integer> keys = new ArrayList<>();

        if (ccfg.getCacheMode() == PARTITIONED)
            keys.add(nearKey(cache));

        keys.add(primaryKey(cache));

        if (ccfg.getBackups() != 0)
            keys.add(backupKey(cache));

        return keys;
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param parts Number of partitions.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        int parts) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    private void verifyCoordinatorInternalState() throws Exception {
        for (Ignite node : G.allGrids()) {
            final CacheCoordinatorsSharedManager crd = ((IgniteKernal)node).context().cache().context().coordinators();

            Map activeTxs = GridTestUtils.getFieldValue(crd, "activeTxs");

            assertTrue(activeTxs.isEmpty());

            Map cntrFuts = GridTestUtils.getFieldValue(crd, "verFuts");

            assertTrue(cntrFuts.isEmpty());

            Map ackFuts = GridTestUtils.getFieldValue(crd, "ackFuts");

            assertTrue(ackFuts.isEmpty());

            // TODO IGNITE-3478
//            assertTrue(GridTestUtils.waitForCondition(
//                new GridAbsPredicate() {
//                    @Override public boolean apply() {
//                        Map activeQrys = GridTestUtils.getFieldValue(crd, "activeQueries");
//
//                        return activeQrys.isEmpty();
//                    }
//                }, 5000)
//            );
        }
    }

    /**
     * @param caches Caches.
     * @param rnd Random.
     * @return Random cache.
     */
    private static <K, V> IgniteCache<K, V> randomCache(List<IgniteCache> caches, ThreadLocalRandom rnd) {
        return caches.size() > 1 ? caches.get(rnd.nextInt(caches.size())): caches.get(0);
    }

    /**
     *
     */
    static class MvccTestAccount {
        /** */
        private final int val;

        /** */
        private final int updateCnt;

        /**
         * @param val Value.
         * @param updateCnt Updates counter.
         */
        MvccTestAccount(int val, int updateCnt) {
            assert updateCnt > 0;

            this.val = val;
            this.updateCnt = updateCnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MvccTestAccount.class, this);
        }
    }

    /**
     *
     */
    static class Value {
        /** */
        int key;

        /** */
        int cnt;

        /**
         * @param key Key.
         * @param cnt Update count.
         */
        Value(int key, int cnt) {
            this.key = key;
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Value.class, this);
        }
    }

    /**
     *
     */
    enum ReadMode {
        /** */
        GET_ALL,

        /** */
        SCAN
    }
}
