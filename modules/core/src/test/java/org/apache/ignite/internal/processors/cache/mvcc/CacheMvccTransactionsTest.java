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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.TestCacheNodeExcludingFilter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * TODO IGNITE-3478: extend tests to use single/mutiple nodes, all tx types.
 * TODO IGNITE-3478: test with cache groups.
 * TODO IGNITE-3478: add check for cleanup in all test (at the and do update for all keys, check there are 2 versions left).
 */
@SuppressWarnings("unchecked")
public class CacheMvccTransactionsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int DFLT_PARTITION_COUNT = RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

    /** */
    private static final String CRD_ATTR = "testCrd";

    /** */
    private static final long DFLT_TEST_TIME = 30_000;

    /** */
    private static final int SRVS = 4;

    /** */
    private boolean client;

    /** */
    private boolean testSpi;

    /** */
    private String nodeAttr;

    /** */
    private static final int PAGE_SIZE = MemoryConfiguration.DFLT_PAGE_SIZE;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMvccEnabled(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        if (nodeAttr != null)
            cfg.setUserAttributes(F.asMap(nodeAttr, true));

        MemoryConfiguration memCfg = new MemoryConfiguration();

        memCfg.setPageSize(PAGE_SIZE);

        cfg.setMemoryConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DFLT_TEST_TIME + 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CacheCoordinatorsProcessor.coordinatorAssignClosure(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            verifyCoordinatorInternalState();
        }
        finally {
            stopAllGrids();
        }

        CacheCoordinatorsProcessor.coordinatorAssignClosure(null);

        super.afterTest();
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
    public void testActiveQueriesCleanup() throws Exception {
        startGridsMultiThreaded(SRVS);

        client = true;

        Ignite srv0 = startGrid(SRVS);

        final int NODES = SRVS + 1;

        CacheConfiguration ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, 512);

        srv0.createCache(ccfg);

        final long stopTime = System.currentTimeMillis() + 5000;

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                IgniteCache cache = ignite(idx % NODES).cache(DEFAULT_CACHE_NAME);

                while (System.currentTimeMillis() < stopTime) {
                    int keyCnt = rnd.nextInt(10) + 1;

                    Set<Integer> keys = new HashSet<>();

                    for (int i = 0; i < keyCnt; i++)
                        keys.add(rnd.nextInt());

                    cache.getAll(keys);
                }
            }
        }, NODES * 2, "get-thread");

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
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
    public void testSimplePutRemove() throws Exception {
        simplePutRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimplePutRemove_LargeKeys() throws Exception {
        simplePutRemove(true);
    }

    /**
     * @throws Exception If failed.
     * @param largeKeys {@code True} to use large keys (not fitting in single page).
     */
    private void simplePutRemove(boolean largeKeys) throws Exception {
        Ignite node = startGrid(0);

        IgniteTransactions txs = node.transactions();

        final IgniteCache<Object, Object> cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        final int KEYS = 100;

        checkValues(new HashMap<>(), cache);

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int k = 0; k < KEYS; k++)
                cache.remove(testKey(largeKeys, k));

            tx.commit();
        }

        checkValues(new HashMap<>(), cache);

        Map<Object, Object> expVals = new HashMap<>();

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int k = 0; k < KEYS; k++) {
                Object key = testKey(largeKeys, k);

                expVals.put(key, k);

                cache.put(key, k);
            }

            tx.commit();
        }

        checkValues(expVals, cache);

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int k = 0; k < KEYS; k++) {
                if (k % 2 == 0) {
                    Object key = testKey(largeKeys, k);

                    cache.remove(key);

                    expVals.remove(key);
                }
            }

            tx.commit();
        }

        checkValues(expVals, cache);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Object key = testKey(largeKeys, 0);

        for (int i = 0; i < 500; i++) {
            boolean rmvd;

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                if (rnd.nextBoolean()) {
                    cache.remove(key);

                    rmvd = true;
                }
                else {
                    cache.put(key, i);

                    rmvd = false;
                }

                tx.commit();
            }

            if (rmvd) {
                assertNull(cache.get(key));
                assertTrue(cache.getAll(F.asSet(key)).isEmpty());
            }
            else {
                assertEquals(i, cache.get(key));

                Map<Object, Object> res = cache.getAll(F.asSet(key));

                assertEquals(i, res.get(key));
            }
        }
    }

    /**
     * @param largeKeys {@code True} to use large keys (not fitting in single page).
     * @param idx Index.
     * @return Key instance.
     */
    private static Object testKey(boolean largeKeys, int idx) {
        if (largeKeys) {
            int payloadSize = PAGE_SIZE + ThreadLocalRandom.current().nextInt(PAGE_SIZE * 10);

            return new TestKey(idx, payloadSize);
        }
        else
            return idx;
    }

    /**
     * @param expVals Expected values.
     * @param cache Cache.
     */
    private void checkValues(Map<Object, Object> expVals, IgniteCache<Object, Object> cache) {
        for (Map.Entry<Object, Object> e : expVals.entrySet())
            assertEquals(e.getValue(), cache.get(e.getKey()));

        Map<Object, Object> res = cache.getAll(expVals.keySet());

        assertEquals(expVals, res);

        res = new HashMap<>();

        for (IgniteCache.Entry<Object, Object> e : cache)
            res.put(e.getKey(), e.getValue());

        assertEquals(expVals, res);
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

        final Ignite ignite = startGrid(2);

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
                        cache.put(primaryKeys(jcache(0), 1, 100_000).get(0), 1);

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
    public void testCleanupWaitsForGet3() throws Exception {
        for (int i = 0; i < 4; i++) {
            cleanupWaitsForGet3(i + 1);

            afterTest();
        }
    }

    /**
     * @param updates Number of updates.
     * @throws Exception If failed.
     */
    private void cleanupWaitsForGet3(int updates) throws Exception {
        /*
        Simulate case when coordinator assigned query version has active transaction,
        query is delayed, after this active transaction finish and the same key is
        updated several more times before query starts.
         */
        testSpi = true;

        client = false;

        startGrids(1);

        client = true;

        final Ignite client = startGrid(1);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache = client.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 16));

        final Integer key1 = 1;
        final Integer key2 = 2;

        for (int i = 0; i < updates; i++) {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key1, i);
                cache.put(key2, i);

                tx.commit();
            }
        }

        TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(grid(0));

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);

        clientSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            /** */
            private boolean blocked;

            @Override public boolean apply(ClusterNode node, Message msg) {
                if (!blocked && (msg instanceof CoordinatorTxAckRequest)) {
                    blocked = true;

                    return true;
                }
                return false;
            }
        });

        final IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key2, 3);

                    tx.commit();
                }

                return null;
            }
        }, "put");

        clientSpi.waitForBlocked();

        for (int i = 0; i < updates; i++) {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key1, i + 3);

                tx.commit();
            }
        }

        // Delay version for getAll.
        crdSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            /** */
            private boolean blocked;

            @Override public boolean apply(ClusterNode node, Message msg) {
                if (!blocked && (msg instanceof MvccCoordinatorVersionResponse)) {
                    blocked = true;

                    return true;
                }
                return false;
            }
        });

        final IgniteInternalFuture<?> getFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Map<Object, Object> res = cache.getAll(F.asSet(key1, key2));

                assertEquals(2, res.size());

                return null;
            }
        }, "get");

        crdSpi.waitForBlocked();

        clientSpi.stopBlock(true);

        putFut.get();

        for (int i = 0; i < updates; i++) {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key2, i + 4);

                tx.commit();
            }
        }

        crdSpi.stopBlock(true);

        getFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode() throws Exception {
        putAllGetAll(false, 1, 0, 0, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode_SinglePartition() throws Exception {
        putAllGetAll(false, 1, 0, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0() throws Exception {
        putAllGetAll(false, 4, 2, 0, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1() throws Exception {
        putAllGetAll(false, 4, 2, 1, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups2() throws Exception {
        putAllGetAll(false, 4, 2, 2, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_RestartCoordinator() throws Exception {
        putAllGetAll(true, 4, 2, 1, 64);
    }

    /**
     * @param restartCrd Coordinator restart flag.
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @throws Exception If failed.
     */
    private void putAllGetAll(
        boolean restartCrd,
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

        readWriteTest(
            restartCrd,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            DFLT_TEST_TIME,
            null,
            writer,
            reader);

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_SingleNode() throws Exception {
        accountsTxGetAll(1, 0, 0, 64, false, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_SingleNode_SinglePartition() throws Exception {
        accountsTxGetAll(1, 0, 0, 1, false, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_WithRemoves_SingleNode_SinglePartition() throws Exception {
        accountsTxGetAll(1, 0, 0, 1, true, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups0() throws Exception {
        accountsTxGetAll(4, 2, 0, 64, false, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups1() throws Exception {
        accountsTxGetAll(4, 2, 1, 64, false, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups2() throws Exception {
        accountsTxGetAll(4, 2, 2, 64, false, ReadMode.GET_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_SingleNode_SinglePartition() throws Exception {
        accountsTxGetAll(1, 0, 0, 1, false, ReadMode.SCAN);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param withRmvs If {@code true} then in addition to puts tests also executes removes.
     * @param readMode Read mode.
     * @throws Exception If failed.
     */
    private void accountsTxGetAll(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        final boolean withRmvs,
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

        final Set<Integer> rmvdIds = new HashSet<>();

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

                        Integer cntr1 = null;
                        Integer cntr2 = null;

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            MvccTestAccount a1;
                            MvccTestAccount a2;

                            Map<Integer, MvccTestAccount> accounts = cache.getAll(keys);

                            a1 = accounts.get(id1);
                            a2 = accounts.get(id2);

                            if (!withRmvs) {
                                assertNotNull(a1);
                                assertNotNull(a2);

                                cntr1 = a1.updateCnt + 1;
                                cntr2 = a2.updateCnt + 1;

                                cache.put(id1, new MvccTestAccount(a1.val + 1, cntr1));
                                cache.put(id2, new MvccTestAccount(a2.val - 1, cntr2));
                            }
                            else {
                                if (a1 != null || a2 != null) {
                                    if (a1 != null && a2 != null) {
                                        Integer rmvd = null;

                                        if (rnd.nextInt(10) == 0) {
                                            synchronized (rmvdIds) {
                                                if (rmvdIds.size() < ACCOUNTS / 2) {
                                                    rmvd = rnd.nextBoolean() ? id1 : id2;

                                                    assertTrue(rmvdIds.add(rmvd));
                                                }
                                            }
                                        }

                                        if (rmvd != null) {
                                            cache.remove(rmvd);

                                            cache.put(rmvd.equals(id1) ? id2 : id1,
                                                new MvccTestAccount(a1.val + a2.val, 1));
                                        }
                                        else {
                                            cache.put(id1, new MvccTestAccount(a1.val + 1, 1));
                                            cache.put(id2, new MvccTestAccount(a2.val - 1, 1));
                                        }
                                    }
                                    else {
                                        if (a1 == null) {
                                            cache.put(id1, new MvccTestAccount(100, 1));
                                            cache.put(id2, new MvccTestAccount(a2.val - 100, 1));

                                            assertTrue(rmvdIds.remove(id1));
                                        }
                                        else {
                                            cache.put(id1, new MvccTestAccount(a1.val - 100, 1));
                                            cache.put(id2, new MvccTestAccount(100, 1));

                                            assertTrue(rmvdIds.remove(id2));
                                        }
                                    }
                                }
                            }

                            tx.commit();
                        }

                        if (!withRmvs) {
                            Map<Integer, MvccTestAccount> accounts = cache.getAll(keys);

                            MvccTestAccount a1 = accounts.get(id1);
                            MvccTestAccount a2 = accounts.get(id2);

                            assertNotNull(a1);
                            assertNotNull(a2);

                            assertTrue(a1.updateCnt >= cntr1);
                            assertTrue(a2.updateCnt >= cntr2);
                        }
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

                        if (!withRmvs)
                            assertEquals(ACCOUNTS, accounts.size());

                        int sum = 0;

                        for (int i = 0; i < ACCOUNTS; i++) {
                            MvccTestAccount account = accounts.get(i);

                            if (account != null) {
                                sum += account.val;

                                Integer cntr = lastUpdateCntrs.get(i);

                                if (cntr != null)
                                    assertTrue(cntr <= account.updateCnt);

                                lastUpdateCntrs.put(i, cntr);
                            }
                            else
                                assertTrue(withRmvs);
                        }

                        assertEquals(ACCOUNTS * ACCOUNT_START_VAL, sum);
                    }

                    if (idx == 0) {
                        IgniteCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);

                        Map<Integer, MvccTestAccount> accounts = cache.getAll(keys);

                        int sum = 0;

                        for (int i = 0; i < ACCOUNTS; i++) {
                            MvccTestAccount account = accounts.get(i);

                            assertTrue(account != null || withRmvs);

                            info("Account [id=" + i + ", val=" + (account != null ? account.val : null) + ']');

                            if (account != null)
                                sum += account.val;
                        }

                        info("Sum: " + sum);
                    }
                }
            };

        readWriteTest(
            false,
            srvs,
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

        readWriteTest(
            false,
            srvs,
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

        checkActiveQueriesCleanup(ignite(0));

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
    public void testSimpleRebalance() throws Exception {
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
    public void testSimpleRebalanceWithRemovedValues() throws Exception {
        Ignite node = startGrid(0);

        IgniteTransactions txs = node.transactions();

        final IgniteCache<Object, Object> cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, 64));

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int k = 0; k < 100; k++)
                cache.remove(k);

            tx.commit();
        }

        Map<Object, Object> expVals = new HashMap<>();

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int k = 100; k < 200; k++) {
                cache.put(k, k);

                expVals.put(k, k);
            }

            tx.commit();
        }

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int k = 100; k < 200; k++) {
                if (k % 2 == 0) {
                    cache.remove(k);

                    expVals.remove(k);
                }
            }

            tx.commit();
        }

        startGrid(1);

        awaitPartitionMapExchange();

        checkValues(expVals, jcache(1));

        stopGrid(0);

        checkValues(expVals, jcache(1));
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
    public void testReadInProgressCoordinatorFailsSimple_FromServer() throws Exception {
        for (int i = 1; i <= 3; i++) {
            readInProgressCoordinatorFailsSimple(false, i);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadInProgressCoordinatorFailsSimple_FromClient() throws Exception {
        for (int i = 1; i <= 3; i++) {
            readInProgressCoordinatorFailsSimple(true, i);

            afterTest();
        }
    }

    /**
     * @param fromClient {@code True} if read from client node, otherwise from server node.
     * @param crdChangeCnt Number of coordinator changes.
     * @throws Exception If failed.
     */
    private void readInProgressCoordinatorFailsSimple(boolean fromClient, int crdChangeCnt) throws Exception {
        info("readInProgressCoordinatorFailsSimple [fromClient=" + fromClient + ", crdChangeCnt=" + crdChangeCnt + ']');

        testSpi = true;

        client = false;

        final int SRVS = 3;
        final int COORDS = crdChangeCnt + 1;

        startGrids(SRVS + COORDS);

        client = true;

        assertTrue(startGrid(SRVS + COORDS).configuration().isClientMode());

        final Ignite getNode = fromClient ? ignite(SRVS + COORDS) : ignite(COORDS);

        String[] excludeNodes = new String[COORDS];

        for (int i = 0; i < COORDS; i++)
            excludeNodes[i] = testNodeName(i);

        final IgniteCache cache = getNode.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setNodeFilter(new TestCacheNodeExcludingFilter(excludeNodes)));

        final Set<Integer> keys = new HashSet<>();

        List<Integer> keys1 = primaryKeys(jcache(COORDS), 10);

        keys.addAll(keys1);
        keys.addAll(primaryKeys(jcache(COORDS + 1), 10));

        Map<Integer, Integer> vals = new HashMap();

        for (Integer key : keys)
            vals.put(key, -1);

        try (Transaction tx = getNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.putAll(vals);

            tx.commit();
        }

        final TestRecordingCommunicationSpi getNodeSpi = TestRecordingCommunicationSpi.spi(getNode);

        getNodeSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
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

        getNodeSpi.waitForBlocked();

        for (int i = 0; i < crdChangeCnt; i++)
            stopGrid(i);

        for (int i = 0; i < 10; i++) {
            vals = new HashMap();

            for (Integer key : keys)
                vals.put(key, i);

            while (true) {
                try (Transaction tx = getNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.putAll(vals);

                    tx.commit();

                    break;
                }
                catch (Exception e) {
                    if (!X.hasCause(e, ClusterTopologyException.class))
                        fail("Unexpected error: " + e);
                    else
                        info("Tx error, need retry: " + e);
                }
            }
        }

        getNodeSpi.stopBlock(true);

        getFut.get();

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCoordinatorChangeActiveQueryClientFails_Simple() throws Exception {
        testSpi = true;

        client = false;

        final int SRVS = 3;
        final int COORDS = 1;

        startGrids(SRVS + COORDS);

        client = true;

        Ignite client = startGrid(SRVS + COORDS);

        final IgniteCache cache = client.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setNodeFilter(new TestCacheNodeExcludingFilter(testNodeName(0))));

        final Map<Integer, Integer> vals = new HashMap();

        for (int i = 0; i < 100; i++)
            vals.put(i, i);

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
                cache.getAll(vals.keySet());

                return null;
            }
        }, "get-thread");

        clientSpi.waitForBlocked();

        stopGrid(0);

        stopGrid(client.name());

        try {
            getFut.get();

            fail();
        }
        catch (Exception ignore) {
            // No-op.
        }

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadInProgressCoordinatorFails() throws Exception {
        readInProgressCoordinatorFails(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadInProgressCoordinatorFails_ReadDelay() throws Exception {
        readInProgressCoordinatorFails(true);
    }

    /**
     * @param readDelay {@code True} if delays get requests.
     * @throws Exception If failed.
     */
    private void readInProgressCoordinatorFails(boolean readDelay) throws Exception {
        final int COORD_NODES = 5;
        final int SRV_NODES = 4;

        if (readDelay)
            testSpi = true;

        startGrids(COORD_NODES);

        startGridsMultiThreaded(COORD_NODES, SRV_NODES);

        client = true;

        Ignite client = startGrid(COORD_NODES + SRV_NODES);

        final List<String> cacheNames = new ArrayList<>();

        final int KEYS = 100;

        final Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < KEYS; i++)
            vals.put(i, 0);

        String[] exclude = new String[COORD_NODES];

        for (int i = 0; i < COORD_NODES; i++)
            exclude[i] = testNodeName(i);

        for (CacheConfiguration ccfg : cacheConfigurations()) {
            ccfg.setName("cache-" + cacheNames.size());

            // First server nodes are 'dedicated' coordinators.
            ccfg.setNodeFilter(new TestCacheNodeExcludingFilter(exclude));

            cacheNames.add(ccfg.getName());

            IgniteCache cache = client.createCache(ccfg);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.putAll(vals);

                tx.commit();
            }
        }

        if (readDelay) {
            for (int i = COORD_NODES; i < COORD_NODES + SRV_NODES + 1; i++) {
                TestRecordingCommunicationSpi.spi(ignite(i)).closure(new IgniteBiInClosure<ClusterNode, Message>() {
                    @Override public void apply(ClusterNode node, Message msg) {
                        if (msg instanceof GridNearGetRequest)
                            doSleep(ThreadLocalRandom.current().nextLong(50) + 1);
                    }
                });
            }
        }

        final AtomicBoolean done = new AtomicBoolean();

        try {
            final AtomicInteger readNodeIdx = new AtomicInteger(0);

            IgniteInternalFuture getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        Ignite node = ignite(COORD_NODES + (readNodeIdx.getAndIncrement() % (SRV_NODES + 1)));

                        int cnt = 0;

                        while (!done.get()) {
                            for (String cacheName : cacheNames) {
                                IgniteCache cache = node.cache(cacheName);

                                Map<Integer, Integer> res = cache.getAll(vals.keySet());

                                assertEquals(vals.size(), res.size());

                                Integer val0 = null;

                                for (Integer val : res.values()) {
                                    if (val0 == null)
                                        val0 = val;
                                    else
                                        assertEquals(val0, val);
                                }
                            }

                            cnt++;
                        }

                        log.info("Finished [node=" + node.name() + ", readCnt=" + cnt + ']');

                        return null;
                    }
                    catch (Throwable e) {
                        error("Unexpected error: " + e, e);

                        throw e;
                    }
                }
            }, (SRV_NODES + 1) + 1, "get-thread");

            IgniteInternalFuture putFut1 = GridTestUtils.runAsync(new Callable() {
                @Override public Void call() throws Exception {
                    Ignite node = ignite(COORD_NODES);

                    List<IgniteCache> caches = new ArrayList<>();

                    for (String cacheName : cacheNames)
                        caches.add(node.cache(cacheName));

                    Integer val = 1;

                    while (!done.get()) {
                        Map<Integer, Integer> vals = new HashMap<>();

                        for (int i = 0; i < KEYS; i++)
                            vals.put(i, val);

                        for (IgniteCache cache : caches) {
                            try {
                                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    cache.putAll(vals);

                                    tx.commit();
                                }
                            }
                            catch (ClusterTopologyException e) {
                                info("Tx failed: " + e);
                            }
                        }

                        val++;
                    }

                    return null;
                }
            }, "putAll-thread");

            IgniteInternalFuture putFut2 = GridTestUtils.runAsync(new Callable() {
                @Override public Void call() throws Exception {
                    Ignite node = ignite(COORD_NODES);

                    IgniteCache cache = node.cache(cacheNames.get(0));

                    Integer val = 0;

                    while (!done.get()) {
                        try {
                            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                cache.put(Integer.MAX_VALUE, val);

                                tx.commit();
                            }
                        }
                        catch (ClusterTopologyException e) {
                            info("Tx failed: " + e);
                        }

                        val++;
                    }

                    return null;
                }
            }, "put-thread");

            for (int i = 0; i < COORD_NODES && !getFut.isDone(); i++) {
                U.sleep(3000);

                stopGrid(i);

                awaitPartitionMapExchange();
            }

            done.set(true);

            getFut.get();
            putFut1.get();
            putFut2.get();

            for (Ignite node : G.allGrids())
                checkActiveQueriesCleanup(node);
        }
        finally {
            done.set(true);
        }

    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccCoordinatorChangeSimple() throws Exception {
        Ignite srv0 = startGrid(0);

        final List<String> cacheNames = new ArrayList<>();

        for (CacheConfiguration ccfg : cacheConfigurations()) {
            ccfg.setName("cache-" + cacheNames.size());

            cacheNames.add(ccfg.getName());

            srv0.createCache(ccfg);
        }

        checkPutGet(cacheNames);

        for (int i = 0; i < 3; i++) {
            startGrid(i + 1);

            checkPutGet(cacheNames);

            checkCoordinatorsConsistency(null);
        }

        client = true;

        for (int i = 0; i < 3; i++) {
            Ignite node = startGrid(i + 4);

            // Init client caches outside of transactions.
            for (String cacheName : cacheNames)
                node.cache(cacheName);

            checkPutGet(cacheNames);

            checkCoordinatorsConsistency(null);
        }

        for (int i = 0; i < 3; i++) {
            stopGrid(i);

            awaitPartitionMapExchange();

            checkPutGet(cacheNames);

            checkCoordinatorsConsistency(null);
        }
    }

    /**
     * @param cacheNames Cache names.
     */
    private void checkPutGet(List<String> cacheNames) {
        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        Ignite putNode = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

        Map<Integer, Integer> vals = new HashMap();

        Integer val = ThreadLocalRandom.current().nextInt();

        for (int i = 0; i < 10; i++)
            vals.put(i, val);

        try (Transaction tx = putNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (String cacheName : cacheNames)
                putNode.cache(cacheName).putAll(vals);

            tx.commit();
        }

        for (Ignite node : nodes) {
            for (String cacheName : cacheNames) {
                Map<Object, Object> res = node.cache(cacheName).getAll(vals.keySet());

                assertEquals(vals, res);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccCoordinatorInfoConsistency() throws Exception {
        for (int i = 0; i < 4; i++) {
            startGrid(i);

            checkCoordinatorsConsistency(i + 1);
        }

        client = true;

        startGrid(4);

        checkCoordinatorsConsistency(5);

        startGrid(5);

        checkCoordinatorsConsistency(6);

        client = false;

        stopGrid(0);

        awaitPartitionMapExchange();

        checkCoordinatorsConsistency(5);
    }

    /**
     * @param expNodes Expected nodes number.
     */
    private void checkCoordinatorsConsistency(@Nullable Integer expNodes) {
        List<Ignite> nodes = G.allGrids();

        if (expNodes != null)
            assertEquals(expNodes, (Integer)nodes.size());

        MvccCoordinator crd = null;

        for (Ignite node : G.allGrids()) {
            CacheCoordinatorsProcessor crdProc = ((IgniteKernal) node).context().cache().context().coordinators();

            MvccCoordinator crd0 = crdProc.currentCoordinator();

            if (crd != null)
                assertEquals(crd, crd0);
            else
                crd = crd0;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetVersionRequestFailover() throws Exception {
        final int NODES = 5;

        testSpi = true;

        startGridsMultiThreaded(NODES - 1);

        client = true;

        Ignite client = startGrid(NODES - 1);

        final List<String> cacheNames = new ArrayList<>();

        final Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 100; i++)
            vals.put(i, i);

        for (CacheConfiguration ccfg : cacheConfigurations()) {
            ccfg.setName("cache-" + cacheNames.size());

            ccfg.setNodeFilter(new TestCacheNodeExcludingFilter(getTestIgniteInstanceName(0)));

            cacheNames.add(ccfg.getName());

            IgniteCache cache = client.createCache(ccfg);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.putAll(vals);

                tx.commit();
            }
        }

        final AtomicInteger nodeIdx = new AtomicInteger(1);

        final AtomicBoolean done = new AtomicBoolean();

        try {
            IgniteInternalFuture getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Ignite node = ignite(nodeIdx.getAndIncrement());

                    int cnt = 0;

                    while (!done.get()) {
                        for (String cacheName : cacheNames) {
                            IgniteCache cache = node.cache(cacheName);

                            Map<Integer, Integer> res = cache.getAll(vals.keySet());

                            assertEquals(vals, res);
                        }

                        cnt++;
                    }

                    log.info("Finished [node=" + node.name() + ", cnt=" + cnt + ']');

                    return null;
                }
            }, NODES - 1, "get-thread");

            doSleep(1000);

            TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(ignite(0));

            crdSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof MvccCoordinatorVersionResponse;
                }
            });

            crdSpi.waitForBlocked();

            stopGrid(0);

            doSleep(1000);

            done.set(true);

            getFut.get();
        }
        finally {
            done.set(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadWithStreamer() throws Exception {
        startGridsMultiThreaded(5);

        client = true;

        startGrid(5);

        Ignite node = ignite(0);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, 64));

        final int KEYS = 10_000;

        Map<Integer, Integer> data = new HashMap<>();

        try (IgniteDataStreamer<Integer, Integer> streamer = node.dataStreamer(cache.getName())) {
            for (int i = 0; i < KEYS; i++) {
                streamer.addData(i, i);

                data.put(i, i);
            }
        }

        checkCacheData(data, cache.getName());

        checkPutGet(F.asList(cache.getName()));
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

        readWriteTest(
            false,
            srvs,
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
     * @param restartCrd If {@code true} dedicated coordinator node is restarted during test.
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
        final boolean restartCrd,
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
        if (restartCrd)
            CacheCoordinatorsProcessor.coordinatorAssignClosure(new CoordinatorAssignClosure());

        Ignite srv0 = startGridsMultiThreaded(srvs);

        if (clients > 0) {
            client = true;

            startGridsMultiThreaded(srvs, clients);

            client = false;
        }

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            FULL_SYNC,
            cacheBackups,
            cacheParts);

        if (restartCrd)
            ccfg.setNodeFilter(new CoordinatorNodeFilter());

        IgniteCache<Object, Object> cache = srv0.createCache(ccfg);

        int crdIdx = srvs + clients;

        if (restartCrd) {
            nodeAttr = CRD_ATTR;

            startGrid(crdIdx);
        }

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
                        if (restartCrd && X.hasCause(e, ClusterTopologyException.class)) {
                            log.info("Writer error: " + e);

                            return null;
                        }

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

            while (System.currentTimeMillis() < stopTime && !stop.get()) {
                Thread.sleep(1000);

                if (restartCrd) {
                    log.info("Start new coordinator: " + (crdIdx + 1));

                    startGrid(crdIdx + 1);

                    log.info("Stop current coordinator: " + crdIdx);

                    stopGrid(crdIdx);

                    crdIdx++;

                    awaitPartitionMapExchange();
                }
            }

            stop.set(true);

            writeFut.get();
            readFut.get();
        }
        finally {
            stop.set(true);
        }
    }
    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSize() throws Exception {
        Ignite node = startGrid(0);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        assertEquals(cache.size(), 0);

        final int KEYS = 10;

        for (int i = 0; i < KEYS; i++) {
            final Integer key = i;

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key, i);

                tx.commit();
            }

            assertEquals(i + 1, cache.size());
        }

        for (int i = 0; i < KEYS; i++) {
            final Integer key = i;

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key, i);

                tx.commit();
            }

            assertEquals(KEYS, cache.size());
        }

        int size = KEYS;

        for (int i = 0; i < KEYS; i++) {
            if (i % 2 == 0) {
                final Integer key = i;

                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.remove(key);

                    tx.commit();
                }

                size--;

                assertEquals(size, cache.size());
            }
        }

        // Check size does not change if remove already removed keys.
        for (int i = 0; i < KEYS; i++) {
            if (i % 2 == 0) {
                final Integer key = i;

                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.remove(key);

                    tx.commit();
                }

                assertEquals(size, cache.size());
            }
        }

        for (int i = 0; i < KEYS; i++) {
            if (i % 2 == 0) {
                final Integer key = i;

                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key, i);

                    tx.commit();
                }

                size++;

                assertEquals(size, cache.size());
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInternalApi() throws Exception {
        Ignite node = startGrid(0);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        GridCacheContext cctx =
            ((IgniteKernal)node).context().cache().context().cacheContext(CU.cacheId(cache.getName()));

        CacheCoordinatorsProcessor crd = cctx.kernalContext().coordinators();

        // Start query to prevent cleanup.
        IgniteInternalFuture<MvccCoordinatorVersion> fut = crd.requestQueryCounter(crd.currentCoordinator());

        fut.get();

        final int KEYS = 1000;

        for (int i = 0; i < 10; i++) {
            for (int k = 0; k < KEYS; k++) {
                final Integer key = k;

                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key, i);

                    tx.commit();
                }
            }
        }

        for (int k = 0; k < KEYS; k++) {
            final Integer key = k;

            KeyCacheObject key0 = cctx.toCacheKeyObject(key);

            List<T2<Object, MvccCounter>> vers = cctx.offheap().mvccAllVersions(cctx, key0);

            assertEquals(10, vers.size());

            CacheDataRow row = cctx.offheap().read(cctx, key0);

            checkRow(cctx, row, key0, vers.get(0).get1());

            for (T2<Object, MvccCounter> ver : vers) {
                MvccCounter cntr = ver.get2();

                MvccCoordinatorVersion readVer =
                    new MvccCoordinatorVersionResponse(cntr.coordinatorVersion(), cntr.counter(), 0);

                row = cctx.offheap().mvccRead(cctx, key0, readVer);

                checkRow(cctx, row, key0, ver.get1());
            }

            checkRow(cctx,
                cctx.offheap().mvccRead(cctx, key0, version(vers.get(0).get2().coordinatorVersion() + 1, 1)),
                key0,
                vers.get(0).get1());

            checkRow(cctx,
                cctx.offheap().mvccRead(cctx, key0, version(vers.get(0).get2().coordinatorVersion(), vers.get(0).get2().counter() + 1)),
                key0,
                vers.get(0).get1());

            MvccCoordinatorVersionResponse ver = version(vers.get(0).get2().coordinatorVersion(), 100000);

            for (int v = 0; v < vers.size(); v++) {
                MvccCounter cntr = vers.get(v).get2();

                ver.addTx(cntr.counter());

                row = cctx.offheap().mvccRead(cctx, key0, ver);

                if (v == vers.size() - 1)
                    assertNull(row);
                else
                    checkRow(cctx, row, key0, vers.get(v + 1).get1());
            }
        }
    }

    /**
     * @param cctx Context.
     * @param row Row.
     * @param expKey Expected row key.
     * @param expVal Expected row value.
     */
    private void checkRow(GridCacheContext cctx, CacheDataRow row, KeyCacheObject expKey, Object expVal) {
        assertNotNull(row);
        assertEquals(expKey, row.key());
        assertEquals(expVal, row.value().value(cctx.cacheObjectContext(), false));
    }

    /**
     * @param crdVer Coordinator version.
     * @param cntr Counter.
     * @return Version.
     */
    private MvccCoordinatorVersionResponse version(long crdVer, long cntr) {
        return new MvccCoordinatorVersionResponse(crdVer, cntr, 0);
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
            final CacheCoordinatorsProcessor crd = ((IgniteKernal)node).context().cache().context().coordinators();

            Map activeTxs = GridTestUtils.getFieldValue(crd, "activeTxs");

            assertTrue(activeTxs.isEmpty());

            Map cntrFuts = GridTestUtils.getFieldValue(crd, "verFuts");

            assertTrue(cntrFuts.isEmpty());

            Map ackFuts = GridTestUtils.getFieldValue(crd, "ackFuts");

            assertTrue(ackFuts.isEmpty());

            // TODO IGNITE-3478
            // checkActiveQueriesCleanup(node);
        }
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    private void checkActiveQueriesCleanup(Ignite node) throws Exception {
        final CacheCoordinatorsProcessor crd = ((IgniteKernal)node).context().cache().context().coordinators();

        assertTrue("Active queries not cleared: " + node.name(), GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    Object activeQueries = GridTestUtils.getFieldValue(crd, "activeQueries");

                    synchronized (activeQueries) {
                        Long minQry = GridTestUtils.getFieldValue(activeQueries, "minQry");

                        if (minQry != null)
                            log.info("Min query: " + minQry);

                        Map<Object, Map> queriesMap = GridTestUtils.getFieldValue(activeQueries, "activeQueries");

                        boolean empty = true;

                        for (Map.Entry<Object, Map> e : queriesMap.entrySet()) {
                            if (!e.getValue().isEmpty()) {
                                empty = false;

                                log.info("Active queries: " + e);
                            }
                        }

                        return empty && minQry == null;
                    }
                }
            }, 8_000)
        );

        assertTrue("Previous coordinator queries not empty: " + node.name(), GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    Map queries = GridTestUtils.getFieldValue(crd, "prevCrdQueries", "activeQueries");
                    Boolean prevDone = GridTestUtils.getFieldValue(crd, "prevCrdQueries", "prevQueriesDone");

                    if (!queries.isEmpty() || !prevDone)
                        log.info("Previous coordinator state [prevDone=" + prevDone + ", queries=" + queries + ']');

                    return queries.isEmpty();
                }
            }, 8_000)
        );
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

    /**
     *
     */
    static class CoordinatorNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(CRD_ATTR) == null;
        }
    }

    /**
     *
     */
    static class CoordinatorAssignClosure implements IgniteClosure<Collection<ClusterNode>, ClusterNode> {
        @Override public ClusterNode apply(Collection<ClusterNode> clusterNodes) {
            for (ClusterNode node : clusterNodes) {
                if (node.attribute(CRD_ATTR) != null) {
                    assert !CU.clientNode(node) : node;

                    return node;
                }
            }

            return null;
        }
    }

    /**
     *
     */
    static class TestKey implements Serializable {
        /** */
        private final int key;

        /** */
        private final byte[] payload;

        /**
         * @param key Key.
         * @param payloadSize Payload size.
         */
        public TestKey(int key, int payloadSize) {
            this.key = key;
            this.payload = new byte[payloadSize];
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey testKey = (TestKey)o;

            if (key != testKey.key)
                return false;

            return Arrays.equals(payload, testKey.payload);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = key;

            res = 31 * res + Arrays.hashCode(payload);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestKey [k=" + key + ", payloadLen=" + payload.length + ']';
        }
    }
}
