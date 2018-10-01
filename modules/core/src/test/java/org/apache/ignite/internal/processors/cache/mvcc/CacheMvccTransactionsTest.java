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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.TestCacheNodeExcludingFilter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQueryCntr;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccSnapshotResponse;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SCAN;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.PUT;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker.MVCC_TRACKER_ID_NA;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * TODO IGNITE-6739: tests reload
 * TODO IGNITE-6739: extend tests to use single/mutiple nodes, all tx types.
 * TODO IGNITE-6739: test with cache groups.
 */
@SuppressWarnings("unchecked")
public class CacheMvccTransactionsTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception if failed.
     */
    public void testEmptyTx() throws Exception {
        Ignite node = startGrids(2);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, DFLT_PARTITION_COUNT));

        cache.putAll(Collections.emptyMap());

        IgniteTransactions txs = node.transactions();
        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.commit();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testImplicitTxOps() throws Exception {
        checkTxWithAllCaches(new CI1<IgniteCache<Integer, Integer>>() {
            @Override public void apply(IgniteCache<Integer, Integer> cache) {
                try {
                    List<Integer> keys = testKeys(cache);

                    for (Integer key : keys) {
                        log.info("Test key: " + key);

                        Integer val = cache.get(key);

                        assertNull(val);

                        assertFalse(cache.containsKey(key));

                        cache.put(key, -1);

                        val = (Integer)checkAndGet(true, cache, key, GET, SCAN);

                        assertEquals(Integer.valueOf(-1), val);

                        assertTrue(cache.containsKey(key));

                        cache.put(key, key);

                        val = (Integer)checkAndGet(true, cache, key, GET, SCAN);

                        assertEquals(key, val);

                        cache.remove(key);

                        val = cache.get(key);

                        assertNull(val);

                        val = (Integer)checkAndGet(false, cache, key, SCAN, GET);

                        assertNull(val);

                        assertTrue(cache.putIfAbsent(key, key));

                        val = (Integer)checkAndGet(true, cache, key, GET, SCAN);

                        assertEquals(key, val);

                        val = cache.getAndReplace(key, -1);

                        assertEquals(key, val);

                        val = (Integer)checkAndGet(true, cache, key, GET, SCAN);

                        assertEquals(Integer.valueOf(-1), val);

                        val = cache.getAndRemove(key);

                        assertEquals(Integer.valueOf(-1), val);

                        val = cache.get(key);

                        assertNull(val);

                        val = (Integer)checkAndGet(false, cache, key, SCAN, GET);

                        assertNull(val);
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
    public void testPessimisticTx1() throws Exception {
        checkTxWithAllCaches(new CI1<IgniteCache<Integer, Integer>>() {
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

                            val = (Integer)checkAndGet(true, cache, key, GET, SCAN);

                            assertEquals(key, val);

                            tx.commit();
                        }

                        Integer val = (Integer)checkAndGet(false, cache, key, SCAN, GET);

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
        checkTxWithAllCaches(new CI1<IgniteCache<Integer, Integer>>() {
            @Override public void apply(IgniteCache<Integer, Integer> cache) {
                try {
                    IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                    List<Integer> keys = testKeys(cache);

                    for (Integer key : keys) {
                        log.info("Test key: " + key);

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache.put(key, key);
                            cache.put(key + 1, key + 1);

                            assertEquals(key, checkAndGet(true, cache, key, GET, SCAN));
                            assertEquals(key + 1, checkAndGet(true, cache, key + 1, GET, SCAN));

                            tx.commit();
                        }

                        assertEquals(key, checkAndGet(false, cache, key, GET, SCAN));
                        assertEquals(key + 1, checkAndGet(false, cache, key + 1, GET, SCAN));
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
    private void checkTxWithAllCaches(IgniteInClosure<IgniteCache<Integer, Integer>> c) throws Exception {
        client = false;

        startGridsMultiThreaded(SRVS);

        client = true;

        startGrid(SRVS);

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
    public void testWithCacheGroups() throws Exception {
        Ignite srv0 = startGrid(0);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        for (int c = 0; c < 3; c++) {
            CacheConfiguration ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT);

            ccfg.setName("cache-" + c);
            ccfg.setGroupName("grp1");

            ccfgs.add(ccfg);
        }

        srv0.createCaches(ccfgs);

        final int PUTS = 5;

        for (int i = 0; i < PUTS; i++) {
            for (int c = 0; c < 3; c++) {
                IgniteCache cache = srv0.cache("cache-" + c);

                Map<Integer, Integer> vals = new HashMap<>();

                for (int k = 0; k < 10; k++) {
                    cache.put(k, i);

                    vals.put(k, i);

                    assertEquals(i, checkAndGet(false, cache, k, SCAN, GET));
                }

                assertEquals(vals, checkAndGetAll(false, cache, vals.keySet(), GET, SCAN));
            }
        }

        for (int c = 0; c < 3; c++) {
            IgniteCache cache = srv0.cache("cache-" + c);

            Map<Integer, Integer> vals = new HashMap<>();

            for (int k = 0; k < 10; k++) {
                if (k % 2 == 0)
                    vals.put(k, PUTS - 1);
                else {
                    cache.remove(k);

                    assertNull(checkAndGet(false, cache, k, SCAN, GET));
                }
            }

            assertEquals(vals, checkAndGetAll(false, cache, vals.keySet(), GET, SCAN));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheRecreate() throws Exception {
        cacheRecreate(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActiveQueriesCleanup() throws Exception {
        activeQueriesCleanup(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActiveQueriesCleanupTx() throws Exception {
        activeQueriesCleanup(true);
    }

    /**
     * @param tx If {@code true} tests reads inside transaction.
     * @throws Exception If failed.
     */
    private void activeQueriesCleanup(final boolean tx) throws Exception {
        startGridsMultiThreaded(SRVS);

        client = true;

        Ignite client = startGrid(SRVS);

        final int NODES = SRVS + 1;

        CacheConfiguration ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, 512);

        client.createCache(ccfg);

        final long stopTime = System.currentTimeMillis() + 5000;

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                Ignite node = ignite(idx % NODES);

                IgniteTransactions txs = node.transactions();

                IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

                while (System.currentTimeMillis() < stopTime) {
                    int keyCnt = rnd.nextInt(10) + 1;

                    Set<Integer> keys = new HashSet<>();

                    for (int i = 0; i < keyCnt; i++)
                        keys.add(rnd.nextInt());

                    if (tx) {
                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache.getAll(keys);

                            if (rnd.nextBoolean())
                                tx.commit();
                            else
                                tx.rollback();
                        }
                    }
                    else
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
    public void testTxReadIsolationSimple() throws Exception {
        Ignite srv0 = startGrids(4);

        client = true;

        startGrid(4);

        for (CacheConfiguration ccfg : cacheConfigurations()) {
            IgniteCache<Object, Object> cache0 = srv0.createCache(ccfg);

            final Map<Integer, Integer> startVals = new HashMap<>();

            final int KEYS = 10;

            for (int i = 0; i < KEYS; i++)
                startVals.put(i, 0);

            for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                for (final Ignite node : G.allGrids()) {
                    info("Run test [node=" + node.name() + ", isolation=" + isolation + ']');

                    try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache0.putAll(startVals);

                        tx.commit();
                    }

                    final CountDownLatch readStart = new CountDownLatch(1);

                    final CountDownLatch readProceed = new CountDownLatch(1);

                    IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

                            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                assertEquals(0, checkAndGet(false, cache, 0, SCAN, GET));

                                readStart.countDown();

                                assertTrue(readProceed.await(5, TimeUnit.SECONDS));

                                assertEquals(0, checkAndGet(true, cache, 1, GET, SCAN));

                                assertEquals(0, checkAndGet(true, cache, 2, GET, SCAN));

                                Map<Object, Object> res = checkAndGetAll(true, cache, startVals.keySet(), GET, SCAN);

                                assertEquals(startVals.size(), res.size());

                                for (Map.Entry<Object, Object> e : res.entrySet())
                                    assertEquals("Invalid value for key: " + e.getKey(), 0, e.getValue());

                                tx.rollback();
                            }

                            return null;
                        }
                    });

                    assertTrue(readStart.await(5, TimeUnit.SECONDS));

                    for (int i = 0; i < KEYS; i++) {
                        try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            if (i % 2 == 0)
                                cache0.put(i, 1);
                            else
                                cache0.remove(i);

                            tx.commit();
                        }
                    }

                    readProceed.countDown();

                    fut.get();
                }
            }

            srv0.destroyCache(cache0.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetAllSimple() throws Exception {
        Ignite node = startGrid(0);

        IgniteTransactions txs = node.transactions();

        final IgniteCache<Object, Object> cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        final int KEYS = 10_000;

        Set<Integer> keys = new HashSet<>();

        for (int k = 0; k < KEYS; k++)
            keys.add(k);

        Map<Object, Object> map = checkAndGetAll(false, cache, keys, SCAN, GET);

        assertTrue(map.isEmpty());

        for (int v = 0; v < 3; v++) {
            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int k = 0; k < KEYS; k++) {
                    if (k % 2 == 0)
                        cache.put(k, v);
                }

                tx.commit();
            }

            map = checkAndGetAll(false, cache, keys, SCAN, GET);

            for (int k = 0; k < KEYS; k++) {
                if (k % 2 == 0)
                    assertEquals(v, map.get(k));
                else
                    assertNull(map.get(k));
            }

            assertEquals(KEYS / 2, map.size());

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                map = checkAndGetAll(true, cache, keys, SCAN, GET);

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
    public void testPutRemoveSimple() throws Exception {
        putRemoveSimple(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoveSimple_LargeKeys() throws Exception {
        putRemoveSimple(true);
    }

    /**
     * @param largeKeys {@code True} to use large keys (not fitting in single page).
     * @throws Exception If failed.
     */
    private void putRemoveSimple(boolean largeKeys) throws Exception {
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
                assertNull(checkAndGet(false, cache, key, SCAN, GET));
                assertTrue(checkAndGetAll(false, cache, F.asSet(key), SCAN, GET).isEmpty());
            }
            else {
                assertEquals(i, checkAndGet(false, cache, key, SCAN, GET));

                Map<Object, Object> res = checkAndGetAll(false, cache, F.asSet(key), SCAN, GET);

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
            assertEquals(e.getValue(), checkAndGet(false, cache, e.getKey(), SCAN, GET));

        Map<Object, Object> res = checkAndGetAll(false, cache, expVals.keySet(), SCAN, GET);

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

                        Map<Object, Object> res = checkAndGetAll(false, cache, keys, SCAN, GET);

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
        fail("https://issues.apache.org/jira/browse/IGNITE-9470");

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
                if (block && msg instanceof MvccAckRequestTx) {
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
                Map<Object, Object> res = checkAndGetAll(false, cache, F.asSet(1, 2, 3), SCAN, GET);

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

        Map<Object, Object> res = checkAndGetAll(false, cache, F.asSet(1, 2, 3), SCAN, GET);

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

                res = checkAndGetAll(false, cache, keys, SCAN, GET);

                assertEquals(val, res.get(key1));
                assertEquals(val, res.get(key2));

                clientSpi.stopBlock(true);

                fut.get();

                res = checkAndGetAll(false, cache, keys, SCAN, GET);

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
                for (boolean inTx : vals) {
                    cleanupWaitsForGet1(otherPuts, putOnStart, inTx);

                    afterTest();
                }
            }
        }
    }

    /**
     * @param otherPuts {@code True} to update unrelated keys to increment mvcc counter.
     * @param putOnStart {@code True} to put data in cache before getAll.
     * @param inTx {@code True} to read inside transaction.
     * @throws Exception If failed.
     */
    private void cleanupWaitsForGet1(boolean otherPuts, final boolean putOnStart, final boolean inTx) throws Exception {
        info("cleanupWaitsForGet [otherPuts=" + otherPuts +
            ", putOnStart=" + putOnStart +
            ", inTx=" + inTx + "]");

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

                Map<Integer, Integer> vals;

                if (inTx) {
                    try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        vals = checkAndGetAll(false, cache, F.asSet(key1, key2), SCAN, GET);

                        tx.rollback();
                    }
                }
                else
                    vals = checkAndGetAll(false, cache, F.asSet(key1, key2), SCAN, GET);

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

        Map<Integer, Integer> vals = checkAndGetAll(false, cache, F.asSet(key1, key2), SCAN, GET);

        assertEquals(2, vals.size());
        assertEquals(5, (Object)vals.get(key1));
        assertEquals(5, (Object)vals.get(key2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCleanupWaitsForGet2() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9470");
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
                if (msg instanceof MvccAckRequestTx)
                    doSleep(2000);
            }
        });

        crdSpi.closure(new IgniteBiInClosure<ClusterNode, Message>() {
            /** */
            private AtomicInteger cntr = new AtomicInteger();

            @Override public void apply(ClusterNode node, Message msg) {
                if (msg instanceof MvccSnapshotResponse) {
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
                    Map<Object, Object> vals1 = checkAndGetAll(false, cache, F.asSet(key1, key2), SCAN);
                    Map<Object, Object> vals2 = checkAndGetAll(false, cache, F.asSet(key1, key2), GET);

                    assertEquals(2, vals1.size());
                    assertEquals(2, vals2.size());
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
                if (!blocked && (msg instanceof MvccAckRequestTx)) {
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
                if (!blocked && (msg instanceof MvccSnapshotResponse)) {
                    blocked = true;

                    return true;
                }
                return false;
            }
        });

        final IgniteInternalFuture<?> getFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                final Map<Object, Object> res1 = checkAndGetAll(false, cache, F.asSet(key1, key2), SCAN);
                final Map<Object, Object> res2 = checkAndGetAll(false, cache, F.asSet(key1, key2), GET);

                assertEquals(2, res1.size());
                assertEquals(2, res2.size());

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
    public void testPutAllGetAll_SingleNode_GetAll() throws Exception {
        putAllGetAll(null, 1, 0, 0, 64, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode_SinglePartition_GetAll() throws Exception {
        putAllGetAll(null, 1, 0, 0, 1, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0_GetAll() throws Exception {
        putAllGetAll(null, 4, 2, 0, 64, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0_Persistence_GetAll() throws Exception {
        persistence = true;

        testPutAllGetAll_ClientServer_Backups0_GetAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_GetAll() throws Exception {
        putAllGetAll(null, 4, 2, 1, 64, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups2_GetAll() throws Exception {
        putAllGetAll(null, 4, 2, 2, 64, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_RestartCoordinator_GetAll() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 4, 2, 1, 64, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode_Scan() throws Exception {
        putAllGetAll(null, 1, 0, 0, 64, null, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_SingleNode_SinglePartition_Scan() throws Exception {
        putAllGetAll(null, 1, 0, 0, 1, null, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0_Scan() throws Exception {
        putAllGetAll(null, 4, 2, 0, 64, null, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups0_Persistence_Scan() throws Exception {
        persistence = true;

        testPutAllGetAll_ClientServer_Backups0_Scan();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_Scan() throws Exception {
        putAllGetAll(null, 4, 2, 1, 64, null, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups2_Scan() throws Exception {
        putAllGetAll(null, 4, 2, 2, 64, null, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_RestartCoordinator_Scan() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 4, 2, 1, 64, null, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAll_ClientServer_Backups1_Restart_Scan() throws Exception {
        putAllGetAll(RestartMode.RESTART_RND_SRV, 4, 2, 1, 64, null, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, null, false, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_WithRemoves_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, null, true, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, null, false, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_WithRemoves_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, null, true, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups0() throws Exception {
        accountsTxReadAll(4, 2, 0, 64, null, false, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_WithRemoves_ClientServer_Backups0() throws Exception {
        accountsTxReadAll(4, 2, 0, 64, null, true, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups1() throws Exception {
        accountsTxReadAll(4, 2, 1, 64, null, false, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_WithRemoves_ClientServer_Backups1() throws Exception {
        accountsTxReadAll(4, 2, 1, 64, null, true, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64, null, false, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll_WithRemoves_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64, null, true, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, null, false, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_WithRemoves_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, null, true, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, null, false, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_WithRemoves_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, null, true, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_ClientServer_Backups0() throws Exception {
        accountsTxReadAll(4, 2, 0, 64, null, false, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_WithRemoves_ClientServer_Backups0() throws Exception {
        accountsTxReadAll(4, 2, 0, 64, null, true, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_ClientServer_Backups1() throws Exception {
        accountsTxReadAll(4, 2, 1, 64, null, false, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_WithRemoves_ClientServer_Backups1() throws Exception {
        accountsTxReadAll(4, 2, 1, 64, null, true, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64, null, false, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxScan_WithRemoves_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64, null, true, SCAN, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxGetAllReadsSnapshot_SingleNode_SinglePartition() throws Exception {
        txReadsSnapshot(1, 0, 0, 1, GET);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxGetAllReadsSnapshot_ClientServer() throws Exception {
        txReadsSnapshot(4, 2, 1, 64, GET);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxScanReadsSnapshot_SingleNode_SinglePartition() throws Exception {
        txReadsSnapshot(1, 0, 0, 1, SCAN);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxScanReadsSnapshot_ClientServer() throws Exception {
        txReadsSnapshot(4, 2, 1, 64, SCAN);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param readMode Read mode.
     * @throws Exception If failed.
     */
    private void txReadsSnapshot(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        ReadMode readMode
    ) throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9470");

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

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);

                        try {
                            IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                            cnt++;

                            Integer id1 = rnd.nextInt(ACCOUNTS);
                            Integer id2 = rnd.nextInt(ACCOUNTS);

                            while (id1.equals(id2))
                                id2 = rnd.nextInt(ACCOUNTS);

                            if(id1 > id2) {
                                int tmp = id1;
                                id1 = id2;
                                id2 = tmp;
                            }

                            Set<Integer> keys = new HashSet<>();

                            keys.add(id1);
                            keys.add(id2);

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                MvccTestAccount a1;
                                MvccTestAccount a2;

                                Map<Integer, MvccTestAccount> accounts = checkAndGetAll(false, cache.cache, keys, readMode);

                                a1 = accounts.get(id1);
                                a2 = accounts.get(id2);

                                assertNotNull(a1);
                                assertNotNull(a2);

                                cache.cache.put(id1, new MvccTestAccount(a1.val + 1, 1));
                                cache.cache.put(id2, new MvccTestAccount(a2.val - 1, 1));

                                tx.commit();
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);
                        IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                        Map<Integer, MvccTestAccount> accounts = new HashMap<>();

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            int remaining = ACCOUNTS;

                            do {
                                int readCnt = rnd.nextInt(remaining) + 1;

                                Set<Integer> readKeys = new TreeSet<>();

                                for (int i = 0; i < readCnt; i++)
                                    readKeys.add(accounts.size() + i);

                                Map<Integer, MvccTestAccount> readRes =
                                    checkAndGetAll(false, cache.cache, readKeys, readMode);

                                assertEquals(readCnt, readRes.size());

                                accounts.putAll(readRes);

                                remaining = ACCOUNTS - accounts.size();
                            }
                            while (remaining > 0);

                            validateSum(accounts);

                            tx.commit();

                            cnt++;
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Reader finished, txs: " + cnt);
                }

                /**
                 * @param accounts Read accounts.
                 */
                private void validateSum(Map<Integer, MvccTestAccount> accounts) {
                    int sum = 0;

                    for (int i = 0; i < ACCOUNTS; i++) {
                        MvccTestAccount account = accounts.get(i);

                        assertNotNull(account);

                        sum += account.val;
                    }

                    assertEquals(ACCOUNTS * ACCOUNT_START_VAL, sum);
                }
            };

        readWriteTest(
            null,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            DFLT_TEST_TIME,
            null,
            init,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceScanConsistency_SingleNode_SinglePartition() throws Exception {
        operationsSequenceConsistency(1, 0, 0, 1, SCAN);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceScanConsistency_SingleNode() throws Exception {
        operationsSequenceConsistency(1, 0, 0, 64, SCAN);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceScanConsistency_ClientServer_Backups0() throws Exception {
        operationsSequenceConsistency(4, 2, 0, 64, SCAN);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceScanConsistency_ClientServer_Backups1() throws Exception {
        operationsSequenceConsistency(4, 2, 1, 64, SCAN);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceGetConsistency_SingleNode_SinglePartition() throws Exception {
        operationsSequenceConsistency(1, 0, 0, 1, GET);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceGetConsistency_SingleNode() throws Exception {
        operationsSequenceConsistency(1, 0, 0, 64, GET);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceGetConsistency_ClientServer_Backups0() throws Exception {
        operationsSequenceConsistency(4, 2, 0, 64, GET);
    }

    /**
     * @throws Exception If failed
     */
    public void testOperationsSequenceGetConsistency_ClientServer_Backups1() throws Exception {
        operationsSequenceConsistency(4, 2, 1, 64, GET);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param readMode Read mode.
     * @throws Exception If failed.
     */
    private void operationsSequenceConsistency(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        ReadMode readMode
    ) throws Exception {
        final int writers = 4;

        final int readers = 4;

        final long time = 10_000;

        final AtomicInteger keyCntr = new AtomicInteger();

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Integer, Value> cache = randomCache(caches, rnd);

                        try {
                            IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                            Integer key = keyCntr.incrementAndGet();

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                cache.cache.put(key, new Value(idx, cnt++));

                                tx.commit();
                            }

                            if (key > 100_000)
                                break;
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set keys = new HashSet();

                    while (!stop.get()) {
                        TestCache<Integer, Value> cache = randomCache(caches, rnd);

                        try {
                            Map<Integer, TreeSet<Integer>> vals = new HashMap<>();

                            switch (readMode) {
                                case SCAN:
                                    for (Cache.Entry<Integer, Value> e : cache.cache) {
                                        Value val = e.getValue();

                                        assertNotNull(val);

                                        TreeSet<Integer> cntrs = vals.get(val.key);

                                        if (cntrs == null)
                                            vals.put(val.key, cntrs = new TreeSet<>());

                                        boolean add = cntrs.add(val.cnt);

                                        assertTrue(add);
                                    }

                                    break;

                                case GET:
                                    for (int i = keys.size(); i < keyCntr.get(); i++)
                                        keys.add(i);

                                    Iterable<Map.Entry<Integer, Value>> entries = cache.cache.getAll(keys).entrySet();

                                    for (Map.Entry<Integer, Value> e : entries) {
                                        Value val = e.getValue();

                                        assertNotNull(val);

                                        TreeSet<Integer> cntrs = vals.get(val.key);

                                        if (cntrs == null)
                                            vals.put(val.key, cntrs = new TreeSet<>());

                                        boolean add = cntrs.add(val.cnt);

                                        assertTrue(add);
                                    }

                                    break;

                                default:
                                    fail("Unsupported read mode: " + readMode.name() + '.');
                            }

                            for (TreeSet<Integer> readCntrs : vals.values()) {
                                for (int i = 0; i < readCntrs.size(); i++)
                                    assertTrue(readCntrs.contains(i));
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }
                }
            };

        readWriteTest(
            null,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            time,
            null,
            null,
            writer,
            reader);
    }

    /**
     * TODO IGNITE-5935 enable when recovery is implemented.
     *
     * @throws Exception If failed.
     */
    public void _testNodesRestartNoHang() throws Exception {
        final int srvs = 4;
        final int clients = 4;
        final int writers = 6;
        final int readers = 2;

        final int KEYS = 100_000;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Map<Integer, Integer> map = new TreeMap<>();
                    Set<Integer> keys = new LinkedHashSet();

                    int cnt = 0;

                    while (!stop.get()) {
                        int keysCnt = rnd.nextInt(32) + 1;

                        while (keys.size() < keysCnt) {
                            int key = rnd.nextInt(KEYS);

                            if(keys.add(key))
                                map.put(key, cnt);
                        }

                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        try {
                            IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                if (rnd.nextBoolean()) {
                                    Map<Integer, Integer> res = checkAndGetAll(false, cache.cache, map.keySet(),
                                        rnd.nextBoolean() ? GET : SCAN);

                                    assertNotNull(res);
                                }

                                cache.cache.putAll(map);

                                tx.commit();
                            }
                            catch (Exception e) {
                                Assert.assertTrue("Unexpected error: " + e, X.hasCause(e, ClusterTopologyException.class));
                            }
                        }
                        finally {
                            cache.readUnlock();

                            keys.clear();
                            map.clear();
                        }

                        cnt++;
                    }

                    info("Writer done, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    while (!stop.get()) {
                        int keyCnt = rnd.nextInt(64) + 1;

                        while (keys.size() < keyCnt)
                            keys.add(rnd.nextInt(KEYS));

                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        Map<Integer, Integer> map;

                        try {
                            map = checkAndGetAll(false, cache.cache, keys, rnd.nextBoolean() ? GET : SCAN);

                            assertNotNull(map);
                        }
                        finally {
                            cache.readUnlock();
                        }

                        keys.clear();
                    }
                }
            };

        readWriteTest(
            RestartMode.RESTART_RND_SRV,
            srvs,
            clients,
            1,
            256,
            writers,
            readers,
            DFLT_TEST_TIME,
            null,
            null,
            writer,
            reader);

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
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

        TestRecordingCommunicationSpi.spi(client).blockMessages(MvccAckRequestQueryCntr.class,
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
    public void testRebalanceSimple() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9451");

        Ignite srv0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = (IgniteCache)srv0.createCache(
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

        resMap = checkAndGetAll(false, cache, map.keySet(), GET, SCAN);

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

        resMap = checkAndGetAll(false, cache, map.keySet(), GET, SCAN);

        assertEquals(map.size(), map.size());

        for (int i = 0; i < map.size(); i++)
            assertEquals(i + 2, (Object)resMap.get(i));

        // Run fake transaction
        try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Integer val = cache.get(0);

            cache.put(0, val);

            tx.commit();
        }

        resMap = checkAndGetAll(false, cache, map.keySet(), GET, SCAN);

        assertEquals(map.size(), map.size());

        for (int i = 0; i < map.size(); i++)
            assertEquals(i + 2, (Object)resMap.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceWithRemovedValuesSimple() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9451");

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
    public void testTxPrepareFailureSimplePessimisticTx() throws Exception {
        testSpi = true;

        startGrids(3);

        client = true;

        final Ignite client = startGrid(3);

        final IgniteCache cache = client.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT));

        final Integer key1 = primaryKey(jcache(1));
        final Integer key2 = primaryKey(jcache(2));

        TestRecordingCommunicationSpi srv1Spi = TestRecordingCommunicationSpi.spi(ignite(1));

        srv1Spi.blockMessages(GridNearTxPrepareResponse.class, client.name());

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

        GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    return srv1Spi.hasBlockedMessages() || fut.isDone() && fut.error() != null;
                }
            }, 10_000
        );

        if (fut.isDone())
            fut.get(); // Just to fail with future error.

        assertFalse(fut.isDone());

        stopGrid(1);

        fut.get();

        assertNull(cache.get(key1));
        assertNull(cache.get(key2));

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key1, 1);
            cache.put(key2, 2);

            tx.commit();
        }

        assertEquals(1, checkAndGet(false, cache, key1, GET, SCAN));
        assertEquals(2, checkAndGet(false, cache, key2, GET, SCAN));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccCoordinatorChangeSimple() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9722");

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
                Map<Object, Object> res = checkAndGetAll(false, node.cache(cacheName), vals.keySet(), SCAN, GET);

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

            if (persistence && i == 0)
                ignite(i).active(true);

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
     * @throws Exception If failed.
     */
    public void testMvccCoordinatorInfoConsistency_Persistence() throws Exception {
        persistence = true;

        testMvccCoordinatorInfoConsistency();
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
            MvccCoordinator crd0 = mvccProcessor(node).currentCoordinator();

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
                writeAllByMode(cache, vals, PUT, INTEGER_CODEC);

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
                            // TODO IGNITE-6754 add SQL and SCAN support.
                            Map<Integer, Integer> res = readAllByMode(node.cache(cacheName), vals.keySet(), GET, INTEGER_CODEC);

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
                    return msg instanceof MvccSnapshotResponse;
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

        final int KEYS = 1_000;

        Map<Object, Object> data = new HashMap<>();

        try (IgniteDataStreamer<Integer, Integer> streamer = node.dataStreamer(cache.getName())) {
            for (int i = 0; i < KEYS; i++) {
                streamer.addData(i, i);

                data.put(i, i);
            }
        }

        checkValues(data, cache);

        checkCacheData(data, cache.getName());

        checkPutGet(F.asList(cache.getName()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_SingleNode_SinglePartition_Get() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 1, 0, 0, 1, 10_000, null, GET, PUT, null);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_SingleNode_Get() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 1, 0, 0, 64, 10_000, null, GET, PUT, null);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_SingleNode_SinglePartition_Scan() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 1, 0, 0, 1, 10_000, null, SCAN, PUT, null);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_SingleNode_Scan() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 1, 0, 0, 64, 10_000, null, SCAN, PUT, null);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_ClientServer_Backups2_Get() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 4, 2, 2, DFLT_PARTITION_COUNT, 10_000, null, GET, PUT, null);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdate_N_Objects_ClientServer_Backups1_Scan() throws Exception {
        int[] nValues = {3, 5, 10};

        for (int n : nValues) {
            updateNObjectsTest(n, 2, 1, 1, DFLT_PARTITION_COUNT, 10_000, null, SCAN, PUT, null);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testImplicitPartsScan_SingleNode_SinglePartition() throws Exception {
        doImplicitPartsScanTest(1, 0, 0, 1, 10_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testImplicitPartsScan_SingleNode() throws Exception {
        doImplicitPartsScanTest(1, 0, 0, 64, 10_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testImplicitPartsScan_ClientServer_Backups0() throws Exception {
        doImplicitPartsScanTest(4, 2, 0, 64, 10_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testImplicitPartsScan_ClientServer_Backups1() throws Exception {
        doImplicitPartsScanTest(4, 2, 1, 64, 10_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testImplicitPartsScan_ClientServer_Backups2() throws Exception {
        doImplicitPartsScanTest(4, 2, 2, 64, 10_000);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param time Test time.
     * @throws Exception If failed.
     */
    private void doImplicitPartsScanTest(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        long time) throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9470");

        final int KEYS_PER_PART = 20;

        final int writers = 4;

        final int readers = 4;

        Map<Integer, List<Integer>> keysByParts = new HashMap<>();

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                for (int i = 0; i < cacheParts; i++) {
                    List<Integer> keys = new ArrayList<>();

                    keysByParts.put(i, keys);
                }

                Affinity aff = affinity(cache);

                int cntr = 0;
                int key = 0;

                while (cntr < KEYS_PER_PART * cacheParts) {
                    int part = aff.partition(key);

                    List<Integer> keys = keysByParts.get(part);

                    if (keys.size() < KEYS_PER_PART) {
                        keys.add(key);

                        cntr++;
                    }

                    key++;
                }

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    for (List<Integer> keys : keysByParts.values())
                        for (Integer k : keys)
                            cache.put(k, new MvccTestAccount(0, 1));

                    tx.commit();
                }
            }
        };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        int part = rnd.nextInt(cacheParts);

                        List<Integer> partKeys = keysByParts.get(part);

                        TestCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);
                        IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                        Integer k1 = partKeys.get(rnd.nextInt(KEYS_PER_PART));
                        Integer k2 = partKeys.get(rnd.nextInt(KEYS_PER_PART));

                        while (k1.equals(k2))
                            k2 = partKeys.get(rnd.nextInt(KEYS_PER_PART));

                        if(k1 > k2) {
                            int tmp = k1;
                            k1 = k2;
                            k2 = tmp;
                        }

                        TreeSet<Integer> keys = new TreeSet<>();

                        keys.add(k1);
                        keys.add(k2);

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Map<Integer, MvccTestAccount> accs = cache.cache.getAll(keys);

                            MvccTestAccount acc1 = accs.get(k1);
                            MvccTestAccount acc2 = accs.get(k2);

                            assertNotNull(acc1);
                            assertNotNull(acc2);

                            cache.cache.put(k1, new MvccTestAccount(acc1.val + 1, acc1.updateCnt + 1));
                            cache.cache.put(k2, new MvccTestAccount(acc2.val - 1, acc2.updateCnt + 1));

                            tx.commit();
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        int part = rnd.nextInt(cacheParts);

                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        try {
                            Affinity aff = affinity(cache.cache);

                            ScanQuery<Integer, MvccTestAccount> qry = new ScanQuery<>(part);

                            List<Cache.Entry<Integer, MvccTestAccount>> res = cache.cache.query(qry).getAll();

                            int sum = 0;

                            for (Cache.Entry<Integer, MvccTestAccount> entry : res) {
                                Integer key = entry.getKey();
                                MvccTestAccount acc = entry.getValue();

                                assertEquals(part, aff.partition(key));

                                sum += acc.val;
                            }

                            assertEquals(0, sum);

                        }
                        finally {
                            cache.readUnlock();
                        }

                        if (idx == 0) {
                            cache = randomCache(caches, rnd);

                            try {
                                ScanQuery<Integer, MvccTestAccount> qry = new ScanQuery<>();

                                List<Cache.Entry<Integer, MvccTestAccount>> res = cache.cache.query(qry).getAll();

                                int sum = 0;

                                for (Cache.Entry<Integer, MvccTestAccount> entry : res) {
                                    Integer key = entry.getKey();
                                    MvccTestAccount acc = entry.getValue();

                                    sum += acc.val;
                                }

                                assertEquals(0, sum);
                            }
                            finally {
                                cache.readUnlock();
                            }
                        }
                    }
                }
            };

        readWriteTest(
            null,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            time,
            null,
            init,
            writer,
            reader);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSize() throws Exception {
        Ignite node = startGrid(0);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        assertEquals(cache.size(), 0);

        final int KEYS = 10;

        // Initial put.
        for (int i = 0; i < KEYS; i++) {
            final Integer key = i;

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key, i);

                tx.commit();
            }

            assertEquals(i + 1, cache.size());
        }

        // Update.
        for (int i = 0; i < KEYS; i++) {
            final Integer key = i;

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key, i);

                tx.commit();
            }

            assertEquals(KEYS, cache.size());
        }

        int size = KEYS;

        // Remove.
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

        // Check rollback create.
        for (int i = 0; i < KEYS; i++) {
            if (i % 2 == 0) {
                final Integer key = i;

                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key, i);

                    tx.rollback();
                }

                assertEquals(size, cache.size());
            }
        }

        // Check rollback update.
        for (int i = 0; i < KEYS; i++) {
            final Integer key = i;

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key, -1);

                tx.rollback();
            }

            assertEquals(size, cache.size());
        }

        // Check rollback remove.
        for (int i = 0; i < KEYS; i++) {
            final Integer key = i;

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.remove(key);

                tx.rollback();
            }

            assertEquals(size, cache.size());
        }

        // Restore original state.
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

        // Check state.
        for (int i = 0; i < KEYS; i++)
            assertEquals(i, cache.get(i));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInternalApi() throws Exception {
        Ignite node = startGrid(0);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        GridCacheContext cctx =
            ((IgniteKernal)node).context().cache().context().cacheContext(CU.cacheId(cache.getName()));

        MvccProcessorImpl crd = mvccProcessor(node);

        // Start query to prevent cleanup.
        IgniteInternalFuture<MvccSnapshot> fut = crd.requestSnapshotAsync();

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

            List<IgniteBiTuple<Object, MvccVersion>> vers = cctx.offheap().mvccAllVersions(cctx, key0);

            assertEquals(10, vers.size());

            CacheDataRow row = cctx.offheap().read(cctx, key0);

            Object val = ((CacheObject)vers.get(0).get1()).value(cctx.cacheObjectContext(), false);

            checkRow(cctx, row, key0, val);

            for (IgniteBiTuple<Object, MvccVersion> ver : vers) {
                MvccVersion cntr = ver.get2();

                MvccSnapshot readVer =
                    new MvccSnapshotWithoutTxs(cntr.coordinatorVersion(), cntr.counter(), Integer.MAX_VALUE, 0);

                row = cctx.offheap().mvccRead(cctx, key0, readVer);

                Object verVal = ((CacheObject)ver.get1()).value(cctx.cacheObjectContext(), false);

                checkRow(cctx, row, key0, verVal);
            }

            checkRow(cctx,
                cctx.offheap().mvccRead(cctx, key0, version(vers.get(0).get2().coordinatorVersion() + 1, 1)),
                key0,
                val);

            checkRow(cctx,
                cctx.offheap().mvccRead(cctx, key0, version(vers.get(0).get2().coordinatorVersion(), vers.get(0).get2().counter() + 1)),
                key0,
                val);

            MvccSnapshotResponse ver = version(vers.get(0).get2().coordinatorVersion(), 100000);

            for (int v = 0; v < vers.size(); v++) {
                MvccVersion cntr = vers.get(v).get2();

                ver.addTx(cntr.counter());

                row = cctx.offheap().mvccRead(cctx, key0, ver);

                if (v == vers.size() - 1)
                    assertNull(row);
                else {
                    Object nextVal = ((CacheObject)vers.get(v + 1).get1()).value(cctx.cacheObjectContext(), false);

                    checkRow(cctx, row, key0, nextVal);
                }
            }
        }

        KeyCacheObject key = cctx.toCacheKeyObject(KEYS);

        cache.put(key, 0);

        cache.remove(key);

        cctx.offheap().mvccRemoveAll((GridCacheMapEntry)cctx.cache().entryEx(key));

        crd.ackQueryDone(fut.get(), MVCC_TRACKER_ID_NA);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpiration() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7311");

        final IgniteEx node = startGrid(0);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, 64));

        final IgniteCache expiryCache =
            cache.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1)));

        for (int i = 0; i < 10; i++)
            expiryCache.put(1, i);

        assertTrue("Failed to wait for expiration", GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expiryCache.localPeek(1) == null;
            }
        }, 5000));

        for (int i = 0; i < 11; i++) {
            if (i % 2 == 0)
                expiryCache.put(1, i);
            else
                expiryCache.remove(1);
        }

        assertTrue("Failed to wait for expiration", GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expiryCache.localPeek(1) == null;
            }
        }, 5000));

        expiryCache.put(1, 1);

        assertTrue("Failed to wait for expiration", GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    GridCacheContext cctx = node.context().cache().context().cacheContext(CU.cacheId(DEFAULT_CACHE_NAME));

                    KeyCacheObject key = cctx.toCacheKeyObject(1);

                    return cctx.offheap().read(cctx, key) == null;
                }
                catch (Exception e) {
                    fail();

                    return false;
                }
            }
        }, 5000));
    }

    /**
     * @throws Exception If failed.
     */
    public void testChangeExpireTime() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7311");

        final IgniteEx node = startGrid(0);

        IgniteCache cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, 64));

        cache.put(1, 1);

        final IgniteCache expiryCache =
            cache.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1)));

        expiryCache.get(1);
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
    private MvccSnapshotResponse version(long crdVer, long cntr) {
        MvccSnapshotResponse res = new MvccSnapshotResponse();

        res.init(0, crdVer, cntr, MvccUtils.MVCC_START_OP_CNTR, MvccUtils.MVCC_COUNTER_NA, 0);

        return res;
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
     * Checks values obtained with different read modes.
     * And returns value in case of it's equality for all read modes.
     * Do not use in tests with writers contention.
     *
     * // TODO remove inTx flag in IGNITE-6938
     * @param inTx Flag whether current read is inside transaction.
     * This is because reads can't see writes made in current transaction.
     * @param cache Cache.
     * @param key Key.
     * @param readModes Read modes to check.
     * @return Value.
     */
    private Object checkAndGet(boolean inTx, IgniteCache cache, Object key, ReadMode... readModes) {
        assert readModes != null && readModes.length > 0;

        if (inTx)
            return getByReadMode(inTx, cache, key, GET);

        Object prevVal = null;

        for (int i = 0; i < readModes.length; i++) {
            ReadMode readMode = readModes[i];

            Object curVal = getByReadMode(inTx, cache, key, readMode);

            if (i == 0)
                prevVal = curVal;
            else {
                assertEquals("Different results on " + readModes[i - 1].name() + " and " +
                    readMode.name() + " read modes.", prevVal, curVal);

                prevVal = curVal;
            }
        }

        return prevVal;
    }

    /**
     * Reads value from cache for the given key using given read mode.
     *
     * // TODO IGNITE-6938 remove inTx flag
     * // TODO IGNITE-6739 add SQL-get support "select _key, _val from cache where _key = key"
     * @param inTx Flag whether current read is inside transaction.
     * This is because reads can't see writes made in current transaction.
     * @param cache Cache.
     * @param key Key.
     * @param readMode Read mode.
     * @return Value.
     */
    private Object getByReadMode(boolean inTx, IgniteCache cache, final Object key, ReadMode readMode) {

        // TODO Remove in IGNITE-6938
        if (inTx)
            readMode = GET;

        switch (readMode) {
            case GET:
                return cache.get(key);

            case SCAN:
                List res = cache.query(new ScanQuery(new IgniteBiPredicate() {
                    @Override public boolean apply(Object k, Object v) {
                        return k.equals(key);
                    }
                })).getAll();

                assertTrue(res.size() <= 1);

                return res.isEmpty() ? null : ((IgniteBiTuple)res.get(0)).getValue();

            default:
                throw new IgniteException("Unsupported read mode: " + readMode);
        }
    }

    /**
     * Checks values obtained with different read modes.
     * And returns value in case of it's equality for all read modes.
     * Do not use in tests with writers contention.
     *
     * // TODO remove inTx flag in IGNITE-6938
     * @param inTx Flag whether current read is inside transaction.
     * This is because reads can't see writes made in current transaction.
     * @param cache Cache.
     * @param keys Key.
     * @param readModes Read modes to check.
     * @return Value.
     */
    private Map checkAndGetAll(boolean inTx, IgniteCache cache, Set keys, ReadMode... readModes) {
        assert readModes != null && readModes.length > 0;

        if (inTx)
            return getAllByReadMode(inTx, cache, keys, GET);

        Map prevVal = null;

        for (int i = 0; i < readModes.length; i++) {
            ReadMode readMode = readModes[i];

            Map curVal = getAllByReadMode(inTx, cache, keys, readMode);

            if (i == 0)
                prevVal = curVal;
            else {
                assertEquals("Different results on read modes " + readModes[i - 1] + " and " +
                    readMode.name(), prevVal, curVal);

                prevVal = curVal;
            }
        }

        return prevVal;
    }

    /**
     * Reads value from cache for the given key using given read mode.
     *
     * // TODO IGNITE-6938 remove inTx flag
     * // TODO IGNITE-6739 add SQL-get support "select _key, _val from cache where _key in ... keySet"
     * @param inTx Flag whether current read is inside transaction.
     * This is because reads can't see writes made in current transaction.
     * @param cache Cache.
     * @param keys Key.
     * @param readMode Read mode.
     * @return Value.
     */
    private Map getAllByReadMode(boolean inTx, IgniteCache cache, Set keys, ReadMode readMode) {

        // TODO Remove in IGNITE-6938
        if (inTx)
            readMode = GET;

        switch (readMode) {
            case GET:
                return cache.getAll(keys);

            case SCAN:
                Map res = (Map)cache.query(new ScanQuery(new IgniteBiPredicate() {
                    @Override public boolean apply(Object k, Object v) {
                        return keys.contains(k);
                    }
                })).getAll()
                    .stream()
                    .collect(Collectors.toMap(v -> ((IgniteBiTuple)v).getKey(), v -> ((IgniteBiTuple)v).getValue()));

                assertTrue(res.size() <= keys.size());

                return res;

            default:
                throw new IgniteException("Unsupported read mode: " + readMode);
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
