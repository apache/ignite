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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
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
 */
public class CacheMvccTransactionsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRVS = 4;

    /** */
    private boolean client;

    /** */
    private boolean testSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        verifyCoordinatorInternalState();

        stopAllGrids();
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
    public void testMyUpdatesAreVisible() throws Exception {
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
    public void testPartialCommitGetAll() throws Exception {
        testSpi = true;

        startGrids(2);

        client = true;

        final Ignite ignite = startGrid(3);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache =
            ignite.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 16));

        final Integer key1 = primaryKey(ignite(0).cache(cache.getName()));
        final Integer key2 = primaryKey(ignite(1).cache(cache.getName()));

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

                clientSpi.stopBlock(true);

                fut.get();

                res = cache.getAll(keys);

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
    public void testPutAllGetAll() throws Exception {
        final int RANGE = 20;

        final long time = 10_000;

        final int writers = 4;

        final int readers = 4;

        GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean> writer =
            new GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean>() {
            @Override public void apply(Integer idx, IgniteCache<Object, Object> cache, AtomicBoolean stop) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int min = idx * RANGE;
                int max = min + RANGE;

                info("Thread range [min=" + min + ", max=" + max + ']');

                Map<Integer, Integer> map = new HashMap<>();

                int v = idx * 1_000_000;

                while (!stop.get()) {
                    while (map.size() < RANGE)
                        map.put(rnd.nextInt(min, max), v);

                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.putAll(map);

                        tx.commit();
                    }

                    map.clear();

                    v++;
                }

                info("Writer done, updates: " + v);
            }
        };

        GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean> reader =
            new GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean>() {
                @Override public void apply(Integer idx, IgniteCache<Object, Object> cache, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    Map<Integer, Set<Integer>> uniqueReads = new HashMap<>();

                    for (int i = 0; i < writers; i++)
                        uniqueReads.put(i, new HashSet<Integer>());

                    while (!stop.get()) {
                        int range = rnd.nextInt(0, writers);

                        int min = range * RANGE;
                        int max = min + RANGE;

                        while (keys.size() < RANGE)
                            keys.add(rnd.nextInt(min, max));

                        Map<Object, Object> map = cache.getAll(keys);

                        assertTrue("Invalid map size: " + map.size(),
                            map.isEmpty() || map.size() == RANGE);

                        Integer val0 = null;

                        for (Map.Entry<Object, Object> e: map.entrySet()) {
                            Object val = e.getValue();

                            assertNotNull(val);

                            if (val0 == null) {
                                uniqueReads.get(range).add((Integer)val);

                                val0 = (Integer)val;
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

                    info("Reader done, unique reads: ");

                    for (Map.Entry<Integer, Set<Integer>> e : uniqueReads.entrySet())
                        info("Range [idx=" + e.getKey() + ", uniqueReads=" + e.getValue().size() + ']');
                }
            };

        readWriteTest(time, writers, readers, null, writer, reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxGetAll() throws Exception {
        final int ACCOUNTS = 20;

        final int ACCOUNT_START_VAL = 1000;

        final long time = 10_000;

        final int writers = 4;

        final int readers = 4;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                Map<Integer, Account> accounts = new HashMap<>();

                for (int i = 0; i < ACCOUNTS; i++)
                    accounts.put(i, new Account(ACCOUNT_START_VAL));

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.putAll(accounts);

                    tx.commit();
                }
            }
        };

        GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean> writer =
            new GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean>() {
                @Override public void apply(Integer idx, IgniteCache<Object, Object> cache, AtomicBoolean stop) {
                    final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        cnt++;

                        Integer id1 = rnd.nextInt(ACCOUNTS);
                        Integer id2 = rnd.nextInt(ACCOUNTS);

                        while (id1.equals(id2))
                            id2 = rnd.nextInt(ACCOUNTS);

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Account a1;
                            Account a2;

                            TreeSet<Integer> keys = new TreeSet<>();

                            keys.add(id1);
                            keys.add(id2);

                            Map<Object, Object> accounts = cache.getAll(keys);

                            a1 = (Account)accounts.get(id1);
                            a2 = (Account)accounts.get(id2);

                            assertNotNull(a1);
                            assertNotNull(a2);

                            cache.put(id1, new Account(a1.val + 1));
                            cache.put(id2, new Account(a2.val - 1));

                            tx.commit();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean> reader =
            new GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean>() {
                @Override public void apply(Integer idx, IgniteCache<Object, Object> cache, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    while (!stop.get()) {
                        while (keys.size() < ACCOUNTS)
                            keys.add(rnd.nextInt(ACCOUNTS));

                        Map<Object, Object> accounts = cache.getAll(keys);

                        assertEquals(ACCOUNTS, accounts.size());

                        int sum = 0;

                        for (int i = 0; i < ACCOUNTS; i++) {
                            Account account = (Account)accounts.get(i);

                            assertNotNull(account);

                            sum += account.val;
                        }

                        assertEquals(ACCOUNTS * ACCOUNT_START_VAL, sum);
                    }

                    if (idx == 0) {
                        Map<Object, Object> accounts = cache.getAll(keys);

                        int sum = 0;

                        for (int i = 0; i < ACCOUNTS; i++) {
                            Account account = (Account)accounts.get(i);

                            info("Account [id=" + i + ", val=" + account.val + ']');

                            sum += account.val;
                        }

                        info("Sum: " + sum);
                    }
                }
            };

        readWriteTest(time, writers, readers, init, writer, reader);
    }

    /**
     * @param time Test time.
     * @param writers Number of writers.
     * @param readers Number of readers.
     * @param init Optional init closure.
     * @param writer Writers threads closure.
     * @param reader Readers threads closure.
     * @throws Exception If failed.
     */
    private void readWriteTest(final long time,
        final int writers,
        final int readers,
        IgniteInClosure<IgniteCache<Object, Object>> init,
        final GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean> writer,
        final GridInClosure3<Integer, IgniteCache<Object, Object>, AtomicBoolean> reader) throws Exception {
        final Ignite ignite = startGrid(0);

        final IgniteCache<Object, Object> cache = ignite.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 1));

        if (init != null)
            init.apply(cache);

        final long stopTime = U.currentTimeMillis() + time;

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            final AtomicInteger writerIdx = new AtomicInteger();

            IgniteInternalFuture<?> writeFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        int idx = writerIdx.getAndIncrement();

                        writer.apply(idx, cache, stop);
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

                        reader.apply(idx, cache, stop);
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
        ccfg.setMvccEnabled(true);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    private void verifyCoordinatorInternalState() {
        for (Ignite node : G.allGrids()) {
            CacheCoordinatorsSharedManager crd = ((IgniteKernal)node).context().cache().context().coordinators();

            Map activeTxs = GridTestUtils.getFieldValue(crd, "activeTxs");

            assertTrue(activeTxs.isEmpty());

            Map cntrFuts = GridTestUtils.getFieldValue(crd, "verFuts");

            assertTrue(cntrFuts.isEmpty());

            Map ackFuts = GridTestUtils.getFieldValue(crd, "ackFuts");

            assertTrue(ackFuts.isEmpty());
        }
    }

    /**
     *
     */
    static class Account {
        /** */
        private final int val;

        /**
         * @param val Value.
         */
        public Account(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }
}
