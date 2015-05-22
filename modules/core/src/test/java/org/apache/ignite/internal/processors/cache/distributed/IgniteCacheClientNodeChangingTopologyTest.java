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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class IgniteCacheClientNodeChangingTopologyTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private CacheConfiguration ccfg;

    /** */
    private boolean client;

    /** */
    private volatile CyclicBarrier updateBarrier;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPutAllClockMode() throws Exception {
        atomicPutAll(CLOCK);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPutAllPrimaryMode() throws Exception {
        atomicPutAll(PRIMARY);
    }

    /**
     * @param writeOrder Write order.
     * @throws Exception If failed.
     */
    private void atomicPutAll(CacheAtomicWriteOrderMode writeOrder) throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(writeOrder);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        client = true;

        Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        // Block messages requests for both nodes.
        spi.blockMessages(GridNearAtomicUpdateRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearAtomicUpdateRequest.class, ignite1.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        assertEquals(writeOrder, cache.getConfiguration(CacheConfiguration.class).getAtomicWriteOrderMode());

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                cache.putAll(map);

                return null;
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        IgniteEx ignite3 = startGrid(3);

        log.info("Stop block1.");

        spi.stopBlock();

        putFut.get();

        checkData(map, cache, 4);

        ignite3.close();

        map.clear();

        for (int i = 0; i < 100; i++)
            map.put(i, i + 1);

        // Block messages requests for single node.
        spi.blockMessages(GridNearAtomicUpdateRequest.class, ignite0.localNode().id());

        putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                cache.putAll(map);

                return null;
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        startGrid(3);

        log.info("Stop block2.");

        spi.stopBlock();

        putFut.get();

        checkData(map, cache, 4);

        for (int i = 0; i < 100; i++)
            map.put(i, i + 2);

        cache.putAll(map);

        checkData(map, cache, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxPutAll() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        client = true;

        Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        spi.blockMessages(GridNearTxPrepareRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearTxPrepareRequest.class, ignite1.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                cache.putAll(map);

                return null;
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        IgniteEx ignite3 = startGrid(3);

        log.info("Stop block.");

        spi.stopBlock();

        putFut.get();

        checkData(map, cache, 4);

        map.clear();

        for (int i = 0; i < 100; i++)
            map.put(i, i + 1);

        cache.putAll(map);

        checkData(map, cache, 4);
    }
    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTx() throws Exception {
        pessimisticTx(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxNearEnabled() throws Exception {
        pessimisticTx(new NearCacheConfiguration());
    }

    /**
     * @param nearCfg Near cache configuration.
     * @throws Exception If failed.
     */
    private void pessimisticTx(NearCacheConfiguration nearCfg) throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setNearConfiguration(nearCfg);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        client = true;

        final Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        spi.blockMessages(GridNearLockRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite1.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                try (Transaction tx = ignite2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.putAll(map);

                    tx.commit();
                }

                return null;
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        IgniteEx ignite3 = startGrid(3);

        log.info("Stop block1.");

        spi.stopBlock();

        putFut.get();

        checkData(map, cache, 4);

        ignite3.close();

        for (int i = 0; i < 100; i++)
            map.put(i, i + 1);

        spi.blockMessages(GridNearLockRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite1.localNode().id());

        putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                try (Transaction tx = ignite2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    for (Map.Entry<Integer, Integer> e : map.entrySet())
                        cache.put(e.getKey(), e.getValue());

                    tx.commit();
                }

                return null;
            }
        });

        ignite3 = startGrid(3);

        log.info("Stop block2.");

        spi.stopBlock();

        putFut.get();

        checkData(map, cache, 4);

        for (int i = 0; i < 100; i++)
            map.put(i, i + 2);

        try (Transaction tx = ignite2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.putAll(map);

            tx.commit();
        }

        checkData(map, cache, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testLock() throws Exception {
        lock(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockNearEnabled() throws Exception {
        lock(new NearCacheConfiguration());
    }

    /**
     * @param nearCfg Near cache configuration.
     * @throws Exception If failed.
     */
    private void lock(NearCacheConfiguration nearCfg) throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setNearConfiguration(nearCfg);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        client = true;

        final Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 100; i++)
            keys.add(i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        spi.blockMessages(GridNearLockRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite1.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        final CountDownLatch lockedLatch = new CountDownLatch(1);

        final CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<Lock> lockFut = GridTestUtils.runAsync(new Callable<Lock>() {
            @Override public Lock call() throws Exception {
                Thread.currentThread().setName("put-thread");

                Lock lock = cache.lockAll(keys);

                lock.lock();

                log.info("Locked");

                lockedLatch.countDown();

                unlockLatch.await();

                lock.unlock();

                return lock;
            }
        });

        client = false;

        IgniteEx ignite3 = startGrid(3);

        log.info("Stop block.");

        assertEquals(1, lockedLatch.getCount());

        spi.stopBlock();

        assertTrue(lockedLatch.await(3000, TimeUnit.MILLISECONDS));

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        for (Integer key : keys) {
            Lock lock = cache0.lock(key);

            assertFalse(lock.tryLock());
        }

        unlockLatch.countDown();

        lockFut.get();

        for (Integer key : keys) {
            Lock lock = cache0.lock(key);

            assertTrue(lock.tryLock());

            lock.unlock();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxMessageClientFirstFlag() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        client = true;

        Ignite ignite3 = startGrid(3);

        assertTrue(ignite3.configuration().isClientMode());

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite3.configuration().getCommunicationSpi();

        spi.record(GridNearLockRequest.class);

        IgniteCache<Integer, Integer> cache = ignite3.cache(null);

        try (Transaction tx = ignite3.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 1);
            cache.put(2, 2);
            cache.put(3, 3);

            tx.commit();
        }

        checkClientLockMessages(spi.recordedMessages(), 3);

        Map<Integer, Integer> map = new HashMap<>();

        map.put(4, 4);
        map.put(5, 5);
        map.put(6, 6);
        map.put(7, 7);

        try (Transaction tx = ignite3.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.putAll(map);

            tx.commit();
        }

        checkClientLockMessages(spi.recordedMessages(), 4);

        spi.record(null);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        spi0.record(GridNearLockRequest.class);

        List<Integer> keys = primaryKeys(ignite1.cache(null), 3, 0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache0.put(keys.get(0), 0);
            cache0.put(keys.get(1), 1);
            cache0.put(keys.get(2), 2);

            tx.commit();
        }

        List<Object> msgs = spi0.recordedMessages();

        assertEquals(3, msgs.size());

        for (Object msg : msgs)
            assertFalse(((GridNearLockRequest)msg).firstClientRequest());
    }

    /**
     * @param msgs Messages.
     * @param expCnt Expected number of messages.
     */
    private void checkClientLockMessages(List<Object> msgs, int expCnt) {
        assertEquals(expCnt, msgs.size());

        assertTrue(((GridNearLockRequest)msgs.get(0)).firstClientRequest());

        for (int i = 1; i < msgs.size(); i++)
            assertFalse(((GridNearLockRequest)msgs.get(i)).firstClientRequest());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxMessageClientFirstFlag() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        client = true;

        Ignite ignite3 = startGrid(3);

        assertTrue(ignite3.configuration().isClientMode());

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite3.configuration().getCommunicationSpi();

        IgniteCache<Integer, Integer> cache = ignite3.cache(null);

        List<Integer> keys0 = primaryKeys(ignite0.cache(null), 2, 0);
        List<Integer> keys1 = primaryKeys(ignite1.cache(null), 2, 0);
        List<Integer> keys2 = primaryKeys(ignite2.cache(null), 2, 0);

        LinkedHashMap<Integer, Integer> map = new LinkedHashMap<>();

        map.put(keys0.get(0), 1);
        map.put(keys1.get(0), 2);
        map.put(keys2.get(0), 3);
        map.put(keys0.get(1), 4);
        map.put(keys1.get(1), 5);
        map.put(keys2.get(1), 6);

        spi.record(GridNearTxPrepareRequest.class);

        try (Transaction tx = ignite3.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
            for (Map.Entry<Integer, Integer> e : map.entrySet())
                cache.put(e.getKey(), e.getValue());

            tx.commit();
        }

        checkClientPrepareMessages(spi.recordedMessages(), 6);

        checkData(map, cache, 4);

        cache.putAll(map);

        checkClientPrepareMessages(spi.recordedMessages(), 6);

        spi.record(null);

        checkData(map, cache, 4);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        spi0.record(GridNearTxPrepareRequest.class);

        cache0.putAll(map);

        spi0.record(null);

        List<Object> msgs = spi0.recordedMessages();

        assertEquals(4, msgs.size());

        for (Object msg : msgs)
            assertFalse(((GridNearTxPrepareRequest)msg).firstClientRequest());

        checkData(map, cache, 4);
    }

    /**
     * @param msgs Messages.
     * @param expCnt Expected number of messages.
     */
    private void checkClientPrepareMessages(List<Object> msgs, int expCnt) {
        assertEquals(expCnt, msgs.size());

        assertTrue(((GridNearTxPrepareRequest)msgs.get(0)).firstClientRequest());

        for (int i = 1; i < msgs.size(); i++)
            assertFalse(((GridNearTxPrepareRequest) msgs.get(i)).firstClientRequest());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockRemoveAfterClientFailed() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        client = true;

        Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        IgniteCache<Integer, Integer> cache2 = ignite2.cache(null);

        Lock lock2 = cache2.lock(0);

        lock2.lock();

        ignite2.close();

        IgniteCache<Integer, Integer> cache1 = ignite1.cache(null);

        Lock lock1 = cache1.lock(0);

        assertTrue(lock1.tryLock(5000, TimeUnit.MILLISECONDS));

        lock1.unlock();

        ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        cache2 = ignite2.cache(null);

        lock2 = cache2.lock(0);

        assertTrue(lock2.tryLock(5000, TimeUnit.MILLISECONDS));

        lock2.unlock();
    }

    /**
     * @param map Expected data.
     * @param clientCache Client cache.
     * @param expNodes Expected nodes number.
     * @throws Exception If failed.
     */
    private void checkData(final Map<Integer, Integer> map, IgniteCache<?, ?> clientCache, final int expNodes)
        throws Exception
    {
        final List<Ignite> nodes = G.allGrids();

        final Affinity<Integer> aff = nodes.get(0).affinity(null);

        assertEquals(expNodes, nodes.size());

        boolean hasNearCache = clientCache.getConfiguration(CacheConfiguration.class).getNearConfiguration() != null;

        final Ignite nearCacheNode = hasNearCache ? clientCache.unwrap(Ignite.class) : null;

        boolean wait = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                try {
                    for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                        Integer key = e.getKey();

                        for (Ignite node : nodes) {
                            IgniteCache<Integer, Integer> cache = node.cache(null);

                            if (aff.isPrimaryOrBackup(node.cluster().localNode(), key) || node == nearCacheNode)
                                assertEquals("Unexpected value for " + node.name(), e.getValue(), cache.localPeek(key));
                            else
                                assertNull("Unexpected non-null value for " + node.name(), cache.localPeek(key));
                        }
                    }
                }
                catch (AssertionError e) {
                    log.info("Check failed, will retry: " + e);

                    return false;
                }

                return true;
            }
        }, 10_000);

        assertTrue("Data check failed.", wait);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPrimaryPutAllMultinode() throws Exception {
        putAllMultinode(PRIMARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicClockPutAllMultinode() throws Exception {
        putAllMultinode(CLOCK ,false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxPutAllMultinode() throws Exception {
        putAllMultinode(null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxPutAllMultinode() throws Exception {
        putAllMultinode(null, true);
    }

    /**
     * @param atomicWriteOrder Write order if test atomic cache.
     * @param pessimisticTx {@code True} if use pessimistic tx.
     * @throws Exception If failed.
     */
    private void putAllMultinode(final CacheAtomicWriteOrderMode atomicWriteOrder, final boolean pessimisticTx)
        throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(atomicWriteOrder != null ? ATOMIC : TRANSACTIONAL);
        ccfg.setAtomicWriteOrderMode(atomicWriteOrder);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        final int SRV_CNT = 4;

        for (int i = 0; i < SRV_CNT; i++)
            startGrid(i);

        final int CLIENT_CNT = 4;

        final List<Ignite> clients = new ArrayList<>();

        client = true;

        for (int i = 0; i < CLIENT_CNT; i++) {
            Ignite ignite = startGrid(SRV_CNT + i);

            assertTrue(ignite.configuration().isClientMode());

            clients.add(ignite);
        }

        client = false;

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger threadIdx = new AtomicInteger(0);

        final int THREADS = CLIENT_CNT * 3;

        try {
            GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int clientIdx = threadIdx.getAndIncrement() % CLIENT_CNT;

                    Ignite ignite = clients.get(clientIdx);

                    assertTrue(ignite.configuration().isClientMode());

                    Thread.currentThread().setName("update-thread-" + ignite.name());

                    IgniteCache<Integer, Integer> cache = ignite.cache(null);

                    boolean useTx = atomicWriteOrder == null;

                    if (useTx) {
                        assertEquals(TRANSACTIONAL,
                            cache.getConfiguration(CacheConfiguration.class).getAtomicityMode());
                    }

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cntr = 0;

                    while (!stop.get()) {
                        TreeMap<Integer, Integer> map = new TreeMap<>();

                        for (int i = 0; i < 100; i++)
                            map.put(rnd.nextInt(0, 1000), i);

                        try {
                            if (useTx) {
                                IgniteTransactions txs = ignite.transactions();

                                TransactionConcurrency concurrency = pessimisticTx ? PESSIMISTIC : OPTIMISTIC;

                                try (Transaction tx = txs.txStart(concurrency, REPEATABLE_READ)) {
                                    cache.putAll(map);

                                    tx.commit();
                                }
                            }
                            else
                                cache.putAll(map);
                        }
                        catch (CacheException | IgniteException e) {
                            log.info("Update failed, ignore: " + e);
                        }

                        if (++cntr % 100 == 0)
                            log.info("Iteration: " + cntr);

                        if (updateBarrier != null)
                            updateBarrier.await();
                    }

                    return null;
                }
            }, THREADS, "update-thread");

            for (final Ignite ignite : clients) {

                futs.add(GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        Thread.currentThread().setName("update-" + ignite.name());

                        log.info("Start updates from node: " + ignite.name());


                        return null;
                    }
                }));
            }

            long stopTime = System.currentTimeMillis() + 2 * 60_000;

            while (System.currentTimeMillis() < stopTime) {
                int idx = ThreadLocalRandom.current().nextInt(0, SRV_CNT);

                log.info("Stop node: " + idx);

                stopGrid(idx);

                updateBarrier = new CyclicBarrier(THREADS + 1, new Runnable() {
                    @Override public void run() {
                        updateBarrier = null;
                    }
                });

                try {
                    updateBarrier.await(30_000, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e) {
                    log.info("Failed to wait for update.");

                    U.dumpThreads(log);

                    CyclicBarrier barrier0 = updateBarrier;

                    if (barrier0 != null)
                        barrier0.reset();

                    fail("Failed to wait for update.");
                }

                U.sleep(500);

                log.info("Start node: " + idx);

                startGrid(idx);

                updateBarrier = new CyclicBarrier(THREADS + 1, new Runnable() {
                    @Override public void run() {
                        updateBarrier = null;
                    }
                });

                try {
                    updateBarrier.await(30_000, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e) {
                    log.info("Failed to wait for update.");

                    U.dumpThreads(log);

                    CyclicBarrier barrier0 = updateBarrier;

                    if (barrier0 != null)
                        barrier0.reset();

                    fail("Failed to wait for update.");
                }

                U.sleep(500);
            }
        }
        finally {
            stop.set(true);
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private List<T2<ClusterNode, GridIoMessage>> blockedMsgs = new ArrayList<>();

        /** */
        private Map<Class<?>, Set<UUID>> blockCls = new HashMap<>();

        /** */
        private Class<?> recordCls;

        /** */
        private List<Object> recordedMsgs = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                synchronized (this) {
                    if (recordCls != null && msg0.getClass().equals(recordCls))
                        recordedMsgs.add(msg0);

                    Set<UUID> blockNodes = blockCls.get(msg0.getClass());

                    if (F.contains(blockNodes, node.id())) {
                        log.info("Block message [node=" + node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME) +
                            ", msg=" + msg0 + ']');

                        blockedMsgs.add(new T2<>(node, (GridIoMessage)msg));

                        return;
                    }
                }
            }

            super.sendMessage(node, msg);
        }

        /**
         * @param recordCls Message class to record.
         */
        void record(@Nullable Class<?> recordCls) {
            synchronized (this) {
                this.recordCls = recordCls;
            }
        }

        /**
         * @return Recorded messages.
         */
        List<Object> recordedMessages() {
            synchronized (this) {
                List<Object> msgs = recordedMsgs;

                recordedMsgs = new ArrayList<>();

                return msgs;
            }
        }

        /**
         * @param cls Message class.
         * @param nodeId Node ID.
         */
        void blockMessages(Class<?> cls, UUID nodeId) {
            synchronized (this) {
                Set<UUID> set = blockCls.get(cls);

                if (set == null) {
                    set = new HashSet<>();

                    blockCls.put(cls, set);
                }

                set.add(nodeId);
            }
        }

        /**
         *
         */
        void stopBlock() {
            synchronized (this) {
                blockCls.clear();

                for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
                    ClusterNode node = msg.get1();

                    log.info("Send blocked message: [node=" + node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME) +
                        ", msg=" + msg.get2().message() + ']');

                    super.sendMessage(msg.get1(), msg.get2());
                }
            }
        }
    }
}
