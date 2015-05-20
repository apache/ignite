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

        checkData(map, 4);

        ignite3.close();

        map.clear();

        for (int i = 0; i < 100; i++)
            map.put(i, i + 1);

        // Block messages requests for single node.
        spi.blockMessages(GridNearAtomicUpdateRequest.class, ignite0.localNode().id());

        putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
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

        checkData(map, 4);

        for (int i = 0; i < 100; i++)
            map.put(i, i + 2);

        cache.putAll(map);

        checkData(map, 4);
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

        for (int i = 0; i < 1; i++)
            map.put(i, i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        spi.blockMessages(GridNearTxPrepareRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearTxPrepareRequest.class, ignite1.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
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

        checkData(map, 4);
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
     * @param expNodes Expected nodes number.
     * @throws Exception If failed.
     */
    private void checkData(final Map<Integer, Integer> map, final int expNodes) throws Exception {
        final List<Ignite> nodes = G.allGrids();

        final Affinity<Integer> aff = nodes.get(0).affinity(null);

        assertEquals(expNodes, nodes.size());

        boolean wait = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                try {
                    for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                        Integer key = e.getKey();

                        for (Ignite node : nodes) {
                            IgniteCache<Integer, Integer> cache = node.cache(null);

                            if (aff.isPrimaryOrBackup(node.cluster().localNode(), key))
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

                updateBarrier.await(15_000, TimeUnit.MILLISECONDS);

                CyclicBarrier barrier0 = updateBarrier;

                if (barrier0 != null) {
                    log.info("Failed to wait for update.");

                    U.dumpThreads(log);

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

                updateBarrier.await(15_000, TimeUnit.MILLISECONDS);

                barrier0 = updateBarrier;

                if (barrier0 != null) {
                    log.info("Failed to wait for update.");

                    U.dumpThreads(log);

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

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                synchronized (this) {
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
