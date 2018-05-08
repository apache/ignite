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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder).setForceServerMode(true);

        cfg.setClientMode(client);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param map Expected data.
     * @param keys Expected keys (if expected data is not specified).
     * @param clientCache Client cache.
     * @param expNodes Expected nodes number.
     * @throws Exception If failed.
     */
    private void checkData(final Map<Integer, Integer> map,
        final Set<Integer> keys,
        IgniteCache<?, ?> clientCache,
        final int expNodes)
        throws Exception
    {
        final List<Ignite> nodes = G.allGrids();

        final Affinity<Integer> aff = nodes.get(0).affinity(DEFAULT_CACHE_NAME);

        assertEquals(expNodes, nodes.size());

        boolean hasNearCache = clientCache.getConfiguration(CacheConfiguration.class).getNearConfiguration() != null;

        final Ignite nearCacheNode = hasNearCache ? clientCache.unwrap(Ignite.class) : null;

        boolean wait = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                try {
                    Set<Integer> keys0 = map != null ? map.keySet() : keys;

                    assertNotNull(keys0);

                    for (Integer key : keys0) {
                        GridCacheVersion ver = null;
                        Object val = null;

                        for (Ignite node : nodes) {
                            IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

                            boolean affNode = aff.isPrimaryOrBackup(node.cluster().localNode(), key);

                            Object val0 = cache.localPeek(key);

                            if (affNode || node == nearCacheNode) {
                                if (map != null)
                                    assertEquals("Unexpected value for " + node.name(), map.get(key), val0);
                                else
                                    assertNotNull("Unexpected value for " + node.name(), val0);

                                GridCacheAdapter cache0 = ((IgniteKernal)node).internalCache(DEFAULT_CACHE_NAME);

                                if (affNode && cache0.isNear())
                                    cache0 = ((GridNearCacheAdapter)cache0).dht();

                                GridCacheEntryEx entry = cache0.entryEx(key);

                                try {
                                    entry.unswap(true);

                                    assertNotNull("No entry [node=" + node.name() + ", key=" + key + ']', entry);

                                    GridCacheVersion ver0 = entry instanceof GridNearCacheEntry ?
                                        ((GridNearCacheEntry)entry).dhtVersion() : entry.version();

                                    assertNotNull("Null version [node=" + node.name() + ", key=" + key + ']', ver0);

                                    if (ver == null) {
                                        ver = ver0;
                                        val = val0;
                                    }
                                    else {
                                        assertEquals("Version check failed [node=" + node.name() +
                                            ", key=" + key +
                                            ", affNode=" + affNode +
                                            ", primary=" + aff.isPrimary(node.cluster().localNode(), key) + ']',
                                            ver0,
                                            ver);

                                        assertEquals("Value check failed [node=" + node.name() +
                                            ", key=" + key +
                                            ", affNode=" + affNode +
                                            ", primary=" + aff.isPrimary(node.cluster().localNode(), key) + ']',
                                            val0,
                                            val);
                                    }
                                }
                                finally {
                                    cache0.context().evicts().touch(entry,
                                        cache0.context().affinity().affinityTopologyVersion());
                                }
                            }
                            else
                                assertNull("Unexpected non-null value for " + node.name(), val0);
                        }
                    }
                }
                catch (AssertionError e) {
                    log.info("Check failed, will retry: " + e);

                    return false;
                }
                catch (Exception e) {
                    fail("Unexpected exception: " + e);
                }

                return true;
            }
        }, 10_000);

        assertTrue("Data check failed.", wait);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxPutAllMultinode() throws Exception {
        multinode(TRANSACTIONAL, TestType.PESSIMISTIC_TX);
    }

    /**
     * @param atomicityMode Atomicity mode cache.
     * @param testType Test type.
     * @throws Exception If failed.
     */
    private void multinode(CacheAtomicityMode atomicityMode, final TestType testType)
        throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(atomicityMode);
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

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger threadIdx = new AtomicInteger(0);

        final int THREADS = CLIENT_CNT * 3;

        final ConcurrentHashSet<Integer> putKeys = new ConcurrentHashSet<>();

        IgniteInternalFuture<?> fut;

        try {
            fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int clientIdx = threadIdx.getAndIncrement() % CLIENT_CNT;

                    Ignite ignite = clients.get(clientIdx);

                    assertTrue(ignite.configuration().isClientMode());

                    Thread.currentThread().setName("update-thread-" + ignite.name());

                    IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    boolean useTx = testType == TestType.OPTIMISTIC_TX ||
                        testType == TestType.OPTIMISTIC_SERIALIZABLE_TX ||
                        testType == TestType.PESSIMISTIC_TX;

                    if (useTx || testType == TestType.LOCK) {
                        assertEquals(TRANSACTIONAL,
                            cache.getConfiguration(CacheConfiguration.class).getAtomicityMode());
                    }

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cntr = 0;

                    while (!stop.get()) {
                        TreeMap<Integer, Integer> map = new TreeMap<>();

                        for (int i = 0; i < 100; i++) {
                            Integer key = rnd.nextInt(0, 1000);

                            map.put(key, rnd.nextInt());
                        }

                        try {
                            if (testType == TestType.LOCK) {
                                Lock lock = cache.lockAll(map.keySet());

                                lock.lock();

                                lock.unlock();
                            }
                            else {
                                if (useTx) {
                                    IgniteTransactions txs = ignite.transactions();

                                    TransactionConcurrency concurrency =
                                        testType == TestType.PESSIMISTIC_TX ? PESSIMISTIC : OPTIMISTIC;

                                    TransactionIsolation isolation = testType == TestType.OPTIMISTIC_SERIALIZABLE_TX ?
                                        SERIALIZABLE : REPEATABLE_READ;

                                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                                        cache.putAll(map);

                                        tx.commit();
                                    }
                                }
                                else
                                    cache.putAll(map);

                                putKeys.addAll(map.keySet());
                            }
                        }
                        catch (CacheException | IgniteException e) {
                            log.info("Operation failed, ignore: " + e);
                        }

                        if (++cntr % 100 == 0)
                            log.info("Iteration: " + cntr);

                        if (updateBarrier != null)
                            updateBarrier.await();
                    }

                    return null;
                }
            }, THREADS, "update-thread");

            long stopTime = System.currentTimeMillis() + 60_000;

            while (System.currentTimeMillis() < stopTime) {
                boolean restartClient = ThreadLocalRandom.current().nextBoolean();

                Integer idx = null;

                if (restartClient) {
                    log.info("Start client node.");

                    client = true;

                    IgniteEx ignite = startGrid(SRV_CNT + CLIENT_CNT);

                    IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    assertNotNull(cache);
                }
                else {
                    idx = ThreadLocalRandom.current().nextInt(0, SRV_CNT);

                    log.info("Stop server node: " + idx);

                    stopGrid(idx);
                }

                updateBarrier = new CyclicBarrier(THREADS + 1, new Runnable() {
                    @Override public void run() {
                        updateBarrier = null;
                    }
                });

                try {
                    updateBarrier.await(30_000, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException ignored) {
                    log.error("Failed to wait for update.");

                    for (Ignite ignite : G.allGrids())
                        ((IgniteKernal)ignite).dumpDebugInfo();

                    U.dumpThreads(log);

                    CyclicBarrier barrier0 = updateBarrier;

                    if (barrier0 != null)
                        barrier0.reset();

                    fail("Failed to wait for update.");
                }

                U.sleep(500);

                if (restartClient) {
                    log.info("Stop client node.");

                    stopGrid(SRV_CNT + CLIENT_CNT);
                }
                else {
                    log.info("Start server node: " + idx);

                    client = false;

                    startGrid(idx);
                }

                updateBarrier = new CyclicBarrier(THREADS + 1, new Runnable() {
                    @Override public void run() {
                        updateBarrier = null;
                    }
                });

                try {
                    updateBarrier.await(30_000, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException ignored) {
                    log.error("Failed to wait for update.");

                    for (Ignite ignite : G.allGrids())
                        ((IgniteKernal)ignite).dumpDebugInfo();

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

        fut.get(30_000);

        if (testType != TestType.LOCK)
            checkData(null, putKeys, grid(SRV_CNT).cache(DEFAULT_CACHE_NAME), SRV_CNT + CLIENT_CNT);
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
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                synchronized (this) {
                    if (recordCls != null && msg0.getClass().equals(recordCls))
                        recordedMsgs.add(msg0);

                    Set<UUID> blockNodes = blockCls.get(msg0.getClass());

                    if (F.contains(blockNodes, node.id())) {
                        log.info("Block message [node=" +
                            node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) + ", msg=" + msg0 + ']');

                        blockedMsgs.add(new T2<>(node, (GridIoMessage)msg));

                        return;
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
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

                    log.info("Send blocked message: [node=" +
                        node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) +
                        ", msg=" + msg.get2().message() + ']');

                    super.sendMessage(msg.get1(), msg.get2());
                }

                blockedMsgs.clear();
            }
        }
    }

    /**
     *
     */
    enum TestType {
        /** */
        PUT_ALL,

        /** */
        OPTIMISTIC_TX,

        /** */
        OPTIMISTIC_SERIALIZABLE_TX,

        /** */
        PESSIMISTIC_TX,

        /** */
        LOCK
    }
}
