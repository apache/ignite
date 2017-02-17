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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

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
     * @throws Exception If failed.
     */
    public void testAtomicPutAllClockMode() throws Exception {
        atomicPut(CLOCK, true, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPutAllPrimaryMode() throws Exception {
        atomicPut(PRIMARY, true, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPutAllNearEnabledClockMode() throws Exception {
        atomicPut(CLOCK, true, new NearCacheConfiguration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPutAllNearEnabledPrimaryMode() throws Exception {
        atomicPut(PRIMARY, true, new NearCacheConfiguration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPutClockMode() throws Exception {
        atomicPut(CLOCK, false, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPutPrimaryMode() throws Exception {
        atomicPut(PRIMARY, false, null);
    }

    /**
     * @param writeOrder Write order.
     * @param putAll If {@code true} executes putAll.
     * @param nearCfg Near cache configuration.
     * @throws Exception If failed.
     */
    private void atomicPut(CacheAtomicWriteOrderMode writeOrder,
        final boolean putAll,
        @Nullable NearCacheConfiguration nearCfg) throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(writeOrder);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ccfg.setNearConfiguration(nearCfg);

        client = true;

        ccfg.setNearConfiguration(null);

        Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        final int KEYS = putAll ? 100 : 1;

        for (int i = 0; i < KEYS; i++)
            map.put(i, i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        // Block messages requests for both nodes.
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite1.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        assertEquals(writeOrder, cache.getConfiguration(CacheConfiguration.class).getAtomicWriteOrderMode());

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                if (putAll)
                    cache.putAll(map);
                else
                    cache.put(0, 0);

                return null;
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        IgniteEx ignite3 = startGrid(3);

        log.info("Stop block1.");

        spi.stopBlock();

        putFut.get();

        checkData(map, null, cache, 4);

        ignite3.close();

        map.clear();

        for (int i = 0; i < KEYS; i++)
            map.put(i, i + 1);

        // Block messages requests for single node.
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite0.localNode().id());

        putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                if (putAll)
                    cache.putAll(map);
                else
                    cache.put(0, 1);

                return null;
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        startGrid(3);

        log.info("Stop block2.");

        spi.stopBlock();

        putFut.get();

        checkData(map, null, cache, 4);

        for (int i = 0; i < KEYS; i++)
            map.put(i, i + 2);

        if (putAll)
            cache.putAll(map);
        else
            cache.put(0, 2);

        checkData(map, null, cache, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoRemapClockMode() throws Exception {
        atomicNoRemap(CLOCK);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoRemapPrimaryMode() throws Exception {
        atomicNoRemap(PRIMARY);
    }

    /**
     * @param writeOrder Write order.
     * @throws Exception If failed.
     */
    private void atomicNoRemap(CacheAtomicWriteOrderMode writeOrder) throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(writeOrder);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        client = true;

        Ignite ignite3 = startGrid(3);

        awaitPartitionMapExchange();

        assertTrue(ignite3.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        map.put(primaryKey(ignite0.cache(null)), 0);
        map.put(primaryKey(ignite1.cache(null)), 1);
        map.put(primaryKey(ignite2.cache(null)), 2);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite3.configuration().getCommunicationSpi();

        // Block messages requests for both nodes.
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite1.localNode().id());
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite2.localNode().id());

        spi.record(GridNearAtomicFullUpdateRequest.class);

        final IgniteCache<Integer, Integer> cache = ignite3.cache(null);

        assertEquals(writeOrder, cache.getConfiguration(CacheConfiguration.class).getAtomicWriteOrderMode());

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                cache.putAll(map);

                return null;
            }
        });

        IgniteEx ignite4 = startGrid(4);

        assertTrue(ignite4.configuration().isClientMode());

        assertFalse(putFut.isDone());

        log.info("Stop block.");

        spi.stopBlock();

        putFut.get();

        spi.record(null);

        checkData(map, null, cache, 5);

        List<Object> msgs = spi.recordedMessages();

        assertEquals(3, msgs.size());

        for (Object msg : msgs)
            assertTrue(((GridNearAtomicFullUpdateRequest)msg).clientRequest());

        map.put(primaryKey(ignite0.cache(null)), 3);
        map.put(primaryKey(ignite1.cache(null)), 4);
        map.put(primaryKey(ignite2.cache(null)), 5);

        cache.putAll(map);

        checkData(map, null, cache, 5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicGetAndPutClockMode() throws Exception {
        atomicGetAndPut(CLOCK);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicGetAndPutPrimaryMode() throws Exception {
        atomicGetAndPut(PRIMARY);
    }

    /**
     * @param writeOrder Write order.
     * @throws Exception If failed.
     */
    private void atomicGetAndPut(CacheAtomicWriteOrderMode writeOrder) throws Exception {
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

        ignite0.cache(null).put(0, 0);

        Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        map.put(0, 1);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        // Block messages requests for both nodes.
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearAtomicFullUpdateRequest.class, ignite1.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        assertEquals(writeOrder, cache.getConfiguration(CacheConfiguration.class).getAtomicWriteOrderMode());

        IgniteInternalFuture<Integer> putFut = GridTestUtils.runAsync(new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                Thread.currentThread().setName("put-thread");

                return cache.getAndPut(0, 1);
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        startGrid(3);

        log.info("Stop block.");

        spi.stopBlock();

        Integer old = putFut.get();

        checkData(map, null, cache, 4);

        assertEquals((Object)0, old);
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

        startGrid(3);

        log.info("Stop block.");

        spi.stopBlock();

        putFut.get();

        checkData(map, null, cache, 4);

        map.clear();

        for (int i = 0; i < 100; i++)
            map.put(i, i + 1);

        cache.putAll(map);

        checkData(map, null, cache, 4);
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

        awaitPartitionMapExchange();

        client = true;

        final Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        spi.blockMessages(GridNearLockRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite1.localNode().id());

        spi.record(GridNearLockRequest.class);

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

        spi.record(null);

        checkData(map, null, cache, 4);

        List<Object> msgs = spi.recordedMessages();

        assertTrue(((GridNearLockRequest)msgs.get(0)).firstClientRequest());
        assertTrue(((GridNearLockRequest)msgs.get(1)).firstClientRequest());

        for (int i = 2; i < msgs.size(); i++)
            assertFalse(((GridNearLockRequest)msgs.get(i)).firstClientRequest());

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

        startGrid(3);

        log.info("Stop block2.");

        spi.stopBlock();

        putFut.get();

        checkData(map, null, cache, 4);

        for (int i = 0; i < 100; i++)
            map.put(i, i + 2);

        try (Transaction tx = ignite2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.putAll(map);

            tx.commit();
        }

        checkData(map, null, cache, 4);
    }

    /**
     * Tries to find keys for two partitions: for one partition assignment should not change after node join,
     * for another primary node should change.
     *
     * @param ignite Ignite.
     * @param nodes Current nodes.
     * @return Found keys.
     */
    private IgniteBiTuple<Integer, Integer> findKeys(Ignite ignite, ClusterNode...nodes) {
        ClusterNode newNode = new TcpDiscoveryNode();

        GridTestUtils.setFieldValue(newNode, "consistentId", getTestGridName(4));
        GridTestUtils.setFieldValue(newNode, "id", UUID.randomUUID());

        List<ClusterNode> topNodes = new ArrayList<>();

        Collections.addAll(topNodes, nodes);

        topNodes.add(newNode);

        DiscoveryEvent discoEvt = new DiscoveryEvent(newNode, "", EventType.EVT_NODE_JOINED, newNode);

        final long topVer = ignite.cluster().topologyVersion();

        GridAffinityFunctionContextImpl ctx = new GridAffinityFunctionContextImpl(topNodes,
            null,
            discoEvt,
            new AffinityTopologyVersion(topVer + 1),
            1);

        AffinityFunction affFunc = ignite.cache(null).getConfiguration(CacheConfiguration.class).getAffinity();

        List<List<ClusterNode>> newAff = affFunc.assignPartitions(ctx);

        List<List<ClusterNode>> curAff = ((IgniteKernal)ignite).context().cache().internalCache(null).context().
            affinity().assignments(new AffinityTopologyVersion(topVer));

        Integer key1 = null;
        Integer key2 = null;

        Affinity<Integer> aff = ignite.affinity(null);

        for (int i = 0; i < curAff.size(); i++) {
            if (key1 == null) {
                List<ClusterNode> oldNodes = curAff.get(i);
                List<ClusterNode> newNodes = newAff.get(i);

                if (oldNodes.equals(newNodes))
                    key1 = findKey(aff, i);
            }

            if (key2 == null) {
                ClusterNode oldPrimary = F.first(curAff.get(i));
                ClusterNode newPrimary = F.first(newAff.get(i));

                if (!oldPrimary.equals(newPrimary))
                    key2 = findKey(aff, i);
            }

            if (key1 != null && key2 != null)
                break;
        }

        if (key1 == null || key2 == null)
            fail("Failed to find nodes required for test.");

        return new IgniteBiTuple<>(key1, key2);
    }

    /**
     * @param aff Affinity.
     * @param part Required key partition.
     * @return Key.
     */
    private Integer findKey(Affinity<Integer> aff, int part) {
        for (int i = 0; i < 10_000; i++) {
            Integer key = i;

            if (aff.partition(key) == part)
                return key;
        }

        fail();

        return null;
    }

    /**
     * Tests specific scenario when mapping for first locked keys does not change, but changes for second one.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticTx2() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        awaitPartitionMapExchange();

        client = true;

        final Ignite ignite3 = startGrid(3);

        assertTrue(ignite3.configuration().isClientMode());

        AffinityTopologyVersion topVer1 = new AffinityTopologyVersion(4, 0);

        assertEquals(topVer1, ignite0.context().cache().internalCache(null).context().topology().topologyVersion());

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite3.configuration().getCommunicationSpi();

        IgniteBiTuple<Integer, Integer> keys =
            findKeys(ignite0, ignite0.localNode(), ignite1.localNode(), ignite2.localNode());

        final Integer key1 = keys.get1();
        final Integer key2 = keys.get2();

        spi.blockMessages(GridNearLockRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite1.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite2.localNode().id());

        final IgniteCache<Integer, Integer> cache = ignite3.cache(null);

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                try (Transaction tx = ignite3.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key1, 1);
                    cache.put(key2, 2);

                    tx.commit();
                }

                return null;
            }
        });

        client = false;

        IgniteEx ignite4 = startGrid(4);

        int minorVer = ignite4.configuration().isLateAffinityAssignment() ? 1 : 0;

        AffinityTopologyVersion topVer2 = new AffinityTopologyVersion(5, minorVer);

        ignite0.context().cache().context().exchange().affinityReadyFuture(topVer2).get();

        assertEquals(topVer2, ignite0.context().cache().internalCache(null).context().topology().topologyVersion());

        GridCacheAffinityManager aff = ignite0.context().cache().internalCache(null).context().affinity();

        List<ClusterNode> nodes1 = aff.nodesByKey(key1, topVer1);
        List<ClusterNode> nodes2 = aff.nodesByKey(key1, topVer2);

        assertEquals(nodes1, nodes2);

        nodes1 = aff.nodesByKey(key2, topVer1);
        nodes2 = aff.nodesByKey(key2, topVer2);

        assertFalse(nodes1.get(0).equals(nodes2.get(0)));

        assertFalse(putFut.isDone());

        log.info("Stop block.");

        spi.stopBlock();

        putFut.get();

        checkData(F.asMap(key1, 1, key2, 2), null, cache, 5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxNearEnabledNoRemap() throws Exception {
        pessimisticTxNoRemap(new NearCacheConfiguration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxNoRemap() throws Exception {
        pessimisticTxNoRemap(null);
    }

    /**
     * @param nearCfg Near cache configuration.
     * @throws Exception If failed.
     */
    private void pessimisticTxNoRemap(@Nullable NearCacheConfiguration nearCfg) throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setNearConfiguration(nearCfg);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        client = true;

        final Ignite ignite3 = startGrid(3);

        assertTrue(ignite3.configuration().isClientMode());

        awaitPartitionMapExchange();

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite3.configuration().getCommunicationSpi();

        for (int i = 0; i < 100; i++)
            primaryCache(i, null).put(i, -1);

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        spi.blockMessages(GridNearLockRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite1.localNode().id());
        spi.blockMessages(GridNearLockRequest.class, ignite2.localNode().id());

        spi.record(GridNearLockRequest.class);

        final IgniteCache<Integer, Integer> cache = ignite3.cache(null);

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                try (Transaction tx = ignite3.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    for (Map.Entry<Integer, Integer> e : map.entrySet())
                        cache.put(e.getKey(), e.getValue());

                    tx.commit();
                }

                return null;
            }
        });

        IgniteEx ignite4 = startGrid(4);

        assertTrue(ignite4.configuration().isClientMode());

        assertFalse(putFut.isDone());

        log.info("Stop block.");

        spi.stopBlock();

        putFut.get();

        spi.record(null);

        checkData(map, null, cache, 5);

        List<Object> msgs = spi.recordedMessages();

        checkClientLockMessages(msgs, map.size());

        for (int i = 0; i < 100; i++)
            map.put(i, i + 1);

        try (Transaction tx = ignite3.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.putAll(map);

            tx.commit();
        }

        checkData(map, null, cache, 5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTx() throws Exception {
        optimisticSerializableTx(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxNearEnabled() throws Exception {
        optimisticSerializableTx(new NearCacheConfiguration());
    }

    /**
     * @param nearCfg Near cache configuration.
     * @throws Exception If failed.
     */
    private void optimisticSerializableTx(NearCacheConfiguration nearCfg) throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setNearConfiguration(nearCfg);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        client = true;

        final Ignite ignite2 = startGrid(2);

        assertTrue(ignite2.configuration().isClientMode());

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        spi.blockMessages(GridNearTxPrepareRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearTxPrepareRequest.class, ignite1.localNode().id());

        spi.record(GridNearTxPrepareRequest.class);

        final IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        IgniteInternalFuture<?> putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                try (Transaction tx = ignite2.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.putAll(map);

                    tx.commit();
                }

                return null;
            }
        });

        assertFalse(putFut.isDone());

        client = false;

        IgniteEx ignite3 = startGrid(3);

        awaitPartitionMapExchange();

        log.info("Stop block1.");

        spi.stopBlock();

        putFut.get();

        spi.record(null);

        checkData(map, null, cache, 4);

        List<Object> msgs = spi.recordedMessages();

        for (Object msg : msgs)
            assertTrue(((GridNearTxPrepareRequest)msg).firstClientRequest());

        assertEquals(5, msgs.size());

        ignite3.close();

        awaitPartitionMapExchange();

        for (int i = 0; i < 100; i++)
            map.put(i, i + 1);

        spi.blockMessages(GridNearTxPrepareRequest.class, ignite0.localNode().id());
        spi.blockMessages(GridNearTxPrepareRequest.class, ignite1.localNode().id());

        spi.record(GridNearTxPrepareRequest.class);

        putFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                try (Transaction tx = ignite2.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    for (Map.Entry<Integer, Integer> e : map.entrySet())
                        cache.put(e.getKey(), e.getValue());

                    tx.commit();
                }

                return null;
            }
        });

        startGrid(3);

        awaitPartitionMapExchange();

        log.info("Stop block2.");

        spi.stopBlock();

        putFut.get();

        spi.record(null);

        msgs = spi.recordedMessages();

        for (Object msg : msgs)
            assertTrue(((GridNearTxPrepareRequest)msg).firstClientRequest());

        assertEquals(5, msgs.size());

        checkData(map, null, cache, 4);

        for (int i = 0; i < 100; i++)
            map.put(i, i + 2);

        try (Transaction tx = ignite2.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
            cache.putAll(map);

            tx.commit();
        }

        checkData(map, null, cache, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLock() throws Exception {
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

        final IgniteEx ignite0 = startGrid(0);
        final IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();

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

        startGrid(3);

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

        awaitPartitionMapExchange();

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < 4; i++) {
                    if (!unlocked(ignite(i)))
                        return false;
                }

                return true;
            }

            private boolean unlocked(Ignite ignite) {
                IgniteCache<Integer, Integer> cache = ignite.cache(null);

                for (Integer key : keys) {
                    if (cache.isLocalLocked(key, false)) {
                        log.info("Key is locked [key=" + key + ", node=" + ignite.name() + ']');

                        return false;
                    }
                }

                return true;
            }
        }, 10_000);

        assertTrue(wait);

        for (Integer key : keys) {
            Lock lock = cache0.lock(key);

            assertTrue("Failed to lock: " + key, lock.tryLock());

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

        awaitPartitionMapExchange();

        client = true;

        Ignite ignite3 = startGrid(3);

        assertTrue(ignite3.configuration().isClientMode());

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite3.configuration().getCommunicationSpi();

        spi.record(GridNearLockRequest.class);

        IgniteCache<Integer, Integer> cache = ignite3.cache(null);

        Affinity<Integer> aff = ignite0.affinity(null);

        try (Transaction tx = ignite3.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Integer key1 = findKey(aff, 1);
            Integer key2 = findKey(aff, 2);
            Integer key3 = findKey(aff, 3);

            cache.put(key1, 1);
            cache.put(key2, 2);
            cache.put(key3, 3);

            tx.commit();
        }

        checkClientLockMessages(spi.recordedMessages(), 3);

        Map<Integer, Integer> map = new LinkedHashMap<>();

        map.put(primaryKey(ignite0.cache(null)), 4);
        map.put(primaryKey(ignite1.cache(null)), 5);
        map.put(primaryKey(ignite2.cache(null)), 6);
        map.put(primaryKeys(ignite0.cache(null), 1, 10_000).get(0), 7);

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

        awaitPartitionMapExchange();

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

        checkData(map, null, cache, 4);

        cache.putAll(map);

        checkClientPrepareMessages(spi.recordedMessages(), 6);

        spi.record(null);

        checkData(map, null, cache, 4);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        spi0.record(GridNearTxPrepareRequest.class);

        cache0.putAll(map);

        spi0.record(null);

        List<Object> msgs = spi0.recordedMessages();

        assertEquals(4, msgs.size());

        for (Object msg : msgs)
            assertFalse(((GridNearTxPrepareRequest)msg).firstClientRequest());

        checkData(map, null, cache, 4);
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

        final Integer key = 0;

        Lock lock2 = cache2.lock(key);

        lock2.lock();

        ignite2.close();

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        assertFalse(cache0.isLocalLocked(key, false));

        IgniteCache<Integer, Integer> cache1 = ignite1.cache(null);

        assertFalse(cache1.isLocalLocked(key, false));

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
     * @throws Exception If failed.
     */
    public void testLockFromClientBlocksExchange() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        startGrid(0);
        startGrid(1);

        client = true;

        Ignite ignite2 = startGrid(2);

        IgniteCache<Integer, Integer> cache = ignite2.cache(null);

        Lock lock = cache.lock(0);

        lock.lock();

        IgniteInternalFuture<?> startFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                client = false;

                startGrid(3);

                return null;
            }
        });

        U.sleep(2000);

        assertFalse(startFut.isDone());

        AffinityTopologyVersion ver = new AffinityTopologyVersion(4);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        U.sleep(2000);

        for (int i = 0; i < 3; i++) {
            Ignite ignite = ignite(i);

            IgniteInternalFuture<?> fut =
                ((IgniteKernal)ignite).context().cache().context().exchange().affinityReadyFuture(ver);

            assertNotNull(fut);

            assertFalse(fut.isDone());

            futs.add(fut);
        }

        lock.unlock();

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(10_000);

        startFut.get(10_000);
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

        final Affinity<Integer> aff = nodes.get(0).affinity(null);

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
                            IgniteCache<Integer, Integer> cache = node.cache(null);

                            boolean affNode = aff.isPrimaryOrBackup(node.cluster().localNode(), key);

                            Object val0 = cache.localPeek(key);

                            if (affNode || node == nearCacheNode) {
                                if (map != null)
                                    assertEquals("Unexpected value for " + node.name(), map.get(key), val0);
                                else
                                    assertNotNull("Unexpected value for " + node.name(), val0);

                                GridCacheAdapter cache0 = ((IgniteKernal)node).internalCache(null);

                                if (affNode && cache0.isNear())
                                    cache0 = ((GridNearCacheAdapter)cache0).dht();

                                GridCacheEntryEx entry = cache0.peekEx(key);

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
    public void testAtomicPrimaryPutAllMultinode() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1685");

        multinode(PRIMARY, TestType.PUT_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicClockPutAllMultinode() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1685");

        multinode(CLOCK, TestType.PUT_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxPutAllMultinode() throws Exception {
        multinode(null, TestType.OPTIMISTIC_TX);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxPutAllMultinode() throws Exception {
        multinode(null, TestType.OPTIMISTIC_SERIALIZABLE_TX);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxPutAllMultinode() throws Exception {
        multinode(null, TestType.PESSIMISTIC_TX);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockAllMultinode() throws Exception {
        multinode(null, TestType.LOCK);
    }

    /**
     * @param atomicWriteOrder Write order if test atomic cache.
     * @param testType Test type.
     * @throws Exception If failed.
     */
    private void multinode(final CacheAtomicWriteOrderMode atomicWriteOrder, final TestType testType)
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

                    IgniteCache<Integer, Integer> cache = ignite.cache(null);

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

                    IgniteCache<Integer, Integer> cache = ignite.cache(null);

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
            checkData(null, putKeys, grid(SRV_CNT).cache(null), SRV_CNT + CLIENT_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServersLeaveOnStart() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        Ignite ignite0 = startGrid(0);

        client = true;

        final AtomicInteger nodeIdx = new AtomicInteger(2);

        final int CLIENTS = 10;

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int idx = nodeIdx.getAndIncrement();

                startGrid(idx);

                return null;
            }
        }, CLIENTS, "start-client");

        ignite0.close();

        fut.get();

        for (int i = 0; i < CLIENTS; i++) {
            Ignite ignite = grid(i + 2);

            assertEquals(CLIENTS, ignite.cluster().nodes().size());
        }

        client = false;

        startGrid(0);
        startGrid(1);

        awaitPartitionMapExchange();

        for (int i = 0; i < CLIENTS; i++) {
            Ignite ignite = grid(i + 2);

            IgniteCache<Integer, Integer> cache = ignite.cache(null);

            cache.put(i, i);

            assertEquals((Object)i, cache.get(i));
        }
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
                        log.info("Block message [node=" + node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME) +
                            ", msg=" + msg0 + ']');

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

                    log.info("Send blocked message: [node=" + node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME) +
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
