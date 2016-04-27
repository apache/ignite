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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class IgniteTxCachePrimarySyncTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRVS = 4;

    /** */
    private static final int CLIENTS = 2;

    /** */
    private static final int NODES = SRVS + CLIENTS;

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(clientMode);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);

        try {
            for (int i = 0; i < CLIENTS; i++) {
                clientMode = true;

                Ignite client = startGrid(SRVS + i);

                assertTrue(client.configuration().isClientMode());
            }
        }
        finally {
            clientMode = false;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleKeyCommitFromPrimary() throws Exception {
        singleKeyCommitFromPrimary(cacheConfiguration(null, PRIMARY_SYNC, 1, true, false));

        singleKeyCommitFromPrimary(cacheConfiguration(null, PRIMARY_SYNC, 2, false, false));

        singleKeyCommitFromPrimary(cacheConfiguration(null, PRIMARY_SYNC, 2, false, true));

        singleKeyCommitFromPrimary(cacheConfiguration(null, PRIMARY_SYNC, 3, false, false));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void singleKeyCommitFromPrimary(CacheConfiguration<Object, Object> ccfg) throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        try {
            for (int i = 0; i < SRVS; i++) {
                Ignite node = ignite(i);

                singleKeyCommitFromPrimary(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                    @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                        cache.put(key, key);
                    }
                });

                for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                        singleKeyCommitFromPrimary(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                            @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                                Ignite ignite = cache.unwrap(Ignite.class);

                                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                                    cache.put(key, key);

                                    tx.commit();
                                }
                            }
                        });
                    }
                }
            }
        }
        finally {
            ignite.destroyCache(cache.getName());
        }
    }

    /**
     * @param ignite Node executing cache operation.
     * @param ccfg Cache configuration.
     * @param c Cache update closure.
     * @throws Exception If failed.
     */
    private void singleKeyCommitFromPrimary(
        Ignite ignite,
        final CacheConfiguration<Object, Object> ccfg,
        IgniteBiInClosure<Integer, IgniteCache<Object, Object>> c) throws Exception {
        TestRecordingCommunicationSpi commSpi0 =
            (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

        IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

        final Integer key = primaryKey(cache);

        cache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        commSpi0.record(GridDhtTxFinishRequest.class);

        commSpi0.blockMessages(new IgnitePredicateX<GridIoMessage>() {
            @Override public boolean applyx(GridIoMessage e) throws IgniteCheckedException {
                return e.message() instanceof GridDhtTxFinishRequest;
            }
        });

        c.apply(key, cache);

        assertEquals(key, cache.localPeek(key));

        U.sleep(50);

        for (int i = 0; i < SRVS; i++) {
            Ignite node = ignite(i);

            if (node != ignite)
                assertNull(node.cache(null).localPeek(key));
        }

        commSpi0.stopBlock(true);

        waitKeyUpdated(ignite, ccfg.getBackups() + 1, ccfg.getName(), key);

        List<Object> msgs = commSpi0.recordedMessages(true);

        assertEquals(ccfg.getBackups(), msgs.size());

        cache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        c.apply(key, cache);

        waitKeyUpdated(ignite, ccfg.getBackups() + 1, ccfg.getName(), key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleKeyPrimaryNodeFail1() throws Exception {
        singleKeyPrimaryNodeLeft(cacheConfiguration(null, PRIMARY_SYNC, 1, true, false));

        singleKeyPrimaryNodeLeft(cacheConfiguration(null, PRIMARY_SYNC, 2, false, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleKeyPrimaryNodeFail2() throws Exception {
        singleKeyPrimaryNodeLeft(cacheConfiguration(null, PRIMARY_SYNC, 2, true, false));

        singleKeyPrimaryNodeLeft(cacheConfiguration(null, PRIMARY_SYNC, 3, false, false));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void singleKeyPrimaryNodeLeft(CacheConfiguration<Object, Object> ccfg) throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        try {
            ignite(NODES - 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

            for (int i = 0; i < NODES; i++) {
                Ignite node = ignite(i);

                singleKeyPrimaryNodeLeft(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                    @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                        cache.put(key, key);
                    }
                });

                for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                        singleKeyPrimaryNodeLeft(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                            @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                                Ignite ignite = cache.unwrap(Ignite.class);

                                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                                    cache.put(key, key);

                                    tx.commit();
                                }
                            }
                        });
                    }
                }
            }
        }
        finally {
            ignite.destroyCache(cache.getName());
        }
    }

    /**
     * @param client Node executing cache operation.
     * @param ccfg Cache configuration.
     * @param c Cache update closure.
     * @throws Exception If failed.
     */
    private void singleKeyPrimaryNodeLeft(
        Ignite client,
        final CacheConfiguration<Object, Object> ccfg,
        final IgniteBiInClosure<Integer, IgniteCache<Object, Object>> c) throws Exception {
        Ignite ignite = startGrid(NODES);

        final TestRecordingCommunicationSpi commSpiClient =
            (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

        final Integer key = primaryKey(cache);

        cache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        commSpiClient.blockMessages(GridNearTxFinishRequest.class, ignite.name());

        final IgniteCache<Object, Object> clientCache = client.cache(ccfg.getName());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                c.apply(key, clientCache);

                return null;
            }
        });

        boolean waitMsgSnd = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return commSpiClient.hasBlockedMessages();
            }
        }, 5000);

        assertTrue(waitMsgSnd);

        ignite.close();

        commSpiClient.stopBlock(false);

        fut.get();

        awaitPartitionMapExchange();

        waitKeyUpdated(client, ccfg.getBackups() + 1, ccfg.getName(), key);

        clientCache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        c.apply(key, clientCache);

        waitKeyUpdated(client, ccfg.getBackups() + 1, ccfg.getName(), key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleKeyCommit() throws Exception {
        singleKeyCommit(cacheConfiguration(null, PRIMARY_SYNC, 1, true, false));

        singleKeyCommit(cacheConfiguration(null, PRIMARY_SYNC, 2, false, false));

        singleKeyCommit(cacheConfiguration(null, PRIMARY_SYNC, 2, false, true));

        singleKeyCommit(cacheConfiguration(null, PRIMARY_SYNC, 3, false, false));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void singleKeyCommit(CacheConfiguration<Object, Object> ccfg) throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        try {
            ignite(NODES - 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

            for (int i = 1; i < NODES; i++) {
                Ignite node = ignite(i);

                log.info("Test node: " + node.name());

                singleKeyCommit(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                    @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                        cache.put(key, key);
                    }
                });

                for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                        singleKeyCommit(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                            @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                                Ignite ignite = cache.unwrap(Ignite.class);

                                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                                    cache.put(key, key);

                                    tx.commit();
                                }
                            }
                        });
                    }
                }
            }
        }
        finally {
            ignite.destroyCache(cache.getName());
        }
    }

    /**
     * @param client Node executing cache operation.
     * @param ccfg Cache configuration.
     * @param c Cache update closure.
     * @throws Exception If failed.
     */
    private void singleKeyCommit(
        Ignite client,
        final CacheConfiguration<Object, Object> ccfg,
        IgniteBiInClosure<Integer, IgniteCache<Object, Object>> c) throws Exception {
        Ignite ignite = ignite(0);

        assertNotSame(ignite, client);

        TestRecordingCommunicationSpi commSpiClient =
            (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        TestRecordingCommunicationSpi commSpi0 =
            (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

        IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

        final Integer key = primaryKey(cache);

        cache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        IgniteCache<Object, Object> clientCache = client.cache(ccfg.getName());

        commSpiClient.record(GridNearTxFinishRequest.class);

        commSpi0.record(GridDhtTxFinishRequest.class);

        commSpi0.blockMessages(new IgnitePredicateX<GridIoMessage>() {
            @Override public boolean applyx(GridIoMessage e) throws IgniteCheckedException {
                return e.message() instanceof GridDhtTxFinishRequest;
            }
        });

        c.apply(key, clientCache);

        assertEquals(key, cache.localPeek(key));

        U.sleep(50);

        boolean nearCache = ((IgniteCacheProxy)clientCache).context().isNear();

        for (int i = 1; i < NODES; i++) {
            Ignite node = ignite(i);

            if (nearCache
                && node == client &&
                !node.affinity(ccfg.getName()).isPrimaryOrBackup(node.cluster().localNode(), key))
                assertEquals("Invalid value for node: " + i, key, ignite(i).cache(null).localPeek(key));
            else
                assertNull("Invalid value for node: " + i, ignite(i).cache(null).localPeek(key));
        }

        commSpi0.stopBlock(true);

        waitKeyUpdated(ignite, ccfg.getBackups() + 1, ccfg.getName(), key);

        List<Object> msgs = commSpiClient.recordedMessages(true);

        assertEquals(1, msgs.size());

        GridNearTxFinishRequest req = (GridNearTxFinishRequest)msgs.get(0);

        assertEquals(PRIMARY_SYNC, req.syncMode());

        msgs = commSpi0.recordedMessages(true);

        assertEquals(ccfg.getBackups(), msgs.size());

        clientCache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        c.apply(key, clientCache);

        waitKeyUpdated(ignite, ccfg.getBackups() + 1, ccfg.getName(), key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitPrimaryResponse() throws Exception {
        checkWaitPrimaryResponse(cacheConfiguration(null, PRIMARY_SYNC, 1, true, false));

        checkWaitPrimaryResponse(cacheConfiguration(null, PRIMARY_SYNC, 2, false, false));

        checkWaitPrimaryResponse(cacheConfiguration(null, PRIMARY_SYNC, 2, false, true));

        checkWaitPrimaryResponse(cacheConfiguration(null, PRIMARY_SYNC, 3, false, false));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkWaitPrimaryResponse(CacheConfiguration<Object, Object> ccfg) throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        try {
            ignite(NODES - 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

            for (int i = 1; i < NODES; i++) {
                Ignite node = ignite(i);

                log.info("Test node: " + node.name());

                checkWaitPrimaryResponse(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                    @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                        cache.put(key, key);
                    }
                });

                checkWaitPrimaryResponse(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                    @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                        Map<Integer, Integer> map = new HashMap<>();

                        for (int i = 0; i < 50; i++)
                            map.put(i, i);

                        map.put(key, key);

                        cache.putAll(map);
                    }
                });

                for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                        checkWaitPrimaryResponse(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                            @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                                Ignite ignite = cache.unwrap(Ignite.class);

                                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                                    cache.put(key, key);

                                    tx.commit();
                                }
                            }
                        });

                        checkWaitPrimaryResponse(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                            @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                                Map<Integer, Integer> map = new HashMap<>();

                                for (int i = 0; i < 50; i++)
                                    map.put(i, i);

                                map.put(key, key);

                                Ignite ignite = cache.unwrap(Ignite.class);

                                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                                    cache.putAll(map);

                                    tx.commit();
                                }
                            }
                        });
                    }
                }
            }
        }
        finally {
            ignite.destroyCache(cache.getName());
        }
    }

    /**
     * @param client Node executing cache operation.
     * @param ccfg Cache configuration.
     * @param c Cache update closure.
     * @throws Exception If failed.
     */
    private void checkWaitPrimaryResponse(
        Ignite client,
        final CacheConfiguration<Object, Object> ccfg,
        final IgniteBiInClosure<Integer, IgniteCache<Object, Object>> c) throws Exception {
        Ignite ignite = ignite(0);

        assertNotSame(ignite, client);

        TestRecordingCommunicationSpi commSpi0 =
            (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

        IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

        final Integer key = primaryKey(cache);

        cache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        final IgniteCache<Object, Object> clientCache = client.cache(ccfg.getName());

        commSpi0.blockMessages(GridNearTxFinishResponse.class, client.name());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                c.apply(key, clientCache);

                return null;
            }
        }, "tx-thread");

        U.sleep(100);

        assertFalse(fut.isDone());

        commSpi0.stopBlock(true);

        fut.get();

        waitKeyUpdated(ignite, ccfg.getBackups() + 1, ccfg.getName(), key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnePhaseMessages() throws Exception {
        checkOnePhaseMessages(cacheConfiguration(null, PRIMARY_SYNC, 1, false, false));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkOnePhaseMessages(CacheConfiguration<Object, Object> ccfg) throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        try {
            for (int i = 1; i < NODES; i++) {
                Ignite node = ignite(i);

                log.info("Test node: " + node.name());

                checkOnePhaseMessages(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                    @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                        cache.put(key, key);
                    }
                });

                for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                        checkOnePhaseMessages(node, ccfg, new IgniteBiInClosure<Integer, IgniteCache<Object, Object>>() {
                            @Override public void apply(Integer key, IgniteCache<Object, Object> cache) {
                                Ignite ignite = cache.unwrap(Ignite.class);

                                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                                    cache.put(key, key);

                                    tx.commit();
                                }
                            }
                        });
                    }
                }
            }
        }
        finally {
            ignite.destroyCache(cache.getName());
        }
    }

    /**
     * @param client Node executing cache operation.
     * @param ccfg Cache configuration.
     * @param c Cache update closure.
     * @throws Exception If failed.
     */
    private void checkOnePhaseMessages(
        Ignite client,
        final CacheConfiguration<Object, Object> ccfg,
        final IgniteBiInClosure<Integer, IgniteCache<Object, Object>> c) throws Exception {
        Ignite ignite = ignite(0);

        assertNotSame(ignite, client);

        TestRecordingCommunicationSpi commSpiClient =
            (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        TestRecordingCommunicationSpi commSpi0 =
            (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

        IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

        final Integer key = primaryKey(cache);

        cache.remove(key);

        waitKeyRemoved(ccfg.getName(), key);

        final IgniteCache<Object, Object> clientCache = client.cache(ccfg.getName());

        commSpi0.record(GridNearTxFinishResponse.class, GridNearTxPrepareResponse.class);
        commSpiClient.record(GridNearTxPrepareRequest.class, GridNearTxFinishRequest.class);

        c.apply(key, clientCache);

        List<Object> srvMsgs = commSpi0.recordedMessages(true);

        assertEquals("Unexpected messages: " + srvMsgs, 1, srvMsgs.size());
        assertTrue("Unexpected message: " + srvMsgs.get(0), srvMsgs.get(0) instanceof GridNearTxPrepareResponse);

        List<Object> clientMsgs = commSpiClient.recordedMessages(true);

        assertEquals("Unexpected messages: " + clientMsgs, 1, clientMsgs.size());
        assertTrue("Unexpected message: " + clientMsgs.get(0), clientMsgs.get(0) instanceof GridNearTxPrepareRequest);

        GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)clientMsgs.get(0);

        assertTrue(req.onePhaseCommit());

        for (Ignite ignite0 : G.allGrids())
            assertEquals(key, ignite0.cache(cache.getName()).get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxSyncMode() throws Exception {
        Ignite ignite = ignite(0);

        List<IgniteCache<Object, Object>> caches = new ArrayList<>();

        try {
            caches.add(createCache(ignite, cacheConfiguration("fullSync1", FULL_SYNC, 1, false, false), true));
            caches.add(createCache(ignite, cacheConfiguration("fullSync2", FULL_SYNC, 1, false, false), true));
            caches.add(createCache(ignite, cacheConfiguration("fullAsync1", FULL_ASYNC, 1, false, false), true));
            caches.add(createCache(ignite, cacheConfiguration("fullAsync2", FULL_ASYNC, 1, false, false), true));
            caches.add(createCache(ignite, cacheConfiguration("primarySync1", PRIMARY_SYNC, 1, false, false), true));
            caches.add(createCache(ignite, cacheConfiguration("primarySync2", PRIMARY_SYNC, 1, false, false), true));

            for (int i = 0; i < NODES; i++) {
                checkTxSyncMode(ignite(i), true);
                checkTxSyncMode(ignite(i), false);
            }
        }
        finally {
            for (IgniteCache<Object, Object> cache : caches)
                ignite.destroyCache(cache.getName());
        }
    }

    /**
     * @param cacheName Cache name.
     * @param key Cache key.
     * @throws Exception If failed.
     */
    private void waitKeyRemoved(final String cacheName, final Object key) throws Exception {
        boolean waitRmv = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite ignite : G.allGrids()) {
                    if (ignite.cache(cacheName).get(key) != null)
                        return false;
                }

                return true;
            }
        }, 5000);

        assertTrue(waitRmv);
    }

    /**
     * @param ignite Node.
     * @param expNodes Expected number of cache server nodes.
     * @param cacheName Cache name.
     * @param key Cache key.
     * @throws Exception If failed.
     */
    private void waitKeyUpdated(Ignite ignite, int expNodes, final String cacheName, final Object key) throws Exception {
        Affinity<Object> aff = ignite.affinity(cacheName);

        final Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        assertEquals(expNodes, nodes.size());

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (ClusterNode node : nodes) {
                    Ignite ignite = grid(node);

                    if (!key.equals(ignite.cache(cacheName).get(key)))
                        return false;
                }

                return true;
            }
        }, 5000);

        assertTrue(wait);

        for (Ignite ignite0 : G.allGrids())
            assertEquals(key, ignite0.cache(cacheName).get(key));
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @param nearCache If {@code true} creates near cache on one of client nodes.
     * @return Created cache.
     */
    private <K, V> IgniteCache<K, V> createCache(Ignite ignite, CacheConfiguration<K, V> ccfg,
        boolean nearCache) {
        IgniteCache<K, V> cache = ignite.createCache(ccfg);

        if (nearCache)
            ignite(NODES - 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

        return cache;
    }

    /**
     * @param ignite Node.
     * @param commit If {@code true} commits transaction.
     */
    private void checkTxSyncMode(Ignite ignite, boolean commit) {
        IgniteTransactions txs = ignite.transactions();

        IgniteCache<Object, Object> fullSync1 = ignite.cache("fullSync1");
        IgniteCache<Object, Object> fullSync2 = ignite.cache("fullSync2");
        IgniteCache<Object, Object> fullAsync1 = ignite.cache("fullAsync1");
        IgniteCache<Object, Object> fullAsync2 = ignite.cache("fullAsync2");
        IgniteCache<Object, Object> primarySync1 = ignite.cache("primarySync1");
        IgniteCache<Object, Object> primarySync2 = ignite.cache("primarySync2");

        for (int i = 0; i < 3; i++) {
            int key = 0;

            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullSync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullAsync1.put(key++, 1);

                        checkSyncMode(tx, FULL_ASYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        primarySync1.put(key++, 1);

                        checkSyncMode(tx, PRIMARY_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        for (int j = 0; j < 100; j++)
                            fullSync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        for (int j = 0; j < 100; j++)
                            fullAsync1.put(key++, 1);

                        checkSyncMode(tx, FULL_ASYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        for (int j = 0; j < 100; j++)
                            primarySync1.put(key++, 1);

                        checkSyncMode(tx, PRIMARY_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullSync1.put(key++, 1);
                        fullSync2.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullAsync1.put(key++, 1);
                        fullAsync2.put(key++, 1);

                        checkSyncMode(tx, FULL_ASYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        primarySync1.put(key++, 1);
                        primarySync2.put(key++, 1);

                        checkSyncMode(tx, PRIMARY_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullSync1.put(key++, 1);
                        primarySync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        primarySync1.put(key++, 1);
                        fullSync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullSync1.put(key++, 1);
                        fullAsync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullAsync1.put(key++, 1);
                        fullSync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullAsync1.put(key++, 1);
                        primarySync1.put(key++, 1);

                        checkSyncMode(tx, PRIMARY_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullAsync1.put(key++, 1);
                        primarySync1.put(key++, 1);
                        fullAsync2.put(key++, 1);

                        checkSyncMode(tx, PRIMARY_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        primarySync1.put(key++, 1);
                        fullAsync1.put(key++, 1);

                        checkSyncMode(tx, PRIMARY_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullSync1.put(key++, 1);
                        fullAsync1.put(key++, 1);
                        primarySync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        fullAsync1.put(key++, 1);
                        primarySync1.put(key++, 1);
                        fullSync1.put(key++, 1);

                        checkSyncMode(tx, FULL_SYNC);

                        if (commit)
                            tx.commit();
                    }
                }
            }
        }
    }

    /**
     * @param tx Transaction.
     * @param syncMode Expected write synchronization mode.
     */
    private void checkSyncMode(Transaction tx, CacheWriteSynchronizationMode syncMode) {
        assertEquals(syncMode, ((TransactionProxyImpl)tx).tx().syncMode());
    }

    /**
     * @param name Cache name.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param store If {@code true} configures cache store.
     * @param nearCache If {@code true} configures near cache.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean store,
        boolean nearCache) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName(name);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setBackups(backups);

        if (store) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
        }

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }
}
