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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.junit.Assert.assertNotEquals;

/**
 *
 */
public class IgniteClientReconnectCacheTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static final int SRV_CNT = 3;

    /** */
    private static final String STATIC_CACHE = "static-cache";

    /** */
    private static final int CACHE_PUTS_CNT = 3;

    /** */
    public static final String NEAR_CACHE_NAME = "nearCache";

    /** */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setNetworkTimeout(5000);

        if (nodeId != null) {
            cfg.setNodeId(nodeId);

            nodeId = null;
        }

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(STATIC_CACHE);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(SRV_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnect() throws Exception {
        IgniteEx client = startClientGrid(SRV_CNT);

        final IgniteDiscoverySpi clientSpi = spi0(client);

        Ignite srv = ignite(0);

        DiscoverySpi srvSpi = ignite(0).configuration().getDiscoverySpi();

        final IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .withAllowAtomicOpsInTx();

        final IgniteCache<Object, Object> staticCache = client.cache(STATIC_CACHE).withAllowAtomicOpsInTx();

        staticCache.put(1, 1);

        assertEquals(1, staticCache.get(1));

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setName(NEAR_CACHE_NAME);

        final IgniteCache<Object, Object> nearCache = client.getOrCreateCache(ccfg, new NearCacheConfiguration<>())
            .withAllowAtomicOpsInTx();

        nearCache.put(1, 1);

        assertEquals(1, nearCache.localPeek(1));

        cache.put(1, 1);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        log.info("Block reconnect.");

        DiscoverySpiTestListener lsnr = new DiscoverySpiTestListener();

        clientSpi.setInternalListener(lsnr);

        lsnr.startBlockJoin();

        final AtomicReference<IgniteInternalFuture> blockPutRef = new AtomicReference<>();

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    assertEquals(1, reconnectLatch.getCount());

                    blockPutRef.set(GridTestUtils.runAsync(new Callable() {
                        @Override public Object call() throws Exception {
                            log.info("Start put.");

                            try {
                                cache.put(2, 2);

                                fail();
                            }
                            catch (CacheException e) {
                                log.info("Expected exception: " + e);

                                IgniteClientDisconnectedException e0 = (IgniteClientDisconnectedException)e.getCause();

                                e0.reconnectFuture().get();
                            }

                            cache.put(2, 2);

                            log.info("Finish put.");

                            return null;
                        }
                    }));

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    assertEquals(0, disconnectLatch.getCount());

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        log.info("Fail client.");

        srvSpi.failNode(client.cluster().localNode().id(), null);

        waitReconnectEvent(disconnectLatch);

        IgniteInternalFuture putFut = blockPutRef.get();

        assertNotDone(putFut);

        U.sleep(5000);

        assertNotDone(putFut);

        log.info("Allow reconnect.");

        lsnr.stopBlockJoin();

        assertTrue(reconnectLatch.await(5000, MILLISECONDS));

        checkCacheDiscoveryData(srv, client, DEFAULT_CACHE_NAME, true, true, false);

        checkCacheDiscoveryData(srv, client, NEAR_CACHE_NAME, true, true, true);

        checkCacheDiscoveryData(srv, client, STATIC_CACHE, true, true, false);

        assertEquals(1, cache.get(1));

        putFut.get();

        assertEquals(2, cache.get(2));

        cache.put(3, 3);

        assertEquals(3, cache.get(3));

        assertNull(nearCache.localPeek(1));

        staticCache.put(10, 10);

        assertEquals(10, staticCache.get(10));

        nearCache.put(20, 20);

        srv.cache(nearCache.getName()).put(20, 21);

        assertEquals(21, nearCache.localPeek(20));

        IgniteEx srv2 = startGrid(SRV_CNT + 1);

        Integer key = primaryKey(srv2.cache(DEFAULT_CACHE_NAME));

        cache.put(key, 4);

        assertEquals(4, cache.get(key));

        checkCacheDiscoveryData(srv2, client, DEFAULT_CACHE_NAME, true, true, false);

        checkCacheDiscoveryData(srv2, client, NEAR_CACHE_NAME, true, true, true);

        checkCacheDiscoveryData(srv2, client, STATIC_CACHE, true, true, false);

        staticCache.put(20, 20);

        assertEquals(20, staticCache.get(20));

        for (int i = 0; i < 100; i++) {
            srv.cache(nearCache.getName()).put(i, 22);
            Object actual = nearCache.localPeek(i);
            // Change of topology may start partitions moving. It leads to invalidate near cache and
            // null-values can be valid in such case.
            if (actual == null) {
                actual = nearCache.get(i);
                assertEquals(22, actual);
                actual = nearCache.localPeek(i);
            }
            assertEquals(22, actual);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectTransactions() throws Exception {
        IgniteEx client = startClientGrid(SRV_CNT);

        Ignite srv = ignite(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(ccfg);

        final IgniteTransactions txs = client.transactions();

        final Transaction tx = txs.txStart(OPTIMISTIC, REPEATABLE_READ);

        cache.put(1, 1);

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                try {
                    tx.commit();

                    fail();
                }
                catch (IgniteClientDisconnectedException e) {
                    log.info("Expected error: " + e);

                    assertNotNull(e.reconnectFuture());
                }

                try {
                    txs.txStart();

                    fail();
                }
                catch (IgniteClientDisconnectedException e) {
                    log.info("Expected error: " + e);

                    assertNotNull(e.reconnectFuture());
                }
            }
        });

        assertNull(txs.tx());

        try (Transaction tx0 = txs.txStart(OPTIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 1);

            assertEquals(1, cache.get(1));

            tx0.commit();
        }

        try (Transaction tx0 = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(2, 2);

            assertEquals(2, cache.get(2));

            tx0.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxStateAfterClientReconnect() throws Exception {
        IgniteEx client = startClientGrid(SRV_CNT);

        Ignite srv = ignite(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(ccfg);

        final IgniteTransactions txs = client.transactions();

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                Transaction tx = txs.txStart(concurrency, isolation);

                cache.put(1, 1);

                reconnectClientNode(client, srv, null);

                GridTestUtils.assertThrowsWithCause(() -> {
                    tx.commit();

                    return null;
                }, TransactionRollbackException.class);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectTransactionInProgress1() throws Exception {
        IgniteEx client = startClientGrid(SRV_CNT);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(ccfg);

        reconnectTransactionInProgress1(client, OPTIMISTIC, cache);

        reconnectTransactionInProgress1(client, PESSIMISTIC, cache);
    }

    /**
     * @param client Client.
     * @param txConcurrency Transaction concurrency mode.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void reconnectTransactionInProgress1(IgniteEx client,
        final TransactionConcurrency txConcurrency,
        final IgniteCache<Object, Object> cache)
        throws Exception {
        Ignite srv = ignite(0);

        final IgniteDiscoverySpi clientSpi = spi0(client);
        final DiscoverySpi srvSpi = spi0(srv);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        log.info("Block reconnect.");

        DiscoverySpiTestListener lsnr = new DiscoverySpiTestListener();

        clientSpi.setInternalListener(lsnr);

        lsnr.startBlockJoin();

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        final IgniteTransactions txs = client.transactions();

        final CountDownLatch afterPut1 = new CountDownLatch(1);

        final CountDownLatch afterPut2 = new CountDownLatch(1);

        final CountDownLatch putFailed = new CountDownLatch(1);

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try {
                    log.info("Start tx1: " + txConcurrency);

                    try (Transaction tx = txs.txStart(txConcurrency, REPEATABLE_READ)) {
                        cache.put(1, 1);

                        afterPut1.countDown();

                        afterPut2.await();

                        cache.put(2, 2);

                        fail();
                    }
                    catch (CacheException e) {
                        log.info("Expected exception: " + e);

                        putFailed.countDown();

                        IgniteClientDisconnectedException e0 = (IgniteClientDisconnectedException)e.getCause();

                        e0.reconnectFuture().get();
                    }

                    log.info("Start tx2: " + txConcurrency);

                    try (Transaction tx = txs.txStart(txConcurrency, REPEATABLE_READ)) {
                        cache.put(1, 1);

                        cache.put(2, 2);

                        tx.commit();
                    }

                    assertEquals(1, cache.get(1));
                    assertEquals(2, cache.get(2));

                    try (Transaction tx = txs.txStart(txConcurrency, REPEATABLE_READ)) {
                        cache.put(3, 3);

                        cache.put(4, 4);

                        tx.commit();
                    }

                    assertEquals(1, cache.get(1));
                    assertEquals(2, cache.get(2));
                    assertEquals(3, cache.get(3));
                    assertEquals(4, cache.get(4));

                    cache.removeAll();

                    return true;
                }
                catch (AssertionError e) {
                    throw e;
                }
                catch (Throwable e) {
                    log.error("Unexpected error", e);

                    fail("Unexpected error: " + e);

                    return false;
                }
            }
        });

        assertTrue(afterPut1.await(5000, MILLISECONDS));

        assertNotDone(fut);

        srvSpi.failNode(client.localNode().id(), null);

        waitReconnectEvent(disconnectLatch);

        afterPut2.countDown();

        assertTrue(putFailed.await(5000, MILLISECONDS));

        lsnr.stopBlockJoin();

        waitReconnectEvent(reconnectLatch);

        assertTrue(fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectTransactionInProgress2() throws Exception {
        final IgniteEx client = startClientGrid(SRV_CNT);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        txInProgressFails(client, ccfg, GridNearTxPrepareResponse.class, OPTIMISTIC, 1);

        txInProgressFails(client, ccfg, GridNearTxPrepareResponse.class, PESSIMISTIC, 2);

        txInProgressFails(client, ccfg, GridNearTxFinishResponse.class, OPTIMISTIC, 3);

        txInProgressFails(client, ccfg, GridNearTxFinishResponse.class, PESSIMISTIC, 4);

        txInProgressFails(client, ccfg, GridNearLockResponse.class, PESSIMISTIC, 5);
    }

    /**
     * @param client Client.
     * @param ccfg Cache configuration.
     * @param msgToBlock Message to block.
     * @param txConcurrency Transaction concurrency mode.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void txInProgressFails(final IgniteEx client,
        final CacheConfiguration<Object, Object> ccfg,
        Class<?> msgToBlock,
        final TransactionConcurrency txConcurrency,
        final Integer key) throws Exception {
        log.info("Test tx failure [msg=" + msgToBlock + ", txMode=" + txConcurrency + ", key=" + key + ']');

        checkOperationInProgressFails(client, ccfg, msgToBlock,
            new CI1<IgniteCache<Object, Object>>() {
                @Override public void apply(IgniteCache<Object, Object> cache) {
                    try (Transaction tx = client.transactions().txStart(txConcurrency, REPEATABLE_READ)) {
                        for (int i = 0; i < CACHE_PUTS_CNT; ++i)
                            cache.put(key + i, key + i);

                        tx.commit();
                    }
                }
            }
        );

        IgniteCache<Object, Object> cache = client.cache(ccfg.getName());

        assertEquals(key, cache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectExchangeInProgress() throws Exception {
        IgniteEx client = startClientGrid(SRV_CNT);

        Ignite srv = ignite(0);

        DiscoverySpi srvSpi = spi0(srv);

        TestCommunicationSpi coordCommSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        coordCommSpi.blockMessages(GridDhtPartitionsFullMessage.class, client.localNode().id());

        startGrid(SRV_CNT + 1);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        assertTrue(reconnectLatch.await(5000, MILLISECONDS));

        try {
            coordCommSpi.stopBlock(true);

            fail();
        }
        catch (IgniteException e) {
            log.info("Expected error: " + e);
        }

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setName("newCache");

        ccfg.setCacheMode(REPLICATED);

        log.info("Start new cache.");

        IgniteCache<Object, Object> cache = client.getOrCreateCache(ccfg);

        cache.put(1, 1);

        assertEquals(1, cache.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectInitialExchangeInProgress() throws Exception {
        final UUID clientId = UUID.randomUUID();

        Ignite srv = grid(0);

        final CountDownLatch joinLatch = new CountDownLatch(1);

        srv.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_NODE_JOINED && ((DiscoveryEvent)evt).eventNode().id().equals(clientId)) {
                    info("Client joined: " + evt);

                    joinLatch.countDown();
                }

                return true;
            }
        }, EVT_NODE_JOINED);

        TestCommunicationSpi srvCommSpi = (TestCommunicationSpi)srv.configuration().getCommunicationSpi();

        srvCommSpi.blockMessages(GridDhtPartitionsFullMessage.class, clientId);

        nodeId = clientId;

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try {
                    startClientGrid(optimize(getConfiguration(getTestIgniteInstanceName(SRV_CNT))));

                    // Commented due to IGNITE-4473, because
                    // IgniteClientDisconnectedException won't
                    // be thrown, but client will reconnect.
//                    fail();

                    return true;
                }
                catch (IgniteClientDisconnectedException e) {
                    log.info("Expected start error: " + e);

                    try {
                        e.reconnectFuture().get();

                        fail();
                    }
                    catch (IgniteException e0) {
                        log.info("Expected future error: " + e0);
                    }

                    return true;
                }
                catch (Throwable e) {
                    log.error("Unexpected error: " + e, e);

                    throw e;
                }
            }
        });

        DiscoverySpi srvSpi = spi0(srv);

        try {
            if (!joinLatch.await(10_000, MILLISECONDS)) {
                log.error("Failed to wait for join event, will dump threads.");

                U.dumpThreads(log);

                fail("Failed to wait for join event.");
            }

            U.sleep(1000);

            assertNotDone(fut);

            srvSpi.failNode(clientId, null);
        }
        finally {
            srvCommSpi.stopBlock(false);
        }

        assertTrue(fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectOperationInProgress() throws Exception {
        IgniteEx client = startClientGrid(SRV_CNT);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED)
                    info("Client disconnected: " + evt);
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED)
                    info("Client reconnected: " + evt);

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        IgniteInClosure<IgniteCache<Object, Object>> putOp = new CI1<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                while (true) {
                    try {
                        cache.put(1, 1);

                        break;
                    }
                    catch (Exception e) {
                        if (e.getCause() instanceof IgniteClientDisconnectedException)
                            throw e;
                        else
                            MvccFeatureChecker.assertMvccWriteConflict(e);
                    }
                }
            }
        };

        IgniteInClosure<IgniteCache<Object, Object>> getOp = new CI1<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                cache.get(1);
            }
        };

        IgniteInClosure<IgniteCache<Object, Object>> getAllOp = new CI1<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                cache.getAll(F.asSet(1, 2));
            }
        };

        int cnt = 0;

        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

                ccfg.setAtomicityMode(atomicityMode);

                ccfg.setName("cache-" + cnt++);

                ccfg.setWriteSynchronizationMode(syncMode);

                if (syncMode != FULL_ASYNC) {
                    Class<?> cls = (ccfg.getAtomicityMode() == ATOMIC) ?
                        GridNearAtomicUpdateResponse.class : GridNearTxPrepareResponse.class;

                    log.info("Test cache put [atomicity=" + atomicityMode +
                        ", syncMode=" + syncMode + ']');

                    checkOperationInProgressFails(client, ccfg, cls, putOp);

                    client.destroyCache(ccfg.getName());
                }

                log.info("Test cache get [atomicity=" + atomicityMode + ", syncMode=" + syncMode + ']');

                checkOperationInProgressFails(client, ccfg, GridNearSingleGetResponse.class, getOp);

                checkOperationInProgressFails(client, ccfg, GridNearGetResponse.class, getAllOp);

                client.destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectCacheDestroyed() throws Exception {
        final IgniteEx client = startClientGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(DEFAULT_CACHE_NAME);
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return clientCache.get(1);
            }
        }, IllegalStateException.class, null);

        checkCacheDiscoveryData(srv, client, DEFAULT_CACHE_NAME, false, false, false);

        IgniteCache<Object, Object> clientCache0 = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        checkCacheDiscoveryData(srv, client, DEFAULT_CACHE_NAME, true, true, false);

        clientCache0.put(1, 1);

        assertEquals(1, clientCache0.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectCacheDestroyedAndCreated() throws Exception {
        final Ignite client = startClientGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        assertEquals(ATOMIC,
            clientCache.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(DEFAULT_CACHE_NAME);

                CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

                ccfg.setAtomicityMode(TRANSACTIONAL);

                srv.getOrCreateCache(ccfg);
            }
        });

        checkCacheDiscoveryData(srv, client, DEFAULT_CACHE_NAME, true, false, false);

        IgniteCache<Object, Object> clientCache0 = client.cache(DEFAULT_CACHE_NAME);

        checkCacheDiscoveryData(srv, client, DEFAULT_CACHE_NAME, true, true, false);

        assertEquals(TRANSACTIONAL,
            clientCache0.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        clientCache0.put(1, 1);

        assertEquals(1, clientCache0.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectMarshallerCache() throws Exception {
        final Ignite client = startClientGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
        final IgniteCache<Object, Object> srvCache = srv.cache(DEFAULT_CACHE_NAME);

        assertNotNull(srvCache);

        clientCache.put(1, new TestClass1());
        srvCache.put(2, new TestClass2());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                assertNotNull(srvCache.get(1));
                assertNotNull(srvCache.get(2));

                srvCache.put(3, new TestClass3());
            }
        });

        srvCache.put(4, new TestClass4());

        assertNotNull(clientCache.get(1));
        assertNotNull(clientCache.get(2));
        assertNotNull(clientCache.get(3));
        assertNotNull(clientCache.get(4));

        clientCache.put(5, new TestClass5());

        assertNotNull(srvCache.get(5));
        assertNotNull(clientCache.get(5));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectClusterRestart() throws Exception {
        final Ignite client = startClientGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        clientCache.put(1, new TestClass1());

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        for (int i = 0; i < SRV_CNT; i++)
            stopGrid(i);

        assertTrue(disconnectLatch.await(30_000, MILLISECONDS));

        Ignite srv = startGrid(0);

        assertTrue(reconnectLatch.await(10_000, MILLISECONDS));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return clientCache.get(1);
            }
        }, IllegalStateException.class, null);

        IgniteCache<Object, Object> srvCache = srv.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        srvCache.put(1, new TestClass1());
        srvCache.put(2, new TestClass2());

        IgniteCache<Object, Object> clientCache2 = client.cache(DEFAULT_CACHE_NAME);

        assertNotNull(clientCache2);

        assertNotNull(clientCache2.get(1));
        assertNotNull(clientCache2.get(2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectClusterRestartMultinode() throws Exception {
        final int CLIENTS = 5;

        CountDownLatch disconnectLatch = new CountDownLatch(CLIENTS);
        CountDownLatch reconnectLatch = new CountDownLatch(CLIENTS);

        List<IgniteCache> caches = new ArrayList<>();

        for (int i = 0; i < CLIENTS; i++) {
            int g = SRV_CNT + i;

            IgniteEx client = startClientGrid(g);

            info(">>>>> Started client: " + g);

            addListener(client, disconnectLatch, reconnectLatch);

            IgniteCache cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

            assertNotNull(cache);

            caches.add(cache);
        }

        for (int i = 0; i < SRV_CNT; i++)
            stopGrid(i);

        assertTrue(disconnectLatch.await(30_000, MILLISECONDS));

        log.info("Restart servers.");

        startGridsMultiThreaded(0, SRV_CNT);

        assertTrue(reconnectLatch.await(30_000, MILLISECONDS));

        for (final IgniteCache clientCache : caches) {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return clientCache.get(1);
                }
            }, IllegalStateException.class, null);
        }

        for (int i = 0; i < SRV_CNT + CLIENTS; i++) {
            Ignite ignite = grid(i);

            ClusterGroup grp = ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME);

            assertEquals(0, grp.nodes().size());

            grp = ignite.cluster().forClientNodes(DEFAULT_CACHE_NAME);

            assertEquals(0, grp.nodes().size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectMultinode() throws Exception {
        reconnectMultinode(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectMultinodeLongHistory() throws Exception {
        reconnectMultinode(true);
    }

    /**
     * @param longHist If {@code true} generates many discovery events to overflow events history.
     * @throws Exception If failed.
     */
    private void reconnectMultinode(boolean longHist) throws Exception {
        grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        final int CLIENTS = 5;

        List<Ignite> clients = new ArrayList<>();

        for (int i = 0; i < CLIENTS; i++) {
            Ignite client = startClientGrid(SRV_CNT + i);

            assertNotNull(client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)));

            clients.add(client);
        }

        if (longHist) {
            // Generate many discovery events to overflow discovery events history.
            final AtomicInteger nodeIdx = new AtomicInteger(SRV_CNT + CLIENTS);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = nodeIdx.incrementAndGet();

                    for (int i = 0; i < 25; i++) {
                        startClientGrid(idx);

                        stopGrid(idx);
                    }

                    return null;
                }
            }, 4, "restart-thread");
        }

        int nodes = SRV_CNT + CLIENTS;
        int srvNodes = SRV_CNT;

        for (int iter = 0; iter < 5; iter++) {
            log.info("Iteration: " + iter);

            reconnectClientNodes(log, clients, grid(0), null);

            final int expNodes = CLIENTS + srvNodes;

            for (final Ignite client : clients) {
                IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

                assertNotNull(cache);

                cache.put(client.name(), 1);

                assertEquals(1, cache.get(client.name()));

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        ClusterGroup grp = client.cluster().forCacheNodes(DEFAULT_CACHE_NAME);

                        return grp.nodes().size() == expNodes;
                    }
                }, 5000);

                ClusterGroup grp = client.cluster().forCacheNodes(DEFAULT_CACHE_NAME);

                assertEquals(expNodes, grp.nodes().size());

                grp = client.cluster().forClientNodes(DEFAULT_CACHE_NAME);

                assertEquals(CLIENTS, grp.nodes().size());
            }

            for (int i = 0; i < nodes; i++) {
                final Ignite ignite = grid(i);

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        ClusterGroup grp = ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME);

                        return grp.nodes().size() == expNodes;
                    }
                }, 5000);

                ClusterGroup grp = ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME);

                assertEquals(CLIENTS + srvNodes, grp.nodes().size());

                grp = ignite.cluster().forClientNodes(DEFAULT_CACHE_NAME);

                assertEquals(CLIENTS, grp.nodes().size());
            }

            startGrid(nodes++);

            srvNodes++;

            startClientGrid(nodes++);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectDestroyCache() throws Exception {
        Ignite client = startClientGrid(SRV_CNT);

        CacheConfiguration<Integer, Integer> ccfg1 = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg1.setName("cache1");

        CacheConfiguration<Integer, Integer> ccfg2 = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg2.setName("cache2");

        final Ignite srv = grid(0);

        srv.createCache(ccfg1);
        srv.createCache(ccfg2).put(1, 1);

        IgniteCache<Integer, Integer> cache = client.cache("cache2");

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache("cache1");
            }
        });

        cache.put(2, 2);

        assertEquals(1, (Object)cache.get(1));
        assertEquals(2, (Object)cache.get(2));
    }

    /**
     * @param client Client.
     * @param disconnectLatch Disconnect event latch.
     * @param reconnectLatch Reconnect event latch.
     */
    private void addListener(Ignite client, final CountDownLatch disconnectLatch, final CountDownLatch reconnectLatch) {
        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);
    }

    /**
     *
     */
    static class TestClass1 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class TestClass2 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class TestClass3 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class TestClass4 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class TestClass5 implements Serializable {
        // No-op.
    }

    /**
     * @param client Client.
     * @param ccfg Cache configuration.
     * @param msgToBlock Message to block.
     * @param c Cache operation closure.
     * @throws Exception If failed.
     */
    private void checkOperationInProgressFails(final IgniteEx client,
        final CacheConfiguration<Object, Object> ccfg,
        Class<?> msgToBlock,
        final IgniteInClosure<IgniteCache<Object, Object>> c)
        throws Exception {
        Ignite srv = ignite(0);

        final UUID id = client.localNode().id();

        DiscoverySpi srvSpi = spi0(srv);

        final IgniteCache<Object, Object> cache = client.getOrCreateCache(ccfg);

        for (int i = 0; i < SRV_CNT; i++) {
            TestCommunicationSpi srvCommSpi = (TestCommunicationSpi)grid(i).configuration().getCommunicationSpi();

            srvCommSpi.blockMessages(msgToBlock, client.localNode().id());
        }

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteClientDisconnectedException e0 = null;

                try {
                    assertEquals(id, client.localNode().id());

                    c.apply(cache);

                    fail();
                }
                catch (IgniteClientDisconnectedException e) {
                    log.info("Expected exception: " + e);

                    e0 = e;
                }
                catch (CacheException e) {
                    log.info("Expected exception: " + e);

                    assertTrue("Unexpected cause: " + e.getCause(),
                        e.getCause() instanceof IgniteClientDisconnectedException);

                    e0 = (IgniteClientDisconnectedException)e.getCause();
                }

                assertNotNull(e0);
                assertNotNull(e0.reconnectFuture());

                e0.reconnectFuture().get();

                assertNotEquals(id, client.localNode().id());

                c.apply(cache);

                return null;
            }
        });

        Thread.sleep(1000);

        assertNotDone(fut);

        log.info("Fail client: " + client.localNode().id());

        srvSpi.failNode(client.localNode().id(), null);

        try {
            fut.get();
        }
        finally {
            for (int i = 0; i < SRV_CNT; i++)
                ((TestCommunicationSpi)grid(i).configuration().getCommunicationSpi()).stopBlock(false);
        }

        assertNotEquals(id, client.localNode().id());

        while (true) {
            try {
                cache.put(1, 1);

                break;
            }
            catch (Exception e) {
                MvccFeatureChecker.assertMvccWriteConflict(e);
            }
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.get(1) != null;
            }
        }, 5000);

        assertEquals(1, cache.get(1));
    }

    /**
     * @param srv Server node.
     * @param client Client node.
     * @param cacheName Cache name.
     * @param cacheExists Cache exists flag.
     * @param clientCache {@code True} if client node has client cache.
     * @param clientNear {@code True} if client node has near-enabled client cache.
     * @throws Exception If failed.
     */
    private void checkCacheDiscoveryData(Ignite srv,
        Ignite client,
        final String cacheName,
        boolean cacheExists,
        final boolean clientCache,
        boolean clientNear) throws Exception {
        final GridDiscoveryManager srvDisco = ((IgniteKernal)srv).context().discovery();
        GridDiscoveryManager clientDisco = ((IgniteKernal)client).context().discovery();

        ClusterNode srvNode = ((IgniteKernal)srv).localNode();
        final ClusterNode clientNode = ((IgniteKernal)client).localNode();

        assertFalse(srvDisco.cacheAffinityNode(clientNode, cacheName));
        assertFalse(clientDisco.cacheAffinityNode(clientNode, cacheName));

        assertEquals(cacheExists, srvDisco.cacheAffinityNode(srvNode, cacheName));

        if (clientNear) {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return srvDisco.cacheNearNode(clientNode, cacheName);
                }
            }, 5000));

            assertTrue(srvDisco.cacheNearNode(clientNode, cacheName));
        }
        else {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return F.eq(clientCache, srvDisco.cacheClientNode(clientNode, cacheName));
                }
            }, 5000));

            assertEquals(clientCache, srvDisco.cacheClientNode(clientNode, cacheName));
        }

        assertEquals(cacheExists, clientDisco.cacheAffinityNode(srvNode, cacheName));

        if (clientNear)
            assertTrue(clientDisco.cacheNearNode(clientNode, cacheName));
        else
            assertEquals(clientCache, clientDisco.cacheClientNode(clientNode, cacheName));

        if (cacheExists) {
            if (clientCache || clientNear) {
                assertTrue(client.cluster().forClientNodes(cacheName).nodes().contains(clientNode));
                assertTrue(srv.cluster().forClientNodes(cacheName).nodes().contains(clientNode));
            }
            else {
                assertFalse(client.cluster().forClientNodes(cacheName).nodes().contains(clientNode));
                assertFalse(srv.cluster().forClientNodes(cacheName).nodes().contains(clientNode));
            }
        }
        else {
            assertTrue(client.cluster().forClientNodes(cacheName).nodes().isEmpty());
            assertTrue(srv.cluster().forClientNodes(cacheName).nodes().isEmpty());
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

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                synchronized (this) {
                    Set<UUID> blockNodes = blockCls.get(msg0.getClass());

                    if (F.contains(blockNodes, node.id())) {
                        log.info("Block message [node=" +
                            node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) + ", msg=" + msg0 + ']');

                        blockedMsgs.add(new T2<>(node, (GridIoMessage)msg));

                        return;
                    }
                }
            }

            super.sendMessage(node, msg, ackClosure);
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
         * @param snd Send messages flag.
         */
        void stopBlock(boolean snd) {
            synchronized (this) {
                blockCls.clear();

                if (snd) {
                    for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
                        ClusterNode node = msg.get1();

                        log.info("Send blocked message: [node=" +
                            node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) +
                            ", msg=" + msg.get2().message() + ']');

                        super.sendMessage(msg.get1(), msg.get2());
                    }
                }

                blockedMsgs.clear();
            }
        }
    }
}
