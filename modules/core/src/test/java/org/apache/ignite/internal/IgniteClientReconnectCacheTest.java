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
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;

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

/**
 *
 */
public class IgniteClientReconnectCacheTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static final int SRV_CNT = 3;

    /** */
    private static final String STATIC_CACHE = "static-cache";

    /** */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setNetworkTimeout(5000);

        if (nodeId != null) {
            cfg.setNodeId(nodeId);

            nodeId = null;
        }

        CacheConfiguration ccfg = new CacheConfiguration();

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
    public void testReconnect() throws Exception {
        clientMode = true;

        IgniteEx client = startGrid(SRV_CNT);

        final TestTcpDiscoverySpi clientSpi = spi(client);

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        final IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>());

        final IgniteCache<Object, Object> staticCache = client.cache(STATIC_CACHE);

        staticCache.put(1, 1);

        assertEquals(1, staticCache.get(1));

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setName("nearCache");

        final IgniteCache<Object, Object> nearCache = client.getOrCreateCache(ccfg, new NearCacheConfiguration<>());

        nearCache.put(1, 1);

        assertEquals(1, nearCache.localPeek(1));

        cache.put(1, 1);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        log.info("Block reconnect.");

        clientSpi.writeLatch = new CountDownLatch(1);

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

                                IgniteClientDisconnectedException e0 = (IgniteClientDisconnectedException) e.getCause();

                                e0.reconnectFuture().get();
                            }

                            cache.put(2, 2);

                            log.info("Finish put.");

                            return null;
                        }
                    }));

                    disconnectLatch.countDown();
                } else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
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

        clientSpi.writeLatch.countDown();

        assertTrue(reconnectLatch.await(5000, MILLISECONDS));

        checkCacheDiscoveryData(srv, client, null, true, true, false);

        checkCacheDiscoveryData(srv, client, "nearCache", true, true, true);

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

        this.clientMode = false;

        IgniteEx srv2 = startGrid(SRV_CNT + 1);

        Integer key = primaryKey(srv2.cache(null));

        cache.put(key, 4);

        assertEquals(4, cache.get(key));

        checkCacheDiscoveryData(srv2, client, null, true, true, false);

        checkCacheDiscoveryData(srv2, client, "nearCache", true, true, true);

        checkCacheDiscoveryData(srv2, client, STATIC_CACHE, true, true, false);

        staticCache.put(20, 20);

        assertEquals(20, staticCache.get(20));

        srv.cache(nearCache.getName()).put(20, 22);

        assertEquals(22, nearCache.localPeek(20));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectTransactions() throws Exception {
        clientMode = true;

        IgniteEx client = startGrid(SRV_CNT);

        Ignite srv = clientRouter(client);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

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
                } catch (IgniteClientDisconnectedException e) {
                    log.info("Expected error: " + e);

                    assertNotNull(e.reconnectFuture());
                }

                try {
                    txs.txStart();

                    fail();
                } catch (IgniteClientDisconnectedException e) {
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
    public void testReconnectTransactionInProgress1() throws Exception {
        clientMode = true;

        IgniteEx client = startGrid(SRV_CNT);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

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
        throws Exception
    {
        Ignite srv = clientRouter(client);

        final TestTcpDiscoverySpi clientSpi = spi(client);
        final TestTcpDiscoverySpi srvSpi = spi(srv);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        log.info("Block reconnect.");

        clientSpi.writeLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                } else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
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
                catch (AssertionFailedError e) {
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

        clientSpi.writeLatch.countDown();

        waitReconnectEvent(reconnectLatch);

        assertTrue(fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectTransactionInProgress2() throws Exception {
        clientMode = true;

        final IgniteEx client = startGrid(SRV_CNT);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

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
                        log.info("Put1: " + key);

                        cache.put(key, key);

                        Integer key2 = key + 1;

                        log.info("Put2: " + key2);

                        cache.put(key2, key2);

                        log.info("Commit [key1=" + key + ", key2=" + key2 + ']');

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
    public void testReconnectExchangeInProgress() throws Exception {
        clientMode = true;

        IgniteEx client = startGrid(SRV_CNT);

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        TestCommunicationSpi coordCommSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        coordCommSpi.blockMessages(GridDhtPartitionsFullMessage.class, client.localNode().id());

        clientMode = false;

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

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

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

        clientMode = true;

        nodeId = clientId;

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try {
                    Ignition.start(getConfiguration(getTestGridName(SRV_CNT)));

                    fail();

                    return false;
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

        TestTcpDiscoverySpi srvSpi = spi(srv);

        assertTrue(joinLatch.await(5000, MILLISECONDS));

        U.sleep(1000);

        assertNotDone(fut);

        srvSpi.failNode(clientId, null);

        srvCommSpi.stopBlock(false);

        assertTrue(fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectOperationInProgress() throws Exception {
        clientMode = true;

        IgniteEx client = startGrid(SRV_CNT);

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
                cache.put(1, 1);
            }
        };

        IgniteInClosure<IgniteCache<Object, Object>> getOp = new CI1<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                cache.get(1);
            }
        };

        int cnt = 0;

        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            CacheAtomicWriteOrderMode[] writeOrders =
                atomicityMode == ATOMIC ? CacheAtomicWriteOrderMode.values() :
                new CacheAtomicWriteOrderMode[]{CacheAtomicWriteOrderMode.CLOCK};

            for (CacheAtomicWriteOrderMode writeOrder : writeOrders) {
                for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
                    CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

                    ccfg.setAtomicityMode(atomicityMode);

                    ccfg.setAtomicWriteOrderMode(writeOrder);

                    ccfg.setName("cache-" + cnt++);

                    ccfg.setWriteSynchronizationMode(syncMode);

                    if (syncMode != FULL_ASYNC) {
                        Class<?> cls = (ccfg.getAtomicityMode() == ATOMIC) ?
                            GridNearAtomicUpdateResponse.class : GridNearTxPrepareResponse.class;

                        log.info("Test cache put [atomicity=" + atomicityMode +
                            ", writeOrder=" + writeOrder +
                            ", syncMode=" + syncMode + ']');

                        checkOperationInProgressFails(client, ccfg, cls, putOp);

                        client.destroyCache(ccfg.getName());
                    }

                    log.info("Test cache get [atomicity=" + atomicityMode + ", syncMode=" + syncMode + ']');

                    checkOperationInProgressFails(client, ccfg, GridNearGetResponse.class, getOp);

                    client.destroyCache(ccfg.getName());
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectCacheDestroyed() throws Exception {
        clientMode = true;

        final IgniteEx client = startGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(null);
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return clientCache.get(1);
            }
        }, IllegalStateException.class, null);

        checkCacheDiscoveryData(srv, client, null, false, false, false);

        IgniteCache<Object, Object> clientCache0 = client.getOrCreateCache(new CacheConfiguration<>());

        checkCacheDiscoveryData(srv, client, null, true, true, false);

        clientCache0.put(1, 1);

        assertEquals(1, clientCache0.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectCacheDestroyedAndCreated() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>());

        assertEquals(ATOMIC,
            clientCache.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(null);

                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setAtomicityMode(TRANSACTIONAL);

                srv.getOrCreateCache(ccfg);
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return clientCache.get(1);
            }
        }, IllegalStateException.class, null);

        checkCacheDiscoveryData(srv, client, null, true, false, false);

        IgniteCache<Object, Object> clientCache0 = client.cache(null);

        checkCacheDiscoveryData(srv, client, null, true, true, false);

        assertEquals(TRANSACTIONAL,
            clientCache0.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        clientCache0.put(1, 1);

        assertEquals(1, clientCache0.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectMarshallerCache() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>());
        final IgniteCache<Object, Object> srvCache = srv.cache(null);

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
    public void testReconnectClusterRestart() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>());

        clientCache.put(1, new TestClass1());

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                } else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        for (int i = 0; i < SRV_CNT; i++)
            stopGrid(i);

        assertTrue(disconnectLatch.await(30_000, MILLISECONDS));

        clientMode = false;

        Ignite srv = startGrid(0);

        assertTrue(reconnectLatch.await(10_000, MILLISECONDS));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return clientCache.get(1);
            }
        }, IllegalStateException.class, null);

        IgniteCache<Object, Object> srvCache = srv.getOrCreateCache(new CacheConfiguration<>());

        srvCache.put(1, new TestClass1());
        srvCache.put(2, new TestClass2());

        IgniteCache<Object, Object> clientCache2 = client.cache(null);

        assertNotNull(clientCache2);

        assertNotNull(clientCache2.get(1));
        assertNotNull(clientCache2.get(2));
    }

    /**
     *
     */
    static class TestClass1 implements Serializable {}

    /**
     *
     */
    static class TestClass2 implements Serializable {}

    /**
     *
     */
    static class TestClass3 implements Serializable {}

    /**
     *
     */
    static class TestClass4 implements Serializable {}

    /**
     *
     */
    static class TestClass5 implements Serializable {}

    /**
     * @param client Client.
     * @param ccfg Cache configuration.
     * @param msgToBlock Message to block.
     * @param c Cache operation closure.
     * @throws Exception If failed.
     */
    private void checkOperationInProgressFails(IgniteEx client,
        final CacheConfiguration<Object, Object> ccfg,
        Class<?> msgToBlock,
        final IgniteInClosure<IgniteCache<Object, Object>> c)
        throws Exception
    {
        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        final IgniteCache<Object, Object> cache = client.getOrCreateCache(ccfg);

        for (int i = 0; i < SRV_CNT; i++) {
            TestCommunicationSpi srvCommSpi = (TestCommunicationSpi)grid(i).configuration().getCommunicationSpi();

            srvCommSpi.blockMessages(msgToBlock, client.localNode().id());
        }

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteClientDisconnectedException e0 = null;

                try {
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

                c.apply(cache);

                return null;
            }
        });

        Thread.sleep(1000);

        assertNotDone(fut);

        log.info("Fail client: " + client.localNode().id());

        srvSpi.failNode(client.localNode().id(), null);

        fut.get();

        for (int i = 0; i < SRV_CNT; i++)
            ((TestCommunicationSpi)grid(i).configuration().getCommunicationSpi()).stopBlock(false);

        cache.put(1, 1);

        assertEquals(1, cache.get(1));
    }

    /**
     * @param srv Server node.
     * @param client Client node.
     * @param cacheName Cache name.
     * @param cacheExists Cache exists flag.
     * @param clientCache {@code True} if client node has client cache.
     * @param clientNear {@code True} if client node has near-enabled client cache.
     */
    private void checkCacheDiscoveryData(Ignite srv,
        Ignite client,
        String cacheName,
        boolean cacheExists,
        boolean clientCache,
        boolean clientNear)
    {
        GridDiscoveryManager srvDisco = ((IgniteKernal)srv).context().discovery();
        GridDiscoveryManager clientDisco = ((IgniteKernal)client).context().discovery();

        ClusterNode srvNode = ((IgniteKernal)srv).localNode();
        ClusterNode clientNode = ((IgniteKernal)client).localNode();

        assertFalse(srvDisco.cacheAffinityNode(clientNode, cacheName));
        assertFalse(clientDisco.cacheAffinityNode(clientNode, cacheName));

        assertEquals(cacheExists, srvDisco.cacheAffinityNode(srvNode, cacheName));

        if (clientNear)
            assertTrue(srvDisco.cacheNearNode(clientNode, cacheName));
        else
            assertEquals(clientCache, srvDisco.cacheClientNode(clientNode, cacheName));

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
                        log.info("Block message [node=" + node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME) +
                            ", msg=" + msg0 + ']');

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

                        log.info("Send blocked message: [node=" + node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME) +
                            ", msg=" + msg.get2().message() + ']');

                        super.sendMessage(msg.get1(), msg.get2());
                    }
                }

                blockedMsgs.clear();
            }
        }
    }
}