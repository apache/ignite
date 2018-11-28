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
import junit.framework.AssertionFailedError;
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
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
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

        IgniteInClosure<IgniteCache<Object, Object>> getAllOp = new CI1<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                cache.getAll(F.asSet(1, 2));
            }
        };

        int cnt = 0;

        CacheAtomicityMode atomicityMode = TRANSACTIONAL_SNAPSHOT;
        //for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
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
        //}
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

        cache.put(1, 1);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.get(1) != null;
            }
        }, 5000);

        assertEquals(1, cache.get(1));
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
