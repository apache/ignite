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

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class IgniteClientReconnectCacheDelayExchangeTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static final int SRV_CNT = 3;

    /** */
    private static final String STATIC_CACHE = "static-cache";

    /** */
    private UUID nodeId;

    /**
     * Map of destination node ID to runnable with logic for real message sending.
     * To apply real message sending use run method
     */
    private final ConcurrentHashMap<UUID, Runnable> rs = new ConcurrentHashMap<>();
    /**
     * Flag to redirect {@link GridDhtPartitionsFullMessage}s from real communication channel to {@link #rs} map.
     * Applied only to messages not related to particular exchange
     */
    private volatile boolean record = false;

    /** */
    private AtomicBoolean replay = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestCommunicationDelayedSpi commSpi = new TestCommunicationDelayedSpi();

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
    public void testReconnectCacheDestroyedAndCreatedDelayed() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(SRV_CNT);

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        assertEquals(ATOMIC,
            clientCache.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        awaitPartitionMapExchange();

        record = true;

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(DEFAULT_CACHE_NAME);

                CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

                ccfg.setAtomicityMode(TRANSACTIONAL);

                srv.getOrCreateCache(ccfg);
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return clientCache.get(1);
            }
        }, IllegalStateException.class, null);

        // Emulate latest GridDhtPartitionsFullMessages.
        //grid(0).context().cache().context().exchange().scheduleResendPartitions();

        checkCacheDiscoveryData(srv, client, DEFAULT_CACHE_NAME, true, false, false);

        IgniteCache<Object, Object> clientCache0 = client.cache(DEFAULT_CACHE_NAME);

        replayMessages();

        checkCacheDiscoveryData(srv, client, DEFAULT_CACHE_NAME, true, true, false);

        assertEquals(TRANSACTIONAL,
            clientCache0.getConfiguration(CacheConfiguration.class).getAtomicityMode());

        clientCache0.put(1, 1);

        assertEquals(1, clientCache0.get(1));
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
     * Replays all saved messages from map, actual sent is performed.
     *
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void replayMessages() throws IgniteInterruptedCheckedException {
        record = false;

        log.info("Start replaying messages");

        for (Runnable r : rs.values())
            r.run(); // Causes real messages sending.

        assertTrue(replay.compareAndSet(false, true));

        U.sleep(10000); // Enough time to process delayed GridDhtPartitionsFullMessages.
    }

    /**
     *
     */
    private class TestCommunicationDelayedSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            final Object msg0 = ((GridIoMessage)msg).message();

            log.info("Communication message [thread=" + Thread.currentThread().getName() +
                ", msg=" + msg0 +
                ", node.id()=" + node.id() +
                ']');

            if (msg0 instanceof GridDhtPartitionsSingleMessage && record &&
                ((GridDhtPartitionsAbstractMessage)msg0).exchangeId() == null) {

                Runnable prevValue = rs.putIfAbsent(node.id(), new Runnable() {
                    @Override public void run() {
                        log.info("Message replayed: " + msg);

                        TestCommunicationDelayedSpi.super.sendMessage(node, msg, ackClosure);
                    }
                });

                assert prevValue == null : "Duplicate message registered to [" + node.id() + "]";
            }
            else
                try {
                    super.sendMessage(node, msg, ackClosure);
                }
                catch (Exception e) {
                    U.log(null, e);
                }
        }

    }
}
