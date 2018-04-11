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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test cases with {@link TestCommunicationDelayedSpi} for capturing messages and resending at moment we need it.
 */
public class IgniteClientReconnectDelayedSpiTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static final String PRECONFIGURED_CACHE = "preconfigured-cache";

    /** */
    private static final Map<UUID, Runnable> recordedMsgs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestCommunicationDelayedSpi delayedCommSpi = new TestCommunicationDelayedSpi();
        delayedCommSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(delayedCommSpi);
        cfg.setCacheConfiguration(new CacheConfiguration(PRECONFIGURED_CACHE));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 3;
    }

    /**
     * Test checks correctness of stale {@link CacheAffinityChangeMessage} processing while delayed
     * {@link GridDhtPartitionsSingleMessage} with exchId = null sends after client node reconnect happened.
     *
     * @throws Exception If failed.
     */
    public void testReconnectCacheDestroyedDelayedAffinityChange() throws Exception {
        clientMode = true;

        final Ignite client = startGrid();
        final Ignite srv = clientRouter(client);

        clientMode = false;

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(DEFAULT_CACHE_NAME);

                CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

                ccfg.setAtomicityMode(TRANSACTIONAL);

                srv.getOrCreateCache(ccfg);
            }
        });

        IgniteCache<Object, Object> clientCache = client.cache(DEFAULT_CACHE_NAME);

        // Resend delayed GridDhtPartitionsSingleMessage
        for (Runnable r : recordedMsgs.values())
            r.run();

        final GridDiscoveryManager srvDisco = ((IgniteKernal)srv).context().discovery();
        final ClusterNode clientNode = ((IgniteKernal)client).localNode();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return F.eq(true, srvDisco.cacheClientNode(clientNode, DEFAULT_CACHE_NAME));
            }
        }, 5000));

        clientCache.put(1, 1);

        assertEquals(1, clientCache.get(1));
    }

    /**
     * Capturing {@link GridDhtPartitionsSingleMessage} at moment when they occurs for resending them at the moment
     * when we need it by our scenario.
     */
    private static class TestCommunicationDelayedSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            final Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridDhtPartitionsSingleMessage &&
                ((GridDhtPartitionsAbstractMessage)msg0).exchangeId() == null)
                recordedMsgs.putIfAbsent(node.id(), new Runnable() {
                    @Override public void run() {
                        TestCommunicationDelayedSpi.super.sendMessage(node, msg, ackC);
                    }
                });
            else
                super.sendMessage(node, msg, ackC);
        }
    }
}
