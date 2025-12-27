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

package org.apache.ignite.internal.metric;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.metric.impl.MaxValueMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.BlockTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.internal.util.nio.GridNioServer.MAX_MESSAGES_QUEUE_SIZE_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;

/**
 * Test for discovery/communication outbound message queue size metrics.
 */
public class OutboundIoMessageQueueSizeTest extends GridCommonAbstractTest {
    /** */
    private static final int MSG_LIMIT = 50;

    /** */
    private final ListeningTestLogger log = new ListeningTestLogger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new BlockTcpDiscoverySpi().setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder()));
        cfg.setGridLogger(log);

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
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_TCP_COMM_MSG_QUEUE_WARN_SIZE, value = "" + MSG_LIMIT)
    public void testCommunicationMsgQueue() throws Exception {
        IgniteEx srv0 = startGrid(0);
        IgniteEx srv1 = startGrid(1);

        String logMsg = "Outbound message queue size for node exceeded";

        // Only one message to log should be printed due to throttling.
        LogListener logLsnr = LogListener.matches(logMsg).times(1).build();

        log.registerListener(logLsnr);

        IgniteCache<Object, Object> cache0 = srv0.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache1 = srv1.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache1.query(new ContinuousQuery<>().setLocalListener(evt -> {}));

        cache0.put(0, 0);

        assertFalse(logLsnr.check());

        MaxValueMetric metric = srv0.context().metric().registry(COMMUNICATION_METRICS_GROUP_NAME)
            .findMetric(MAX_MESSAGES_QUEUE_SIZE_METRIC_NAME);

        assertTrue(metric.value() < MSG_LIMIT);

        GridTestUtils.skipCommNioServerRead(srv1, true);

        // Initiate messages for srv1.
        // Some messages still may be sent until buffers overflow, so use MSG_LIMIT * 2 messages.
        for (int i = 0; i < MSG_LIMIT * 2; i++)
            cache0.put(0, new byte[10 * 1024]);

        assertTrue(metric.value() >= MSG_LIMIT);

        assertTrue(logLsnr.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoveryMsgQueue() throws Exception {
        IgniteEx srv0 = startGrids(2);

        MaxValueMetric metric = srv0.context().metric().registry(DISCO_METRICS)
            .findMetric("MaxMsgQueueSize");

        metric.reset(); // Reset value accumulated before discovery SPI startup.

        srv0.context().discovery().sendCustomEvent(new DummyCustomDiscoveryMessage(IgniteUuid.randomUuid()));

        // Assume our message can be added to queue concurrently with other messages
        // (for example, with metrics update message).
        assertTrue(metric.value() < 3);

        BlockTcpDiscoverySpi discoverySpi = (BlockTcpDiscoverySpi)srv0.context().config().getDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoverySpi.setClosure((node, msg) -> {
            U.awaitQuiet(latch);

            return null;
        });

        try {
            for (int i = 0; i <= MSG_LIMIT; i++)
                srv0.context().discovery().sendCustomEvent(new DummyCustomDiscoveryMessage(IgniteUuid.randomUuid()));

            assertTrue(metric.value() >= MSG_LIMIT);
        }
        finally {
            latch.countDown();
        }
    }

    /** */
    private static class DummyCustomDiscoveryMessage implements DiscoveryCustomMessage {
        /** */
        private final IgniteUuid id;

        /**
         * @param id Message id.
         */
        DummyCustomDiscoveryMessage(IgniteUuid id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Nullable @Override public DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }
    }
}
