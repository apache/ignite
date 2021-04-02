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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Tests behavior of DataStreamer when communication channel fails to send a data streamer request due to some reason,
 * for example, handshake problems.
 */
public class DataStreamerCommunicationSpiExceptionTest extends GridCommonAbstractTest {
    /** Data size. */
    public static final int DATA_SIZE = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStreamerThreadPoolSize(1);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests that flushing data streamer does not hang due to SPI exception on communication layer.
     * @throws Exception If failed.
     */
    @Test
    public void testSpiOperationTimeoutException() throws Exception {
        startGrids(2);

        Ignite client = startClientGrid(3);

        IgniteCache<IgniteUuid, Integer> cache = client.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();

        TestCommunicationSpi.spi(client).victim(grid(0).cluster().localNode().id());

        int threadBufSize = 10;

        try (IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.perThreadBufferSize(threadBufSize);

            for (int i = 0; i < DATA_SIZE; i++)
                streamer.addData(i, i);

            streamer.flush();

            int sz = cache.size(ALL);
            assertEquals(
                "Unexpected cache size (data was not flushed) [expected=" + DATA_SIZE + ", actual=" + sz + ']',
                DATA_SIZE,
                sz);
        }
    }

    /**
     * Test communication SPI.
     */
    public static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Node id. */
        private volatile UUID victim;

        /** Indicates that one data streamer request was blocked. */
        private final AtomicBoolean firstBlocked = new AtomicBoolean(false);

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            boolean dataStreamerReq = ((GridIoMessage)msg).message() instanceof DataStreamerRequest;

            if (node.id().equals(victim) && dataStreamerReq && firstBlocked.compareAndSet(false, true))
                throw new IgniteSpiException("Test Spi Exception");

            super.sendMessage(node, msg, ackC);
        }

        /**
         * Sets node identifier.
         *
         * This {@code id} is used for blocking the first data streamer request to this node and throws test exception.
         */
        public void victim(UUID id) {
            victim = id;
        }

        /**
         * Returns instance of TestCommunicationSpi configured for the given {@code node}.
         *
         * @param node Ignite instance.
         * @return Communication SPI.
         */
        public static TestCommunicationSpi spi(Ignite node) {
            return (TestCommunicationSpi)node.configuration().getCommunicationSpi();
        }
    }
}
