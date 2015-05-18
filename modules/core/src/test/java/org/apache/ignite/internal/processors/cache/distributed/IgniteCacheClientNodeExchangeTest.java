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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.eclipse.jetty.util.*;

import java.util.*;

/**
 *
 */
public class IgniteCacheClientNodeExchangeTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

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
    public void testNoPartitionExchangeForClient() throws Exception {
        Ignite ignite0 = startGrid(0);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        Ignite ignite1 = startGrid(1);

        TestCommunicationSpi spi1 = (TestCommunicationSpi)ignite1.configuration().getCommunicationSpi();

        assertEquals(0, spi0.partitionsSingleMessages().size());
        assertEquals(1, spi0.partitionsFullMessages().size());

        assertEquals(1, spi1.partitionsSingleMessages().size());
        assertEquals(0, spi1.partitionsFullMessages().size());

        spi0.reset();
        spi1.reset();

        client = true;

        for (int i = 0; i < 3; i++) {
            log.info("Start client node: " + i);

            Ignite ignite2 = startGrid(2);

            TestCommunicationSpi spi2 = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

            assertEquals(0, spi0.partitionsSingleMessages().size());
            assertEquals(1, spi0.partitionsFullMessages().size());

            assertEquals(0, spi1.partitionsSingleMessages().size());
            assertEquals(0, spi1.partitionsFullMessages().size());

            assertEquals(1, spi2.partitionsSingleMessages().size());
            assertEquals(0, spi2.partitionsFullMessages().size());

            spi0.reset();
            spi1.reset();
            spi2.reset();

            log.info("Stop client node.");

            ignite2.close();

            assertEquals(0, spi0.partitionsSingleMessages().size());
            assertEquals(0, spi0.partitionsFullMessages().size());

            assertEquals(0, spi1.partitionsSingleMessages().size());
            assertEquals(0, spi1.partitionsFullMessages().size());
        }
    }

    /**
     * Test communication SPI.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private ConcurrentHashSet<GridDhtPartitionsSingleMessage> partSingleMsgs = new ConcurrentHashSet<>();

        /** */
        private ConcurrentHashSet<GridDhtPartitionsFullMessage> partFullMsgs = new ConcurrentHashSet<>();

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) {
            super.sendMessage(node, msg);

            Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridDhtPartitionsSingleMessage) {
                if (((GridDhtPartitionsSingleMessage)msg0).exchangeId() != null) {
                    log.info("Partitions message: " + msg0.getClass().getSimpleName());

                    partSingleMsgs.add((GridDhtPartitionsSingleMessage) msg0);
                }
            }
            else if (msg0 instanceof GridDhtPartitionsFullMessage) {
                if (((GridDhtPartitionsFullMessage)msg0).exchangeId() != null) {
                    log.info("Partitions message: " + msg0.getClass().getSimpleName());

                    partFullMsgs.add((GridDhtPartitionsFullMessage) msg0);
                }
            }
        }

        /**
         *
         */
        void reset() {
            partSingleMsgs.clear();
            partFullMsgs.clear();
        }

        /**
         * @return Sent partitions single messages.
         */
        Collection<GridDhtPartitionsSingleMessage> partitionsSingleMessages() {
            return partSingleMsgs;
        }

        /**
         * @return Sent partitions full messages.
         */
        Collection<GridDhtPartitionsFullMessage> partitionsFullMessages() {
            return partFullMsgs;
        }
    }

}
