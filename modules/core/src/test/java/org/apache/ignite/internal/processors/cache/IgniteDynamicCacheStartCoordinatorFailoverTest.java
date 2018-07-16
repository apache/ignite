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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class IgniteDynamicCacheStartCoordinatorFailoverTest extends GridCommonAbstractTest {
    /** Default IP finder for single-JVM cloud grid. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Latch which blocks DynamicCacheChangeFailureMessage until main thread has sent node fail signal. */
    private static volatile CountDownLatch latch;

    /** */
    private static final String COORDINATOR_ATTRIBUTE = "coordinator";

    /** Client mode flag. */
    private Boolean appendCustomAttribute;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        latch = new CountDownLatch(1);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new CustomCommunicationSpi();
        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));

        cfg.setCommunicationSpi(commSpi);

        cfg.setFailureDetectionTimeout(15_000);

        if (appendCustomAttribute) {
            Map<String, Object> attrs = new HashMap<>();

            attrs.put(COORDINATOR_ATTRIBUTE, Boolean.TRUE);

            cfg.setUserAttributes(attrs);
        }

        return cfg;
    }

    /**
     * Tests coordinator failover during cache start failure.
     *
     * @throws Exception If test failed.
     */
    public void testCoordinatorFailure() throws Exception {
        // Start coordinator node.
        appendCustomAttribute = true;

        Ignite g = startGrid(0);

        appendCustomAttribute = false;

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        awaitPartitionMapExchange();

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName("test-coordinator-failover");

        cfg.setAffinity(new BrokenAffinityFunction(false, getTestIgniteInstanceName(2)));

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        g1.getOrCreateCache(cfg);
                        return null;
                    }
                }, CacheException.class, null);

                return null;
            }
        }, "cache-starter-thread");

        latch.await();

        stopGrid(0, true);

        awaitPartitionMapExchange();

        // Correct the cache configuration.
        cfg.setAffinity(new RendezvousAffinityFunction());

        IgniteCache cache = g1.getOrCreateCache(cfg);

        checkCacheOperations(g1, cache);
    }

    /**
     * Test the basic cache operations.
     *
     * @param cache Cache.
     * @throws Exception If test failed.
     */
    protected void checkCacheOperations(Ignite ignite, IgniteCache cache) throws Exception {
        int cnt = 1000;

        // Check base cache operations.
        for (int i = 0; i < cnt; ++i)
            cache.put(i, i);

        for (int i = 0; i < cnt; ++i) {
            Integer v = (Integer) cache.get(i);

            assertNotNull(v);
            assertEquals(i, v.intValue());
        }

        // Check Data Streamer capabilities.
        try (IgniteDataStreamer streamer = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < 10_000; ++i)
                streamer.addData(i, i);
        }
    }

    /**
     * Communication SPI which could optionally block outgoing messages.
     */
    private static class CustomCommunicationSpi extends TcpCommunicationSpi {
        /**
         * Send message optionally either blocking it or throwing an exception if it is of
         * {@link GridJobExecuteResponse} type.
         *
         * @param node Destination node.
         * @param msg Message to be sent.
         * @param ackClosure Ack closure.
         * @throws org.apache.ignite.spi.IgniteSpiException If failed.
         */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {

            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof GridDhtPartitionsSingleMessage) {
                    Boolean attr = (Boolean) node.attributes().get(COORDINATOR_ATTRIBUTE);

                    GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage) msg0.message();

                    Exception err = singleMsg.getError();

                    if (Boolean.TRUE.equals(attr) && err != null) {
                        // skip message
                        latch.countDown();

                        return;
                    }
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }

    /**
     * Affinity function that throws an exception when affinity nodes are calculated on the given node.
     */
    public static class BrokenAffinityFunction extends RendezvousAffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Exception should arise on all nodes. */
        private boolean eOnAllNodes = false;

        /** Exception should arise on node with certain name. */
        private String gridName;

        /**
         * Default constructor.
         */
        public BrokenAffinityFunction() {
            // No-op.
        }

        /**
         * @param eOnAllNodes {@code True} if exception should be thrown on all nodes.
         * @param gridName Exception should arise on node with certain name.
         */
        public BrokenAffinityFunction(boolean eOnAllNodes, String gridName) {
            this.eOnAllNodes = eOnAllNodes;
            this.gridName = gridName;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            if (eOnAllNodes || ignite.name().equals(gridName))
                throw new IllegalStateException("Simulated exception [locNodeId="
                    + ignite.cluster().localNode().id() + "]");
            else
                return super.assignPartitions(affCtx);
        }
    }
}
