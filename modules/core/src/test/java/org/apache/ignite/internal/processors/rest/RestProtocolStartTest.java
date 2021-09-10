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

package org.apache.ignite.internal.processors.rest;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test of start rest protocol and stop node.
 */
public class RestProtocolStartTest extends GridCommonAbstractTest {
    /** Node local host. */
    private static final String HOST = "127.0.0.1";

    /** Binary rest port. */
    private static final int BINARY_PORT = 11212;

    /** Recording communication spi. */
    private TestRecordingCommunicationSpi testBlockingCommunicationSpi = new TestRecordingCommunicationSpi();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setSystemWorkerBlockedTimeout(10_000)
            .setFailureDetectionTimeout(2_000)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setCommunicationSpi(igniteInstanceName.equals(getTestIgniteInstanceName(0))
                ? testBlockingCommunicationSpi :
                new TestRecordingCommunicationSpi())
            .setLocalHost(HOST)
            .setConnectorConfiguration(
                getTestIgniteInstanceName(1).equals(igniteInstanceName)
                    ? new ConnectorConfiguration()
                    .setPort(BINARY_PORT) : null);
    }

    /** */
    @Test
    public void test() throws Exception {
        Ignite ignite = startGrids(2);

        testBlockingCommunicationSpi.blockMessages(GridDhtPartitionSupplyMessage.class, getTestIgniteInstanceName(1));

        info("Bock supply messages.");

        ignite(1).close();

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++)
                streamer.addData(i, i);
        }

        AtomicReference<UUID> ign1IDHolder = new AtomicReference<>();

        CountDownLatch proceed = new CountDownLatch(1);

        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                DiscoveryEvent devt = ((DiscoveryEvent)evt);

                if (!devt.eventNode().isClient()) {
                    assertTrue("Only one server node is expedted.", ign1IDHolder.compareAndSet(null, devt.eventNode().id()));

                    proceed.countDown();
                }

                return true;
            }
        }, EventType.EVT_NODE_JOINED);

        IgniteInternalFuture startFut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(1);

                fail("Node should not started before rebalance completed.");
            }
            catch (Exception e) {
                assertEquals("Err = " + e.getMessage(), e.getClass(), IgniteInterruptedCheckedException.class);
            }
        });

        assertTrue("Is active " + ignite.cluster().active(), ignite.cluster().active());

        client();

        // Wait for the second node 1 joins again and for it's ID available.
        proceed.await();

        ignite.configuration().getDiscoverySpi().failNode(ign1IDHolder.get(), "Test failure.");

        doSleep(ignite.configuration().getFailureDetectionTimeout());

        testBlockingCommunicationSpi.stopBlock();

        try {
            startFut.get(10_000);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            fail("Failed to wait rebalance completed. Node has hang.");
        }

        assertTrue("Is active " + ignite.cluster().active(), ignite.cluster().active());
    }

    /**
     * @return Client.
     * @throws GridClientException In case of error.
     */
    protected GridClient client() throws GridClientException {
        return GridClientFactory.start(new GridClientConfiguration()
            .setConnectTimeout(300)
            .setServers(Collections
                .singleton(HOST + ":" + BINARY_PORT)));
    }
}
