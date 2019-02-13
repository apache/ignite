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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test of start rest protocol and stop node.
 */
public class RestProtocolStartTest extends GridCommonAbstractTest {
    /** Failure detection timeout. */
    private static final int FAILURE_DETECTION_TIMEOUT = 2_000;

    /** Node local host. */
    private static final String HOST = "127.0.0.1";

    /** Binary rest port. */
    private static final int BINARY_PORT = 11212;

    /** Recording communication spi. */
    private TestRecordingCommunicationSpi recordingCommunicationSpi = new TestRecordingCommunicationSpi();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setSystemWorkerBlockedTimeout(10_000)
            .setFailureDetectionTimeout(FAILURE_DETECTION_TIMEOUT)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 8)))
            .setCommunicationSpi(igniteInstanceName.equals(getTestIgniteInstanceName(0))
                ? recordingCommunicationSpi :
                new TestRecordingCommunicationSpi())
            .setLocalHost(HOST)
            .setConnectorConfiguration(
                getTestIgniteInstanceName(1).equals(igniteInstanceName)
                    ? new ConnectorConfiguration()
                    .setPort(BINARY_PORT) : null);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        Ignite ignite = startGrids(2);

        recordingCommunicationSpi.blockMessages(GridDhtPartitionSupplyMessage.class, getTestIgniteInstanceName(1));

        info("Bock supply messages.");

        ignite(1).close();

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++)
                streamer.addData(i, i);
        }

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

        GridClient gridClient = client();

        ((TcpDiscoverySpi)ignite.configuration().getDiscoverySpi()).brakeConnection();

        doSleep(FAILURE_DETECTION_TIMEOUT);

        recordingCommunicationSpi.stopBlock();

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
