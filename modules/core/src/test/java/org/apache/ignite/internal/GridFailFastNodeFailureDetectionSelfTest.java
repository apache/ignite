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
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.messaging.MessagingListenActor;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;

/**
 * Fail fast test.
 */
public class GridFailFastNodeFailureDetectionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);
        disco.setHeartbeatFrequency(10_000);

        // Set parameters for fast ping failure.
        disco.setSocketTimeout(100);
        disco.setNetworkTimeout(100);
        disco.setReconnectCount(2);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailFast() throws Exception {
        startGridsMultiThreaded(5);

        final CountDownLatch failLatch = new CountDownLatch(4);

        for (int i = 0; i < 5; i++) {
            ignite(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    info(evt.shortDisplay());

                    failLatch.countDown();

                    return true;
                }
            }, EVT_NODE_FAILED);
        }

        Ignite ignite1 = ignite(0);
        Ignite ignite2 = ignite(1);

        final CountDownLatch evtLatch = new CountDownLatch(1);

        ignite1.message().localListen(null, new MessagingListenActor<Object>() {
            @Override protected void receive(UUID nodeId, Object rcvMsg) throws Throwable {
                respond(rcvMsg);
            }
        });

        ignite2.message().localListen(null, new MessagingListenActor<Object>() {
            @Override protected void receive(UUID nodeId, Object rcvMsg) throws Throwable {
                evtLatch.countDown();

                respond(rcvMsg);
            }
        });

        ignite1.message(ignite1.cluster().forRemotes()).send(null, "Message");

        evtLatch.await(); // Wait when connection is established.

        log.info("Fail node: " + ignite1.cluster().localNode());

        failNode(ignite1);

        assert failLatch.await(1000, MILLISECONDS);
    }

    /**
     * @param ignite Ignite.
     * @throws Exception In case of error.
     */
    private void failNode(Ignite ignite) throws Exception {
        DiscoverySpi disco = ignite.configuration().getDiscoverySpi();

        U.invoke(disco.getClass(), disco, "simulateNodeFailure");

        CommunicationSpi comm = ignite.configuration().getCommunicationSpi();

        U.invoke(comm.getClass(), comm, "simulateNodeFailure");
    }
}