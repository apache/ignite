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

package org.apache.ignite.spi.discovery.tcp;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * This class tests that a client is able to connect to another server node without leaving the cluster.
 */
public class TcpClientDiscoverySpiCoordinatorChangeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks that a client node doesn't fail because of coordinator change.
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testClientNotFailed() throws Exception {
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

        // Start server A.
        Ignite srvA = startNode("server-a", ipFinder, false);

        // Start the client.
        Ignite client = startNode("client", ipFinder, true);

        AtomicBoolean clientReconnectState = getClientReconnectState(client);

        // Start server B.
        Ignite srvB = startNode("server-b", ipFinder, false);

        // Stop server A.
        srvA.close();

        // Will throw an exception if the client is disconnected.
        client.getOrCreateCache("CACHE-NAME");

        // Check that the client didn't disconnect/reconnect quickly.
        assertFalse("Client node was failed and reconnected to the cluster.", clientReconnectState.get());

        // Stop the client.
        client.close();

        // Stop server B.
        srvB.close();
    }

    /**
     * @param instanceName Instance name.
     * @param ipFinder IP-finder.
     * @param clientMode Client mode flag.
     * @return Started node.
     * @throws Exception If a node was not started.
     */
    private Ignite startNode(String instanceName, TcpDiscoveryIpFinder ipFinder, boolean clientMode) throws Exception {
        IgniteConfiguration cfg = getConfiguration(instanceName)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder))
            .setClientMode(clientMode);

        return Ignition.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName)
            .setMetricsUpdateFrequency(Integer.MAX_VALUE)
            .setClientFailureDetectionTimeout(Integer.MAX_VALUE)
            .setFailureDetectionTimeout(Integer.MAX_VALUE);
    }

    /**
     * @param ignite Client node.
     * @return Client reconnect state.
     */
    private AtomicBoolean getClientReconnectState(Ignite ignite) {
        final AtomicBoolean reconnectState = new AtomicBoolean(false);

        ignite.events().localListen(
            new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EventType.EVT_CLIENT_NODE_RECONNECTED)
                        reconnectState.set(true);

                    return true;
                }
            },
            EventType.EVT_CLIENT_NODE_RECONNECTED
        );

        return reconnectState;
    }
}
