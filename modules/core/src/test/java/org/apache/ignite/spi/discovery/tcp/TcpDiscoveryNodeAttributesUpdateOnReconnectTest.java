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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.TestReconnectSecurityPluginProvider;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteClientReconnectAbstractTest.reconnectClientNode;

/**
 * Checks whether on client reconnect node attributes from kernal context are sent.
 */
public class TcpDiscoveryNodeAttributesUpdateOnReconnectTest extends GridCommonAbstractTest {
    /** */
    private volatile String rejoinAttr;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("client")) {
            Map<String, String> attrs = new HashMap<>();

            attrs.put("test", "1");

            cfg.setUserAttributes(attrs);
        }

        IgniteClientReconnectAbstractTest.TestTcpDiscoverySpi spi = new IgniteClientReconnectAbstractTest.TestTcpDiscoverySpi();

        TcpDiscoveryIpFinder finder = ((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder();

        spi.setIpFinder(finder);

        cfg.setDiscoverySpi(spi);

        cfg.setPluginProviders(new TestReconnectSecurityPluginProvider());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnect() throws Exception {
        Ignite srv = startGrid("server");

        IgniteEvents evts = srv.events();

        evts.enableLocal(EventType.EVTS_DISCOVERY_ALL);
        evts.localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                ClusterNode node = ((DiscoveryEvent)evt).eventNode();

                rejoinAttr = node.attribute("test");

                return true;
            }
        }, EventType.EVT_NODE_JOINED);

        Ignite client = startClientGrid("client");

        reconnectClientNode(log, client, srv, null);

        assertEquals("2", rejoinAttr);
    }
}
