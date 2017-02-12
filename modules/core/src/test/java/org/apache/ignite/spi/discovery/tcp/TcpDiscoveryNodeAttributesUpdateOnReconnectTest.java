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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checks whether on client reconnect node attributes from kernal context are sent.
 */
public class TcpDiscoveryNodeAttributesUpdateOnReconnectTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoverySpi clntSpi;

    /** */
    private CountDownLatch joinLatch = new CountDownLatch(2);

    /** */
    private volatile String rejoinAttr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("client")) {
            Map<String, String> attrs = new HashMap<>();

            attrs.put("test", "1");

            cfg.setUserAttributes(attrs);

            cfg.setClientMode(true);

            clntSpi = new TestDiscoverySpi();

            TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
            clntSpi.setIpFinder(spi.getIpFinder());

            cfg.setDiscoverySpi(clntSpi);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect() throws Exception {
        IgniteEx srv = (IgniteEx)startGrid("server");

        IgniteEvents evts = srv.events();

        evts.enableLocal(EventType.EVTS_DISCOVERY_ALL);
        evts.localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                ClusterNode node = ((DiscoveryEvent)evt).eventNode();

                rejoinAttr = node.attribute("test");

                joinLatch.countDown();

                return true;
            }
        }, EventType.EVT_NODE_JOINED);

        IgniteEx client = (IgniteEx)startGrid("client");

        client.context().addNodeAttribute("test", "2");

        System.out.println(">>> Brake connection...");
        clntSpi.brakeConnection();

        assert joinLatch.await(30, TimeUnit.SECONDS);

        assertEquals("2", rejoinAttr);
    }

    /**
     * Drops reconnect message to force client initiate new join process.
     */
    private class TestDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                return;

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                return;

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                return;

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                return;

            super.writeToSocket(msg, sock, res, timeout);
        }
    }
}
