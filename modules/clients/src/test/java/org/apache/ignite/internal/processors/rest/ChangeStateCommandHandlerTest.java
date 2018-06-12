/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest;

import java.util.Collections;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.client.GridClientProtocol.TCP;

/**
 *
 */
public class ChangeStateCommandHandlerTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final String HOST = "127.0.0.1";

    /** */
    public static final int BINARY_PORT = 11212;

    /** */
    private GridClient client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(HOST);

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setPort(BINARY_PORT);

        cfg.setConnectorConfiguration(clientCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridClientConfiguration cfg = new GridClientConfiguration();
        cfg.setProtocol(TCP);
        cfg.setServers(Collections.singletonList("localhost:" + BINARY_PORT));

        client = GridClientFactory.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridClientFactory.stop(client.id());
    }

    /**
     *
     */
    public void testActivateDeActivate() throws GridClientException {
        GridClientClusterState state = client.state();

        boolean active = state.active();

        assertTrue(active);

        state.active(false);

        IgniteEx ig1 = grid(0);
        IgniteEx ig2 = grid(1);

        assertFalse(ig1.active());
        assertFalse(ig2.active());
        assertFalse(state.active());

        state.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(state.active());
    }
}
