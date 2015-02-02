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

package org.apache.ignite.client;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.client.GridClientProtocol.*;
import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Tests that client is able to connect to a grid with only default cache enabled.
 */
public class ClientDefaultCacheSelfTest extends GridCommonAbstractTest {
    /** Path to jetty config configured with SSL. */
    private static final String REST_JETTY_CFG = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** Port. */
    private static final int TCP_PORT = 11211;

    /** Cached local node id. */
    private UUID locNodeId;

    /** Http port. */
    private static final int HTTP_PORT = 8081;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_JETTY_PORT, String.valueOf(HTTP_PORT));

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();

        System.clearProperty (IGNITE_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        locNodeId = grid().localNode().id();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        assert cfg.getClientConnectionConfiguration() == null;

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        clientCfg.setRestJettyPath(REST_JETTY_CFG);

        cfg.setClientConnectionConfiguration(clientCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /**
     * @return Client.
     * @throws GridClientException In case of error.
     */
    private GridClient clientTcp() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setProtocol(TCP);
        cfg.setServers(getServerList(TCP_PORT));
        cfg.setDataConfigurations(Collections.singleton(new GridClientDataConfiguration()));

        GridClient gridClient = GridClientFactory.start(cfg);

        assert F.exist(gridClient.compute().nodes(), new IgnitePredicate<GridClientNode>() {
            @Override public boolean apply(GridClientNode n) {
                return n.nodeId().equals(locNodeId);
            }
        });

        return gridClient;
    }

    /**
     * Builds list of connection strings with few different ports.
     * Used to avoid possible failures in case of port range active.
     *
     * @param startPort Port to start list from.
     * @return List of client connection strings.
     */
    private Collection<String> getServerList(int startPort) {
        Collection<String> srvs = new ArrayList<>();

        for (int i = startPort; i < startPort + 10; i++)
            srvs.add(HOST + ":" + i);

        return srvs;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTcp() throws Exception {
        try {
            boolean putRes = cache().putx("key", 1);

            assert putRes : "Put operation failed";

            GridClient client = clientTcp();

            Integer val = client.data().<String, Integer>get("key");

            assert val != null;

            assert val == 1;
        }
        finally {
            GridClientFactory.stopAll();
        }
    }
}
