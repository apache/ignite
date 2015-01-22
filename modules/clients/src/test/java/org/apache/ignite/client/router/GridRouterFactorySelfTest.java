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

package org.apache.ignite.client.router;

import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Test routers factory.
 */
public class GridRouterFactorySelfTest extends GridCommonAbstractTest {
    /** Shared IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_HTTP_PORT = 11087;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(discoSpi);
        cfg.setGridName(gridName);

        return cfg;
    }

    /**
     * Test router's start/stop.
     *
     * @throws Exception In case of any exception.
     */
    public void testRouterFactory() throws Exception {
        try {
            System.setProperty(GG_JETTY_PORT, String.valueOf(GRID_HTTP_PORT));

            try {
                startGrid();
            }
            finally {
                System.clearProperty(GG_JETTY_PORT);
            }

            final int size = 20;
            final Collection<GridTcpRouter> tcpRouters = new ArrayList<>(size);
            final GridTcpRouterConfiguration tcpCfg = new GridTcpRouterConfiguration();

            tcpCfg.setPortRange(size);

            for (int i = 0; i < size; i++)
                tcpRouters.add(GridRouterFactory.startTcpRouter(tcpCfg));

            for (GridTcpRouter tcpRouter : tcpRouters) {
                assertEquals(tcpCfg, tcpRouter.configuration());
                assertEquals(tcpRouter, GridRouterFactory.tcpRouter(tcpRouter.id()));
            }

            assertEquals("Validate all started tcp routers.", new HashSet<>(tcpRouters),
                new HashSet<>(GridRouterFactory.allTcpRouters()));

            for (Iterator<GridTcpRouter> it = tcpRouters.iterator(); it.hasNext(); ) {
                GridTcpRouter tcpRouter = it.next();

                assertEquals("Validate all started tcp routers.", new HashSet<>(tcpRouters),
                    new HashSet<>(GridRouterFactory.allTcpRouters()));

                it.remove();

                GridRouterFactory.stopTcpRouter(tcpRouter.id());

                assertEquals("Validate all started tcp routers.", new HashSet<>(tcpRouters),
                    new HashSet<>(GridRouterFactory.allTcpRouters()));
            }

            assertEquals(Collections.<GridTcpRouter>emptyList(), GridRouterFactory.allTcpRouters());
        }
        finally {
            GridRouterFactory.stopAllRouters();
        }
    }
}
