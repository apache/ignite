/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.client.router;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;

/**
 * Test routers factory.
 */
public class RouterFactorySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_HTTP_PORT = 11087;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(sharedStaticIpFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(discoSpi);
        cfg.setIgniteInstanceName(igniteInstanceName);

        return cfg;
    }

    /**
     * Test router's start/stop.
     *
     * @throws Exception In case of any exception.
     */
    @Test
    public void testRouterFactory() throws Exception {
        try {
            System.setProperty(IGNITE_JETTY_PORT, String.valueOf(GRID_HTTP_PORT));

            try {
                startGrid();
            }
            finally {
                System.clearProperty(IGNITE_JETTY_PORT);
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

            stopAllGrids();
        }
    }
}
