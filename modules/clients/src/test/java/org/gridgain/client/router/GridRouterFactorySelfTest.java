/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Test routers factory.
 */
public class GridRouterFactorySelfTest extends GridCommonAbstractTest {
    /** Shared IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_HTTP_PORT = 11087;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

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
