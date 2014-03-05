/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Test routers factory.
 */
public class GridRouterFactorySelfTest extends GridCommonAbstractTest {
    /** Shared IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_HTTP_PORT = 11087;

    /** */
    private static final int ROUTER_HTTP_PORT = 12207;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        GridConfiguration cfg = new GridConfiguration();

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

            final Collection<GridHttpRouter> httpRouters = new LinkedList<>();
            final GridHttpRouterConfiguration httpCfg = new GridHttpRouterConfiguration();

            httpCfg.setServers(Collections.singleton("127.0.0.1:" + GRID_HTTP_PORT));

            System.setProperty(GG_JETTY_PORT, String.valueOf(ROUTER_HTTP_PORT));

            try {
                httpRouters.add(GridRouterFactory.startHttpRouter(httpCfg));

                // Expects fail due to port is already binded with previous http router.
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        GridRouterFactory.startHttpRouter(httpCfg);

                        return null;
                    }
                }, GridException.class, null);
            }
            finally {
                System.clearProperty(GG_JETTY_PORT);
            }

            for (GridTcpRouter tcpRouter : tcpRouters) {
                assertEquals(tcpCfg, tcpRouter.configuration());
                assertEquals(tcpRouter, GridRouterFactory.tcpRouter(tcpRouter.id()));
            }

            for (GridHttpRouter httpRouter : httpRouters) {
                assertEquals(httpCfg, httpRouter.configuration());
                assertEquals(httpRouter, GridRouterFactory.httpRouter(httpRouter.id()));
            }

            assertEquals("Validate all started tcp routers.", new HashSet<>(tcpRouters),
                new HashSet<>(GridRouterFactory.allTcpRouters()));

            assertEquals("Validate all started http routers.", new HashSet<>(httpRouters),
                new HashSet<>(GridRouterFactory.allHttpRouters()));

            for (Iterator<GridTcpRouter> it = tcpRouters.iterator(); it.hasNext(); ) {
                GridTcpRouter tcpRouter = it.next();

                GridRouterFactory.stopHttpRouter(tcpRouter.id());

                assertEquals("Validate all started tcp routers.", new HashSet<>(tcpRouters),
                    new HashSet<>(GridRouterFactory.allTcpRouters()));

                it.remove();

                GridRouterFactory.stopTcpRouter(tcpRouter.id());

                assertEquals("Validate all started tcp routers.", new HashSet<>(tcpRouters),
                    new HashSet<>(GridRouterFactory.allTcpRouters()));
            }

            for (Iterator<GridHttpRouter> it = httpRouters.iterator(); it.hasNext(); ) {
                GridHttpRouter httpRouter = it.next();

                GridRouterFactory.stopTcpRouter(httpRouter.id());

                assertEquals("Validate all started http routers.", new HashSet<>(httpRouters),
                    new HashSet<>(GridRouterFactory.allHttpRouters()));

                it.remove();

                GridRouterFactory.stopHttpRouter(httpRouter.id());

                assertEquals("Validate all started http routers.", new HashSet<>(httpRouters),
                    new HashSet<>(GridRouterFactory.allHttpRouters()));
            }

            assertEquals(Collections.<GridTcpRouter>emptyList(), GridRouterFactory.allTcpRouters());
            assertEquals(Collections.<GridHttpRouter>emptyList(), GridRouterFactory.allHttpRouters());
        }
        finally {
            GridRouterFactory.stopAllRouters();
        }
    }
}
