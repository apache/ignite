package org.gridgain.client.router;

import org.gridgain.client.*;
import org.gridgain.client.router.impl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.log4j.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.client.router.GridClientAbstractSelfTest.*;
import static org.gridgain.grid.GridSystemProperties.*;

/**
 *
 */
public class GridHttpRouterMultiNodeSelfTest extends GridClientAbstractMultiNodeSelfTest {
    /** Template fo paths to jetty configurations. */
    private static final String ROUTER_JETTY_CFG = "modules/tests/config/jetty/router-jetty.xml";

    /** Number of routers to start in this test. */
    private static final int ROUTERS_CNT = 3;

    /** Jetty does not really stop the server if port is busy. */
    private static final AtomicInteger PORT_SHIFT = new AtomicInteger(0);

    /**
     * Where to start routers' port numeration.
     * Using higher ports than single node tests because of improper Jetty shutdown.
     */
    private static volatile int routerHttpPortBase;

    /** Collection of routers. */
    protected static final Collection<GridHttpRouterImpl> routers = new ArrayList<>(ROUTERS_CNT);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // Re-initialize each time routers are started due to invalid Jetty stop() behaviour.
        routerHttpPortBase = 8113 + PORT_SHIFT.addAndGet(ROUTERS_CNT * 2);

        info("Router base port initialised: " + routerHttpPortBase);

        for (int i = 0; i < ROUTERS_CNT; i++) {
            System.setProperty(GG_JETTY_PORT, Integer.toString(routerHttpPortBase + i));

            GridHttpRouterConfiguration cfg = routerConfiguration();
            GridHttpRouterImpl router = new GridHttpRouterImpl(cfg);

            routers.add(router);

            router.start();

            System.clearProperty(GG_JETTY_PORT);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // Stop routers.
        for (GridHttpRouterImpl router : routers)
            router.stop();

        routers.clear();

        // Stop grids.
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setRestJettyPath(REST_JETTY_CFG);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.HTTP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /**
     * @return Router configuration.
     * @throws GridException If failed.
     */
    @Nullable protected GridHttpRouterConfiguration routerConfiguration() throws GridException {
        GridHttpRouterConfiguration cfg = new GridHttpRouterConfiguration();

        cfg.setLogger(new GridLog4jLogger(ROUTER_LOG_CFG));
        cfg.setJettyConfigurationPath(ROUTER_JETTY_CFG);
        cfg.setServers(Collections.singleton(HOST + ":" + REST_HTTP_PORT_BASE));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() {
        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());

        Collection<String> rtrs = new ArrayList<>(ROUTERS_CNT);

        info("Router base port for client configuration: " + routerHttpPortBase);

        for (int i = 0; i < ROUTERS_CNT; i++)
            rtrs.add(HOST + ':' + (routerHttpPortBase + i));

        cfg.setRouters(rtrs);

        return cfg;
    }
}
