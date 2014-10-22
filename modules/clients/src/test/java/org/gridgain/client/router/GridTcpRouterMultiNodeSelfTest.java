package org.gridgain.client.router;

import org.gridgain.client.*;
import org.gridgain.client.integration.*;
import org.gridgain.client.router.impl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.log4j.*;

import java.util.*;

import static org.gridgain.client.integration.GridClientAbstractSelfTest.*;

/**
 *
 */
public class GridTcpRouterMultiNodeSelfTest extends GridClientAbstractMultiNodeSelfTest {
    /** Number of routers to start in this test. */
    private static final int ROUTERS_CNT = 5;

    /** Where to start routers' port numeration. */
    private static final int ROUTER_TCP_PORT_BASE = REST_TCP_PORT_BASE + NODES_CNT;

    /** Collection of routers. */
    private static Collection<GridTcpRouterImpl> routers = new ArrayList<>(ROUTERS_CNT);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < ROUTERS_CNT; i++)
            routers.add(new GridTcpRouterImpl(routerConfiguration(i++)));

        for (GridTcpRouterImpl r : routers)
            r.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        info("Stopping routers...");

        for (GridTcpRouterImpl r : routers)
            r.stop();

        info("Routers stopped.");

        routers.clear();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /**
     * @param i Number of router. Used to avoid configuration conflicts.
     * @return Router configuration.
     * @throws GridException If failed.
     */
    private GridTcpRouterConfiguration routerConfiguration(int i) throws GridException {
        GridTcpRouterConfiguration cfg = new GridTcpRouterConfiguration();

        cfg.setHost(HOST);
        cfg.setPort(ROUTER_TCP_PORT_BASE + i);
        cfg.setPortRange(0);
        cfg.setServers(Collections.singleton(HOST + ":" + REST_TCP_PORT_BASE));
        cfg.setLogger(new GridLog4jLogger(ROUTER_LOG_CFG));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());

        Collection<String> rtrs = new ArrayList<>(ROUTERS_CNT);

        for (int i = 0; i < ROUTERS_CNT; i++)
            rtrs.add(HOST + ':' + (ROUTER_TCP_PORT_BASE + i));

        cfg.setRouters(rtrs);

        return cfg;
    }
}
