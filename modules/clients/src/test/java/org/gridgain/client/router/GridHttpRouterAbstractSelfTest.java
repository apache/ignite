/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import net.sf.json.*;
import org.gridgain.client.*;
import org.gridgain.client.integration.*;
import org.gridgain.client.router.impl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.log4j.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Abstract base class for http routing tests.
 */
abstract class GridHttpRouterAbstractSelfTest extends GridClientAbstractSelfTest {
    /** Port number to use by router. */
    private static final int ROUTER_PORT = 8081;

    /** TCp router instance. */
    private static GridHttpRouterImpl router;

    /** Requests count. */
    private static long reqCnt;

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.HTTP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        reqCnt = router.getRequestsCount();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assert router.getRequestsCount() > reqCnt;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(GG_JETTY_PORT, Integer.toString(ROUTER_PORT));

        router = new GridHttpRouterImpl(routerConfiguration());

        router.start();

        System.clearProperty(GG_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        router.stop();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() {
        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());
        cfg.setRouters(Collections.singleton(HOST + ":" + ROUTER_PORT));

        return cfg;
    }

    /**
     * @return Router configuration.
     * @throws GridException If failed.
     */
    public GridHttpRouterConfiguration routerConfiguration() throws GridException {
        GridHttpRouterConfiguration cfg = new GridHttpRouterConfiguration();

        cfg.setLogger(new GridLog4jLogger(ROUTER_LOG_CFG));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected String getTaskName() {
        return HttpTestTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override protected String getSleepTaskName() {
        return SleepHttpTestTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override protected Object getTaskArgument() {
        return JSONSerializer.toJSON(super.getTaskArgument()).toString();
    }

    /**
     * @throws Exception If failed.
     */
    @Override public void testConnectable() throws Exception {
        GridClient client = client();

        List<GridClientNode> nodes = client.compute().refreshTopology(false, false);

        assertFalse(F.first(nodes).connectable());
    }
}
