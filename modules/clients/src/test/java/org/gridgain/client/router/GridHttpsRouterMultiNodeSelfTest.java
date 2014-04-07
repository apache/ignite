/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.log4j.*;
import org.gridgain.testframework.*;

import java.util.*;

import static org.gridgain.client.integration.GridClientAbstractSelfTest.*;

/**
 *
 */
public class GridHttpsRouterMultiNodeSelfTest extends GridHttpRouterMultiNodeSelfTest {
    /** Path to jetty config configured with SSL. */
    private static final String REST_JETTY_SSL_CFG = "modules/clients/src/test/resources/jetty/router-jetty-ssl.xml";

    /** Base for http rest ports. */
    public static final int ROUTER_HTTPS_PORT_BASE = 12400;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setRestJettyPath(REST_JETTY_SSL_CFG);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridHttpRouterConfiguration routerConfiguration() throws GridException {
        GridHttpRouterConfiguration cfg = new GridHttpRouterConfiguration();

        cfg.setLogger(new GridLog4jLogger(ROUTER_LOG_CFG));
        cfg.setJettyConfigurationPath(REST_JETTY_SSL_CFG);
        cfg.setServers(Collections.singleton(HOST + ":" + ROUTER_HTTPS_PORT_BASE));
        cfg.setClientSslContextFactory(sslContextFactory());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return GridTestUtils.sslContextFactory();
    }
}
