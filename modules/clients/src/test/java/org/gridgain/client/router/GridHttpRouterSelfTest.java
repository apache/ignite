package org.gridgain.client.router;

import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;

/**
 *
 */
public class GridHttpRouterSelfTest extends GridHttpRouterAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setRestJettyPath(REST_JETTY_CFG);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public GridHttpRouterConfiguration routerConfiguration() throws GridException {
        GridHttpRouterConfiguration cfg = super.routerConfiguration();

        cfg.setJettyConfigurationPath(ROUTER_JETTY_CFG);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return null;
    }
}
