package org.gridgain.client.router;

import org.apache.ignite.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;

/**
 *
 */
public class GridTcpSslRouterSelfTest extends GridTcpRouterAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return GridTestUtils.sslContextFactory();
    }

    /**
     * @return Router configuration.
     */
    @Override public GridTcpRouterConfiguration routerConfiguration() throws IgniteCheckedException {
        GridTcpRouterConfiguration cfg = super.routerConfiguration();

        cfg.setSslContextFactory(sslContextFactory());

        return cfg;
    }
}
