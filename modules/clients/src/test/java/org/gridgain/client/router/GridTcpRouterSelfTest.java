package org.gridgain.client.router;

import org.gridgain.client.ssl.*;

/**
 * Tests the simplest use case for router: singe router proxies connections to a single node.
 */
public class GridTcpRouterSelfTest extends GridTcpRouterAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return null;
    }
}
