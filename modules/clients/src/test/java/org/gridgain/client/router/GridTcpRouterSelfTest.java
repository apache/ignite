package org.gridgain.client.router;

import org.gridgain.client.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

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
