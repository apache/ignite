/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.apache.ignite.*;
import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

/**
 * Tests the REST client-server TCP connectivity with various configurations.
 */
public class GridClientTcpConnectivitySelfTest extends GridClientAbstractConnectivitySelfTest {
    /** {@inheritDoc} */
    @Override protected Ignite startRestNode(String name, @Nullable String addr, @Nullable Integer port)
        throws Exception {
        IgniteConfiguration cfg = getConfiguration(name);

        assert cfg.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        if (addr != null)
            clientCfg.setRestTcpHost(addr);

        if (port != null)
            clientCfg.setRestTcpPort(port);

        cfg.setClientConnectionConfiguration(clientCfg);

        return G.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected int defaultRestPort() {
        return IgniteConfiguration.DFLT_TCP_PORT;
    }

    /** {@inheritDoc} */
    @Override protected String restAddressAttributeName() {
        return GridNodeAttributes.ATTR_REST_TCP_ADDRS;
    }

    /** {@inheritDoc} */
    @Override protected String restHostNameAttributeName() {
        return GridNodeAttributes.ATTR_REST_TCP_HOST_NAMES;
    }

    /** {@inheritDoc} */
    @Override protected String restPortAttributeName() {
        return GridNodeAttributes.ATTR_REST_TCP_PORT;
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }
}
