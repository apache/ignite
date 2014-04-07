/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Tests the REST client-server HTTP connectivity with various configurations.
 */
public class GridClientHttpConnectivitySelfTest extends GridClientAbstractConnectivitySelfTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(GG_JETTY_HOST);
        System.clearProperty(GG_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected Grid startRestNode(String name, @Nullable String addr, @Nullable Integer port)
        throws Exception {
        GridConfiguration cfg = getConfiguration(name);

        cfg.setRestEnabled(true);

        if (addr != null) {
            cfg.setLocalHost(null);

            System.setProperty(GG_JETTY_HOST, addr);
        }
        else
            System.clearProperty(GG_JETTY_HOST);

        if (port != null)
            System.setProperty(GG_JETTY_PORT, String.valueOf(port));
        else
            System.clearProperty(GG_JETTY_PORT);

        return G.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected int defaultRestPort() {
        return 8080;
    }

    /** {@inheritDoc} */
    @Override protected String restAddressAttributeName() {
        return GridNodeAttributes.ATTR_REST_JETTY_ADDRS;
    }

    /** {@inheritDoc} */
    @Override protected String restHostNameAttributeName() {
        return GridNodeAttributes.ATTR_REST_JETTY_HOST_NAMES;
    }

    /** {@inheritDoc} */
    @Override protected String restPortAttributeName() {
        return GridNodeAttributes.ATTR_REST_JETTY_PORT;
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.HTTP;
    }
}
