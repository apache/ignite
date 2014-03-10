/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.grid.*;

/**
 * Test client security when client operates in HTTP mode.
 */
public class GridClientHttpSecuritySelfTest extends GridClientAbstractSecuritySelfTest {
    /** Path to jetty config configured with SSL. */
    private static final String REST_JETTY_CFG = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

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
        return "127.0.0.1:" + JETTY_PORT;
    }
}
