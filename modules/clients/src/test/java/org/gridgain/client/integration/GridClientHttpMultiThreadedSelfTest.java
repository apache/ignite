/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.gridgain.client.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;

/**
 * Runs multithreaded client tests over http protocol without ssl. Note that since HTTP is stateless protocol,
 * our test will produce a lot of connections in TIME_WAIT state that be in this state up to 4 minutes on
 * some platforms. Since the total count of connections is limited near 65k connections, we reduce count
 * of iterations for HTTP protocol.
 */
public class GridClientHttpMultiThreadedSelfTest extends GridClientAbstractMultiThreadedSelfTest {
    /** Base for https rest ports. */
    public static final int REST_HTTP_PORT_BASE = 11080;

    /** Path to jetty config configured with SSL. */
    private static final String REST_JETTY_CFG = "modules/tests/config/jetty/rest-jetty.xml";

    /** {@inheritDoc} */
    @Override protected int taskExecutionCount() {
        // Total count of operations is reduced due to TIME_WAIT socket state issue.
        return 100;
    }

    /** {@inheritDoc} */
    @Override protected int cachePutCount() {
        // Total count of operations is reduced due to TIME_WAIT socket state issue.
        return 100;
    }

    /** {@inheritDoc} */
    @Override protected int syncCommitIterCount() {
        // Total count of operations is reduced due to TIME_WAIT socket state issue.
        return 10;
    }

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
        return HOST + ":" + REST_HTTP_PORT_BASE;
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
