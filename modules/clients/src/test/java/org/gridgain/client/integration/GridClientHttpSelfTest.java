/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.integration;

import net.sf.json.*;
import org.gridgain.client.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;

/**
 * Tests HTTP protocol.
 */
public class GridClientHttpSelfTest extends GridClientAbstractSelfTest {
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
        return HOST + ":" + JETTY_PORT;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return null;
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
}
