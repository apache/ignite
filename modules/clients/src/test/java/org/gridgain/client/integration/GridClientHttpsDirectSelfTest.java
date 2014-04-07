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
import org.gridgain.testframework.*;

import javax.net.ssl.*;
import java.util.*;

/**
 *
 */
public class GridClientHttpsDirectSelfTest extends GridClientAbstractSelfTest {
    /** Hostname verifier */
    private HostnameVerifier verifier;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // Save hostname verifier.
        verifier = HttpsURLConnection.getDefaultHostnameVerifier();

        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String s, SSLSession sslSes) {
                return true;
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        // Restore hostname verifier.
        if (verifier != null)
            HttpsURLConnection.setDefaultHostnameVerifier(verifier);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setRestJettyPath(REST_JETTY_SSL_CFG);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.HTTP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() {
        GridClientConfiguration cfg = super.clientConfiguration();

        cfg.setServers(Collections.<String>emptySet());
        cfg.setRouters(Collections.singleton(HOST + ":" + JETTY_PORT));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return GridTestUtils.sslContextFactory();
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
