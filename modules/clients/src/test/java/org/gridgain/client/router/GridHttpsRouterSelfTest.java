package org.gridgain.client.router;

import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;

import javax.net.ssl.*;

/**
 *
 */
public class GridHttpsRouterSelfTest extends GridHttpRouterAbstractSelfTest {
    /** Hostname verifier */
    private HostnameVerifier verifier;

    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return GridTestUtils.sslContextFactory();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // Save hostname verifier.
        verifier = HttpsURLConnection.getDefaultHostnameVerifier();

        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String s, SSLSession sslSes) {
                return true;
            }
        });

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        // Restore hostname verifier.
        if (verifier != null)
            HttpsURLConnection.setDefaultHostnameVerifier(verifier);
    }

    /** {@inheritDoc} */
    @Override public GridHttpRouterConfiguration routerConfiguration() throws GridException {
        GridHttpRouterConfiguration cfg = super.routerConfiguration();

        cfg.setJettyConfigurationPath(ROUTER_JETTY_SSL_CFG);
        cfg.setClientSslContextFactory(sslContextFactory());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setRestJettyPath(REST_JETTY_SSL_CFG);

        return cfg;
    }
}
