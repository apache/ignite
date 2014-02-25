// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.router;

import org.gridgain.client.router.*;
import org.gridgain.client.ssl.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;

import javax.net.ssl.*;
import javax.swing.*;

/**
 * This example shows how to configure and run TCP and HTTP routers from java API.
 * <p>
 * Refer to {@link GridRouterFactory} documentation for more details on
 * how to manage routers' lifecycle. Also see {@link GridTcpRouterConfiguration}
 * and {@link GridHttpRouterConfiguration} for more configuration options.
 * <p>
 * Note that to start the example, {@code GRIDGAIN_HOME} system property or environment variable
 * must be set.
 *
 * @author @java.author
 * @version @java.version
 */
public class RouterStartup {
    static {
        // Disable host verification for testing with example certificates.
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String hostname, SSLSession sslSes) {
                return true;
            }
        });
    }

    /** Path to SSL-enabled Jetty configuration for router. */
    private static final String ROUTER_JETTY_SSL_CFG = "examples/config/router-jetty-ssl.xml";

    /** Change to {@code false} to disable {@code TCP_NODELAY}. */
    private static final boolean TCP_NODELAY = true;

    /**
     * Change to {@code true} to enable SSL.
     * Note that you need to appropriately update node and client configurations.
     */
    private static final boolean SSL_ENABLED = false;

    /**
     * Starts up a router with default configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If router failed to start.
     */
    public static void main(String[] args) throws GridException {
        try {
            GridRouterFactory.startTcpRouter(tcpRouterConfiguration());

            GridRouterFactory.startHttpRouter(httpRouterConfiguration());

            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[]{
                    new JLabel("GridGain router started."),
                    new JLabel("Press OK to stop GridGain router.")
                },
                "GridGain router",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
        finally {
            GridRouterFactory.stopAllRouters();
        }
    }

    /**
     * Creates a default TCP router configuration.
     *
     * @return TCP router configuration
     */
    private static GridTcpRouterConfiguration tcpRouterConfiguration() {
        GridTcpRouterConfiguration cfg = new GridTcpRouterConfiguration();

        cfg.setNoDelay(TCP_NODELAY);

        if (SSL_ENABLED) {
            String home = ExamplesUtils.resolveGridGainHome();

            GridSslBasicContextFactory sslFactory = new GridSslBasicContextFactory();

            sslFactory.setKeyStoreFilePath(home + "/examples/keystore/client.jks");
            sslFactory.setKeyStorePassword("123456".toCharArray());

            sslFactory.setTrustStoreFilePath(home + "/examples/keystore/trust.jks");
            sslFactory.setTrustStorePassword("123456".toCharArray());

            cfg.setSslContextFactory(sslFactory);

            cfg.setCredentials("s3cret");
        }

        return cfg;
    }

    /**
     * Creates a default HTTP router configuration.
     *
     * @return HTTP router configuration
     */
    private static GridHttpRouterConfiguration httpRouterConfiguration() {
        GridHttpRouterConfiguration cfg = new GridHttpRouterConfiguration();

        // Uncomment the following line to provide custom Jetty configuration.
        //cfg.setJettyConfigurationPath("config/my-router-jetty.xml");

        if (SSL_ENABLED) {
            String home = ExamplesUtils.resolveGridGainHome();

            GridSslBasicContextFactory sslFactory = new GridSslBasicContextFactory();

            sslFactory.setKeyStoreFilePath(home + "/examples/keystore/client.jks");
            sslFactory.setKeyStorePassword("123456".toCharArray());

            sslFactory.setTrustStoreFilePath(home + "/examples/keystore/trust.jks");
            sslFactory.setTrustStorePassword("123456".toCharArray());

            cfg.setClientSslContextFactory(sslFactory);

            cfg.setCredentials("s3cret");

            cfg.setJettyConfigurationPath(ROUTER_JETTY_SSL_CFG);
        }

        return cfg;
    }
}
