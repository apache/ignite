// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.router;

import org.gridgain.client.*;
import org.gridgain.client.router.*;
import org.gridgain.client.ssl.*;
import org.gridgain.examples.*;
import org.gridgain.examples.misc.client.api.*;

import javax.net.ssl.*;
import java.util.*;

/**
 * This example demonstrates use of Java client, connected to Grid through router.
 * To execute this example you should start an instance of
 * {@link GridClientExampleNodeStartup} class which will start up a GridGain node.
 * And an instance of {@link GridExampleRouterStartup} which will start up
 * a GridGain router.
 * <p>
 * Alternatively you can run node and router instances from command line.
 * To do so you need to execute commands
 * {@code GRIDGAIN_HOME/bin/ggstart.sh examples/config/example-cache-client.xml}
 * and {@code GRIDGAIN_HOME/bin/ggrouter.sh config/router/default-router.xml}
 * For more details on how to configure standalone router instances please refer to
 * configuration file {@code GRIDGAIN_HOME/config/router/default-router.xml}.
 * <p>
 * This example creates client, configured to work with router and runs an example task
 * calculating number of nodes in grid.
 * <p>
 * Note that different nodes and routers cannot share the same port for rest services.
 * If you want to start more than one node and/or router on the same physical machine
 * you must provide different configurations for each node and router.
 * <p>
 * Another option for routing is to use one of node as a router itself. You can
 * transparently pass node address in {@link GridClientConfiguration#getRouters()}
 * an it will be routing incoming requests, To run the example in this mode just
 * uncomment alternative port number constants below and run few Grid nodes.
 * <p>
 * Note that to start the example, {@code GRIDGAIN_HOME} system property or environment variable
 * must be set.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridRouterExample {
    static {
        // Disable host verification for testing with example certificates.
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String hostname, SSLSession sslSes) {
                return true;
            }
        });
    }

    /** Grid node address to connect to. */
    private static final String ROUTER_ADDRESS = "127.0.0.1";

    /**
     * Change to {@code true} to enable SSL.
     * Note that you need to appropriately update node and client configurations.
     */
    private static final boolean SSL_ENABLED = false;

    /** Grid node port to connect to using binary protocol. */
    private static final int ROUTER_TCP_PORT = GridTcpRouterConfiguration.DFLT_TCP_PORT;

    // Run few nodes and uncomment the following line to use one of nodes as a TCP router.
    // private static final int ROUTER_TCP_PORT = GridConfiguration.DFLT_TCP_PORT;

    /** Port number from config/router/router-jetty.xml. */
    private static final int ROUTER_HTTP_PORT = 8180;

    // Run few nodes and uncomment the following line to use one of nodes as a HTTP router.
    // private static final int ROUTER_HTTP_PORT = 8080;

    /**
     * Runs router example using both TCP and HTTP protocols.
     *
     * @param args Command line arguments, none required.
     * @throws GridClientException If failed.
     */
    public static void main(String[] args) throws GridClientException {
        try (GridClient tcpClient = createTcpClient()) {
            System.out.println(">>> TCP client created, current grid topology: " + tcpClient.compute().nodes());

            runExample(tcpClient);
        }

        try (GridClient httpClient = createHttpClient()) {
            System.out.println(">>> HTTP client created, current grid topology: " + httpClient.compute().nodes());

            runExample(httpClient);
        }
    }

    /**
     * Performs few cache operations on the given client.
     * <p>
     * Running this method should produce
     * {@code >>> Client job is run by: Router example.}
     * output on every node in grid.
     *
     * @param client Client to run example.
     * @throws GridClientException If failed.
     */
    private static void runExample(GridClient client) throws GridClientException {
        System.out.println(">>> Executing task...");
        System.out.println(">>> Task result: " + client.compute().
            execute(GridClientExampleTask.class.getName(), "Router example."));
    }

    /**
     * This method will create a client configured to work with locally started router
     * on TCP REST protocol.
     *
     * @return Client instance.
     * @throws GridClientException If client could not be created.
     */
    private static GridClient createTcpClient() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        if (SSL_ENABLED) {
            String home = GridExamplesUtils.resolveGridGainHome();

            GridSslBasicContextFactory sslFactory = new GridSslBasicContextFactory();

            sslFactory.setKeyStoreFilePath(home + "/examples/keystore/client.jks");
            sslFactory.setKeyStorePassword("123456".toCharArray());

            sslFactory.setTrustStoreFilePath(home + "/examples/keystore/trust.jks");
            sslFactory.setTrustStorePassword("123456".toCharArray());

            cfg.setSslContextFactory(sslFactory);

            cfg.setCredentials("s3cret");
        }

        // Point client to a local TCP router.
        cfg.setRouters(Collections.singletonList(ROUTER_ADDRESS + ':' + ROUTER_TCP_PORT));

        return GridClientFactory.start(cfg);
    }

    /**
     * This method will create a client configured to work with locally started router
     * on HTTP REST protocol.
     *
     * @return Client instance.
     * @throws GridClientException If client could not be created.
     */
    private static GridClient createHttpClient() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setProtocol(GridClientProtocol.HTTP);

        if (SSL_ENABLED) {
            String home = GridExamplesUtils.resolveGridGainHome();

            GridSslBasicContextFactory sslFactory = new GridSslBasicContextFactory();

            sslFactory.setKeyStoreFilePath(home + "/examples/keystore/client.jks");
            sslFactory.setKeyStorePassword("123456".toCharArray());

            sslFactory.setTrustStoreFilePath(home + "/examples/keystore/trust.jks");
            sslFactory.setTrustStorePassword("123456".toCharArray());

            cfg.setSslContextFactory(sslFactory);

            cfg.setCredentials("s3cret");
        }

        // Point client to a local HTTP router.
        cfg.setRouters(Collections.singletonList(ROUTER_ADDRESS + ":" + ROUTER_HTTP_PORT));

        return GridClientFactory.start(cfg);
    }
}
