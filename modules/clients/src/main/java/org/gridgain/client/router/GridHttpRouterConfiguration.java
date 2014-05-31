package org.gridgain.client.router;

import org.gridgain.client.ssl.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * This class defines runtime configuration for HTTP router. This configuration is passed to
 * {@link GridRouterFactory#startHttpRouter(GridHttpRouterConfiguration)} method.
 * <p>
 * Note that you should only set values
 * that differ from defaults, as router will automatically pick default values
 * for all values that are not set.
 * <p>
 * For more information about router configuration and startup refer to {@link GridRouterFactory}
 * documentation.
 */
public class GridHttpRouterConfiguration {
    /**
     * Default value for {@code #getServers} property.
     * Connects to the default HTTP rest server configuration.
     */
    public static final Collection<String> DFLT_SERVERS = Collections.singleton("127.0.0.1:8080");

    /** Default value for {@link #getJettyConfigurationPath()} property. */
    public static final String DFLT_CFG_PATH = "clients/java/config/router/router-jetty.xml";

    /** Default value for {@link #getConnectionsTotal()} property. */
    public static final int DFLT_CONNECTIONS_TOTAL = 200;

    /** Default value for {@link #getConnectionsPerRoute()} property. */
    public static final int DFLT_CONNECTIONS_PER_ROUTE = 20;

    /** Default value for {@link #getRequestTimeout()} property. */
    public static final long DFLT_REQUEST_TIMEOUT = 10000;

    /** Jetty configuration path. */
    private String cfgPath = DFLT_CFG_PATH;

    /** List of servers. */
    private Collection<String> srvs = DFLT_SERVERS;

    /** Ssl context factory. */
    private GridSslContextFactory clientSslCtxFactory;

    /** Max connections from router to grid. */
    private int connsTotal = DFLT_CONNECTIONS_TOTAL;

    /** Max connections per route. */
    private int connsPerRoute = DFLT_CONNECTIONS_PER_ROUTE;

    /** Request timeout. */
    private long reqTimeout = DFLT_REQUEST_TIMEOUT;

    /** Logger. */
    private GridLogger log;

    /** Credentials. */
    private GridSecurityCredentialsProvider credsProvider;

    /**
     * Gets path, either absolute or relative to {@code GRIDGAIN_HOME}, to {@code Jetty}
     * XML configuration file. {@code Jetty} is used to support REST over HTTP protocol for
     * accessing GridGain APIs remotely.
     * <p>
     * By default, {@code Jetty} configuration file is located under
     * {@code GRIDGAIN_HOME/config/router/router-jetty.xml}.
     *
     * @return Path to {@code JETTY} XML configuration file.
     */
    public String getJettyConfigurationPath() {
        return cfgPath;
    }

    /**
     * Gets list of server addresses where router's embedded client should connect.
     * <p>
     * Node that this list will be used only for initial grid exploration.
     * When running router could establish connections to any node in grid.
     *
     * @return List of server addresses.
     */
    public Collection<String> getServers() {
        return srvs;
    }

    /**
     * Gets SSL context factory that will be used for creating a secure socket layer for out coming connections.
     *
     * @return SslContextFactory instance.
     * @see GridSslContextFactory
     */
    @Nullable public GridSslContextFactory getClientSslContextFactory() {
        return clientSslCtxFactory;
    }

    /**
     * Gets maximum connections from router to grid.
     *
     * @return Maximum connections from router to grid.
     * @see <a href="http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html">
     *     HTTP client connection management</a>
     */
    public int getConnectionsTotal() {
        return connsTotal;
    }

    /**
     * Gets maximum connections per route.
     *
     * @return Maximum connections per route.
     * @see <a href="http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html">
     *     HTTP client connection management</a>
     */
    public int getConnectionsPerRoute() {
        return connsPerRoute;
    }

    /**
     * Gets request timeout.
     * <p>
     * Any request taking loner than this time will be aborted.
     *
     * @return Request timeout.
     */
    public long getRequestTimeout() {
        return reqTimeout;
    }

    /**
     * Gets logger for the router instance.
     * If no logger provided JDK logging will be used by router implementation.
     *
     * @return Logger or {@code null} if no logger provided by configuration..
     */
    public GridLogger getLogger() {
        return log;
    }

    /**
     * Gets credentials for grid access.
     * This credentials will be used only for initial connection and topology discovery
     * by the router, not for client's request authorization.
     *
     * @return Credentials provider.
     */
    @Nullable public GridSecurityCredentialsProvider getSecurityCredentialsProvider() {
        return credsProvider;
    }

    /**
     * Sets path, either absolute or relative to {@code GRIDGAIN_HOME},
     * to {@code Jetty} XML configuration file.
     *
     * @param cfgPath Jetty configuration path.
     */
    public void setJettyConfigurationPath(String cfgPath) {
        this.cfgPath = cfgPath;
    }

    /**
     * Sets list of server addresses where router's embedded client should connect.
     *
     * @param srvs List of servers.
     */
    public void setServers(Collection<String> srvs) {
        this.srvs = srvs;
    }

    /**
     * Sets SSL context factory that will be used for creating a secure socket layer
     * for out coming connections.
     *
     * @param clientSslCtxFactory Ssl context factory.
     */
    public void setClientSslContextFactory(GridSslContextFactory clientSslCtxFactory) {
        this.clientSslCtxFactory = clientSslCtxFactory;
    }

    /**
     * Sets maximum connections from router to grid.
     *
     * @param connsTotal Maximum connections from router to grid.
     * @see <a href="http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html">
     *     HTTP client connection management</a>
     */
    public void setConnectionsTotal(int connsTotal) {
        this.connsTotal = connsTotal;
    }

    /**
     * Gets maximum connections per route.
     *
     * @param connsPerRoute Maximum connections per route.
     * @see <a href="http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html">
     *     HTTP client connection management</a>
     */
    public void setConnectionsPerRoute(int connsPerRoute) {
        this.connsPerRoute = connsPerRoute;
    }

    /**
     * Gets request timeout,
     *
     * @param reqTimeout Request timeout.
     */
    public void setRequestTimeout(long reqTimeout) {
        this.reqTimeout = reqTimeout;
    }

    /**
     * Sets logger for the router instance.
     *
     * @param log Logger.
     */
    public void setLogger(GridLogger log) {
        this.log = log;
    }

    /**
     * Sets credentials provider for grid access.
     *
     * @param credsProvider Credentials provider..
     */
    public void setSecurityCredentialsProvider(GridSecurityCredentialsProvider credsProvider) {
        this.credsProvider = credsProvider;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHttpRouterConfiguration.class, this);
    }
}
