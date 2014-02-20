// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.*;
import org.eclipse.jetty.xml.*;
import org.gridgain.client.*;
import org.gridgain.client.router.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.logger.java.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.xml.sax.*;

import javax.management.*;
import javax.net.ssl.*;
import java.io.*;
import java.lang.management.*;
import java.net.*;
import java.util.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridHttpRouterImpl implements GridHttpRouter, GridHttpRouterMBean {
    /** Id. */
    private final UUID id = UUID.randomUUID();

    /** Logger. */
    protected final GridLogger log;

    /** Configuration. */
    private final GridHttpRouterConfiguration cfg;

    /** Client. */
    private GridRouterClientImpl client;

    /** HTTP server. */
    private Server httpSrv;

    /** MBean name. */
    private ObjectName mbeanName;

    /** Jetty handler. */
    private GridHttpRouterJettyHandler jettyHnd;

    /**
     * @param cfg Configuration.
     */
    public GridHttpRouterImpl(GridHttpRouterConfiguration cfg) {
        this.cfg = cfg;

        log = cfg.getLogger() != null ?
            cfg.getLogger().getLogger(getClass()) : new GridJavaLogger().getLogger(getClass());
    }

    /**
     * Starts router.
     *
     * @throws GridException If start failed unexpectedly.
     */
    public void start() throws GridException {
        try {
            client = createClient(cfg);
        }
        catch (GridClientException e) {
            throw new GridException("Failed to initialise embedded client.", e);
        }

        URL cfgUrl = U.resolveGridGainUrl(cfg.getJettyConfigurationPath());

        if (cfgUrl != null) {
            SSLContext sslCtx;

            try {
                GridSslContextFactory sslCtxFactory = cfg.getClientSslContextFactory();

                sslCtx = sslCtxFactory == null ? null : sslCtxFactory.createSslContext();
            }
            catch (SSLException e) {
                throw new GridException("Failed to create SSL context.", e);
            }

            jettyHnd = new GridHttpRouterJettyHandler(client, sslCtx,
                cfg.getConnectionsTotal(), cfg.getConnectionsPerRoute(), log);

            startJetty(cfgUrl, jettyHnd);
        }
        else
            throw new GridException("Can't find Jetty XML in path path: " + cfg.getJettyConfigurationPath());

        try {
            ObjectName objName = U.registerMBean(
                ManagementFactory.getPlatformMBeanServer(),
                "Router",
                "HTTP Router " + id,
                getClass().getSimpleName(),
                this,
                GridHttpRouterMBean.class);

            if (log.isDebugEnabled())
                log.debug("Registered MBean: " + objName);

            mbeanName = objName;
        }
        catch (JMException e) {
            U.error(log, "Failed to register MBean.", e);
        }
    }

    /**
     * Stops router.
     */
    public void stop() {
        stopJetty();

        if (client != null)
            client.stop(true);

        if (mbeanName != null)
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbeanName);

                if (log.isDebugEnabled())
                    log.debug("Unregistered MBean: " + mbeanName);
            }
            catch (JMException e) {
                U.error(log, "Failed to unregister MBean.", e);
            }

        if (log.isInfoEnabled())
            log.info("HTTP router successfully stopped.");
    }

    /**
     * Starts embedded jetty server.
     *
     * @param cfgUrl Jetty configuration file URL.
     * @param hnd Handler for incoming requests.
     */
    private void startJetty(URL cfgUrl, Handler hnd) throws GridException {
        XmlConfiguration cfg;

        try {
            cfg = new XmlConfiguration(cfgUrl);
        }
        catch (FileNotFoundException e) {
            throw new GridException("Failed to find configuration file: " + cfgUrl, e);
        }
        catch (SAXException e) {
            throw new GridException("Failed to parse configuration file: " + cfgUrl, e);
        }
        catch (IOException e) {
            throw new GridException("Failed to load configuration file: " + cfgUrl, e);
        }

        try {
            httpSrv = (Server)cfg.configure();

            httpSrv.setHandler(hnd);

            httpSrv.start();

            if (!httpSrv.isStarted())
                throw new GridException("Jetty server did not start.");

            if (log.isInfoEnabled())
                log.info("HTTP router successfully started for endpoints: " + getEndpoints());
        }
        catch (BindException e) {
            throw new GridException("Failed to bind HTTP server to configured endpoints: " + getEndpoints(), e);
        }
        catch (MultiException e) {
            throw new GridException("Caught multi exception.", e);
        }
        catch (Exception e) {
            throw new GridException("Unexpected exception when starting Jetty.", e);
        }
    }

    /**
     * Get configured endpoints.
     *
     * @return Server configured endpoints.
     */
    private Collection<String> getEndpoints() {
        if (httpSrv == null)
            return Collections.emptyList();

        Connector[] connectors = httpSrv.getConnectors();
        Collection<String> endpoints = new ArrayList<>(connectors.length);

        for (Connector conn : connectors) {
            NetworkConnector netCon = (NetworkConnector)conn;

            String host = netCon.getHost();

            if (host == null)
                host = "0.0.0.0";

            endpoints.add(host + ":" + netCon.getPort());
        }

        return endpoints;
    }

    /**
     * Stops embedded jetty server.
     */
    private void stopJetty() {
        // Jetty does not really stop the server if port is busy.
        try {
            if (httpSrv != null) {
                httpSrv.stop();

                httpSrv = null;
            }
        }
        catch (Exception e) {
            U.error(log, "Failed to stop Jetty HTTP server.", e);
        }
    }

    /**
     * Creates a client for forwarding messages to the grid.
     *
     * @param routerCfg Router configuration..
     * @return Client instance.
     * @throws org.gridgain.client.GridClientException If client creation failed.
     */
    private GridRouterClientImpl createClient(GridHttpRouterConfiguration routerCfg) throws GridClientException {
        UUID clientId = UUID.randomUUID();

        return new GridRouterClientImpl(clientId, routerCfg);
    }

    /** {@inheritDoc} */
    @Override public String getHost() {
        return httpSrv.getConnectors().length > 0 ? ((NetworkConnector)httpSrv.getConnectors()[0]).getHost() : "";
    }

    /** {@inheritDoc} */
    @Override public int getPort() {
        return httpSrv.getConnectors().length > 0 ? ((NetworkConnector)httpSrv.getConnectors()[0]).getPort() : 0;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getServers() {
        return cfg.getServers();
    }

    /** {@inheritDoc} */
    @Override public GridHttpRouterConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long getRequestsCount() {
        return jettyHnd != null ? jettyHnd.getRequestsCount() : 0;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridHttpRouterImpl that = (GridHttpRouterImpl) o;

        return id.equals(that.id);
    }
}
