/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientBeforeNodeStart;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientDataAffinity;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientNodeStateBeforeStart;
import org.apache.ignite.internal.client.GridClientPartitionAffinity;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.GridClientTopologyListener;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.balancer.GridClientRandomBalancer;
import org.apache.ignite.internal.client.impl.connection.GridClientConnection;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionManager;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionManagerOsImpl;
import org.apache.ignite.internal.client.impl.connection.GridClientTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.CycleThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Client implementation.
 */
public class GridClientImpl implements GridClient, GridClientBeforeNodeStart {
    /** Null mask object. */
    private static final Object NULL_MASK = new Object();

    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientImpl.class.getName());

    /* Suppression logging if needed. */
    static {
        if (!IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_GRID_CLIENT_LOG_ENABLED, false))
            log.setLevel(Level.OFF);
    }

    /** Client ID. */
    private final UUID id;

    /** Client configuration. */
    protected final GridClientConfiguration cfg;

    /** SSL context if ssl enabled. */
    private final SSLContext sslCtx;

    /** Main compute projection. */
    @Nullable private final GridClientComputeImpl compute;

    /** Cluster state projection. */
    @Nullable private final GridClientClusterStateImpl clusterState;

    /** Data projections. */
    private final ConcurrentMap<Object, GridClientDataImpl> dataMap = new ConcurrentHashMap<>();

    /** Topology. */
    protected final GridClientTopology top;

    /** Topology updater thread. */
    @Nullable private final Thread topUpdateThread;

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Connection manager. */
    protected final GridClientConnectionManager connMgr;

    /** Routers. */
    private final Collection<InetSocketAddress> routers;

    /** Servers. */
    private final Collection<InetSocketAddress> srvs;

    /** Projection of node state before its start. */
    @Nullable private final GridClientNodeStateBeforeStart beforeStartState;

    /**
     * Creates a new client based on a given configuration.
     * <p/>
     * If {@code beforeNodeStart == true}, topology will not be received/updated,
     * and there will also be errors when trying to work with topology, compute, state and cache.
     *
     * @param id Client identifier.
     * @param cfg0 Client configuration.
     * @param routerClient Router client flag.
     * @param beforeNodeStart Connecting to a node before it start.
     * @throws GridClientException If client configuration is incorrect.
     * @throws GridServerUnreachableException If none of the servers specified in configuration can be reached.
     */
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public GridClientImpl(
        UUID id, 
        GridClientConfiguration cfg0,
        boolean routerClient, 
        boolean beforeNodeStart
    ) throws GridClientException {
        this.id = id;

        cfg = new GridClientConfiguration(cfg0);

        boolean success = false;

        try {
            top = new GridClientTopology(cfg);

            if (!beforeNodeStart) {
                for (GridClientDataConfiguration dataCfg : cfg.getDataConfigurations()) {
                    GridClientDataAffinity aff = dataCfg.getAffinity();

                    if (aff instanceof GridClientTopologyListener)
                        addTopologyListener((GridClientTopologyListener)aff);
                }
            }

            if (!beforeNodeStart && cfg.getBalancer() instanceof GridClientTopologyListener)
                top.addTopologyListener((GridClientTopologyListener)cfg.getBalancer());

            Factory<SSLContext> factory = cfg.getSslContextFactory();

            if (factory != null)
                sslCtx = factory.create();
            else
                sslCtx = null;

            if (cfg.isAutoFetchMetrics() && !cfg.isEnableMetricsCache())
                log.warning("Auto-fetch for metrics is enabled without enabling caching for them.");

            if (cfg.isAutoFetchAttributes() && !cfg.isEnableAttributesCache())
                log.warning(
                    "Auto-fetch for node attributes is enabled without enabling caching for them.");

            srvs = parseAddresses(cfg.getServers());
            routers = parseAddresses(cfg.getRouters());

            if (srvs.isEmpty() && routers.isEmpty())
                throw new GridClientException("Servers addresses and routers addresses cannot both be empty " +
                    "for client (please fix configuration and restart): " + this);

            if (!srvs.isEmpty() && !routers.isEmpty())
                throw new GridClientException("Servers addresses and routers addresses cannot both be provided " +
                    "for client (please fix configuration and restart): " + this);

            connMgr = createConnectionManager(id, sslCtx, cfg, routers, top, null, routerClient, beforeNodeStart);

            try {
                // Init connection manager.
                tryInit();
            }
            catch (GridClientException e) {
                top.fail(e);

                log.warning("Failed to initialize topology on client start. Will retry in background.");
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridClientException("Client startup was interrupted.", e);
            }

            if (!beforeNodeStart) {
                beforeStartState = null;

                topUpdateThread = new TopologyUpdaterThread();

                topUpdateThread.setDaemon(true);

                topUpdateThread.start();

                compute = new GridClientComputeImpl(this, null, null, cfg.getBalancer());

                clusterState = new GridClientClusterStateImpl(this, null, null, cfg.getBalancer());
            }
            else {
                topUpdateThread = null;

                compute = null;

                clusterState = null;

                beforeStartState = new GridClientNodeStateBeforeStartImpl(this);
            }

            if (log.isLoggable(Level.INFO))
                log.info("Client started [id=" + id + ", protocol=" + cfg.getProtocol() + ']');

            success = true;
        }
        finally {
            if (!success)
                stop(false);
        }
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /**
     * Closes client.
     * @param waitCompletion If {@code true} will wait for all pending requests to be proceeded.
     */
    public void stop(boolean waitCompletion) {
        if (closed.compareAndSet(false, true)) {
            // Shutdown the topology refresh thread.
            if (topUpdateThread != null)
                topUpdateThread.interrupt();

            // Shutdown listener notification.
            if (top != null)
                top.shutdown();

            if (connMgr != null)
                connMgr.stop(waitCompletion);

            for (GridClientDataConfiguration dataCfg : cfg.getDataConfigurations()) {
                GridClientDataAffinity aff = dataCfg.getAffinity();

                if (aff instanceof GridClientTopologyListener)
                    removeTopologyListener((GridClientTopologyListener)aff);
            }

            if (log.isLoggable(Level.INFO))
                log.info("Client stopped [id=" + id + ", waitCompletion=" + waitCompletion + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public GridClientData data(@Nullable final String cacheName) throws GridClientException {
        checkClosed();

        checkBeforeNodeStartMode();

        Object key = maskNull(cacheName);

        GridClientDataImpl data = dataMap.get(key);

        if (data == null) {
            GridClientDataConfiguration dataCfg = cfg.getDataConfiguration(cacheName);

            if (dataCfg == null && cacheName != null)
                throw new GridClientException("Data configuration for given cache name was not provided: " +
                    cacheName);

            GridClientLoadBalancer balancer = dataCfg != null ? dataCfg.getPinnedBalancer() :
                new GridClientRandomBalancer();

            GridClientPredicate<GridClientNode> cacheNodes = new GridClientPredicate<GridClientNode>() {
                @Override public boolean apply(GridClientNode e) {
                    return e.caches().containsKey(cacheName);
                }

                @Override public String toString() {
                    return "GridClientHasCacheFilter [cacheName=" + cacheName + "]";
                }
            };

            data = new GridClientDataImpl(
                cacheName, this, null, cacheNodes, balancer, null, cfg.isEnableMetricsCache());

            GridClientDataImpl old = dataMap.putIfAbsent(key, data);

            if (old != null)
                data = old;
        }

        return data;
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute compute() {
        checkBeforeNodeStartMode();

        return compute;
    }

    /** {@inheritDoc} */
    @Override public GridClientClusterState state() {
        checkBeforeNodeStartMode();

        return clusterState;
    }

    /** {@inheritDoc} */
    @Override public void addTopologyListener(GridClientTopologyListener lsnr) {
        checkBeforeNodeStartMode();

        top.addTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void removeTopologyListener(GridClientTopologyListener lsnr) {
        checkBeforeNodeStartMode();

        top.removeTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientTopologyListener> topologyListeners() {
        checkBeforeNodeStartMode();

        return top.topologyListeners();
    }

    /** {@inheritDoc} */
    @Override public boolean connected() {
        return !top.failed();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        GridClientFactory.stop(id);
    }

    /**
     * Gets topology instance.
     *
     * @return Topology instance.
     */
    public GridClientTopology topology() {
        checkBeforeNodeStartMode();

        return top;
    }

    /** {@inheritDoc} */
    @Override public GridClientException checkLastError() {
        return top.lastError();
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridClientNodeStateBeforeStart beforeStartState() {
        return beforeStartState;
    }

    /**
     * @return Connection manager.
     */
    public GridClientConnectionManager connectionManager() {
        return connMgr;
    }

    /**
     * Gets data affinity for a given cache name.
     *
     * @param cacheName Name of cache for which affinity is obtained. Data configuration with this name
     *      must be configured at client startup.
     * @return Data affinity object.
     * @throws IllegalArgumentException If client data with given name was not configured.
     */
    GridClientDataAffinity affinity(String cacheName) {
        GridClientDataConfiguration dataCfg = cfg.getDataConfiguration(cacheName);

        return dataCfg == null ? null : dataCfg.getAffinity();
    }

    /**
     * Checks and throws an exception if this client was closed.
     *
     * @throws GridClientClosedException If client was closed.
     */
    private void checkClosed() throws GridClientClosedException {
        if (closed.get())
            throw new GridClientClosedException("Client was closed (no public methods of client can be used anymore).");
    }

    /**
     * Checks and throws an exception if mode is "before node start".
     *
     * @throws IgniteException If mode is "before node start".
     */
    private void checkBeforeNodeStartMode() throws IgniteException {
        if (beforeStartState != null)
            throw new IgniteException("It is possible to work with a node only before it starts.");
    }

    /**
     * Masks null cache name with unique object.
     *
     * @param cacheName Name to be masked.
     * @return Original name or some unique object if name is null.
     */
    private Object maskNull(String cacheName) {
        return cacheName == null ? NULL_MASK : cacheName;
    }

    /**
     * Maps Collection of strings to collection of {@code InetSocketAddress}es.
     *
     * @param cfgAddrs Collection fo string representations of addresses.
     * @return Collection of {@code InetSocketAddress}es
     * @throws GridClientException In case of error.
     */
    private static Collection<InetSocketAddress> parseAddresses(Collection<String> cfgAddrs)
        throws GridClientException {
        Collection<InetSocketAddress> addrs = new ArrayList<>(cfgAddrs.size());

        for (String srvStr : cfgAddrs) {
            try {
                String[] split = srvStr.split(":");

                InetSocketAddress addr = new InetSocketAddress(split[0], Integer.parseInt(split[1]));

                addrs.add(addr);
            }
            catch (RuntimeException e) {
                throw new GridClientException("Failed to create client (invalid server address specified): " +
                    srvStr, e);
            }
        }

        return Collections.unmodifiableCollection(addrs);
    }

    /**
     * @return New connection manager based on current client settings.
     * @throws GridClientException If failed to start connection server.
     */
    public GridClientConnectionManager newConnectionManager(
        @Nullable Byte marshId,
        boolean routerClient
    ) throws GridClientException {
        return createConnectionManager(id, sslCtx, cfg, routers, top, marshId, routerClient, beforeStartState != null);
    }

    /**
     * @param clientId Client ID.
     * @param sslCtx SSL context to enable secured connection or {@code null} to use unsecured one.
     * @param cfg Client configuration.
     * @param routers Routers or empty collection to use endpoints from topology info.
     * @param top Topology.
     * @param beforeNodeStart Connecting to a node before starting it without getting/updating topology.
     * @throws GridClientException In case of error.
     */
    private GridClientConnectionManager createConnectionManager(UUID clientId, SSLContext sslCtx,
        GridClientConfiguration cfg, Collection<InetSocketAddress> routers, GridClientTopology top,
        @Nullable Byte marshId, boolean routerClient, boolean beforeNodeStart) throws GridClientException {
        return new GridClientConnectionManagerOsImpl(
            clientId,
            sslCtx,
            cfg,
            routers,
            top,
            marshId,
            routerClient,
            beforeNodeStart
        );
    }

    /**
     * Tries to init connection manager using configured set of servers or routers.
     *
     * @throws GridClientException If initialisation failed.
     * @throws InterruptedException If initialisation was interrupted.
     */
    private void tryInit() throws GridClientException, InterruptedException {
        connMgr.init(addresses());

        Map<String, GridClientCacheMode> overallCaches = new HashMap<>();

        for (GridClientNodeImpl node : top.nodes())
            overallCaches.putAll(node.caches());

        for (Map.Entry<String, GridClientCacheMode> entry : overallCaches.entrySet()) {
            GridClientDataAffinity affinity = affinity(entry.getKey());

            if (affinity instanceof GridClientPartitionAffinity && entry.getValue() !=
                GridClientCacheMode.PARTITIONED)
                log.warning(GridClientPartitionAffinity.class.getSimpleName() + " is used for a cache configured " +
                    "for non-partitioned mode [cacheName=" + entry.getKey() + ", cacheMode=" + entry.getValue() + ']');
        }
    }

    /**
     * Getting a client connection without topology information.
     *
     * @return Client connection.
     * @throws GridClientException If failed.
     */
    public GridClientConnection connection() throws GridClientException, InterruptedException {
        return connectionManager().connection(addresses());
    }

    /**
     * Return addresses for connection.
     *
     * @return Addresses for connection.
     * @throws GridClientException If failed.
     */
    private Collection<InetSocketAddress> addresses() throws GridClientException {
        boolean hasSrvs = routers.isEmpty();

        final Collection<InetSocketAddress> connSrvs = (hasSrvs) ? new LinkedHashSet<>(srvs) : routers;

        if (hasSrvs) {
            // Add REST endpoints for all nodes from previous topology snapshot.
            try {
                for (GridClientNodeImpl node : top.nodes()) {
                    Collection<InetSocketAddress> endpoints = node.availableAddresses(cfg.getProtocol(), true);

                    List<InetSocketAddress> resolvedEndpoints = new ArrayList<>(endpoints.size());

                    for (InetSocketAddress endpoint : endpoints)
                        if (!endpoint.isUnresolved())
                            resolvedEndpoints.add(endpoint);

                    boolean sameHost = node.attributes().isEmpty() ||
                        F.containsAny(U.allLocalMACs(), node.attribute(ATTR_MACS).toString().split(", "));

                    if (sameHost) {
                        Collections.sort(resolvedEndpoints, U.inetAddressesComparator(true));

                        connSrvs.addAll(resolvedEndpoints);
                    }
                    else {
                        for (InetSocketAddress endpoint : resolvedEndpoints)
                            if (!endpoint.getAddress().isLoopbackAddress())
                                connSrvs.add(endpoint);
                    }
                }
            }
            catch (GridClientDisconnectedException ignored) {
                // Ignore if latest topology update failed.
            }
        }

        return connSrvs;
    }

    /**
     * Thread that updates topology according to refresh interval specified in configuration.
     */
    private class TopologyUpdaterThread extends CycleThread {
        /**
         * Creates topology refresh thread.
         */
        private TopologyUpdaterThread() {
            super(id + "-topology-update", cfg.getTopologyRefreshFrequency());
        }

        /** {@inheritDoc} */
        @Override public void iteration() throws InterruptedException {
            try {
                tryInit();
            }
            catch (GridClientException e) {
                top.fail(e);

                if (log.isLoggable(Level.FINE))
                    log.fine("Failed to update topology: " + e.getMessage());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientImpl [id=" + id + ", closed=" + closed + ']';
    }
}
