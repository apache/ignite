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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientDataAffinity;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPartitionAffinity;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.GridClientTopologyListener;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.balancer.GridClientRandomBalancer;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionManager;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionManagerOsImpl;
import org.apache.ignite.internal.client.impl.connection.GridClientTopology;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Client implementation.
 */
public class GridClientImpl implements GridClient {
    /** Enterprise connection manager class name. */
    private static final String ENT_CONN_MGR_CLS =
        "org.apache.ignite.internal.client.impl.connection.GridClientConnectionManagerEntImpl";

    /** Null mask object. */
    private static final Object NULL_MASK = new Object();

    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientImpl.class.getName());

    /** */
    static {
        boolean isLog4jUsed = U.gridClassLoader().getResource("org/apache/log4j/Appender.class") != null;

        try {
            if (isLog4jUsed)
                U.addLog4jNoOpLogger();
            else
                U.addJavaNoOpLogger();
        }
        catch (IgniteCheckedException ignored) {
            // Our log4j warning suppression failed, leave it as is.
        }
    }

    /** Client ID. */
    private final UUID id;

    /** Client configuration. */
    protected final GridClientConfiguration cfg;

    /** SSL context if ssl enabled. */
    private SSLContext sslCtx;

    /** Main compute projection. */
    private final GridClientComputeImpl compute;

    /** Data projections. */
    private ConcurrentMap<Object, GridClientDataImpl> dataMap = new ConcurrentHashMap<>();

    /** Topology. */
    protected GridClientTopology top;

    /** Topology updater thread. */
    private final Thread topUpdateThread;

    /** Closed flag. */
    private AtomicBoolean closed = new AtomicBoolean();

    /** Connection manager. */
    protected GridClientConnectionManager connMgr;

    /** Routers. */
    private final Collection<InetSocketAddress> routers;

    /** Servers. */
    private final Collection<InetSocketAddress> srvs;

    /**
     * Creates a new client based on a given configuration.
     *
     * @param id Client identifier.
     * @param cfg0 Client configuration.
     * @param routerClient Router client flag.
     * @throws GridClientException If client configuration is incorrect.
     * @throws GridServerUnreachableException If none of the servers specified in configuration can
     *      be reached.
     */
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public GridClientImpl(UUID id, GridClientConfiguration cfg0, boolean routerClient) throws GridClientException {
        this.id = id;

        cfg = new GridClientConfiguration(cfg0);

        boolean success = false;

        try {
            top = new GridClientTopology(cfg);

            for (GridClientDataConfiguration dataCfg : cfg.getDataConfigurations()) {
                GridClientDataAffinity aff = dataCfg.getAffinity();

                if (aff instanceof GridClientTopologyListener)
                    addTopologyListener((GridClientTopologyListener)aff);
            }

            if (cfg.getBalancer() instanceof GridClientTopologyListener)
                top.addTopologyListener((GridClientTopologyListener)cfg.getBalancer());

            GridSslContextFactory factory = cfg.getSslContextFactory();

            if (factory != null) {
                try {
                    sslCtx = factory.createSslContext();
                }
                catch (SSLException e) {
                    throw new GridClientException("Failed to create client (unable to create SSL context, " +
                        "check ssl context factory configuration): " + e.getMessage(), e);
                }
            }

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

            connMgr = createConnectionManager(id, sslCtx, cfg, routers, top, null, routerClient);

            try {
                // Init connection manager, it should cause topology update.
                tryInitTopology();
            }
            catch (GridClientException e) {
                top.fail(e);

                log.warning("Failed to initialize topology on client start. Will retry in background.");
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridClientException("Client startup was interrupted.", e);
            }

            topUpdateThread = new TopologyUpdaterThread();

            topUpdateThread.setDaemon(true);

            topUpdateThread.start();

            compute = new GridClientComputeImpl(this, null, null, cfg.getBalancer());

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
    @Override public GridClientData data() throws GridClientException {
        return data(null);
    }

    /** {@inheritDoc} */
    @Override public GridClientData data(@Nullable final String cacheName) throws GridClientException {
        checkClosed();

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
        return compute;
    }

    /** {@inheritDoc} */
    @Override public void addTopologyListener(GridClientTopologyListener lsnr) {
        top.addTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void removeTopologyListener(GridClientTopologyListener lsnr) {
        top.removeTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientTopologyListener> topologyListeners() {
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
        return top;
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
    public GridClientConnectionManager newConnectionManager(@Nullable Byte marshId, boolean routerClient)
        throws GridClientException {
        return createConnectionManager(id, sslCtx, cfg, routers, top, marshId, routerClient);
    }

    /**
     * @param clientId Client ID.
     * @param sslCtx SSL context to enable secured connection or {@code null} to use unsecured one.
     * @param cfg Client configuration.
     * @param routers Routers or empty collection to use endpoints from topology info.
     * @param top Topology.
     * @throws GridClientException In case of error.
     */
    private GridClientConnectionManager createConnectionManager(UUID clientId, SSLContext sslCtx,
        GridClientConfiguration cfg, Collection<InetSocketAddress> routers, GridClientTopology top,
        @Nullable Byte marshId, boolean routerClient)
        throws GridClientException {
        GridClientConnectionManager mgr;

        try {
            Class<?> cls = Class.forName(ENT_CONN_MGR_CLS);

            Constructor<?> cons = cls.getConstructor(UUID.class, SSLContext.class, GridClientConfiguration.class,
                Collection.class, GridClientTopology.class, Byte.class, boolean.class);

            mgr = (GridClientConnectionManager)cons.newInstance(clientId, sslCtx, cfg, routers, top, marshId,
                routerClient);
        }
        catch (ClassNotFoundException ignored) {
            mgr = new GridClientConnectionManagerOsImpl(clientId, sslCtx, cfg, routers, top, marshId, routerClient);
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new GridClientException("Failed to create client connection manager.", e);
        }

        return mgr;
    }

    /**
     * Tries to init client topology using configured set of servers or routers.
     *
     * @throws GridClientException If initialisation failed.
     * @throws InterruptedException If initialisation was interrupted.
     */
    private void tryInitTopology() throws GridClientException, InterruptedException {
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

        connMgr.init(connSrvs);

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
     * Thread that updates topology according to refresh interval specified in configuration.
     */
    @SuppressWarnings("BusyWait")
    private class TopologyUpdaterThread extends Thread {
        /**
         * Creates topology refresh thread.
         */
        private TopologyUpdaterThread() {
            super(id + "-topology-update");
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (!isInterrupted()) {
                    Thread.sleep(cfg.getTopologyRefreshFrequency());

                    try {
                        tryInitTopology();
                    }
                    catch (GridClientException e) {
                        top.fail(e);

                        if (log.isLoggable(Level.FINE))
                            log.fine("Failed to update topology: " + e.getMessage());
                    }
                }
            }
            catch (InterruptedException ignored) {
                // Client is shutting down.
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientImpl [id=" + id + ", closed=" + closed + ']';
    }
}