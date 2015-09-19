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

package org.apache.ignite.internal.client;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.balancer.GridClientRandomBalancer;
import org.apache.ignite.internal.client.balancer.GridClientRoundRobinBalancer;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.client.marshaller.optimized.GridClientOptimizedMarshaller;
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityCredentialsProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Java client configuration.
 */
public class GridClientConfiguration {
    /** Default client protocol. */
    public static final GridClientProtocol DFLT_CLIENT_PROTOCOL = GridClientProtocol.TCP;

    /** Default topology refresh frequency is 2 sec. */
    public static final int DFLT_TOP_REFRESH_FREQ = 2000;

    /** Default maximum time connection can be idle. */
    public static final long DFLT_MAX_CONN_IDLE_TIME = 30000;

    /** Default ping interval in milliseconds. */
    public static final long DFLT_PING_INTERVAL = 5000;

    /** Default ping timeout in milliseconds. */
    public static final long DFLT_PING_TIMEOUT = 7000;

    /** Default connect timeout in milliseconds. */
    public static final int DFLT_CONNECT_TIMEOUT = 10000;

    /** Default flag setting for TCP_NODELAY option. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** List of servers to connect to. */
    private Collection<String> srvs = Collections.emptySet();

    /** List of routers to connect to. */
    private Collection<String> routers = Collections.emptySet();

    /** Client protocol. */
    private GridClientProtocol proto = DFLT_CLIENT_PROTOCOL;

    /** Socket connect timeout. */
    private int connectTimeout = DFLT_CONNECT_TIMEOUT;

    /** TCP_NODELAY flag. */
    private boolean tcpNoDelay = DFLT_TCP_NODELAY;

    /** SSL context factory  */
    private GridSslContextFactory sslCtxFactory;

    /** Flag indicating whether metrics cache is enabled. */
    private boolean enableMetricsCache = true;

    /** Flag indicating whether attributes cache is enabled. */
    private boolean enableAttrsCache = true;

    /** Flag indicating whether metrics should be automatically fetched. */
    private boolean autoFetchMetrics = true;

    /** Flag indicating whether attributes should be automatically fetched. */
    private boolean autoFetchAttrs = true;

    /** Topology refresh  frequency. */
    private long topRefreshFreq = DFLT_TOP_REFRESH_FREQ;

    /** Max time of connection idleness. */
    private long maxConnIdleTime = DFLT_MAX_CONN_IDLE_TIME;

    /** Ping interval. */
    private long pingInterval = DFLT_PING_INTERVAL;

    /** Ping timeout. */
    private long pingTimeout = DFLT_PING_TIMEOUT;

    /** Default balancer. */
    private GridClientLoadBalancer balancer = new GridClientRandomBalancer();

    /** Collection of data configurations. */
    private Map<String, GridClientDataConfiguration> dataCfgs = Collections.emptyMap();

    /** Credentials. */
    private SecurityCredentialsProvider credProvider;

    /** Executor. */
    private ExecutorService executor;

    /** Marshaller. */
    private GridClientMarshaller marshaller = new GridClientOptimizedMarshaller(U.allPluginProviders());

    /** Daemon flag. */
    private boolean daemon;

    /**
     * Creates default configuration.
     */
    public GridClientConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to be copied.
     */
    public GridClientConfiguration(GridClientConfiguration cfg) {
        // Preserve alphabetical order for maintenance;
        autoFetchAttrs = cfg.isAutoFetchAttributes();
        autoFetchMetrics = cfg.isAutoFetchMetrics();
        balancer = cfg.getBalancer();
        connectTimeout = cfg.getConnectTimeout();
        credProvider = cfg.getSecurityCredentialsProvider();
        enableAttrsCache = cfg.isEnableAttributesCache();
        enableMetricsCache = cfg.isEnableMetricsCache();
        executor = cfg.getExecutorService();
        marshaller = cfg.getMarshaller();
        maxConnIdleTime = cfg.getMaxConnectionIdleTime();
        pingInterval = cfg.getPingInterval();
        pingTimeout = cfg.getPingTimeout();
        proto = cfg.getProtocol();
        routers = cfg.getRouters();
        srvs = cfg.getServers();
        sslCtxFactory = cfg.getSslContextFactory();
        tcpNoDelay = cfg.isTcpNoDelay();
        topRefreshFreq = cfg.getTopologyRefreshFrequency();
        daemon = cfg.isDaemon();
        marshaller = cfg.getMarshaller();

        setDataConfigurations(cfg.getDataConfigurations());
    }

    /**
     * Creates properties-based configuration based on passed in properties.
     *
     * @param in Client configuration in properties format.
     * @throws GridClientException If parsing configuration failed.
     */
    public GridClientConfiguration(Properties in) throws GridClientException {
        this("ignite.client", in);
    }

    /**
     * Creates properties-based configuration.
     *
     * @param prefix Prefix for the client properties.
     * @param in Properties map to load configuration from.
     * @throws GridClientException If parsing configuration failed.
     */
    public GridClientConfiguration(String prefix, Properties in) throws GridClientException {
        load(prefix, in);
    }

    /**
     * Collection of {@code 'host:port'} pairs representing
     * remote grid servers used to establish initial connection to
     * the grid. Once connection is established, Ignite will get
     * a full view on grid topology and will be able to connect to
     * any available remote node.
     * <p>
     * Note that only these addresses are used to perform
     * topology updates in background and to detect Grid connectivity
     * status.
     *
     * @return Collection of {@code 'host:port'} pairs representing remote
     *      grid servers.
     * @see GridClient#connected()
     */
    public Collection<String> getServers() {
        return Collections.unmodifiableCollection(srvs);
    }

    /**
     * Collection of {@code 'host:port'} pairs representing
     * grid routers used to establish connection to the grid.
     * <p>
     * Addresses here could be owned by Routers as well as
     * by individual Grid nodes. No additional connections
     * will be made even if other Grid nodes are available.
     * <p>
     * This configuration mode is designated for cases when
     * some Grid nodes are unavailable (due to security restrictions
     * for example). So only few nodes acting as routers or
     * dedicated router components used to access entire Grid.
     * <p>
     * This configuration parameter will not be used and direct
     * connections to all grid nodes will be established if
     * {@link #getServers()} return non-empty collection value.
     * <p>
     * Note that only these addresses are used to perform
     * topology updates in background and to detect Grid connectivity
     * status.
     *
     * @return Collection of {@code 'host:port'} pairs
     *      representing routers.
     * @see GridClient#connected()
     */
    public Collection<String> getRouters() {
        return routers;
    }

    /**
     * Sets list of servers this client should connect to.
     *
     * @param srvs List of servers.
     */
    public void setServers(Collection<String> srvs) {
        this.srvs = srvs != null ? srvs : Collections.<String>emptySet();
    }

    /**
     * Sets list of routers this client should connect to.
     *
     * @param routers List of routers.
     */
    public void setRouters(Collection<String> routers) {
        this.routers = routers != null ? routers : Collections.<String>emptySet();
    }

    /**
     * Gets protocol for communication between client and remote grid.
     * Default is defined by {@link #DFLT_CLIENT_PROTOCOL} constant.
     *
     * @return Protocol for communication between client and remote grid.
     */
    public GridClientProtocol getProtocol() {
        return proto;
    }

    /**
     * Sets protocol type that should be used in communication. Protocol type cannot be changed after
     * client is created.
     *
     * @param proto Protocol type.
     * @see GridClientProtocol
     */
    public void setProtocol(GridClientProtocol proto) {
        this.proto = proto;
    }

    /**
     * Gets timeout for socket connect operation in milliseconds. If {@code 0} -
     * then wait infinitely. Default is defined by {@link #DFLT_CONNECT_TIMEOUT} constant.
     *
     * @return Connect timeout in milliseconds.
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Gets flag indicating whether {@code TCP_NODELAY} flag should be enabled for outgoing connections.
     * This flag reduces communication latency and in the majority of cases should be set to true. For more
     * information, see {@link Socket#setTcpNoDelay(boolean)}
     * <p>
     * If not set, default value is {@link #DFLT_TCP_NODELAY}
     *
     * @return If {@code TCP_NODELAY} should be set on underlying sockets.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Sets whether {@code TCP_NODELAY} flag should be set on underlying socket connections.
     *
     * @param tcpNoDelay {@code True} if flag should be set.
     */
    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * Sets timeout for socket connect operation.
     *
     * @param connectTimeout Connect timeout in milliseconds.
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Gets a factory that should be used for SSL context creation.
     * If it returns {@code null} then SSL is considered disabled.
     *
     * @return Factory instance.
     * @see GridSslContextFactory
     */
    public GridSslContextFactory getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * Sets SSL context factory that will be used for creation of secure connections.
     *
     * @param sslCtxFactory Context factory.
     */
    public void setSslContextFactory(GridSslContextFactory sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;
    }

    /**
     * Default balancer to be used for computational client. It can be overridden
     * for different compute instances. By default {@link GridClientRandomBalancer}
     * is used.
     *
     * @return Default balancer to be used for computational client.
     */
    public GridClientLoadBalancer getBalancer() {
        return balancer;
    }

    /**
     * Sets default compute balancer.
     *
     * @param balancer Balancer to use.
     */
    public void setBalancer(GridClientLoadBalancer balancer) {
        this.balancer = balancer;
    }

    /**
     * Gets client credentials provider to authenticate with.
     *
     * @return Credentials provider.
     */
    public SecurityCredentialsProvider getSecurityCredentialsProvider() {
        return credProvider;
    }

    /**
     * Sets client credentials provider used in authentication process.
     *
     * @param credProvider Client credentials provider.
     */
    public void setSecurityCredentialsProvider(SecurityCredentialsProvider credProvider) {
        this.credProvider = credProvider;
    }

    /**
     * Gets a collection of data configurations specified by user.
     *
     * @return Collection of data configurations (possibly empty).
     */
    public Collection<GridClientDataConfiguration> getDataConfigurations() {
        return dataCfgs.values();
    }

    /**
     * Sets data configurations.
     *
     * @param dataCfgs Data configurations.
     */
    public void setDataConfigurations(Collection<? extends GridClientDataConfiguration> dataCfgs) {
        this.dataCfgs = U.newHashMap(dataCfgs.size());

        for (GridClientDataConfiguration dataCfg : dataCfgs)
            this.dataCfgs.put(dataCfg.getName(), new GridClientDataConfiguration(dataCfg));
    }

    /**
     * Gets data configuration for a cache with specified name.
     *
     * @param name Name of grid cache.
     * @return Configuration or {@code null} if there is not configuration for specified name.
     */
    public GridClientDataConfiguration getDataConfiguration(@Nullable String name) {
        return dataCfgs.get(name);
    }

    /**
     * Sets flag indicating whether node and cache metrics should be cached by client.
     *
     * @param enableMetricsCache {@code True} if cache should be enabled.
     */
    public void setEnableMetricsCache(boolean enableMetricsCache) {
        this.enableMetricsCache = enableMetricsCache;
    }

    /**
     * Enables client to cache per-node and per-cache metrics internally. In memory
     * sensitive environments, such as mobile platforms, caching metrics
     * may be expensive and, hence, this parameter should be set to {@code false}.
     * <p>
     * Note that topology is refreshed automatically every {@link #getTopologyRefreshFrequency()}
     * interval, and if {@link #isAutoFetchMetrics()} enabled then metrics will be updated
     * with that frequency.
     * <p>
     * By default this value is {@code true} which means that metrics will be cached
     * on the client side.
     *
     * @return {@code True} if metrics cache is enabled, {@code false} otherwise.
     */
    public boolean isEnableMetricsCache() {
        return enableMetricsCache;
    }

    /**
     * Sets flag indicating whether node attributes should be cached by client.
     *
     * @param enableAttrsCache {@code True} if cache should be enabled.
     */
    public void setEnableAttributesCache(boolean enableAttrsCache) {
        this.enableAttrsCache = enableAttrsCache;
    }

    /**
     * Enables client to cache per-node attributes internally. In memory
     * sensitive environments, such as mobile platforms, caching node attributes
     * may be expensive and, hence, this parameter should be set to {@code false}.
     * <p>
     * Note that node attributes are static and, if cached, there is no need
     * to refresh them again. If {@link #isAutoFetchAttributes()} is enabled then
     * attributes will be cached during client initialization.
     * <p>
     * By default this value is {@code true} which means that node attributes
     * will be cached on the client side.
     *
     * @return {@code True} if attributes cache is enabled, {@code false} otherwise.
     */
    public boolean isEnableAttributesCache() {
        return enableAttrsCache;
    }

    /**
     * Sets flag indicating whether node metrics should be fetched by client automatically.
     *
     * @param autoFetchMetrics {@code True} if metrics should be fetched.
     */
    public void setAutoFetchMetrics(boolean autoFetchMetrics) {
        this.autoFetchMetrics = autoFetchMetrics;
    }

    /**
     * Allows client to fetch node metrics automatically with background topology refresh.
     * <p>
     * Note that this parameter will only affect auto-fetching of node metrics.
     * Cache metrics still need to be fetched explicitly via
     * {@link GridClientData#metrics()} or {@link GridClientData#metricsAsync()} methods.
     * <p>
     * By default this value is {@code true} which means that metrics will be fetched
     * automatically.
     *
     * @return {@code true} if client should fetch metrics on topology refresh,
     *      {@code false} otherwise.
     */
    public boolean isAutoFetchMetrics() {
        return autoFetchMetrics;
    }

    /**
     * Sets flag indicating whether node attributes should be fetched by client automatically.
     *
     * @param autoFetchAttrs {@code True} if attributes should be fetched.
     */
    public void setAutoFetchAttributes(boolean autoFetchAttrs) {
        this.autoFetchAttrs = autoFetchAttrs;
    }

    /**
     * Allows client to fetch node attributes automatically with background topology refresh.
     * <p>
     * By default this value is {@code true} which means that attributes will be fetched
     * automatically.
     *
     * @return {@code True} if client should fetch attributes once on topology refresh,
     *      {@code false} otherwise.
     */
    public boolean isAutoFetchAttributes() {
        return autoFetchAttrs;
    }

    /**
     * Gets topology refresh frequency. Default is defined by {@link #DFLT_TOP_REFRESH_FREQ}
     * constant.
     *
     * @return Topology refresh frequency.
     */
    public long getTopologyRefreshFrequency() {
        return topRefreshFreq;
    }

    /**
     * Sets topology refresh frequency. If topology cache is enabled, grid topology
     * will be refreshed every {@code topRefreshFreq} milliseconds.
     *
     * @param topRefreshFreq Topology refresh frequency in milliseconds.
     */
    public void setTopologyRefreshFrequency(long topRefreshFreq) {
        this.topRefreshFreq = topRefreshFreq;
    }

    /**
     * Gets maximum amount of time that client connection can be idle before it is closed.
     * Default is defined by {@link #DFLT_MAX_CONN_IDLE_TIME} constant.
     *
     * @return Maximum idle time in milliseconds.
     */
    public long getMaxConnectionIdleTime() {
        return maxConnIdleTime;
    }

    /**
     * Sets maximum time in milliseconds which connection can be idle before it is closed by client.
     *
     * @param maxConnIdleTime Maximum time of connection idleness in milliseconds.
     */
    public void setMaxConnectionIdleTime(long maxConnIdleTime) {
        this.maxConnIdleTime = maxConnIdleTime;
    }

    /**
     * Gets time interval in milliseconds between ping requests. Default is defined
     * by {@link #DFLT_PING_INTERVAL} constant.
     * <p>
     * Ping requests used by {@link GridClientProtocol#TCP} protocol
     * to detect network failures and half-opened sockets.
     *
     * @return Ping interval.
     */
    public long getPingInterval() {
        return pingInterval;
    }

    /**
     * Sets ping interval in milliseconds.
     *
     * @param pingInterval Ping interval in milliseconds.
     */
    public void setPingInterval(long pingInterval) {
        this.pingInterval = pingInterval;
    }

    /**
     * Gets ping timeout. Default is defined by {@link #DFLT_PING_TIMEOUT} constant.
     * <p>
     * Ping requests used by {@link GridClientProtocol#TCP} protocol
     * to detect network failures and half-opened sockets.
     * If no response received in period equal to this timeout than connection
     * considered broken and closed.
     *
     * @return Ping timeout.
     */
    public long getPingTimeout() {
        return pingTimeout;
    }

    /**
     * Sets ping timeout in milliseconds.
     *
     * @param pingTimeout Ping interval in milliseconds.
     */
    public void setPingTimeout(long pingTimeout) {
        this.pingTimeout = pingTimeout;
    }

    /**
     * Gets {@link ExecutorService} where client could run asynchronous operations.
     * <p>
     * When using {@link GridClientProtocol#TCP} this executor should be able to serve at least
     * {@code Runtime.getRuntime().availableProcessors()} parallel tasks.
     * <p>
     * Note that this executor will be automatically shut down when client get closed.
     *
     * @return {@link ExecutorService} instance to use.
     */
    public ExecutorService getExecutorService() {
        return executor;
    }

    /**
     * Sets executor service.
     *
     * @param executor Executor service to use in client.
     */
    public void setExecutorService(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Gets the marshaller, that is used to communicate between client and server.
     * <p>
     * Options, that can be used out-of-the-box:
     * <ul>
     *     <li>{@link GridClientOptimizedMarshaller} (default) - Ignite's optimized marshaller.</li>
     *     <li>{@code GridClientPortableMarshaller} - Marshaller that supports portable objects.</li>
     *     <li>{@link org.apache.ignite.internal.client.marshaller.jdk.GridClientJdkMarshaller} - JDK marshaller (not recommended).</li>
     * </ul>
     *
     * @return A marshaller to use.
     */
    public GridClientMarshaller getMarshaller() {
        return marshaller;
    }

    /**
     * Sets the marshaller to use for communication.
     *
     * @param marshaller A marshaller to use.
     */
    public void setMarshaller(GridClientMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    /**
     * Load client configuration from the properties map.
     *
     * @param prefix Prefix for the client properties.
     * @param in Properties map to load configuration from.
     * @throws GridClientException If parsing configuration failed.
     */
    public void load(String prefix, Properties in) throws GridClientException {
        while (prefix.endsWith("."))
            prefix = prefix.substring(0, prefix.length() - 1);

        if (!prefix.isEmpty())
            prefix += ".";

        String balancer = in.getProperty(prefix + "balancer");
        String connectTimeout = in.getProperty(prefix + "connectTimeout");
        String cred = in.getProperty(prefix + "credentials");
        String autoFetchMetrics = in.getProperty(prefix + "autoFetchMetrics");
        String autoFetchAttrs = in.getProperty(prefix + "autoFetchAttributes");
        String maxConnIdleTime = in.getProperty(prefix + "idleTimeout");
        String proto = in.getProperty(prefix + "protocol");
        String srvrs = in.getProperty(prefix + "servers");
        String tcpNoDelay = in.getProperty(prefix + "tcp.noDelay");
        String topRefreshFreq = in.getProperty(prefix + "topology.refresh");

        String sslEnabled = in.getProperty(prefix + "ssl.enabled");

        String sslProto = in.getProperty(prefix + "ssl.protocol", "TLS");
        String sslKeyAlg = in.getProperty(prefix + "ssl.key.algorithm", "SunX509");

        String keyStorePath = in.getProperty(prefix + "ssl.keystore.location");
        String keyStorePwd = in.getProperty(prefix + "ssl.keystore.password");
        String keyStoreType = in.getProperty(prefix + "ssl.keystore.type");

        String trustStorePath = in.getProperty(prefix + "ssl.truststore.location");
        String trustStorePwd = in.getProperty(prefix + "ssl.truststore.password");
        String trustStoreType = in.getProperty(prefix + "ssl.truststore.type");

        String dataCfgs = in.getProperty(prefix + "data.configurations");

        setBalancer(resolveBalancer(balancer));

        if (!F.isEmpty(connectTimeout))
            setConnectTimeout(Integer.parseInt(connectTimeout));

        if (!F.isEmpty(cred)) {
            int idx = cred.indexOf(':');

            if (idx >= 0 && idx < cred.length() - 1) {
                setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(
                    new SecurityCredentials(cred.substring(0, idx), cred.substring(idx + 1))));
            }
            else {
                setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(
                    new SecurityCredentials(null, null, cred)));
            }
        }

        if (!F.isEmpty(autoFetchMetrics))
            setAutoFetchMetrics(Boolean.parseBoolean(autoFetchMetrics));

        if (!F.isEmpty(autoFetchAttrs))
            setAutoFetchAttributes(Boolean.parseBoolean(autoFetchAttrs));

        if (!F.isEmpty(maxConnIdleTime))
            setMaxConnectionIdleTime(Integer.parseInt(maxConnIdleTime));

        if (!F.isEmpty(proto))
            setProtocol(GridClientProtocol.valueOf(proto));

        if (!F.isEmpty(srvrs))
            setServers(Arrays.asList(srvrs.replaceAll("\\s+", "").split(",")));

        if (!F.isEmpty(tcpNoDelay))
            setTcpNoDelay(Boolean.parseBoolean(tcpNoDelay));

        if (!F.isEmpty(topRefreshFreq))
            setTopologyRefreshFrequency(Long.parseLong(topRefreshFreq));

        //
        // SSL configuration section
        //

        if (!F.isEmpty(sslEnabled) && Boolean.parseBoolean(sslEnabled)) {
            GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

            factory.setProtocol(F.isEmpty(sslProto) ? "TLS" : sslProto);
            factory.setKeyAlgorithm(F.isEmpty(sslKeyAlg) ? "SunX509" : sslKeyAlg);

            if (F.isEmpty(keyStorePath))
                throw new IllegalArgumentException("SSL key store location is not specified.");

            factory.setKeyStoreFilePath(keyStorePath);

            if (keyStorePwd != null)
                factory.setKeyStorePassword(keyStorePwd.toCharArray());

            factory.setKeyStoreType(F.isEmpty(keyStoreType) ? "jks" : keyStoreType);

            if (F.isEmpty(trustStorePath))
                factory.setTrustManagers(GridSslBasicContextFactory.getDisabledTrustManager());
            else {
                factory.setTrustStoreFilePath(trustStorePath);

                if (trustStorePwd != null)
                    factory.setTrustStorePassword(trustStorePwd.toCharArray());

                factory.setTrustStoreType(F.isEmpty(trustStoreType) ? "jks" : trustStoreType);
            }

            setSslContextFactory(factory);
        }

        //
        // Data configuration section
        //

        if (!F.isEmpty(dataCfgs)) {
            String[] names = dataCfgs.replaceAll("\\s+", "").split(",");
            Collection<GridClientDataConfiguration> list = new ArrayList<>();

            for (String cfgName : names) {
                if (F.isEmpty(cfgName))
                    continue;

                String name = in.getProperty(prefix + "data." + cfgName + ".name");
                String bal = in.getProperty(prefix + "data." + cfgName + ".balancer");
                String aff = in.getProperty(prefix + "data." + cfgName + ".affinity");

                GridClientDataConfiguration dataCfg = new GridClientDataConfiguration();

                dataCfg.setName(F.isEmpty(name) ? null : name);
                dataCfg.setBalancer(resolveBalancer(bal));
                dataCfg.setAffinity(resolveAffinity(aff));

                list.add(dataCfg);
            }

            setDataConfigurations(list);
        }
    }

    /**
     * Resolve load balancer from string definition.
     *
     * @param balancer Load balancer string definition.
     * @return Resolved load balancer.
     * @throws GridClientException If loading failed.
     */
    private static GridClientLoadBalancer resolveBalancer(String balancer) throws GridClientException {
        if (F.isEmpty(balancer) || "random".equals(balancer))
            return new GridClientRandomBalancer();

        if ("roundrobin".equals(balancer))
            return new GridClientRoundRobinBalancer();

        return newInstance(GridClientLoadBalancer.class, balancer);
    }

    /**
     * Resolve data affinity from string definition.
     *
     * @param affinity Data affinity string definition.
     * @return Resolved data affinity.
     * @throws GridClientException If loading failed.
     */
    private static GridClientDataAffinity resolveAffinity(String affinity) throws GridClientException {
        if (F.isEmpty(affinity))
            return null;

        if ("partitioned".equals(affinity))
            return new GridClientPartitionAffinity();

        return newInstance(GridClientDataAffinity.class, affinity);
    }

    /**
     * Constructs new instance of the specified class.
     *
     * @param exp Expected class for the new instance.
     * @param clsName Class name to create new instance for.
     * @param <T> Expected class type for the new instance.
     * @return New instance of specified class.
     * @throws GridClientException If loading failed.
     */
    private static <T> T newInstance(Class<T> exp, String clsName) throws GridClientException {
        Object obj;

        try {
            obj = Class.forName(clsName).newInstance();
        }
        // Catch all for convenience.
        catch (Exception e) {
            throw new GridClientException("Failed to create class instance: " + clsName, e);
        }

        return exp.cast(obj);
    }

    /**
     * Set the daemon flag value. Communication threads will be created as daemons if this flag is set.
     *
     * @param daemon Daemon flag.
     */
    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    /**
     * Get the daemon flag.
     *
     * @return Daemon flag.
     */
    public boolean isDaemon() {
        return daemon;
    }
}