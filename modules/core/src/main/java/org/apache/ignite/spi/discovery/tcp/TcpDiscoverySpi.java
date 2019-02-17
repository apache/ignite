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

package org.apache.ignite.spi.discovery.tcp;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMBeanAdapter;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.IgniteSpiVersionCheckException;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiMutableCustomMessageSupport;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryStatistics;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryEnsureDelivery;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Discovery SPI implementation that uses TCP/IP for node discovery.
 * <p>
 * Nodes are organized in ring. So almost all network exchange (except few cases) is
 * done across it.
 * <p>
 * If node is configured as client node (see {@link IgniteConfiguration#clientMode})
 * TcpDiscoverySpi starts in client mode as well. In this case node does not take its place in the ring,
 * but it connects to random node in the ring (IP taken from IP finder configured) and
 * use it as a router for discovery traffic.
 * Therefore slow client node or its shutdown will not affect whole cluster. If TcpDiscoverySpi
 * needs to be started in server mode regardless of {@link IgniteConfiguration#clientMode},
 * {@link #forceSrvMode} should be set to true.
 * <p>
 * At startup SPI tries to send messages to random IP taken from
 * {@link TcpDiscoveryIpFinder} about self start (stops when send succeeds)
 * and then this info goes to coordinator. When coordinator processes join request
 * and issues node added messages and all other nodes then receive info about new node.
 * <h1 class="header">Failure Detection</h1>
 * Configuration defaults (see Configuration section below and
 * {@link IgniteConfiguration#getFailureDetectionTimeout()}) for details) are chosen to make possible for discovery
 * SPI work reliably on most of hardware and virtual deployments, but this has made failure detection time worse.
 * <p>
 * If it's needed to tune failure detection then it's highly recommended to do this using
 * {@link IgniteConfiguration#setFailureDetectionTimeout(long)}. This failure timeout automatically controls the
 * following parameters: {@link #getSocketTimeout()}, {@link #getAckTimeout()}, {@link #getMaxAckTimeout()},
 * {@link #getReconnectCount()}. If any of those parameters is set explicitly, then the failure timeout setting will be
 * ignored. As an example, for stable low-latency networks the failure detection timeout may be set to ~120 ms.
 * <p>
 * If it's required to perform advanced settings of failure detection and
 * {@link IgniteConfiguration#getFailureDetectionTimeout()} is unsuitable then various {@code TcpDiscoverySpi}
 * configuration parameters may be used. As an example, for stable low-latency networks the following more aggressive
 * settings are recommended (which allows failure detection time ~200ms):
 * <ul>
 * <li>Socket timeout (see {@link #setSocketTimeout(long)}) - 200ms</li>
 * <li>Message acknowledgement timeout (see {@link #setAckTimeout(long)}) - 50ms</li>
 * </ul>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>IP finder to share info about nodes IP addresses
 * (see {@link #setIpFinder(TcpDiscoveryIpFinder)}).
 * See the following IP finder implementations for details on configuration:
 * <ul>
 * <li>{@link TcpDiscoverySharedFsIpFinder}</li>
 * <li>{@ignitelink org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder}</li>
 * <li>{@link TcpDiscoveryJdbcIpFinder}</li>
 * <li>{@link TcpDiscoveryVmIpFinder}</li>
 * <li>{@link TcpDiscoveryMulticastIpFinder} - default</li>
 * </ul>
 * </li>
 * </ul>
 * <ul>
 * </li>
 * <li>Local address (see {@link #setLocalAddress(String)})</li>
 * <li>Local port to bind to (see {@link #setLocalPort(int)})</li>
 * <li>Local port range to try binding to if previous ports are in use
 *      (see {@link #setLocalPortRange(int)})</li>
 * <li>Number of times node tries to (re)establish connection to another node
 *      (see {@link #setReconnectCount(int)})</li>
 * <li>Network timeout (see {@link #setNetworkTimeout(long)})</li>
 * <li>Socket timeout (see {@link #setSocketTimeout(long)})</li>
 * <li>Message acknowledgement timeout (see {@link #setAckTimeout(long)})</li>
 * <li>Maximum message acknowledgement timeout (see {@link #setMaxAckTimeout(long)})</li>
 * <li>Join timeout (see {@link #setJoinTimeout(long)})</li>
 * <li>Thread priority for threads started by SPI (see {@link #setThreadPriority(int)})</li>
 * <li>IP finder clean frequency (see {@link #setIpFinderCleanFrequency(long)})</li>
 * <li>Statistics print frequency (see {@link #setStatisticsPrintFrequency(long)}</li>
 * <li>Force server mode (see {@link #setForceServerMode(boolean)}</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * TcpDiscoverySpi spi = new TcpDiscoverySpi();
 *
 * TcpDiscoveryVmIpFinder finder =
 *     new GridTcpDiscoveryVmIpFinder();
 *
 * spi.setIpFinder(finder);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default discovery SPI.
 * cfg.setDiscoverySpi(spi);
 *
 * // Start grid.
 * Ignition.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * TcpDiscoverySpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="discoverySpi"&gt;
 *             &lt;bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi"&gt;
 *                 &lt;property name="ipFinder"&gt;
 *                     &lt;bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder" /&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see DiscoverySpi
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
@IgniteSpiMultipleInstancesSupport(true)
@DiscoverySpiOrderSupport(true)
@DiscoverySpiHistorySupport(true)
@DiscoverySpiMutableCustomMessageSupport(true)
public class TcpDiscoverySpi extends IgniteSpiAdapter implements IgniteDiscoverySpi {
    /** Node attribute that is mapped to node's external addresses (value is <tt>disc.tcp.ext-addrs</tt>). */
    public static final String ATTR_EXT_ADDRS = "disc.tcp.ext-addrs";

    /** Default local port range (value is <tt>100</tt>). */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default port to listen (value is <tt>47500</tt>). */
    public static final int DFLT_PORT = 47500;

    /** Default timeout for joining topology (value is <tt>0</tt>). */
    public static final long DFLT_JOIN_TIMEOUT = 0;

    /** Default network timeout in milliseconds (value is <tt>5000ms</tt>). */
    public static final long DFLT_NETWORK_TIMEOUT = 5000;

    /** Default value for thread priority (value is <tt>10</tt>). */
    public static final int DFLT_THREAD_PRI = 10;

    /** Default size of topology snapshots history. */
    public static final int DFLT_TOP_HISTORY_SIZE = 1000;

    /** Default socket operations timeout in milliseconds (value is <tt>5000ms</tt>). */
    public static final long DFLT_SOCK_TIMEOUT = 5000;

    /** Default timeout for receiving message acknowledgement in milliseconds (value is <tt>5000ms</tt>). */
    public static final long DFLT_ACK_TIMEOUT = 5000;

    /** Default socket operations timeout in milliseconds (value is <tt>5000ms</tt>). */
    public static final long DFLT_SOCK_TIMEOUT_CLIENT = 5000;

    /** Default timeout for receiving message acknowledgement in milliseconds (value is <tt>5000ms</tt>). */
    public static final long DFLT_ACK_TIMEOUT_CLIENT = 5000;

    /** Default reconnect attempts count (value is <tt>10</tt>). */
    public static final int DFLT_RECONNECT_CNT = 10;

    /** Default delay between attempts to connect to the cluster in milliseconds (value is <tt>2000</tt>). */
    public static final long DFLT_RECONNECT_DELAY = 2000;

    /** Default IP finder clean frequency in milliseconds (value is <tt>60,000ms</tt>). */
    public static final long DFLT_IP_FINDER_CLEAN_FREQ = 60 * 1000;

    /** Default statistics print frequency in milliseconds (value is <tt>0ms</tt>). */
    public static final long DFLT_STATS_PRINT_FREQ = 0;

    /** Maximum ack timeout value for receiving message acknowledgement in milliseconds (value is <tt>600,000ms</tt>). */
    public static final long DFLT_MAX_ACK_TIMEOUT = 10 * 60 * 1000;

    /** Default connection recovery timeout in ms. */
    public static final long DFLT_CONNECTION_RECOVERY_TIMEOUT = IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT;

    /** Ssl message pattern for StreamCorruptedException. */
    private static Pattern sslMsgPattern = Pattern.compile("invalid stream header: 150\\d0\\d00");

    /** Local address. */
    protected String locAddr;

    /** Address resolver. */
    private AddressResolver addrRslvr;

    /** IP finder. */
    protected TcpDiscoveryIpFinder ipFinder;

    /** Socket operations timeout. */
    private long sockTimeout; // Must be initialized in the constructor of child class.

    /** Message acknowledgement timeout. */
    private long ackTimeout; // Must be initialized in the constructor of child class.

    /** Network timeout. */
    protected long netTimeout = DFLT_NETWORK_TIMEOUT;

    /** Join timeout. */
    @SuppressWarnings("RedundantFieldInitialization")
    protected long joinTimeout = DFLT_JOIN_TIMEOUT;

    /** Thread priority for all threads started by SPI. */
    protected int threadPri = DFLT_THREAD_PRI;

    /** Metrics update messages issuing frequency. */
    protected long metricsUpdateFreq;

    /** Size of topology snapshots history. */
    protected int topHistSize = DFLT_TOP_HISTORY_SIZE;

    /** Default connection recovery timeout in ms. */
    protected long connRecoveryTimeout = DFLT_CONNECTION_RECOVERY_TIMEOUT;

    /** Grid discovery listener. */
    protected volatile DiscoverySpiListener lsnr;

    /** Data exchange. */
    protected DiscoverySpiDataExchange exchange;

    /** Metrics provider. */
    protected DiscoveryMetricsProvider metricsProvider;

    /** Local node attributes. */
    protected Map<String, Object> locNodeAttrs;

    /** Local node version. */
    protected IgniteProductVersion locNodeVer;

    /** Local node. */
    protected TcpDiscoveryNode locNode;

    /** */
    protected UUID cfgNodeId;

    /** Local host. */
    protected InetAddress locHost;

    /** Internal and external addresses of local node. */
    protected Collection<InetSocketAddress> locNodeAddrs;

    /** Start time of the very first grid node. */
    protected volatile long gridStartTime;

    /** Marshaller. */
    private Marshaller marsh;

    /** Statistics. */
    protected final TcpDiscoveryStatistics stats = new TcpDiscoveryStatistics();

    /** Local port which node uses. */
    protected int locPort = DFLT_PORT;

    /** Local port range. */
    protected int locPortRange = DFLT_PORT_RANGE;

    /** Reconnect attempts count. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private int reconCnt = DFLT_RECONNECT_CNT;

    /** Delay between attempts to connect to the cluster. */
    private long reconDelay = DFLT_RECONNECT_DELAY;

    /** Statistics print frequency. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized", "RedundantFieldInitialization"})
    protected long statsPrintFreq = DFLT_STATS_PRINT_FREQ;

    /** Maximum message acknowledgement timeout. */
    private long maxAckTimeout = DFLT_MAX_ACK_TIMEOUT;

    /** IP finder clean frequency. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    protected long ipFinderCleanFreq = DFLT_IP_FINDER_CLEAN_FREQ;

    /** Node authenticator. */
    protected DiscoverySpiNodeAuthenticator nodeAuth;

    /** SSL server socket factory. */
    protected SSLServerSocketFactory sslSrvSockFactory;

    /** SSL socket factory. */
    protected SSLSocketFactory sslSockFactory;

    /** Context initialization latch. */
    @GridToStringExclude
    private final CountDownLatch ctxInitLatch = new CountDownLatch(1);

    /** */
    protected final CopyOnWriteArrayList<IgniteInClosure<TcpDiscoveryAbstractMessage>> sndMsgLsnrs =
        new CopyOnWriteArrayList<>();

    /** */
    protected final CopyOnWriteArrayList<IgniteInClosure<Socket>> incomeConnLsnrs =
        new CopyOnWriteArrayList<>();

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** */
    protected TcpDiscoveryImpl impl;

    /** */
    private boolean forceSrvMode;

    /** */
    private boolean clientReconnectDisabled;

    /** */
    private Serializable consistentId;

    /** Local node addresses. */
    private IgniteBiTuple<Collection<String>, Collection<String>> addrs;

    /** */
    protected IgniteSpiContext spiCtx;

    /** */
    private IgniteDiscoverySpiInternalListener internalLsnr;

    /**
     * Gets current SPI state.
     *
     * @return Current SPI state.
     */
    public String getSpiState() {
        return impl.getSpiState();
    }

    /**
     * Gets message worker queue current size.
     *
     * @return Message worker queue current size.
     */
    public int getMessageWorkerQueueSize() {
        return impl.getMessageWorkerQueueSize();
    }

    /**
     * Gets current coordinator.
     *
     * @return Gets current coordinator.
     */
    public UUID getCoordinator() {
        return impl.getCoordinator();
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return impl.getRemoteNodes();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode getNode(UUID nodeId) {
        return impl.getNode(nodeId);
    }

    /**
     * @param id Id.
     */
    public ClusterNode getNode0(UUID id) {
        if (impl instanceof ServerImpl)
            return ((ServerImpl)impl).getNode0(id);

        return getNode(id);
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return impl.pingNode(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        impl.disconnect();
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        nodeAuth = auth;
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
        IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

        if (internalLsnr != null) {
            if (!internalLsnr.beforeSendCustomEvent(this, log, msg))
                return;
        }

        impl.sendCustomEvent(msg);
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        impl.failNode(nodeId, warning);
    }

    /**
     * Dumps debug info using configured logger.
     */
    public void dumpDebugInfo() {
        impl.dumpDebugInfo(log);
    }

    /** {@inheritDoc} */
    @Override public boolean isClientMode() {
        if (impl == null)
            throw new IllegalStateException("TcpDiscoverySpi has not started.");

        return impl instanceof ClientImpl;
    }

    /**
     * If {@code true} TcpDiscoverySpi will started in server mode regardless
     * of {@link IgniteConfiguration#isClientMode()}
     *
     * @return forceServerMode flag.
     * @deprecated Will be removed at 3.0.
     */
    @Deprecated
    public boolean isForceServerMode() {
        return forceSrvMode;
    }

    /**
     * Sets force server mode flag.
     * <p>
     * If {@code true} TcpDiscoverySpi is started in server mode regardless
     * of {@link IgniteConfiguration#isClientMode()}.
     *
     * @param forceSrvMode forceServerMode flag.
     * @return {@code this} for chaining.
     * @deprecated Will be removed at 3.0.
     */
    @IgniteSpiConfiguration(optional = true)
    @Deprecated
    public TcpDiscoverySpi setForceServerMode(boolean forceSrvMode) {
        this.forceSrvMode = forceSrvMode;

        return this;
    }

    /**
     * If {@code true} client does not try to reconnect after
     * server detected client node failure.
     *
     * @return Client reconnect disabled flag.
     */
    public boolean isClientReconnectDisabled() {
        return clientReconnectDisabled;
    }

    /**
     * Sets client reconnect disabled flag.
     * <p>
     * If {@code true} client does not try to reconnect after
     * server detected client node failure.
     *
     * @param clientReconnectDisabled Client reconnect disabled flag.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setClientReconnectDisabled(boolean clientReconnectDisabled) {
        this.clientReconnectDisabled = clientReconnectDisabled;
    }

    /**
     * Inject resources
     *
     * @param ignite Ignite.
     */
    @IgniteInstanceResource
    @Override protected void injectResources(Ignite ignite) {
        super.injectResources(ignite);

        // Inject resource.
        if (ignite != null) {
            setLocalAddress(ignite.configuration().getLocalHost());
            setAddressResolver(ignite.configuration().getAddressResolver());

            if (ignite instanceof IgniteKernal) // IgniteMock instance can be injected from tests.
                marsh = ((IgniteKernal)ignite).context().marshallerContext().jdkMarshaller();
            else
                marsh = new JdkMarshaller();
        }
    }

    /**
     * Sets local host IP address that discovery SPI uses.
     * <p>
     * If not provided, by default a first found non-loopback address
     * will be used. If there is no non-loopback address available,
     * then {@link InetAddress#getLocalHost()} will be used.
     *
     * @param locAddr IP address.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setLocalAddress(String locAddr) {
        // Injection should not override value already set by Spring or user.
        if (this.locAddr == null)
            this.locAddr = locAddr;

        return this;
    }

    /**
     * Gets local address that was set to SPI with {@link #setLocalAddress(String)} method.
     *
     * @return local address.
     */
    public String getLocalAddress() {
        return locAddr;
    }

    /**
     * Sets address resolver.
     *
     * @param addrRslvr Address resolver.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setAddressResolver(AddressResolver addrRslvr) {
        // Injection should not override value already set by Spring or user.
        if (this.addrRslvr == null)
            this.addrRslvr = addrRslvr;
    }

    /**
     * Gets address resolver.
     *
     * @return Address resolver.
     */
    public AddressResolver getAddressResolver() {
        return addrRslvr;
    }

    /**
     * Gets number of connection attempts.
     *
     * @return Number of connection attempts.
     */
    public int getReconnectCount() {
        return reconCnt;
    }

    /**
     * Number of times node tries to (re)establish connection to another node.
     * <p>
     * Note that SPI implementation will increase {@link #ackTimeout} by factor 2
     * on every retry.
     * <p>
     * If not specified, default is {@link #DFLT_RECONNECT_CNT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param reconCnt Number of retries during message sending.
     * @see #setAckTimeout(long)
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setReconnectCount(int reconCnt) {
        this.reconCnt = reconCnt;

        failureDetectionTimeoutEnabled(false);

        return this;
    }

    /**
     * Gets the amount of time in milliseconds that node waits before retrying to (re)connect to the cluster.
     *
     * @return Delay between attempts to connect to the cluster in milliseconds.
     */
    public long getReconnectDelay() {
        return reconDelay;
    }

    /**
     * Sets the amount of time in milliseconds that node waits before retrying to (re)connect to the cluster.
     * <p>
     * If not specified, default is {@link #DFLT_RECONNECT_DELAY}.
     *
     * @param reconDelay Delay between attempts to connect to the cluster in milliseconds.
     *
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setReconnectDelay(int reconDelay) {
        this.reconDelay = reconDelay;

        return this;
    }

    /**
     * Gets maximum message acknowledgement timeout.
     *
     * @return Maximum message acknowledgement timeout.
     */
    public long getMaxAckTimeout() {
        return maxAckTimeout;
    }

    /**
     * Sets maximum timeout for receiving acknowledgement for sent message.
     * <p>
     * If acknowledgement is not received within this timeout, sending is considered as failed
     * and SPI tries to repeat message sending. Every time SPI retries messing sending, ack
     * timeout will be increased. If no acknowledgement is received and {@code maxAckTimeout}
     * is reached, then the process of message sending is considered as failed.
     * <p>
     * If not specified, default is {@link #DFLT_MAX_ACK_TIMEOUT}.
     * <p>
     * Affected server nodes only.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param maxAckTimeout Maximum acknowledgement timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setMaxAckTimeout(long maxAckTimeout) {
        this.maxAckTimeout = maxAckTimeout;

        failureDetectionTimeoutEnabled(false);

        return this;
    }

    /**
     * Gets local TCP port SPI listens to.
     *
     * @return Local port range.
     */
    public int getLocalPort() {
        TcpDiscoveryNode locNode0 = locNode;

        return locNode0 != null ? locNode0.discoveryPort() : 0;
    }

    /**
     * Sets local port to listen to.
     * <p>
     * If not specified, default is {@link #DFLT_PORT}.
     * <p>
     * Affected server nodes only.
     *
     * @param locPort Local port to bind.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setLocalPort(int locPort) {
        this.locPort = locPort;

        return this;
    }

    /**
     * Gets local TCP port range.
     *
     * @return Local port range.
     */
    public int getLocalPortRange() {
        return locPortRange;
    }

    /**
     * Range for local ports. Local node will try to bind on first available port
     * starting from {@link #getLocalPort()} up until
     * <tt>{@link #getLocalPort()} {@code + locPortRange}</tt>.
     * If port range value is <tt>0</tt>, then implementation will try bind only to the port provided by
     * {@link #setLocalPort(int)} method and fail if binding to this port did not succeed.
     * <p>
     * If not specified, default is {@link #DFLT_PORT_RANGE}.
     * <p>
     * Affected server nodes only.
     *
     * @param locPortRange Local port range to bind.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setLocalPortRange(int locPortRange) {
        this.locPortRange = locPortRange;

        return this;
    }

    /**
     * Gets statistics print frequency.
     *
     * @return Statistics print frequency in milliseconds.
     */
    public long getStatisticsPrintFrequency() {
        return statsPrintFreq;
    }

    /**
     * Sets statistics print frequency.
     * <p>
     * If not set default value is {@link #DFLT_STATS_PRINT_FREQ}.
     * 0 indicates that no print is required. If value is greater than 0 and log is
     * not quiet then statistics are printed out with INFO level.
     * <p>
     * This may be very helpful for tracing topology problems.
     *
     * @param statsPrintFreq Statistics print frequency in milliseconds.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setStatisticsPrintFrequency(long statsPrintFreq) {
        this.statsPrintFreq = statsPrintFreq;

        return this;
    }

    /**
     * Gets IP finder clean frequency.
     *
     * @return IP finder clean frequency.
     */
    public long getIpFinderCleanFrequency() {
        return ipFinderCleanFreq;
    }

    /**
     * Sets IP finder clean frequency in milliseconds.
     * <p>
     * If not provided, default value is {@link #DFLT_IP_FINDER_CLEAN_FREQ}
     * <p>
     * Affected server nodes only.
     *
     * @param ipFinderCleanFreq IP finder clean frequency.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setIpFinderCleanFrequency(long ipFinderCleanFreq) {
        this.ipFinderCleanFreq = ipFinderCleanFreq;

        return this;
    }

    /**
     * Gets IP finder for IP addresses sharing and storing.
     *
     * @return IP finder for IP addresses sharing and storing.
     */
    public TcpDiscoveryIpFinder getIpFinder() {
        return ipFinder;
    }

    /**
     * Sets IP finder for IP addresses sharing and storing.
     * <p>
     * If not provided {@link org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder} will
     * be used by default.
     *
     * @param ipFinder IP finder.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setIpFinder(TcpDiscoveryIpFinder ipFinder) {
        this.ipFinder = ipFinder;

        return this;
    }

    /**
     * Sets socket operations timeout. This timeout is used to limit connection time and
     * write-to-socket time.
     * <p>
     * Note that when running Ignite on Amazon EC2, socket timeout must be set to a value
     * significantly greater than the default (e.g. to {@code 30000}).
     * <p>
     * If not specified, default is {@link #DFLT_SOCK_TIMEOUT} or {@link #DFLT_SOCK_TIMEOUT_CLIENT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param sockTimeout Socket connection timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setSocketTimeout(long sockTimeout) {
        this.sockTimeout = sockTimeout;

        failureDetectionTimeoutEnabled(false);

        return this;
    }

    /**
     * Sets timeout for receiving acknowledgement for sent message.
     * <p>
     * If acknowledgement is not received within this timeout, sending is considered as failed
     * and SPI tries to repeat message sending.
     * <p>
     * If not specified, default is {@link #DFLT_ACK_TIMEOUT} or {@link #DFLT_ACK_TIMEOUT_CLIENT}.
     * <p>
     * When this property is explicitly set {@link IgniteConfiguration#getFailureDetectionTimeout()} is ignored.
     *
     * @param ackTimeout Acknowledgement timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setAckTimeout(long ackTimeout) {
        this.ackTimeout = ackTimeout;

        failureDetectionTimeoutEnabled(false);

        return this;
    }

    /**
     * Sets maximum network timeout to use for network operations.
     * <p>
     * If not specified, default is {@link #DFLT_NETWORK_TIMEOUT}.
     *
     * @param netTimeout Network timeout.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setNetworkTimeout(long netTimeout) {
        this.netTimeout = netTimeout;

        return this;
    }

    /**
     * Gets join timeout.
     *
     * @return Join timeout.
     */
    public long getJoinTimeout() {
        return joinTimeout;
    }

    /**
     * Sets join timeout.
     * <p>
     * If non-shared IP finder is used and node fails to connect to
     * any address from IP finder, node keeps trying to join within this
     * timeout. If all addresses are still unresponsive, exception is thrown
     * and node startup fails.
     * <p>
     * If not specified, default is {@link #DFLT_JOIN_TIMEOUT}.
     *
     * @param joinTimeout Join timeout ({@code 0} means wait forever).
     *
     * @see TcpDiscoveryIpFinder#isShared()
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setJoinTimeout(long joinTimeout) {
        this.joinTimeout = joinTimeout;

        return this;
    }

    /**
     * Sets thread priority. All threads within SPI will be started with it.
     * <p>
     * If not provided, default value is {@link #DFLT_THREAD_PRI}
     *
     * @param threadPri Thread priority.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setThreadPriority(int threadPri) {
        this.threadPri = threadPri;

        return this;
    }

    /**
     * @return Size of topology snapshots history.
     */
    public long getTopHistorySize() {
        return topHistSize;
    }

    /**
     * Sets size of topology snapshots history. Specified size should be greater than or equal to default size
     * {@link #DFLT_TOP_HISTORY_SIZE}.
     *
     * @param topHistSize Size of topology snapshots history.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public TcpDiscoverySpi setTopHistorySize(int topHistSize) {
        if (topHistSize < DFLT_TOP_HISTORY_SIZE) {
            U.warn(log, "Topology history size should be greater than or equal to default size. " +
                "Specified size will not be set [curSize=" + this.topHistSize + ", specifiedSize=" + topHistSize +
                ", defaultSize=" + DFLT_TOP_HISTORY_SIZE + ']');

            return this;
        }

        this.topHistSize = topHistSize;

        return this;
    }

    /**
     * Gets timeout that defines how long server node would try to recovery connection.<br>
     * See {@link #setConnectionRecoveryTimeout(long)} for details.
     *
     * @return Timeout that defines how long server node would try to recovery connection.
     */
    public long getConnectionRecoveryTimeout() {
        return connRecoveryTimeout;
    }

    /**
     * @return Connection recovery timeout that is not greater than failureDetectionTimeout if enabled.
     */
    long getEffectiveConnectionRecoveryTimeout() {
        if (failureDetectionTimeoutEnabled() && failureDetectionTimeout() < connRecoveryTimeout)
            return failureDetectionTimeout();

        return connRecoveryTimeout;
    }

    /**
     * Sets timeout that defines how long server node would try to recovery connection.
     * <p>In case local node has temporary connectivity issues with part of the cluster,
     * it may sequentially fail nodes one-by-one till successfully connect to one that
     * has a fine connection with.
     * This leads to fail of big number of nodes.
     * </p>
     * <p>
     *     To overcome that issue, local node will do a sequential connection tries to next
     *     nodes. But if new next node has connection to previous it forces local node to
     *     retry connect to previous. These tries will last till timeout will not
     *     finished. When timeout is over, but no success in connecting to nodes it will
     *     segment itself.
     * </p>
     * <p>
     *     Cannot be greater than {@link #failureDetectionTimeout()}.
     * </p>
     * <p>
     *     Default is {@link #DFLT_CONNECTION_RECOVERY_TIMEOUT}.
     * </p>
     *
     * @param connRecoveryTimeout Timeout that defines how long server node would try to recovery connection.
     * {@code 0} means node will not recheck failed nodes.
     */
    public void setConnectionRecoveryTimeout(long connRecoveryTimeout) {
        this.connRecoveryTimeout = connRecoveryTimeout;
    }

    /** {@inheritDoc} */
    @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
        assert locNodeAttrs == null;
        assert locNodeVer == null;

        if (log.isDebugEnabled()) {
            log.debug("Node attributes to set: " + attrs);
            log.debug("Node version to set: " + ver);
        }

        locNodeAttrs = attrs;
        locNodeVer = ver;
    }

    /**
     * Gets ID of the local node.
     *
     * @return ID of the local node.
     */
    public UUID getLocalNodeId() {
        return ignite.cluster().localNode().id();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable consistentId() throws IgniteSpiException {
        if (consistentId == null) {
            initializeImpl();

            initAddresses();

            IgniteConfiguration cfg = ignite.configuration();

            final Serializable cfgId = cfg.getConsistentId();

            if (cfgId == null) {
                if (cfg.isClientMode() == Boolean.TRUE)
                    consistentId = cfg.getNodeId();
                else {
                    List<String> sortedAddrs = new ArrayList<>(addrs.get1());

                    Collections.sort(sortedAddrs);

                    if (getBoolean(IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT))
                        consistentId = U.consistentId(sortedAddrs);
                    else
                        consistentId = U.consistentId(sortedAddrs, impl.boundPort());
                }
            }
            else
                consistentId = cfgId;
        }

        return consistentId;
    }

    /**
     *
     */
    private void initAddresses() {
        if (addrs == null) {
            try {
                addrs = U.resolveLocalAddresses(locHost);
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteSpiException("Failed to resolve local host to set of external addresses: " + locHost,
                    e);
            }
        }
    }

    /**
     * @param srvPort Server port.
     * @param addExtAddrAttr If {@code true} adds {@link #ATTR_EXT_ADDRS} attribute.
     */
    protected void initLocalNode(int srvPort, boolean addExtAddrAttr) {
        // Init local node.
        initAddresses();

        locNode = new TcpDiscoveryNode(
            ignite.configuration().getNodeId(),
            addrs.get1(),
            addrs.get2(),
            srvPort,
            metricsProvider,
            locNodeVer,
            consistentId());

        if (addExtAddrAttr) {
            Collection<InetSocketAddress> extAddrs = addrRslvr == null ? null :
                U.resolveAddresses(addrRslvr, F.flat(Arrays.asList(addrs.get1(), addrs.get2())),
                    locNode.discoveryPort());

            locNodeAddrs = new LinkedHashSet<>();
            locNodeAddrs.addAll(locNode.socketAddresses());

            if (extAddrs != null) {
                locNodeAttrs.put(createSpiAttributeName(ATTR_EXT_ADDRS), extAddrs);

                locNodeAddrs.addAll(extAddrs);
            }
        }

        locNode.setAttributes(locNodeAttrs);
        locNode.local(true);

        DiscoverySpiListener lsnr = this.lsnr;

        if (lsnr != null)
            lsnr.onLocalNodeInitialized(locNode);

        if (log.isDebugEnabled())
            log.debug("Local node initialized: " + locNode);
    }

    /**
     * @param node Node.
     * @return {@link LinkedHashSet} of internal and external addresses of provided node.
     *      Internal addresses placed before external addresses.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    LinkedHashSet<InetSocketAddress> getNodeAddresses(TcpDiscoveryNode node) {
        LinkedHashSet<InetSocketAddress> res = new LinkedHashSet<>(node.socketAddresses());

        Collection<InetSocketAddress> extAddrs = node.attribute(createSpiAttributeName(ATTR_EXT_ADDRS));

        if (extAddrs != null)
            res.addAll(extAddrs);

        return res;
    }

    /**
     * @param node Node.
     * @param sameHost Same host flag.
     * @return {@link LinkedHashSet} of internal and external addresses of provided node.
     *      Internal addresses placed before external addresses.
     *      Internal addresses will be sorted with {@code inetAddressesComparator(sameHost)}.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    LinkedHashSet<InetSocketAddress> getNodeAddresses(TcpDiscoveryNode node, boolean sameHost) {
        List<InetSocketAddress> addrs = U.arrayList(node.socketAddresses());

        Collections.sort(addrs, U.inetAddressesComparator(sameHost));

        LinkedHashSet<InetSocketAddress> res = new LinkedHashSet<>();

        InetSocketAddress lastAddr = node.lastSuccessfulAddress();

        if (lastAddr != null)
            res.add(lastAddr);

        res.addAll(addrs);

        Collection<InetSocketAddress> extAddrs = node.attribute(createSpiAttributeName(ATTR_EXT_ADDRS));

        if (extAddrs != null)
            res.addAll(extAddrs);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> injectables() {
        return F.<Object>asList(ipFinder);
    }

    /**
     * Gets socket timeout.
     *
     * @return Socket timeout.
     */
    public long getSocketTimeout() {
        return sockTimeout;
    }

    /**
     * Gets effective or resulting socket timeout with considering failure detection timeout
     *
     * @param srvrOperation {@code True} if socket connect to server node,
     *     {@code False} if socket connect to client node.
     * @return Resulting socket timeout.
     */
    public long getEffectiveSocketTimeout(boolean srvrOperation) {
        if (failureDetectionTimeoutEnabled())
            return srvrOperation ? failureDetectionTimeout() : clientFailureDetectionTimeout();
        else
            return sockTimeout;
    }

    /**
     * Gets message acknowledgement timeout.
     *
     * @return Message acknowledgement timeout.
     */
    public long getAckTimeout() {
        return ackTimeout;
    }

    /**
     * Gets network timeout.
     *
     * @return Network timeout.
     */
    public long getNetworkTimeout() {
        return netTimeout;
    }

    /**
     * Gets thread priority. All threads within SPI will be started with it.
     *
     * @return Thread priority.
     */
    public int getThreadPriority() {
        return threadPri;
    }

    /**
     * Gets {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} (string representation).
     *
     * @return IPFinder (string representation).
     */
    public String getIpFinderFormatted() {
        return ipFinder.toString();
    }

    /**
     * Gets joined nodes count.
     *
     * @return Nodes joined count.
     */
    public long getNodesJoined() {
        return stats.joinedNodesCount();
    }

    /**
     * Gets left nodes count.
     *
     * @return Left nodes count.
     */
    public long getNodesLeft() {
        return stats.leftNodesCount();
    }

    /**
     * Gets failed nodes count.
     *
     * @return Failed nodes count.
     */
    public long getNodesFailed() {
        return stats.failedNodesCount();
    }

    /**
     * Gets pending messages registered count.
     *
     * @return Pending messages registered count.
     */
    public long getPendingMessagesRegistered() {
        return stats.pendingMessagesRegistered();
    }

    /**
     * Gets pending messages discarded count.
     *
     * @return Pending messages registered count.
     */
    public long getPendingMessagesDiscarded() {
        return stats.pendingMessagesDiscarded();
    }

    /**
     * Gets avg message processing time.
     *
     * @return Avg message processing time.
     */
    public long getAvgMessageProcessingTime() {
        return stats.avgMessageProcessingTime();
    }

    /**
     * Gets max message processing time.
     *
     * @return Max message processing time.
     */
    public long getMaxMessageProcessingTime() {
        return stats.maxMessageProcessingTime();
    }

    /**
     * Gets total received messages count.
     *
     * @return Total received messages count.
     */
    public int getTotalReceivedMessages() {
        return stats.totalReceivedMessages();
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Integer> getReceivedMessages() {
        return stats.receivedMessages();
    }

    /**
     * Gets total processed messages count.
     *
     * @return Total processed messages count.
     */
    public int getTotalProcessedMessages() {
        return stats.totalProcessedMessages();
    }

    /**
     * Gets processed messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Integer> getProcessedMessages() {
        return stats.processedMessages();
    }

    /**
     * Gets time local node has been coordinator since.
     *
     * @return Time local node is coordinator since.
     */
    public long getCoordinatorSinceTimestamp() {
        return stats.coordinatorSinceTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        super.onContextInitialized0(spiCtx);

        this.spiCtx = spiCtx;

        ctxInitLatch.countDown();

        ipFinder.onSpiContextInitialized(spiCtx);

        impl.onContextInitialized0(spiCtx);
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        super.onContextDestroyed0();

        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        if (ipFinder != null)
            ipFinder.onSpiContextDestroyed();

        getSpiContext().deregisterPorts();
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiContext getSpiContext() {
        if (ctxInitLatch.getCount() > 0) {
            if (log.isDebugEnabled())
                log.debug("Waiting for context initialization.");

            try {
                U.await(ctxInitLatch);

                if (log.isDebugEnabled())
                    log.debug("Context has been initialized.");
            }
            catch (IgniteInterruptedCheckedException e) {
                U.warn(log, "Thread has been interrupted while waiting for SPI context initialization.", e);
            }
        }

        return super.getSpiContext();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getLocalNode() {
        return locNode;
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public void setDataExchange(DiscoverySpiDataExchange exchange) {
        this.exchange = exchange;
    }

    /** {@inheritDoc} */
    @Override public void setMetricsProvider(DiscoveryMetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    /** {@inheritDoc} */
    @Override public long getGridStartTime() {
        assert gridStartTime != 0;

        return gridStartTime;
    }

    /**
     * @param sockAddr Remote address.
     * @param timeoutHelper Timeout helper.
     * @return Opened socket.
     * @throws IOException If failed.
     * @throws IgniteSpiOperationTimeoutException In case of timeout.
     */
    protected Socket openSocket(InetSocketAddress sockAddr, IgniteSpiOperationTimeoutHelper timeoutHelper)
        throws IOException, IgniteSpiOperationTimeoutException {
        return openSocket(createSocket(), sockAddr, timeoutHelper);
    }

    /**
     * @param sock Socket.
     * @return Buffered stream wrapping socket stream.
     * @throws IOException If failed.
     */
    final BufferedOutputStream socketStream(Socket sock) throws IOException {
        int bufSize = sock.getSendBufferSize();

        return bufSize > 0 ? new BufferedOutputStream(sock.getOutputStream(), bufSize) :
            new BufferedOutputStream(sock.getOutputStream());
    }

    /**
     * Connects to remote address sending {@code U.IGNITE_HEADER} when connection is established.
     *
     * @param sock Socket bound to a local host address.
     * @param remAddr Remote address.
     * @param timeoutHelper Timeout helper.
     * @return Connected socket.
     * @throws IOException If failed.
     * @throws IgniteSpiOperationTimeoutException In case of timeout.
     */
    protected Socket openSocket(Socket sock, InetSocketAddress remAddr, IgniteSpiOperationTimeoutHelper timeoutHelper)
        throws IOException, IgniteSpiOperationTimeoutException {

        assert remAddr != null;

        try {
            InetSocketAddress resolved = remAddr.isUnresolved() ?
                new InetSocketAddress(InetAddress.getByName(remAddr.getHostName()), remAddr.getPort()) : remAddr;

            InetAddress addr = resolved.getAddress();

            assert addr != null;

            sock.connect(resolved, (int)timeoutHelper.nextTimeoutChunk(sockTimeout));

            writeToSocket(sock, null, U.IGNITE_HEADER, timeoutHelper.nextTimeoutChunk(sockTimeout));

            return sock;
        } catch (IOException | IgniteSpiOperationTimeoutException e) {
            if (sock != null)
                U.closeQuiet(sock);

            throw e;
        }
    }

    /**
     * Creates socket binding it to a local host address. This operation is not blocking.
     *
     * @return Created socket.
     * @throws IOException If failed.
     */
    Socket createSocket() throws IOException {
        Socket sock = null;

        try {
            if (isSslEnabled())
                sock = sslSockFactory.createSocket();
            else
                sock = new Socket();

            sock.bind(new InetSocketAddress(locHost, 0));

            sock.setTcpNoDelay(true);

            return sock;
        } catch (IOException e) {
            if (sock != null)
                U.closeQuiet(sock);

            throw e;
        }
    }

    /**
     * Writes message to the socket.
     *
     * @param sock Socket.
     * @param msg Message.
     * @param data Raw data to write.
     * @param timeout Socket write timeout.
     * @throws IOException If IO failed or write timed out.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data, long timeout) throws IOException {
        assert sock != null;
        assert data != null;

        SocketTimeoutObject obj = new SocketTimeoutObject(sock, U.currentTimeMillis() + timeout);

        addTimeoutObject(obj);

        IOException err = null;

        try {
            OutputStream out = sock.getOutputStream();

            out.write(data);

            out.flush();
        }
        catch (IOException e) {
            err = e;
        }
        finally {
            boolean cancelled = obj.cancel();

            if (cancelled)
                removeTimeoutObject(obj);

            // Throw original exception.
            if (err != null)
                throw err;

            if (!cancelled)
                throw new SocketTimeoutException("Write timed out (socket was concurrently closed).");
        }
    }

    /**
     * Writes message to the socket.
     *
     * @param sock Socket.
     * @param msg Message.
     * @param timeout Socket write timeout.
     * @throws IOException If IO failed or write timed out.
     * @throws IgniteCheckedException If marshalling failed.
     */
    protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException,
        IgniteCheckedException {
        writeToSocket(sock, socketStream(sock), msg, timeout);
    }

    /**
     * @param msg Message.
     */
    protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
        // No-op, intended for usage in tests.
    }

    /**
     * @param node Target node.
     * @param sock Socket.
     * @param out Stream to write to.
     * @param msg Message.
     * @param timeout Timeout.
     * @throws IOException If IO failed or write timed out.
     * @throws IgniteCheckedException If marshalling failed.
     */
    protected void writeToSocket(
        ClusterNode node,
        Socket sock,
        OutputStream out,
        TcpDiscoveryAbstractMessage msg,
        long timeout) throws IOException, IgniteCheckedException {
        writeToSocket(sock, out, msg, timeout);
    }

    /**
     * Writes message to the socket.
     *
     * @param sock Socket.
     * @param out Stream to write to.
     * @param msg Message.
     * @param timeout Timeout.
     * @throws IOException If IO failed or write timed out.
     * @throws IgniteCheckedException If marshalling failed.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    protected void writeToSocket(Socket sock,
        OutputStream out,
        TcpDiscoveryAbstractMessage msg,
        long timeout) throws IOException, IgniteCheckedException {
        if (internalLsnr != null && msg instanceof TcpDiscoveryJoinRequestMessage)
            internalLsnr.beforeJoin(locNode, log);

        assert sock != null;
        assert msg != null;
        assert out != null;

        SocketTimeoutObject obj = new SocketTimeoutObject(sock, U.currentTimeMillis() + timeout);

        addTimeoutObject(obj);

        IgniteCheckedException err = null;

        try {
            U.marshal(marshaller(), msg, out);
        }
        catch (IgniteCheckedException e) {
            err = e;
        }
        finally {
            boolean cancelled = obj.cancel();

            if (cancelled)
                removeTimeoutObject(obj);

            // Throw original exception.
            if (err != null)
                throw err;

            if (!cancelled)
                throw new SocketTimeoutException("Write timed out (socket was concurrently closed).");
        }
    }

    /**
     * Writes response to the socket.
     *
     * @param msg Received message.
     * @param sock Socket.
     * @param res Integer response.
     * @param timeout Socket timeout.
     * @throws IOException If IO failed or write timed out.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res, long timeout)
        throws IOException {
        assert sock != null;

        SocketTimeoutObject obj = new SocketTimeoutObject(sock, U.currentTimeMillis() + timeout);

        addTimeoutObject(obj);

        OutputStream out = sock.getOutputStream();

        IOException err = null;

        try {
            out.write(res);

            out.flush();
        }
        catch (IOException e) {
            err = e;
        }
        finally {
            boolean cancelled = obj.cancel();

            if (cancelled)
                removeTimeoutObject(obj);

            // Throw original exception.
            if (err != null)
                throw err;

            if (!cancelled)
                throw new SocketTimeoutException("Write timed out (socket was concurrently closed).");
        }
    }

    /**
     * Reads message from the socket limiting read time.
     *
     * @param sock Socket.
     * @param in Input stream (in case socket stream was wrapped).
     * @param timeout Socket timeout for this operation.
     * @return Message.
     * @throws IOException If IO failed or read timed out.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    protected <T> T readMessage(Socket sock, @Nullable InputStream in, long timeout) throws IOException,
        IgniteCheckedException {
        assert sock != null;

        int oldTimeout = sock.getSoTimeout();

        try {
            sock.setSoTimeout((int)timeout);

            T res = U.unmarshal(marshaller(), in == null ? sock.getInputStream() : in,
                U.resolveClassLoader(ignite.configuration()));

            return res;
        }
        catch (IOException | IgniteCheckedException e) {
            if (X.hasCause(e, SocketTimeoutException.class))
                LT.warn(log, "Timed out waiting for message to be read (most probably, the reason is " +
                    "long GC pauses on remote node) [curTimeout=" + timeout +
                    ", rmtAddr=" + sock.getRemoteSocketAddress() + ", rmtPort=" + sock.getPort() + ']');

            StreamCorruptedException streamCorruptedCause = X.cause(e, StreamCorruptedException.class);

            if (streamCorruptedCause != null) {
                // Lets check StreamCorruptedException for SSL Alert message
                // Sample SSL Alert message: 15:03:03:00:02:02:0a
                // 15 = Alert
                // 03:03 = SSL version
                // 00:02 = payload length
                // 02:0a = critical (02) / unexpected message (0a)
                // So, check message for "invalid stream header: 150X0X00"

                String msg = streamCorruptedCause.getMessage();

                if (msg != null && sslMsgPattern.matcher(msg).matches())
                    streamCorruptedCause.initCause(new SSLException("Detected SSL alert in StreamCorruptedException"));
            }
            throw e;
        }
        finally {
            // Quietly restore timeout.
            try {
                sock.setSoTimeout(oldTimeout);
            }
            catch (SocketException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Reads message delivery receipt from the socket.
     *
     * @param sock Socket.
     * @param timeout Socket timeout for this operation.
     * @return Receipt.
     * @throws IOException If IO failed or read timed out.
     */
    protected int readReceipt(Socket sock, long timeout) throws IOException {
        assert sock != null;

        int oldTimeout = sock.getSoTimeout();

        try {
            sock.setSoTimeout((int)timeout);

            int res = sock.getInputStream().read();

            if (res == -1)
                throw new EOFException();

            return res;
        }
        catch (SocketTimeoutException e) {
            LT.warn(log, "Timed out waiting for message delivery receipt (most probably, the reason is " +
                "in long GC pauses on remote node; consider tuning GC and increasing 'ackTimeout' " +
                "configuration property). Will retry to send message with increased timeout " +
                "[currentTimeout=" + timeout + ", rmtAddr=" + sock.getRemoteSocketAddress() +
                ", rmtPort=" + sock.getPort() + ']');

            stats.onAckTimeout();

            throw e;
        }
        finally {
            // Quietly restore timeout.
            try {
                sock.setSoTimeout(oldTimeout);
            }
            catch (SocketException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Resolves addresses registered in the IP finder, removes duplicates and local host
     * address and returns the collection of.
     *
     * @return Resolved addresses without duplicates and local address (potentially
     *      empty but never null).
     * @throws org.apache.ignite.spi.IgniteSpiException If an error occurs.
     */
    protected Collection<InetSocketAddress> resolvedAddresses() throws IgniteSpiException {
        List<InetSocketAddress> res = new ArrayList<>();

        Collection<InetSocketAddress> addrs;

        // Get consistent addresses collection.
        while (true) {
            try {
                addrs = registeredAddresses();

                break;
            }
            catch (IgniteSpiException e) {
                LT.error(log, e, "Failed to get registered addresses from IP finder on start " +
                    "(retrying every " + getReconnectDelay() + "ms; change 'reconnectDelay' to configure " +
                    "the frequency of retries).");
            }

            try {
                U.sleep(getReconnectDelay());
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }
        }

        for (InetSocketAddress addr : addrs) {
            assert addr != null;

            try {
                InetSocketAddress resolved = addr.isUnresolved() ?
                    new InetSocketAddress(InetAddress.getByName(addr.getHostName()), addr.getPort()) : addr;

                if (locNodeAddrs == null || !locNodeAddrs.contains(resolved))
                    res.add(resolved);
            }
            catch (UnknownHostException ignored) {
                LT.warn(log, "Failed to resolve address from IP finder (host is unknown): " + addr);

                // Add address in any case.
                res.add(addr);
            }
        }

        if (!res.isEmpty())
            Collections.shuffle(res);

        return res;
    }

    /**
     * Gets addresses registered in the IP finder, initializes addresses having no
     * port (or 0 port) with {@link #DFLT_PORT}.
     *
     * @return Registered addresses.
     * @throws org.apache.ignite.spi.IgniteSpiException If an error occurs.
     */
    protected Collection<InetSocketAddress> registeredAddresses() throws IgniteSpiException {
        Collection<InetSocketAddress> res = new ArrayList<>();

        for (InetSocketAddress addr : ipFinder.getRegisteredAddresses()) {
            if (addr.getPort() == 0) {
                // TcpDiscoveryNode.discoveryPort() returns an correct port for a server node and 0 for client node.
                int port = locNode.discoveryPort() != 0 ? locNode.discoveryPort() : DFLT_PORT;

                addr = addr.isUnresolved() ? new InetSocketAddress(addr.getHostName(), port) :
                    new InetSocketAddress(addr.getAddress(), port);
            }

            res.add(addr);
        }

        return res;
    }

    /**
     * @param msg Message.
     * @return Error.
     */
    protected IgniteSpiException duplicateIdError(TcpDiscoveryDuplicateIdMessage msg) {
        assert msg != null;

        return new IgniteSpiException("Local node has the same ID as existing node in topology " +
            "(fix configuration and restart local node) [localNode=" + locNode +
            ", existingNode=" + msg.node() + ']');
    }

    /**
     * @param msg Message.
     * @return Error.
     */
    protected IgniteSpiException authenticationFailedError(TcpDiscoveryAuthFailedMessage msg) {
        assert msg != null;

        return new IgniteSpiException(new IgniteAuthenticationException("Authentication failed [nodeId=" +
            msg.creatorNodeId() + ", addr=" + msg.address().getHostAddress() + ']'));
    }

    /**
     * @param msg Message.
     * @return Error.
     */
    protected IgniteSpiException checkFailedError(TcpDiscoveryCheckFailedMessage msg) {
        assert msg != null;

        return versionCheckFailed(msg) ? new IgniteSpiVersionCheckException(msg.error()) :
            new IgniteSpiException(msg.error());
    }

    /**
     * @param msg Message.
     * @return Whether delivery of the message is ensured.
     */
    protected boolean ensured(TcpDiscoveryAbstractMessage msg) {
        return U.getAnnotation(msg.getClass(), TcpDiscoveryEnsureDelivery.class) != null;
    }

    /**
     * @param msg Failed message.
     * @return {@code True} if specified failed message relates to version incompatibility, {@code false} otherwise.
     * @deprecated Parsing of error message was used for preserving backward compatibility. We should remove it
     *      and create separate message for failed version check with next major release.
     */
    @Deprecated
    private static boolean versionCheckFailed(TcpDiscoveryCheckFailedMessage msg) {
        return msg.error().contains("versions are not compatible");
    }

    /**
     * @param dataPacket Data packet.
     */
    DiscoveryDataPacket collectExchangeData(DiscoveryDataPacket dataPacket) {
        if (locNode.isDaemon())
            return dataPacket;

        assert dataPacket != null;
        assert dataPacket.joiningNodeId() != null;

        //create data bag, pass it to exchange.collect
        DiscoveryDataBag dataBag = dataPacket.bagForDataCollection();

        exchange.collect(dataBag);

        //marshall collected bag into packet, return packet
        if (dataPacket.joiningNodeId().equals(locNode.id()))
            dataPacket.marshalJoiningNodeData(dataBag, marshaller(), log);
        else
            dataPacket.marshalGridNodeData(dataBag, locNode.id(), marshaller(), log);

        return dataPacket;
    }

    /**
     * @param dataPacket object holding discovery data collected during discovery process.
     * @param clsLdr Class loader.
     */
    protected void onExchange(DiscoveryDataPacket dataPacket, ClassLoader clsLdr) {
        if (locNode.isDaemon())
            return;

        assert dataPacket != null;
        assert dataPacket.joiningNodeId() != null;

        DiscoveryDataBag dataBag;

        if (dataPacket.joiningNodeId().equals(locNode.id()))
            dataBag = dataPacket.unmarshalGridData(marshaller(), clsLdr, locNode.clientRouterNodeId() != null, log);
        else
            dataBag = dataPacket.unmarshalJoiningNodeData(marshaller(), clsLdr, locNode.clientRouterNodeId() != null, log);


        exchange.onExchange(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        initializeImpl();

        registerMBean(igniteInstanceName, new TcpDiscoverySpiMBeanImpl(this), TcpDiscoverySpiMBean.class);

        impl.spiStart(igniteInstanceName);
    }

    /**
     *
     */
    private void initializeImpl() {
        if (impl != null)
            return;

        initFailureDetectionTimeout();

        if (!forceSrvMode && (Boolean.TRUE.equals(ignite.configuration().isClientMode()))) {
            if (ackTimeout == 0)
                ackTimeout = DFLT_ACK_TIMEOUT_CLIENT;

            if (sockTimeout == 0)
                sockTimeout = DFLT_SOCK_TIMEOUT_CLIENT;

            impl = new ClientImpl(this);

            ctxInitLatch.countDown();
        }
        else {
            if (ackTimeout == 0)
                ackTimeout = DFLT_ACK_TIMEOUT;

            if (sockTimeout == 0)
                sockTimeout = DFLT_SOCK_TIMEOUT;

            impl = new ServerImpl(this);
        }

        metricsUpdateFreq = ignite.configuration().getMetricsUpdateFrequency();

        if (!failureDetectionTimeoutEnabled()) {
            assertParameter(sockTimeout > 0, "sockTimeout > 0");
            assertParameter(ackTimeout > 0, "ackTimeout > 0");
            assertParameter(maxAckTimeout > ackTimeout, "maxAckTimeout > ackTimeout");
            assertParameter(reconCnt > 0, "reconnectCnt > 0");
        }

        assertParameter(netTimeout > 0, "networkTimeout > 0");
        assertParameter(ipFinder != null, "ipFinder != null");
        assertParameter(metricsUpdateFreq > 0, "metricsUpdateFreq > 0" +
            " (inited from igniteConfiguration.metricsUpdateFrequency)");

        assertParameter(ipFinderCleanFreq > 0, "ipFinderCleanFreq > 0");
        assertParameter(locPort > 1023, "localPort > 1023");
        assertParameter(locPortRange >= 0, "localPortRange >= 0");
        assertParameter(locPort + locPortRange <= 0xffff, "locPort + locPortRange <= 0xffff");
        assertParameter(threadPri > 0, "threadPri > 0");
        assertParameter(statsPrintFreq >= 0, "statsPrintFreq >= 0");

        if (isSslEnabled()) {
            try {
                SSLContext sslCtx = ignite().configuration().getSslContextFactory().create();

                sslSockFactory = sslCtx.getSocketFactory();
                sslSrvSockFactory = sslCtx.getServerSocketFactory();
            }
            catch (IgniteException e) {
                throw new IgniteSpiException("Failed to create SSL context. SSL factory: "
                    + ignite.configuration().getSslContextFactory(), e);
            }
        }

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new IgniteSpiException("Unknown local address: " + locAddr, e);
        }

        if (log.isDebugEnabled()) {
            log.debug(configInfo("localHost", locHost.getHostAddress()));
            log.debug(configInfo("localPort", locPort));
            log.debug(configInfo("localPortRange", locPortRange));
            log.debug(configInfo("threadPri", threadPri));

            if (!failureDetectionTimeoutEnabled()) {
                log.debug("Failure detection timeout is ignored because at least one of the parameters from this list" +
                    " has been set explicitly: 'sockTimeout', 'ackTimeout', 'maxAckTimeout', 'reconnectCount'.");

                log.debug(configInfo("networkTimeout", netTimeout));
                log.debug(configInfo("sockTimeout", sockTimeout));
                log.debug(configInfo("ackTimeout", ackTimeout));
                log.debug(configInfo("maxAckTimeout", maxAckTimeout));
                log.debug(configInfo("reconnectCount", reconCnt));
            }
            else
                log.debug(configInfo("failureDetectionTimeout", failureDetectionTimeout()));

            log.debug(configInfo("ipFinder", ipFinder));
            log.debug(configInfo("ipFinderCleanFreq", ipFinderCleanFreq));
            log.debug(configInfo("metricsUpdateFreq", metricsUpdateFreq));
            log.debug(configInfo("statsPrintFreq", statsPrintFreq));
        }

        // Warn on odd network timeout.
        if (netTimeout < 3000)
            U.warn(log, "Network timeout is too low (at least 3000 ms recommended): " + netTimeout);

        if (ipFinder instanceof TcpDiscoveryMulticastIpFinder) {
            TcpDiscoveryMulticastIpFinder mcastIpFinder = ((TcpDiscoveryMulticastIpFinder)ipFinder);

            if (mcastIpFinder.getLocalAddress() == null)
                mcastIpFinder.setLocalAddress(locAddr);
        }

        cfgNodeId = ignite.configuration().getNodeId();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        if (ipFinder != null) {
            try {
                ipFinder.close();
            }
            catch (Exception e) {
                log.error("Failed to close ipFinder", e);
            }
        }

        unregisterMBean();

        if (impl != null)
            impl.spiStop();
    }

    /**
     *
     */
    void printStartInfo() {
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     *
     */
    void printStopInfo() {
        IgniteLogger log = this.log;

        if (log != null && log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @return {@code True} if node is stopping.
     */
    boolean isNodeStopping0() {
        return isNodeStopping();
    }

    /**
     * @throws IgniteSpiException If any error occurs.
     * @return {@code true} if IP finder contains local address.
     */
    boolean ipFinderHasLocalAddress() throws IgniteSpiException {
        for (InetSocketAddress locAddr : locNodeAddrs) {
            for (InetSocketAddress addr : registeredAddresses())
                try {
                    int port = addr.getPort();

                    InetSocketAddress resolved = addr.isUnresolved() ?
                        new InetSocketAddress(InetAddress.getByName(addr.getHostName()), port) :
                        new InetSocketAddress(addr.getAddress(), port);

                    if (resolved.equals(locAddr))
                        return true;
                }
                catch (UnknownHostException e) {
                    getExceptionRegistry().onException(e.getMessage(), e);
                }
        }

        return false;
    }

    /**
     * @return {@code True} if ssl enabled.
     */
    boolean isSslEnabled() {
        return ignite().configuration().getSslContextFactory() != null;
    }

    /** {@inheritDoc} */
    @Override public void clientReconnect() throws IgniteSpiException {
        impl.reconnect();
    }

    /** {@inheritDoc} */
    @Override public boolean knownNode(UUID nodeId) {
        return getNode0(nodeId) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean clientReconnectSupported() {
        return !clientReconnectDisabled;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCommunicationFailureResolve() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void resolveCommunicationFailure(ClusterNode node, Exception err) {
        throw new UnsupportedOperationException();
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     */
    public int clientWorkerCount() {
        return ((ServerImpl)impl).clientMsgWorkers.size();
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     */
    void forceNextNodeFailure() {
        ((ServerImpl)impl).forceNextNodeFailure();
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     */
    public void addSendMessageListener(IgniteInClosure<TcpDiscoveryAbstractMessage> lsnr) {
        sndMsgLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr) {
        this.internalLsnr = lsnr;
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     */
    public void removeSendMessageListener(IgniteInClosure<TcpDiscoveryAbstractMessage> lsnr) {
        sndMsgLsnrs.remove(lsnr);
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     */
    public void addIncomeConnectionListener(IgniteInClosure<Socket> lsnr) {
        incomeConnLsnrs.add(lsnr);
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     */
    public void removeIncomeConnectionListener(IgniteInClosure<Socket> lsnr) {
        incomeConnLsnrs.remove(lsnr);
    }

    /**
     * FOR TEST PURPOSE ONLY!
     */
    public void waitForClientMessagePrecessed() {
        if (impl instanceof ClientImpl)
            ((ClientImpl)impl).waitForClientMessagePrecessed();
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * Simulates this node failure by stopping service threads. So, node will become
     * unresponsive.
     * <p>
     * This method is intended for test purposes only.
     */
    @Override public void simulateNodeFailure() {
        impl.simulateNodeFailure();
    }

    /**
     * FOR TEST PURPOSE ONLY!
     */
    public void brakeConnection() {
        impl.brakeConnection();
    }

    /**
     * Checks whether local node is coordinator. Nodes that are leaving or failed
     * (but are still in topology) are removed from search.
     *
     * @return {@code true} if local node is coordinator.
     */
    public boolean isLocalNodeCoordinator() {
        if (impl instanceof ServerImpl)
            return ((ServerImpl)impl).isLocalNodeCoordinator();

        return false;
    }

    /**
     * @return Marshaller.
     */
    protected Marshaller marshaller() {
        MarshallerUtils.setNodeName(marsh, igniteInstanceName);

        return marsh;
    }

    /** {@inheritDoc} */
    @Override public TcpDiscoverySpi setName(String name) {
        super.setName(name);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoverySpi.class, this);
    }

    /**
     * Socket timeout object.
     */
    private class SocketTimeoutObject implements IgniteSpiTimeoutObject {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final Socket sock;

        /** */
        private final long endTime;

        /** */
        private final AtomicBoolean done = new AtomicBoolean();

        /**
         * @param sock Socket.
         * @param endTime End time.
         */
        SocketTimeoutObject(Socket sock, long endTime) {
            assert sock != null;
            assert endTime > 0;

            this.sock = sock;
            this.endTime = endTime;
        }

        /**
         * @return {@code True} if object has not yet been processed.
         */
        boolean cancel() {
            return done.compareAndSet(false, true);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (done.compareAndSet(false, true)) {
                // Close socket - timeout occurred.
                U.closeQuiet(sock);

                LT.warn(log, "Socket write has timed out (consider increasing " +
                    (failureDetectionTimeoutEnabled() ?
                        "'IgniteConfiguration.failureDetectionTimeout' configuration property) [" +
                        "failureDetectionTimeout=" + failureDetectionTimeout() :
                        "'sockTimeout' configuration property) [sockTimeout=" + sockTimeout) +
                    ", rmtAddr=" + sock.getRemoteSocketAddress() + ", rmtPort=" + sock.getPort() +
                    ", sockTimeout=" + sockTimeout + ']');

                stats.onSocketTimeout();
            }
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public  IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SocketTimeoutObject.class, this);
        }
    }

    /**
     * MBean implementation for TcpDiscoverySpiMBean.
     */
    private class TcpDiscoverySpiMBeanImpl extends IgniteSpiMBeanAdapter implements TcpDiscoverySpiMBean {
        /** {@inheritDoc} */
        TcpDiscoverySpiMBeanImpl(IgniteSpiAdapter spiAdapter) {
            super(spiAdapter);
        }

        /** {@inheritDoc} */
        @Override public String getSpiState() {
            return impl.getSpiState();
        }

        /** {@inheritDoc} */
        @Override public int getMessageWorkerQueueSize() {
            return impl.getMessageWorkerQueueSize();
        }

        /** {@inheritDoc} */
        @Nullable @Override public UUID getCoordinator() {
            return impl.getCoordinator();
        }

        /** {@inheritDoc} */
        @Nullable @Override public String getCoordinatorNodeFormatted() {
            return String.valueOf(impl.getNode(impl.getCoordinator()));
        }

        /** {@inheritDoc} */
        @Override public String getLocalNodeFormatted() {
            return String.valueOf(getLocalNode());
        }

        /** {@inheritDoc} */
        @Override public void dumpDebugInfo() {
            impl.dumpDebugInfo(log);
        }

        /** {@inheritDoc} */
        @Override public long getSocketTimeout() {
            return TcpDiscoverySpi.this.getSocketTimeout();
        }

        /** {@inheritDoc} */
        @Override public long getMaxAckTimeout() {
            return TcpDiscoverySpi.this.getMaxAckTimeout();
        }

        /** {@inheritDoc} */
        @Override public long getAckTimeout() {
            return TcpDiscoverySpi.this.getAckTimeout();
        }

        /** {@inheritDoc} */
        @Override public long getNetworkTimeout() {
            return TcpDiscoverySpi.this.getNetworkTimeout();
        }

        /** {@inheritDoc} */
        @Override public long getJoinTimeout() {
            return TcpDiscoverySpi.this.getJoinTimeout();
        }

        /** {@inheritDoc} */
        @Override public int getLocalPort() {
            return TcpDiscoverySpi.this.getLocalPort();
        }

        /** {@inheritDoc} */
        @Override public int getLocalPortRange() {
            return TcpDiscoverySpi.this.getLocalPortRange();
        }

        /** {@inheritDoc} */
        @Override public long getIpFinderCleanFrequency() {
            return TcpDiscoverySpi.this.getIpFinderCleanFrequency();
        }

        /** {@inheritDoc} */
        @Override public int getThreadPriority() {
            return TcpDiscoverySpi.this.getThreadPriority();
        }

        /** {@inheritDoc} */
        @Override public long getStatisticsPrintFrequency() {
            return TcpDiscoverySpi.this.getStatisticsPrintFrequency();
        }

        /** {@inheritDoc} */
        @Override public String getIpFinderFormatted() {
            return TcpDiscoverySpi.this.getIpFinderFormatted();
        }

        /** {@inheritDoc} */
        @Override public int getReconnectCount() {
            return TcpDiscoverySpi.this.getReconnectCount();
        }

        /** {@inheritDoc} */
        @Override public long getConnectionCheckInterval() {
            return impl.connectionCheckInterval();
        }

        /** {@inheritDoc} */
        @Override public boolean isClientMode() {
            return TcpDiscoverySpi.this.isClientMode();
        }

        /** {@inheritDoc} */
        @Override public long getNodesJoined() {
            return TcpDiscoverySpi.this.getNodesJoined();
        }

        /** {@inheritDoc} */
        @Override public long getNodesLeft() {
            return TcpDiscoverySpi.this.getNodesLeft();
        }

        /** {@inheritDoc} */
        @Override public long getNodesFailed() {
            return TcpDiscoverySpi.this.getNodesFailed();
        }

        /** {@inheritDoc} */
        @Override public long getPendingMessagesRegistered() {
            return TcpDiscoverySpi.this.getPendingMessagesRegistered();
        }

        /** {@inheritDoc} */
        @Override public long getPendingMessagesDiscarded() {
            return stats.pendingMessagesDiscarded();

        }

        /** {@inheritDoc} */
        @Override public long getAvgMessageProcessingTime() {
            return TcpDiscoverySpi.this.getAvgMessageProcessingTime();
        }

        /** {@inheritDoc} */
        @Override public long getMaxMessageProcessingTime() {
            return TcpDiscoverySpi.this.getMaxMessageProcessingTime();
        }

        /** {@inheritDoc} */
        @Override public int getTotalReceivedMessages() {
            return TcpDiscoverySpi.this.getTotalReceivedMessages();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Integer> getReceivedMessages() {
            return TcpDiscoverySpi.this.getReceivedMessages();
        }

        /** {@inheritDoc} */
        @Override public int getTotalProcessedMessages() {
            return TcpDiscoverySpi.this.getTotalProcessedMessages();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Integer> getProcessedMessages() {
            return TcpDiscoverySpi.this.getProcessedMessages();
        }

        /** {@inheritDoc} */
        @Override public long getCoordinatorSinceTimestamp() {
            return TcpDiscoverySpi.this.getCoordinatorSinceTimestamp();
        }

        /** {@inheritDoc} */
        @Override public void checkRingLatency(int maxHops) {
            TcpDiscoverySpi.this.impl.checkRingLatency(maxHops);
        }

        /** {@inheritDoc} */
        @Override public long getCurrentTopologyVersion() {
            return impl.getCurrentTopologyVersion();
        }

        /** {@inheritDoc} */
        @Override public void dumpRingStructure() {
            impl.dumpRingStructure(log);
        }
    }
}
