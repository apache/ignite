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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.events.*;
import org.apache.ignite.internal.processors.security.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.IgniteNodeAttributes.*;
import static org.apache.ignite.spi.IgnitePortProtocol.*;
import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.*;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHeartbeatMessage.*;
import static org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage.*;

/**
 * Discovery SPI implementation that uses TCP/IP for node discovery.
 * <p>
 * Nodes are organized in ring. So almost all network exchange (except few cases) is
 * done across it.
 * <p>
 * At startup SPI tries to send messages to random IP taken from
 * {@link TcpDiscoveryIpFinder} about self start (stops when send succeeds)
 * and then this info goes to coordinator. When coordinator processes join request
 * and issues node added messages and all other nodes then receive info about new node.
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
 * <li>Heartbeat frequency (see {@link #setHeartbeatFrequency(long)})</li>
 * <li>Max missed heartbeats (see {@link #setMaxMissedHeartbeats(int)})</li>
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
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();
 *
 * GridTcpDiscoveryVmIpFinder finder =
 *     new GridTcpDiscoveryVmIpFinder();
 *
 * spi.setIpFinder(finder);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default discovery SPI.
 * cfg.setDiscoverySpi(spi);
 *
 * // Start grid.
 * Ignition.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridTcpDiscoverySpi can be configured from Spring XML configuration file:
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
 * <img src="http://ignite.incubator.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see DiscoverySpi
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
@IgniteSpiMultipleInstancesSupport(true)
@DiscoverySpiOrderSupport(true)
@DiscoverySpiHistorySupport(true)
public class TcpDiscoverySpi extends TcpDiscoverySpiAdapter implements TcpDiscoverySpiMBean {
    /** Default local port range (value is <tt>100</tt>). */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default timeout for joining topology (value is <tt>0</tt>). */
    public static final long DFLT_JOIN_TIMEOUT = 0;

    /** Default reconnect attempts count (value is <tt>10</tt>). */
    public static final int DFLT_RECONNECT_CNT = 10;

    /** Default max heartbeats count node can miss without initiating status check (value is <tt>1</tt>). */
    public static final int DFLT_MAX_MISSED_HEARTBEATS = 1;

    /** Default max heartbeats count node can miss without failing client node (value is <tt>5</tt>). */
    public static final int DFLT_MAX_MISSED_CLIENT_HEARTBEATS = 5;

    /** Default IP finder clean frequency in milliseconds (value is <tt>60,000ms</tt>). */
    public static final long DFLT_IP_FINDER_CLEAN_FREQ = 60 * 1000;

    /** Default statistics print frequency in milliseconds (value is <tt>0ms</tt>). */
    public static final long DFLT_STATS_PRINT_FREQ = 0;

    /** Maximum ack timeout value for receiving message acknowledgement in milliseconds (value is <tt>600,000ms</tt>). */
    public static final long DFLT_MAX_ACK_TIMEOUT = 10 * 60 * 1000;

    /** Node attribute that is mapped to node's external addresses (value is <tt>disc.tcp.ext-addrs</tt>). */
    public static final String ATTR_EXT_ADDRS = "disc.tcp.ext-addrs";

    /** Address resolver. */
    private AddressResolver addrRslvr;

    /** Local port which node uses. */
    private int locPort = DFLT_PORT;

    /** Local port range. */
    private int locPortRange = DFLT_PORT_RANGE;

    /** Statistics print frequency. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized", "RedundantFieldInitialization"})
    private long statsPrintFreq = DFLT_STATS_PRINT_FREQ;

    /** Maximum message acknowledgement timeout. */
    private long maxAckTimeout = DFLT_MAX_ACK_TIMEOUT;

    /** Join timeout. */
    @SuppressWarnings("RedundantFieldInitialization")
    private long joinTimeout = DFLT_JOIN_TIMEOUT;

    /** Max heartbeats count node can miss without initiating status check. */
    private int maxMissedHbs = DFLT_MAX_MISSED_HEARTBEATS;

    /** Max heartbeats count node can miss without failing client node. */
    private int maxMissedClientHbs = DFLT_MAX_MISSED_CLIENT_HEARTBEATS;

    /** IP finder clean frequency. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private long ipFinderCleanFreq = DFLT_IP_FINDER_CLEAN_FREQ;

    /** Reconnect attempts count. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private int reconCnt = DFLT_RECONNECT_CNT;

    /** Nodes ring. */
    @GridToStringExclude
    private final TcpDiscoveryNodesRing ring = new TcpDiscoveryNodesRing();

    /** Topology snapshots history. */
    private final SortedMap<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

    /** Socket readers. */
    private final Collection<SocketReader> readers = new LinkedList<>();

    /** TCP server for discovery SPI. */
    private TcpServer tcpSrvr;

    /** Message worker. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private RingMessageWorker msgWorker;

    /** Client message workers. */
    private ConcurrentMap<UUID, ClientMessageWorker> clientMsgWorkers = new ConcurrentHashMap8<>();

    /** Metrics sender. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private HeartbeatsSender hbsSnd;

    /** Status checker. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private CheckStatusSender chkStatusSnd;

    /** IP finder cleaner. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private IpFinderCleaner ipFinderCleaner;

    /** Statistics printer thread. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private StatisticsPrinter statsPrinter;

    /** Failed nodes (but still in topology). */
    private Collection<TcpDiscoveryNode> failedNodes = new HashSet<>();

    /** Leaving nodes (but still in topology). */
    private Collection<TcpDiscoveryNode> leavingNodes = new HashSet<>();

    /** If non-shared IP finder is used this flag shows whether IP finder contains local address. */
    private boolean ipFinderHasLocAddr;

    /** Addresses that do not respond during join requests send (for resolving concurrent start). */
    private final Collection<SocketAddress> noResAddrs = new GridConcurrentHashSet<>();

    /** Addresses that incoming join requests send were send from (for resolving concurrent start). */
    private final Collection<SocketAddress> fromAddrs = new GridConcurrentHashSet<>();

    /** Response on join request from coordinator (in case of duplicate ID or auth failure). */
    private final GridTuple<TcpDiscoveryAbstractMessage> joinRes = F.t1();

    /** Context initialization latch. */
    @GridToStringExclude
    private final CountDownLatch ctxInitLatch = new CountDownLatch(1);

    /** Node authenticator. */
    private DiscoverySpiNodeAuthenticator nodeAuth;

    /** Mutex. */
    private final Object mux = new Object();

    /** Map with proceeding ping requests. */
    private final ConcurrentMap<InetSocketAddress, IgniteInternalFuture<IgniteBiTuple<UUID, Boolean>>> pingMap =
        new ConcurrentHashMap8<>();

    /** Debug mode. */
    private boolean debugMode;

    /** Debug messages history. */
    private int debugMsgHist = 512;

    /** Received messages. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private ConcurrentLinkedDeque<String> debugLog;

    /** {@inheritDoc} */
    @IgniteInstanceResource
    @Override public void injectResources(Ignite ignite) {
        super.injectResources(ignite);

        // Inject resource.
        if (ignite != null)
            setAddressResolver(ignite.configuration().getAddressResolver());
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

    /** {@inheritDoc} */
    @Override public int getReconnectCount() {
        return reconCnt;
    }

    /**
     * Number of times node tries to (re)establish connection to another node.
     * <p>
     * Note that SPI implementation will increase {@link #ackTimeout} by factor 2
     * on every retry.
     * <p>
     * If not specified, default is {@link #DFLT_RECONNECT_CNT}.
     *
     * @param reconCnt Number of retries during message sending.
     * @see #setAckTimeout(long)
     */
    @IgniteSpiConfiguration(optional = true)
    public void setReconnectCount(int reconCnt) {
        this.reconCnt = reconCnt;
    }

    /** {@inheritDoc} */
    @Override public long getMaxAckTimeout() {
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
     *
     * @param maxAckTimeout Maximum acknowledgement timeout.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMaxAckTimeout(long maxAckTimeout) {
        this.maxAckTimeout = maxAckTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getJoinTimeout() {
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
     */
    @IgniteSpiConfiguration(optional = true)
    public void setJoinTimeout(long joinTimeout) {
        this.joinTimeout = joinTimeout;
    }

    /** {@inheritDoc} */
    @Override public int getLocalPort() {
        TcpDiscoveryNode locNode0 = locNode;

        return locNode0 != null ? locNode0.discoveryPort() : 0;
    }

    /**
     * Sets local port to listen to.
     * <p>
     * If not specified, default is {@link #DFLT_PORT}.
     *
     * @param locPort Local port to bind.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setLocalPort(int locPort) {
        this.locPort = locPort;
    }

    /** {@inheritDoc} */
    @Override public int getLocalPortRange() {
        return locPortRange;
    }

    /**
     * Range for local ports. Local node will try to bind on first available port
     * starting from {@link #getLocalPort()} up until
     * <tt>{@link #getLocalPort()} {@code + locPortRange}</tt>.
     * <p>
     * If not specified, default is {@link #DFLT_PORT_RANGE}.
     *
     * @param locPortRange Local port range to bind.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setLocalPortRange(int locPortRange) {
        this.locPortRange = locPortRange;
    }

    /** {@inheritDoc} */
    @Override public int getMaxMissedHeartbeats() {
        return maxMissedHbs;
    }

    /**
     * Sets max heartbeats count node can miss without initiating status check.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_MISSED_HEARTBEATS}.
     *
     * @param maxMissedHbs Max missed heartbeats.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMaxMissedHeartbeats(int maxMissedHbs) {
        this.maxMissedHbs = maxMissedHbs;
    }

    /** {@inheritDoc} */
    @Override public int getMaxMissedClientHeartbeats() {
        return maxMissedClientHbs;
    }

    /**
     * Sets max heartbeats count node can miss without failing client node.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_MISSED_CLIENT_HEARTBEATS}.
     *
     * @param maxMissedClientHbs Max missed client heartbeats.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMaxMissedClientHeartbeats(int maxMissedClientHbs) {
        this.maxMissedClientHbs = maxMissedClientHbs;
    }

    /** {@inheritDoc} */
    @Override public long getStatisticsPrintFrequency() {
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
     */
    @IgniteSpiConfiguration(optional = true)
    public void setStatisticsPrintFrequency(long statsPrintFreq) {
        this.statsPrintFreq = statsPrintFreq;
    }

    /** {@inheritDoc} */
    @Override public long getIpFinderCleanFrequency() {
        return ipFinderCleanFreq;
    }

    /**
     * Sets IP finder clean frequency in milliseconds.
     * <p>
     * If not provided, default value is {@link #DFLT_IP_FINDER_CLEAN_FREQ}
     *
     * @param ipFinderCleanFreq IP finder clean frequency.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setIpFinderCleanFrequency(long ipFinderCleanFreq) {
        this.ipFinderCleanFreq = ipFinderCleanFreq;
    }

    /**
     * This method is intended for troubleshooting purposes only.
     *
     * @param debugMode {code True} to start SPI in debug mode.
     */
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    /**
     * This method is intended for troubleshooting purposes only.
     *
     * @param debugMsgHist Message history log size.
     */
    public void setDebugMessageHistory(int debugMsgHist) {
        this.debugMsgHist = debugMsgHist;
    }

    /** {@inheritDoc} */
    @Override public String getSpiState() {
        synchronized (mux) {
            return spiState.name();
        }
    }

    /** {@inheritDoc} */
    @Override public long getSocketTimeout() {
        return sockTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getAckTimeout() {
        return ackTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getNetworkTimeout() {
        return netTimeout;
    }

    /** {@inheritDoc} */
    @Override public int getThreadPriority() {
        return threadPri;
    }

    /** {@inheritDoc} */
    @Override public long getHeartbeatFrequency() {
        return hbFreq;
    }

    /** {@inheritDoc} */
    @Override public String getIpFinderFormatted() {
        return ipFinder.toString();
    }

    /** {@inheritDoc} */
    @Override public int getMessageWorkerQueueSize() {
        return msgWorker.queueSize();
    }

    /** {@inheritDoc} */
    @Override public long getNodesJoined() {
        return stats.joinedNodesCount();
    }

    /** {@inheritDoc} */
    @Override public long getNodesLeft() {
        return stats.leftNodesCount();
    }

    /** {@inheritDoc} */
    @Override public long getNodesFailed() {
        return stats.failedNodesCount();
    }

    /** {@inheritDoc} */
    @Override public long getPendingMessagesRegistered() {
        return stats.pendingMessagesRegistered();
    }

    /** {@inheritDoc} */
    @Override public long getPendingMessagesDiscarded() {
        return stats.pendingMessagesDiscarded();
    }

    /** {@inheritDoc} */
    @Override public long getAvgMessageProcessingTime() {
        return stats.avgMessageProcessingTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaxMessageProcessingTime() {
        return stats.maxMessageProcessingTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalReceivedMessages() {
        return stats.totalReceivedMessages();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Integer> getReceivedMessages() {
        return stats.receivedMessages();
    }

    /** {@inheritDoc} */
    @Override public int getTotalProcessedMessages() {
        return stats.totalProcessedMessages();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Integer> getProcessedMessages() {
        return stats.processedMessages();
    }

    /** {@inheritDoc} */
    @Override public long getCoordinatorSinceTimestamp() {
        return stats.coordinatorSinceTimestamp();
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID getCoordinator() {
        TcpDiscoveryNode crd = resolveCoordinator();

        return crd != null ? crd.id() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode getNode(UUID nodeId) {
        assert nodeId != null;

        UUID locNodeId0 = ignite.configuration().getNodeId();

        if (locNodeId0 != null && locNodeId0.equals(nodeId))
            // Return local node directly.
            return locNode;

        TcpDiscoveryNode node = ring.node(nodeId);

        if (node != null && !node.visible())
            return null;

        return node;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return F.upcast(ring.visibleRemoteNodes());
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> injectables() {
        return F.<Object>asList(ipFinder);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        spiStart0(false);
    }

    /**
     * Starts or restarts SPI after stop (to reconnect).
     *
     * @param restart {@code True} if SPI is restarted after stop.
     * @throws IgniteSpiException If failed.
     */
    private void spiStart0(boolean restart) throws IgniteSpiException {
        if (!restart)
            // It is initial start.
            onSpiStart();

        synchronized (mux) {
            spiState = DISCONNECTED;
        }

        if (debugMode) {
            if (!log.isInfoEnabled())
                throw new IgniteSpiException("Info log level should be enabled for TCP discovery to work " +
                    "in debug mode.");

            debugLog = new ConcurrentLinkedDeque<>();

            U.quietAndWarn(log, "TCP discovery SPI is configured in debug mode.");
        }

        // Clear addresses collections.
        fromAddrs.clear();
        noResAddrs.clear();

        sockTimeoutWorker = new SocketTimeoutWorker();
        sockTimeoutWorker.start();

        msgWorker = new RingMessageWorker();
        msgWorker.start();

        tcpSrvr = new TcpServer();

        // Init local node.
        IgniteBiTuple<Collection<String>, Collection<String>> addrs;

        try {
            addrs = U.resolveLocalAddresses(locHost);
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to resolve local host to set of external addresses: " + locHost, e);
        }

        locNode = new TcpDiscoveryNode(
            ignite.configuration().getNodeId(),
            addrs.get1(),
            addrs.get2(),
            tcpSrvr.port,
            metricsProvider,
            locNodeVer);

        Collection<InetSocketAddress> extAddrs = addrRslvr == null ? null :
            U.resolveAddresses(addrRslvr, F.flat(Arrays.asList(addrs.get1(), addrs.get2())),
                locNode.discoveryPort());

        if (extAddrs != null)
            locNodeAttrs.put(createSpiAttributeName(ATTR_EXT_ADDRS), extAddrs);

        locNode.setAttributes(locNodeAttrs);

        locNode.local(true);

        locNodeAddrs = getNodeAddresses(locNode);

        if (log.isDebugEnabled())
            log.debug("Local node initialized: " + locNode);

        // Start TCP server thread after local node is initialized.
        tcpSrvr.start();

        ring.localNode(locNode);

        if (ipFinder.isShared())
            registerLocalNodeAddress();
        else {
            if (F.isEmpty(ipFinder.getRegisteredAddresses()))
                throw new IgniteSpiException("Non-shared IP finder must have IP addresses specified in " +
                    "GridTcpDiscoveryIpFinder.getRegisteredAddresses() configuration property " +
                    "(specify list of IP addresses in configuration).");

            ipFinderHasLocAddr = ipFinderHasLocalAddress();
        }

        if (statsPrintFreq > 0 && log.isInfoEnabled()) {
            statsPrinter = new StatisticsPrinter();
            statsPrinter.start();
        }

        stats.onJoinStarted();

        joinTopology();

        stats.onJoinFinished();

        hbsSnd = new HeartbeatsSender();
        hbsSnd.start();

        chkStatusSnd = new CheckStatusSender();
        chkStatusSnd.start();

        if (ipFinder.isShared()) {
            ipFinderCleaner = new IpFinderCleaner();
            ipFinderCleaner.start();
        }

        if (log.isDebugEnabled() && !restart)
            log.debug(startInfo());

        if (restart)
            getSpiContext().registerPort(tcpSrvr.port, TCP);
    }

    /**
     * @throws IgniteSpiException If failed.
     */
    @SuppressWarnings("BusyWait")
    private void registerLocalNodeAddress() throws IgniteSpiException {
        // Make sure address registration succeeded.
        while (true) {
            try {
                ipFinder.initializeLocalAddresses(locNode.socketAddresses());

                // Success.
                break;
            }
            catch (IllegalStateException e) {
                throw new IgniteSpiException("Failed to register local node address with IP finder: " +
                    locNode.socketAddresses(), e);
            }
            catch (IgniteSpiException e) {
                LT.error(log, e, "Failed to register local node address in IP finder on start " +
                    "(retrying every 2000 ms).");
            }

            try {
                U.sleep(2000);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }
        }
    }

    /**
     * @throws IgniteSpiException If failed.
     */
    private void onSpiStart() throws IgniteSpiException {
        startStopwatch();

        assertParameter(ipFinder != null, "ipFinder != null");
        assertParameter(ipFinderCleanFreq > 0, "ipFinderCleanFreq > 0");
        assertParameter(locPort > 1023, "localPort > 1023");
        assertParameter(locPortRange >= 0, "localPortRange >= 0");
        assertParameter(locPort + locPortRange <= 0xffff, "locPort + locPortRange <= 0xffff");
        assertParameter(netTimeout > 0, "networkTimeout > 0");
        assertParameter(sockTimeout > 0, "sockTimeout > 0");
        assertParameter(ackTimeout > 0, "ackTimeout > 0");
        assertParameter(maxAckTimeout > ackTimeout, "maxAckTimeout > ackTimeout");
        assertParameter(reconCnt > 0, "reconnectCnt > 0");
        assertParameter(hbFreq > 0, "heartbeatFreq > 0");
        assertParameter(maxMissedHbs > 0, "maxMissedHeartbeats > 0");
        assertParameter(maxMissedClientHbs > 0, "maxMissedClientHeartbeats > 0");
        assertParameter(threadPri > 0, "threadPri > 0");
        assertParameter(statsPrintFreq >= 0, "statsPrintFreq >= 0");

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
            log.debug(configInfo("networkTimeout", netTimeout));
            log.debug(configInfo("sockTimeout", sockTimeout));
            log.debug(configInfo("ackTimeout", ackTimeout));
            log.debug(configInfo("maxAckTimeout", maxAckTimeout));
            log.debug(configInfo("reconnectCount", reconCnt));
            log.debug(configInfo("ipFinder", ipFinder));
            log.debug(configInfo("ipFinderCleanFreq", ipFinderCleanFreq));
            log.debug(configInfo("heartbeatFreq", hbFreq));
            log.debug(configInfo("maxMissedHeartbeats", maxMissedHbs));
            log.debug(configInfo("statsPrintFreq", statsPrintFreq));
        }

        // Warn on odd network timeout.
        if (netTimeout < 3000)
            U.warn(log, "Network timeout is too low (at least 3000 ms recommended): " + netTimeout);

        // Warn on odd heartbeat frequency.
        if (hbFreq < 2000)
            U.warn(log, "Heartbeat frequency is too high (at least 2000 ms recommended): " + hbFreq);

        registerMBean(ignite.name(), this, TcpDiscoverySpiMBean.class);

        if (ipFinder instanceof TcpDiscoveryMulticastIpFinder) {
            TcpDiscoveryMulticastIpFinder mcastIpFinder = ((TcpDiscoveryMulticastIpFinder)ipFinder);

            if (mcastIpFinder.getLocalAddress() == null)
                mcastIpFinder.setLocalAddress(locAddr);
        }
    }

    /** {@inheritDoc} */
    @Override public void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        super.onContextInitialized0(spiCtx);

        ctxInitLatch.countDown();

        spiCtx.registerPort(tcpSrvr.port, TCP);
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
    @Override public void spiStop() throws IgniteSpiException {
        spiStop0(false);
    }

    /**
     * Stops SPI finally or stops SPI for restart.
     *
     * @param disconnect {@code True} if SPI is being disconnected.
     * @throws IgniteSpiException If failed.
     */
    private void spiStop0(boolean disconnect) throws IgniteSpiException {
        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        if (log.isDebugEnabled()) {
            if (disconnect)
                log.debug("Disconnecting SPI.");
            else
                log.debug("Preparing to start local node stop procedure.");
        }

        if (disconnect) {
            synchronized (mux) {
                spiState = DISCONNECTING;
            }
        }

        if (msgWorker != null && msgWorker.isAlive() && !disconnect) {
            // Send node left message only if it is final stop.
            msgWorker.addMessage(new TcpDiscoveryNodeLeftMessage(ignite.configuration().getNodeId()));

            synchronized (mux) {
                long threshold = U.currentTimeMillis() + netTimeout;

                long timeout = netTimeout;

                while (spiState != LEFT && timeout > 0) {
                    try {
                        mux.wait(timeout);

                        timeout = threshold - U.currentTimeMillis();
                    }
                    catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();

                        break;
                    }
                }

                if (spiState == LEFT) {
                    if (log.isDebugEnabled())
                        log.debug("Verification for local node leave has been received from coordinator" +
                            " (continuing stop procedure).");
                }
                else if (log.isInfoEnabled()) {
                    log.info("No verification for local node leave has been received from coordinator" +
                        " (will stop node anyway).");
                }
            }
        }

        U.interrupt(tcpSrvr);
        U.join(tcpSrvr, log);

        Collection<SocketReader> tmp;

        synchronized (mux) {
            tmp = U.arrayList(readers);
        }

        U.interrupt(tmp);
        U.joinThreads(tmp, log);

        U.interrupt(hbsSnd);
        U.join(hbsSnd, log);

        U.interrupt(chkStatusSnd);
        U.join(chkStatusSnd, log);

        U.interrupt(ipFinderCleaner);
        U.join(ipFinderCleaner, log);

        U.interrupt(msgWorker);
        U.join(msgWorker, log);

        U.interrupt(sockTimeoutWorker);
        U.join(sockTimeoutWorker, log);

        U.interrupt(statsPrinter);
        U.join(statsPrinter, log);

        if (ipFinder != null)
            ipFinder.close();

        Collection<TcpDiscoveryNode> rmts = null;

        if (!disconnect) {
            // This is final stop.
            unregisterMBean();

            if (log.isDebugEnabled())
                log.debug(stopInfo());
        }
        else {
            getSpiContext().deregisterPorts();

            rmts = ring.visibleRemoteNodes();
        }

        long topVer = ring.topologyVersion();

        ring.clear();

        if (rmts != null && !rmts.isEmpty()) {
            // This is restart/disconnection and remote nodes are not empty.
            // We need to fire FAIL event for each.
            DiscoverySpiListener lsnr = this.lsnr;

            if (lsnr != null) {
                Set<ClusterNode> processed = new HashSet<>();

                for (TcpDiscoveryNode n : rmts) {
                    assert n.visible();

                    processed.add(n);

                    List<ClusterNode> top = U.arrayList(rmts, F.notIn(processed));

                    topVer++;

                    Map<Long, Collection<ClusterNode>> hist = updateTopologyHistory(topVer,
                        Collections.unmodifiableList(top));

                    lsnr.onDiscovery(EVT_NODE_FAILED, topVer, n, top, hist, null);
                }
            }
        }

        printStatistics();

        stats.clear();

        synchronized (mux) {
            // Clear stored data.
            leavingNodes.clear();
            failedNodes.clear();

            spiState = DISCONNECTED;
        }
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        super.onContextDestroyed0();

        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        getSpiContext().deregisterPorts();
    }

    /**
     * @throws IgniteSpiException If any error occurs.
     * @return {@code true} if IP finder contains local address.
     */
    private boolean ipFinderHasLocalAddress() throws IgniteSpiException {
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
                    onException(e.getMessage(), e);
                }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        assert nodeId != null;

        if (nodeId == ignite.configuration().getNodeId())
            return true;

        TcpDiscoveryNode node = ring.node(nodeId);

        if (node == null || !node.visible())
            return false;

        boolean res = pingNode(node);

        if (!res && !node.isClient()) {
            LT.warn(log, null, "Failed to ping node (status check will be initiated): " + nodeId);

            msgWorker.addMessage(new TcpDiscoveryStatusCheckMessage(locNode, node.id()));
        }

        return res;
    }

    /**
     * Pings the remote node to see if it's alive.
     *
     * @param node Node.
     * @return {@code True} if ping succeeds.
     */
    private boolean pingNode(TcpDiscoveryNode node) {
        assert node != null;

        if (node.id().equals(ignite.configuration().getNodeId()))
            return true;

        UUID clientNodeId = null;

        if (node.isClient()) {
            clientNodeId = node.id();

            node = ring.node(node.clientRouterNodeId());

            if (node == null || !node.visible())
                return false;
        }

        for (InetSocketAddress addr : getNodeAddresses(node, U.sameMacs(locNode, node))) {
            try {
                // ID returned by the node should be the same as ID of the parameter for ping to succeed.
                IgniteBiTuple<UUID, Boolean> t = pingNode(addr, clientNodeId);

                return node.id().equals(t.get1()) && (clientNodeId == null || t.get2());
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to ping node [node=" + node + ", err=" + e.getMessage() + ']');

                onException("Failed to ping node [node=" + node + ", err=" + e.getMessage() + ']', e);
                // continue;
            }
        }

        return false;
    }

    /**
     * Pings the remote node by its address to see if it's alive.
     *
     * @param addr Address of the node.
     * @return ID of the remote node if node alive.
     * @throws IgniteSpiException If an error occurs.
     */
    private IgniteBiTuple<UUID, Boolean> pingNode(InetSocketAddress addr, @Nullable UUID clientNodeId)
        throws IgniteCheckedException {
        assert addr != null;

        UUID locNodeId = ignite.configuration().getNodeId();

        if (F.contains(locNodeAddrs, addr))
            return F.t(ignite.configuration().getNodeId(), false);

        GridFutureAdapter<IgniteBiTuple<UUID, Boolean>> fut = new GridFutureAdapter<>();

        IgniteInternalFuture<IgniteBiTuple<UUID, Boolean>> oldFut = pingMap.putIfAbsent(addr, fut);

        if (oldFut != null)
            return oldFut.get();
        else {
            Collection<Throwable> errs = null;

            try {
                Socket sock = null;

                for (int i = 0; i < reconCnt; i++) {
                    try {
                        if (addr.isUnresolved())
                            addr = new InetSocketAddress(InetAddress.getByName(addr.getHostName()), addr.getPort());

                        long tstamp = U.currentTimeMillis();

                        sock = openSocket(addr);

                        writeToSocket(sock, new TcpDiscoveryPingRequest(locNodeId, clientNodeId));

                        TcpDiscoveryPingResponse res = readMessage(sock, null, netTimeout);

                        if (locNodeId.equals(res.creatorNodeId())) {
                            if (log.isDebugEnabled())
                                log.debug("Ping response from local node: " + res);

                            break;
                        }

                        stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                        IgniteBiTuple<UUID, Boolean> t = F.t(res.creatorNodeId(), res.clientExists());

                        fut.onDone(t);

                        return t;
                    }
                    catch (IOException | IgniteCheckedException e) {
                        if (errs == null)
                            errs = new ArrayList<>();

                        errs.add(e);
                    }
                    finally {
                        U.closeQuiet(sock);
                    }
                }
            }
            catch (Throwable t) {
                fut.onDone(t);

                throw U.cast(t);
            }
            finally {
                if (!fut.isDone())
                    fut.onDone(U.exceptionWithSuppressed("Failed to ping node by address: " + addr, errs));

                boolean b = pingMap.remove(addr, fut);

                assert b;
            }

            return fut.get();
        }
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        spiStop0(true);
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator nodeAuth) {
        this.nodeAuth = nodeAuth;
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(Serializable evt) {
        msgWorker.addMessage(new TcpDiscoveryCustomEventMessage(getLocalNodeId(), evt));
    }

    /**
     * Tries to join this node to topology.
     *
     * @throws IgniteSpiException If any error occurs.
     */
    private void joinTopology() throws IgniteSpiException {
        synchronized (mux) {
            assert spiState == CONNECTING || spiState == DISCONNECTED;

            spiState = CONNECTING;
        }

        GridSecurityCredentials locCred = (GridSecurityCredentials)locNode.getAttributes()
            .get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

        // Marshal credentials for backward compatibility and security.
        marshalCredentials(locNode);

        while (true) {
            if (!sendJoinRequestMessage()) {
                if (log.isDebugEnabled())
                    log.debug("Join request message has not been sent (local node is the first in the topology).");

                if (nodeAuth != null) {
                    // Authenticate local node.
                    try {
                        SecurityContext subj = nodeAuth.authenticateNode(locNode, locCred);

                        if (subj == null)
                            throw new IgniteSpiException("Authentication failed for local node: " + locNode.id());

                        Map<String, Object> attrs = new HashMap<>(locNode.attributes());

                        attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT,
                            ignite.configuration().getMarshaller().marshal(subj));
                        attrs.remove(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

                        locNode.setAttributes(attrs);
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        throw new IgniteSpiException("Failed to authenticate local node (will shutdown local node).", e);
                    }
                }

                locNode.order(1);
                locNode.internalOrder(1);

                gridStartTime = U.currentTimeMillis();

                locNode.visible(true);

                ring.clear();

                ring.topologyVersion(1);

                synchronized (mux) {
                    topHist.clear();

                    spiState = CONNECTED;

                    mux.notifyAll();
                }

                notifyDiscovery(EVT_NODE_JOINED, 1, locNode);

                break;
            }

            if (log.isDebugEnabled())
                log.debug("Join request message has been sent (waiting for coordinator response).");

            synchronized (mux) {
                long threshold = U.currentTimeMillis() + netTimeout;

                long timeout = netTimeout;

                while (spiState == CONNECTING && timeout > 0) {
                    try {
                        mux.wait(timeout);

                        timeout = threshold - U.currentTimeMillis();
                    }
                    catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();

                        throw new IgniteSpiException("Thread has been interrupted.");
                    }
                }

                if (spiState == CONNECTED)
                    break;
                else if (spiState == DUPLICATE_ID)
                    throw duplicateIdError((TcpDiscoveryDuplicateIdMessage)joinRes.get());
                else if (spiState == AUTH_FAILED)
                    throw authenticationFailedError((TcpDiscoveryAuthFailedMessage)joinRes.get());
                else if (spiState == CHECK_FAILED)
                    throw checkFailedError((TcpDiscoveryCheckFailedMessage)joinRes.get());
                else if (spiState == LOOPBACK_PROBLEM) {
                    TcpDiscoveryLoopbackProblemMessage msg = (TcpDiscoveryLoopbackProblemMessage)joinRes.get();

                    boolean locHostLoopback = locHost.isLoopbackAddress();

                    String firstNode = locHostLoopback ? "local" : "remote";

                    String secondNode = locHostLoopback ? "remote" : "local";

                    throw new IgniteSpiException("Failed to add node to topology because " + firstNode +
                        " node is configured to use loopback address, but " + secondNode + " node is not " +
                        "(consider changing 'localAddress' configuration parameter) " +
                        "[locNodeAddrs=" + U.addressesAsString(locNode) + ", rmtNodeAddrs=" +
                        U.addressesAsString(msg.addresses(), msg.hostNames()) + ']');
                }
                else
                    LT.warn(log, null, "Node has not been connected to topology and will repeat join process. " +
                        "Check remote nodes logs for possible error messages. " +
                        "Note that large topology may require significant time to start. " +
                        "Increase 'GridTcpDiscoverySpi.networkTimeout' configuration property " +
                        "if getting this message on the starting nodes [networkTimeout=" + netTimeout + ']');
            }
        }

        assert locNode.order() != 0;
        assert locNode.internalOrder() != 0;

        if (log.isDebugEnabled())
            log.debug("Discovery SPI has been connected to topology with order: " + locNode.internalOrder());
    }

    /**
     * Tries to send join request message to a random node presenting in topology.
     * Address is provided by {@link TcpDiscoveryIpFinder} and message is
     * sent to first node connection succeeded to.
     *
     * @return {@code true} if send succeeded.
     * @throws IgniteSpiException If any error occurs.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean sendJoinRequestMessage() throws IgniteSpiException {
        TcpDiscoveryAbstractMessage joinReq = new TcpDiscoveryJoinRequestMessage(locNode,
            exchange.collect(ignite.configuration().getNodeId()));

        // Time when it has been detected, that addresses from IP finder do not respond.
        long noResStart = 0;

        while (true) {
            Collection<InetSocketAddress> addrs = resolvedAddresses();

            if (F.isEmpty(addrs))
                return false;

            boolean retry = false;
            Collection<Exception> errs = new ArrayList<>();

            try (SocketMultiConnector multiConnector = new SocketMultiConnector(addrs, 2)) {
                GridTuple3<InetSocketAddress, Socket, Exception> tuple;

                while ((tuple = multiConnector.next()) != null) {
                    InetSocketAddress addr = tuple.get1();
                    Socket sock = tuple.get2();
                    Exception ex = tuple.get3();

                    if (ex == null) {
                        assert sock != null;

                        try {
                            Integer res = sendMessageDirectly(joinReq, addr, sock);

                            assert res != null;

                            noResAddrs.remove(addr);

                            // Address is responsive, reset period start.
                            noResStart = 0;

                            switch (res) {
                                case RES_WAIT:
                                    // Concurrent startup, try sending join request again or wait if no success.
                                    retry = true;

                                    break;
                                case RES_OK:
                                    if (log.isDebugEnabled())
                                        log.debug("Join request message has been sent to address [addr=" + addr +
                                            ", req=" + joinReq + ']');

                                    // Join request sending succeeded, wait for response from topology.
                                    return true;

                                default:
                                    // Concurrent startup, try next node.
                                    if (res == RES_CONTINUE_JOIN) {
                                        if (!fromAddrs.contains(addr))
                                            retry = true;
                                    }
                                    else {
                                        if (log.isDebugEnabled())
                                            log.debug("Unexpected response to join request: " + res);

                                        retry = true;
                                    }

                                    break;
                            }
                        }
                        catch (IgniteSpiException e) {
                            e.printStackTrace();

                            ex = e;
                        }
                    }

                    if (ex != null) {
                        errs.add(ex);

                        if (log.isDebugEnabled()) {
                            IOException ioe = X.cause(ex, IOException.class);

                            log.debug("Failed to send join request message [addr=" + addr +
                                ", msg=" + ioe != null ? ioe.getMessage() : ex.getMessage() + ']');

                            onException("Failed to send join request message [addr=" + addr +
                                ", msg=" + ioe != null ? ioe.getMessage() : ex.getMessage() + ']', ioe);
                        }

                        noResAddrs.add(addr);
                    }
                }
            }

            if (retry) {
                if (log.isDebugEnabled())
                    log.debug("Concurrent discovery SPI start has been detected (local node should wait).");

                try {
                    U.sleep(2000);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteSpiException("Thread has been interrupted.", e);
                }
            }
            else if (!ipFinder.isShared() && !ipFinderHasLocAddr) {
                IgniteCheckedException e = null;

                if (!errs.isEmpty()) {
                    e = new IgniteCheckedException("Multiple connection attempts failed.");

                    for (Exception err : errs)
                        e.addSuppressed(err);
                }

                if (e != null && X.hasCause(e, ConnectException.class))
                    LT.warn(log, null, "Failed to connect to any address from IP finder " +
                        "(make sure IP finder addresses are correct and firewalls are disabled on all host machines): " +
                        addrs);

                if (joinTimeout > 0) {
                    if (noResStart == 0)
                        noResStart = U.currentTimeMillis();
                    else if (U.currentTimeMillis() - noResStart > joinTimeout)
                        throw new IgniteSpiException(
                            "Failed to connect to any address from IP finder within join timeout " +
                                "(make sure IP finder addresses are correct, and operating system firewalls are disabled " +
                                "on all host machines, or consider increasing 'joinTimeout' configuration property): " +
                                addrs, e);
                }

                try {
                    U.sleep(2000);
                }
                catch (IgniteInterruptedCheckedException ex) {
                    throw new IgniteSpiException("Thread has been interrupted.", ex);
                }
            }
            else
                break;
        }

        return false;
    }

    /**
     * Establishes connection to an address, sends message and returns the response (if any).
     *
     * @param msg Message to send.
     * @param addr Address to send message to.
     * @return Response read from the recipient or {@code null} if no response is supposed.
     * @throws IgniteSpiException If an error occurs.
     */
    @Nullable private Integer sendMessageDirectly(TcpDiscoveryAbstractMessage msg, InetSocketAddress addr, Socket sock)
        throws IgniteSpiException {
        assert msg != null;
        assert addr != null;

        Collection<Throwable> errs = null;

        long ackTimeout0 = ackTimeout;

        int connectAttempts = 1;

        boolean joinReqSent = false;

        UUID locNodeId = ignite.configuration().getNodeId();

        for (int i = 0; i < reconCnt; i++) {
            // Need to set to false on each new iteration,
            // since remote node may leave in the middle of the first iteration.
            joinReqSent = false;

            boolean openSock = false;

            try {
                long tstamp = U.currentTimeMillis();

                if (sock == null)
                    sock = openSocket(addr);

                openSock = true;

                // Handshake.
                writeToSocket(sock, new TcpDiscoveryHandshakeRequest(locNodeId));

                TcpDiscoveryHandshakeResponse res = readMessage(sock, null, ackTimeout0);

                if (locNodeId.equals(res.creatorNodeId())) {
                    if (log.isDebugEnabled())
                        log.debug("Handshake response from local node: " + res);

                    break;
                }

                stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                // Send message.
                tstamp = U.currentTimeMillis();

                writeToSocket(sock, msg);

                stats.onMessageSent(msg, U.currentTimeMillis() - tstamp);

                if (debugMode)
                    debugLog("Message has been sent directly to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + res.creatorNodeId() + ']');

                if (log.isDebugEnabled())
                    log.debug("Message has been sent directly to address [msg=" + msg + ", addr=" + addr +
                        ", rmtNodeId=" + res.creatorNodeId() + ']');

                // Connection has been established, but
                // join request may not be unmarshalled on remote host.
                // E.g. due to class not found issue.
                joinReqSent = msg instanceof TcpDiscoveryJoinRequestMessage;

                return readReceipt(sock, ackTimeout0);
            }
            catch (ClassCastException e) {
                // This issue is rarely reproducible on AmazonEC2, but never
                // on dedicated machines.
                if (log.isDebugEnabled())
                    U.error(log, "Class cast exception on direct send: " + addr, e);

                onException("Class cast exception on direct send: " + addr, e);

                if (errs == null)
                    errs = new ArrayList<>();

                errs.add(e);
            }
            catch (IOException | IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.error("Exception on direct send: " + e.getMessage(), e);

                onException("Exception on direct send: " + e.getMessage(), e);

                if (errs == null)
                    errs = new ArrayList<>();

                errs.add(e);

                if (!openSock) {
                    // Reconnect for the second time, if connection is not established.
                    if (connectAttempts < 2) {
                        connectAttempts++;

                        continue;
                    }

                    break; // Don't retry if we can not establish connection.
                }

                if (e instanceof SocketTimeoutException || X.hasCause(e, SocketTimeoutException.class)) {
                    ackTimeout0 *= 2;

                    if (!checkAckTimeout(ackTimeout0))
                        break;
                }
            }
            finally {
                U.closeQuiet(sock);

                sock = null;
            }
        }

        if (joinReqSent) {
            if (log.isDebugEnabled())
                log.debug("Join request has been sent, but receipt has not been read (returning RES_WAIT).");

            // Topology will not include this node,
            // however, warning on timed out join will be output.
            return RES_OK;
        }

        throw new IgniteSpiException(
            "Failed to send message to address [addr=" + addr + ", msg=" + msg + ']',
            U.exceptionWithSuppressed("Failed to send message to address " +
                "[addr=" + addr + ", msg=" + msg + ']', errs));
    }

    /**
     * Marshalls credentials with discovery SPI marshaller (will replace attribute value).
     *
     * @param node Node to marshall credentials for.
     * @throws IgniteSpiException If marshalling failed.
     */
    private void marshalCredentials(TcpDiscoveryNode node) throws IgniteSpiException {
        try {
            // Use security-unsafe getter.
            Map<String, Object> attrs = new HashMap<>(node.getAttributes());

            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS,
                marsh.marshal(attrs.get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS)));

            node.setAttributes(attrs);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal node security credentials: " + node.id(), e);
        }
    }

    /**
     * Unmarshalls credentials with discovery SPI marshaller (will not replace attribute value).
     *
     * @param node Node to unmarshall credentials for.
     * @return Security credentials.
     * @throws IgniteSpiException If unmarshal fails.
     */
    private GridSecurityCredentials unmarshalCredentials(TcpDiscoveryNode node) throws IgniteSpiException {
        try {
            byte[] credBytes = (byte[])node.getAttributes().get(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);

            if (credBytes == null)
                return null;

            return marsh.unmarshal(credBytes, null);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to unmarshal node security credentials: " + node.id(), e);
        }
    }

    /**
     * @param ackTimeout Acknowledgement timeout.
     * @return {@code True} if acknowledgement timeout is less or equal to
     * maximum acknowledgement timeout, {@code false} otherwise.
     */
    private boolean checkAckTimeout(long ackTimeout) {
        if (ackTimeout > maxAckTimeout) {
            LT.warn(log, null, "Acknowledgement timeout is greater than maximum acknowledgement timeout " +
                "(consider increasing 'maxAckTimeout' configuration property) " +
                "[ackTimeout=" + ackTimeout + ", maxAckTimeout=" + maxAckTimeout + ']');

            return false;
        }

        return true;
    }

    /**
     * Notify external listener on discovery event.
     *
     * @param type Discovery event type. See {@link DiscoveryEvent} for more details.
     * @param topVer Topology version.
     * @param node Remote node this event is connected with.
     */
    private void notifyDiscovery(int type, long topVer, TcpDiscoveryNode node) {
        assert type > 0;
        assert node != null;

        DiscoverySpiListener lsnr = this.lsnr;

        TcpDiscoverySpiState spiState = spiStateCopy();

        if (lsnr != null && node.visible() && (spiState == CONNECTED || spiState == DISCONNECTING)) {
            if (log.isDebugEnabled())
                log.debug("Discovery notification [node=" + node + ", spiState=" + spiState +
                    ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');

            Collection<ClusterNode> top = F.<TcpDiscoveryNode, ClusterNode>upcast(ring.visibleNodes());

            Map<Long, Collection<ClusterNode>> hist = updateTopologyHistory(topVer, top);

            lsnr.onDiscovery(type, topVer, node, top, hist, null);
        }
        else if (log.isDebugEnabled())
            log.debug("Skipped discovery notification [node=" + node + ", spiState=" + spiState +
                ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');
    }

    /**
     * Update topology history with new topology snapshots.
     *
     * @param topVer Topology version.
     * @param top Topology snapshot.
     * @return Copy of updated topology history.
     */
    @Nullable private Map<Long, Collection<ClusterNode>> updateTopologyHistory(long topVer, Collection<ClusterNode> top) {
        synchronized (mux) {
            if (topHist.containsKey(topVer))
                return null;

            topHist.put(topVer, top);

            while (topHist.size() > topHistSize)
                topHist.remove(topHist.firstKey());

            if (log.isDebugEnabled())
                log.debug("Added topology snapshot to history, topVer=" + topVer + ", historySize=" + topHist.size());

            return new TreeMap<>(topHist);
        }
    }

    /**
     * @param msg Error message.
     * @param e Exception.
     */
    private void onException(String msg, Exception e){
        getExceptionRegistry().onException(msg, e);
    }

    /**
     * @param node Node.
     * @return {@link LinkedHashSet} of internal and external addresses of provided node.
     *      Internal addresses placed before external addresses.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private LinkedHashSet<InetSocketAddress> getNodeAddresses(TcpDiscoveryNode node) {
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
    private LinkedHashSet<InetSocketAddress> getNodeAddresses(TcpDiscoveryNode node, boolean sameHost) {
        List<InetSocketAddress> addrs = U.arrayList(node.socketAddresses());

        Collections.sort(addrs, U.inetAddressesComparator(sameHost));

        LinkedHashSet<InetSocketAddress> res = new LinkedHashSet<>(addrs);

        Collection<InetSocketAddress> extAddrs = node.attribute(createSpiAttributeName(ATTR_EXT_ADDRS));

        if (extAddrs != null)
            res.addAll(extAddrs);

        return res;
    }

    /**
     * Checks whether local node is coordinator. Nodes that are leaving or failed
     * (but are still in topology) are removed from search.
     *
     * @return {@code true} if local node is coordinator.
     */
    private boolean isLocalNodeCoordinator() {
        synchronized (mux) {
            boolean crd = spiState == CONNECTED && locNode.equals(resolveCoordinator());

            if (crd)
                stats.onBecomingCoordinator();

            return crd;
        }
    }

    /**
     * @return Spi state copy.
     */
    private TcpDiscoverySpiState spiStateCopy() {
        TcpDiscoverySpiState state;

        synchronized (mux) {
            state = spiState;
        }

        return state;
    }

    /**
     * Resolves coordinator. Nodes that are leaving or failed (but are still in
     * topology) are removed from search.
     *
     * @return Coordinator node or {@code null} if there are no coordinator
     * (i.e. local node is the last one and is currently stopping).
     */
    @Nullable private TcpDiscoveryNode resolveCoordinator() {
        return resolveCoordinator(null);
    }

    /**
     * Resolves coordinator. Nodes that are leaving or failed (but are still in
     * topology) are removed from search as well as provided filter.
     *
     * @param filter Nodes to exclude when resolving coordinator (optional).
     * @return Coordinator node or {@code null} if there are no coordinator
     * (i.e. local node is the last one and is currently stopping).
     */
    @Nullable private TcpDiscoveryNode resolveCoordinator(
        @Nullable Collection<TcpDiscoveryNode> filter) {
        synchronized (mux) {
            Collection<TcpDiscoveryNode> excluded = F.concat(false, failedNodes, leavingNodes);

            if (!F.isEmpty(filter))
                excluded = F.concat(false, excluded, filter);

            return ring.coordinator(excluded);
        }
    }

    /**
     * Prints SPI statistics.
     */
    private void printStatistics() {
        if (log.isInfoEnabled() && statsPrintFreq > 0) {
            int failedNodesSize;
            int leavingNodesSize;

            synchronized (mux) {
                failedNodesSize = failedNodes.size();
                leavingNodesSize = leavingNodes.size();
            }

            Runtime runtime = Runtime.getRuntime();

            TcpDiscoveryNode coord = resolveCoordinator();

            log.info("Discovery SPI statistics [statistics=" + stats + ", spiState=" + spiStateCopy() +
                ", coord=" + coord +
                ", topSize=" + ring.allNodes().size() +
                ", leavingNodesSize=" + leavingNodesSize + ", failedNodesSize=" + failedNodesSize +
                ", msgWorker.queue.size=" + (msgWorker != null ? msgWorker.queueSize() : "N/A") +
                ", lastUpdate=" + (locNode != null ? U.format(locNode.lastUpdateTime()) : "N/A") +
                ", heapFree=" + runtime.freeMemory() / (1024 * 1024) +
                "M, heapTotal=" + runtime.maxMemory() / (1024 * 1024) + "M]");
        }
    }

    /**
     * @param msg Message to prepare.
     * @param destNodeId Destination node ID.
     * @param msgs Messages to include.
     * @param discardMsgId Discarded message ID.
     */
    private void prepareNodeAddedMessage(TcpDiscoveryAbstractMessage msg, UUID destNodeId,
        @Nullable Collection<TcpDiscoveryAbstractMessage> msgs, @Nullable IgniteUuid discardMsgId) {
        assert destNodeId != null;

        if (msg instanceof TcpDiscoveryNodeAddedMessage) {
            TcpDiscoveryNodeAddedMessage nodeAddedMsg = (TcpDiscoveryNodeAddedMessage)msg;

            TcpDiscoveryNode node = nodeAddedMsg.node();

            if (node.id().equals(destNodeId)) {
                Collection<TcpDiscoveryNode> allNodes = ring.allNodes();
                Collection<TcpDiscoveryNode> topToSend = new ArrayList<>(allNodes.size());

                for (TcpDiscoveryNode n0 : allNodes) {
                    assert n0.internalOrder() != 0 : n0;

                    // Skip next node and nodes added after next
                    // in case this message is resent due to failures/leaves.
                    // There will be separate messages for nodes with greater
                    // internal order.
                    if (n0.internalOrder() < nodeAddedMsg.node().internalOrder())
                        topToSend.add(n0);
                }

                nodeAddedMsg.topology(topToSend);
                nodeAddedMsg.messages(msgs, discardMsgId);

                Map<Long, Collection<ClusterNode>> hist;

                synchronized (mux) {
                    hist = new TreeMap<>(topHist);
                }

                nodeAddedMsg.topologyHistory(hist);
            }
        }
    }

    /**
     * @param msg Message to clear.
     */
    private void clearNodeAddedMessage(TcpDiscoveryAbstractMessage msg) {
        if (msg instanceof TcpDiscoveryNodeAddedMessage) {
            // Nullify topology before registration.
            TcpDiscoveryNodeAddedMessage nodeAddedMsg = (TcpDiscoveryNodeAddedMessage)msg;

            nodeAddedMsg.topology(null);
            nodeAddedMsg.topologyHistory(null);
            nodeAddedMsg.messages(null, null);
        }
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * Simulates this node failure by stopping service threads. So, node will become
     * unresponsive.
     * <p>
     * This method is intended for test purposes only.
     */
    void simulateNodeFailure() {
        U.warn(log, "Simulating node failure: " + ignite.configuration().getNodeId());

        U.interrupt(tcpSrvr);
        U.join(tcpSrvr, log);

        U.interrupt(hbsSnd);
        U.join(hbsSnd, log);

        U.interrupt(chkStatusSnd);
        U.join(chkStatusSnd, log);

        U.interrupt(ipFinderCleaner);
        U.join(ipFinderCleaner, log);

        Collection<SocketReader> tmp;

        synchronized (mux) {
            tmp = U.arrayList(readers);
        }

        U.interrupt(tmp);
        U.joinThreads(tmp, log);

        U.interrupt(msgWorker);
        U.join(msgWorker, log);

        U.interrupt(statsPrinter);
        U.join(statsPrinter, log);
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * Simulates situation when next node is still alive but is bypassed
     * since it has been excluded from the ring, possibly, due to short time
     * network problems.
     * <p>
     * This method is intended for test purposes only.
     */
    void forceNextNodeFailure() {
        U.warn(log, "Next node will be forcibly failed (if any).");

        TcpDiscoveryNode next;

        synchronized (mux) {
            next = ring.nextNode(failedNodes);
        }

        if (next != null)
            msgWorker.addMessage(new TcpDiscoveryNodeFailedMessage(ignite.configuration().getNodeId(), next.id(),
                next.internalOrder()));
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * This method is intended for test purposes only.
     *
     * @param msg Message.
     */
    void onBeforeMessageSentAcrossRing(Serializable msg) {
        // No-op.
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * This method is intended for test purposes only.
     *
     * @return Nodes ring.
     */
    TcpDiscoveryNodesRing ring() {
        return ring;
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo() {
        dumpDebugInfo(log);
    }

    /**
     * @param log Logger.
     */
    public void dumpDebugInfo(IgniteLogger log) {
        if (!debugMode) {
            U.quietAndWarn(log, "Failed to dump debug info (discovery SPI was not configured " +
                "in debug mode, consider setting 'debugMode' configuration property to 'true').");

            return;
        }

        assert log.isInfoEnabled();

        synchronized (mux) {
            StringBuilder b = new StringBuilder(U.nl());

            b.append(">>>").append(U.nl());
            b.append(">>>").append("Dumping discovery SPI debug info.").append(U.nl());
            b.append(">>>").append(U.nl());

            b.append("Local node ID: ").append(ignite.configuration().getNodeId()).append(U.nl()).append(U.nl());
            b.append("Local node: ").append(locNode).append(U.nl()).append(U.nl());
            b.append("SPI state: ").append(spiState).append(U.nl()).append(U.nl());

            b.append("Internal threads: ").append(U.nl());

            b.append("    Message worker: ").append(threadStatus(msgWorker)).append(U.nl());
            b.append("    Check status sender: ").append(threadStatus(chkStatusSnd)).append(U.nl());
            b.append("    HB sender: ").append(threadStatus(hbsSnd)).append(U.nl());
            b.append("    Socket timeout worker: ").append(threadStatus(sockTimeoutWorker)).append(U.nl());
            b.append("    IP finder cleaner: ").append(threadStatus(ipFinderCleaner)).append(U.nl());
            b.append("    Stats printer: ").append(threadStatus(statsPrinter)).append(U.nl());

            b.append(U.nl());

            b.append("Socket readers: ").append(U.nl());

            for (SocketReader rdr : readers)
                b.append("    ").append(rdr).append(U.nl());

            b.append(U.nl());

            b.append("In-memory log messages: ").append(U.nl());

            for (String msg : debugLog)
                b.append("    ").append(msg).append(U.nl());

            b.append(U.nl());

            b.append("Leaving nodes: ").append(U.nl());

            for (TcpDiscoveryNode node : leavingNodes)
                b.append("    ").append(node.id()).append(U.nl());

            b.append(U.nl());

            b.append("Failed nodes: ").append(U.nl());

            for (TcpDiscoveryNode node : failedNodes)
                b.append("    ").append(node.id()).append(U.nl());

            b.append(U.nl());

            b.append("Stats: ").append(stats).append(U.nl());

            U.quietAndInfo(log, b.toString());
        }
    }

    /**
     * @param msg Message.
     */
    private void debugLog(String msg) {
        assert debugMode;

        String msg0 = new SimpleDateFormat("[HH:mm:ss,SSS]").format(new Date(System.currentTimeMillis())) +
            '[' + Thread.currentThread().getName() + "][" + ignite.configuration().getNodeId() +
            "-" + locNode.internalOrder() + "] " +
            msg;

        debugLog.add(msg0);

        int delta = debugLog.size() - debugMsgHist;

        for (int i = 0; i < delta && debugLog.size() > debugMsgHist; i++)
            debugLog.poll();
    }

    /**
     * @param msg Message.
     * @return {@code True} if recordable in debug mode.
     */
    private boolean recordable(TcpDiscoveryAbstractMessage msg) {
        return !(msg instanceof TcpDiscoveryHeartbeatMessage) &&
            !(msg instanceof TcpDiscoveryStatusCheckMessage) &&
            !(msg instanceof TcpDiscoveryDiscardMessage);
    }

    /**
     * @param t Thread.
     * @return Status as string.
     */
    private String threadStatus(Thread t) {
        if (t == null)
            return "N/A";

        return t.isAlive() ? "alive" : "dead";
    }

    /**
     * Checks if two given {@link GridSecurityPermissionSet} objects contain the same permissions.
     * Each permission belongs to one of three groups : cache, task or system.
     *
     * @param locPerms The first set of permissions.
     * @param rmtPerms The second set of permissions.
     * @return {@code True} if given parameters contain the same permissions, {@code False} otherwise.
     */
    private boolean permissionsEqual(GridSecurityPermissionSet locPerms, GridSecurityPermissionSet rmtPerms) {
        boolean dfltAllowMatch = !(locPerms.defaultAllowAll() ^ rmtPerms.defaultAllowAll());

        boolean bothHaveSamePerms = F.eqNotOrdered(rmtPerms.systemPermissions(), locPerms.systemPermissions()) &&
            F.eqNotOrdered(rmtPerms.cachePermissions(), locPerms.cachePermissions()) &&
            F.eqNotOrdered(rmtPerms.taskPermissions(), locPerms.taskPermissions());

        return dfltAllowMatch && bothHaveSamePerms;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoverySpi.class, this);
    }

    /**
     * Thread that sends heartbeats.
     */
    private class HeartbeatsSender extends IgniteSpiThread {
        /**
         * Constructor.
         */
        private HeartbeatsSender() {
            super(ignite.name(), "tcp-disco-hb-sender", log);

            setPriority(threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            while (!isLocalNodeCoordinator())
                Thread.sleep(1000);

            if (log.isDebugEnabled())
                log.debug("Heartbeats sender has been started.");

            while (!isInterrupted()) {
                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping heartbeats sender (SPI is not connected to topology).");

                    return;
                }

                TcpDiscoveryHeartbeatMessage msg = new TcpDiscoveryHeartbeatMessage(ignite.configuration().getNodeId());

                msg.verify(ignite.configuration().getNodeId());

                msgWorker.addMessage(msg);

                Thread.sleep(hbFreq);
            }
        }
    }

    /**
     * Thread that sends status check messages to next node if local node has not
     * been receiving heartbeats ({@link TcpDiscoveryHeartbeatMessage})
     * for {@link TcpDiscoverySpi#getMaxMissedHeartbeats()} *
     * {@link TcpDiscoverySpi#getHeartbeatFrequency()}.
     */
    private class CheckStatusSender extends IgniteSpiThread {
        /**
         * Constructor.
         */
        private CheckStatusSender() {
            super(ignite.name(), "tcp-disco-status-check-sender", log);

            setPriority(threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Status check sender has been started.");

            // Only 1 heartbeat missing is acceptable. 1 sec is added to avoid false alarm.
            long checkTimeout = (long)maxMissedHbs * hbFreq + 1000;

            long lastSent = 0;

            while (!isInterrupted()) {
                // 1. Determine timeout.
                if (lastSent < locNode.lastUpdateTime())
                    lastSent = locNode.lastUpdateTime();

                long timeout = (lastSent + checkTimeout) - U.currentTimeMillis();

                if (timeout > 0)
                    Thread.sleep(timeout);

                // 2. Check if SPI is still connected.
                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping status check sender (SPI is not connected to topology).");

                    return;
                }

                // 3. Was there an update?
                if (locNode.lastUpdateTime() > lastSent || !ring.hasRemoteNodes()) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping status check send " +
                            "[locNodeLastUpdate=" + U.format(locNode.lastUpdateTime()) +
                            ", hasRmts=" + ring.hasRemoteNodes() + ']');

                    continue;
                }

                // 4. Send status check message.
                lastSent = U.currentTimeMillis();

                msgWorker.addMessage(new TcpDiscoveryStatusCheckMessage(locNode, null));
            }
        }
    }

    /**
     * Thread that cleans IP finder and keeps it in the correct state, unregistering
     * addresses of the nodes that has left the topology.
     * <p>
     * This thread should run only on coordinator node and will clean IP finder
     * if and only if {@link TcpDiscoveryIpFinder#isShared()} is {@code true}.
     */
    private class IpFinderCleaner extends IgniteSpiThread {
        /**
         * Constructor.
         */
        private IpFinderCleaner() {
            super(ignite.name(), "tcp-disco-ip-finder-cleaner", log);

            setPriority(threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("IP finder cleaner has been started.");

            while (!isInterrupted()) {
                Thread.sleep(ipFinderCleanFreq);

                if (!isLocalNodeCoordinator())
                    continue;

                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping IP finder cleaner (SPI is not connected to topology).");

                    return;
                }

                if (ipFinder.isShared())
                    cleanIpFinder();
            }
        }

        /**
         * Cleans IP finder.
         */
        private void cleanIpFinder() {
            assert ipFinder.isShared();

            try {
                // Addresses that belongs to nodes in topology.
                Collection<InetSocketAddress> currAddrs = F.flatCollections(
                    F.viewReadOnly(
                        ring.allNodes(),
                        new C1<TcpDiscoveryNode, Collection<InetSocketAddress>>() {
                            @Override public Collection<InetSocketAddress> apply(TcpDiscoveryNode node) {
                                return !node.isClient() ? getNodeAddresses(node) :
                                    Collections.<InetSocketAddress>emptyList();
                            }
                        }
                    )
                );

                // Addresses registered in IP finder.
                Collection<InetSocketAddress> regAddrs = registeredAddresses();

                // Remove all addresses that belong to alive nodes, leave dead-node addresses.
                Collection<InetSocketAddress> rmvAddrs = F.view(
                    regAddrs,
                    F.notContains(currAddrs),
                    new P1<InetSocketAddress>() {
                        private final Map<InetSocketAddress, Boolean> pingResMap =
                            new HashMap<>();

                        @Override public boolean apply(InetSocketAddress addr) {
                            Boolean res = pingResMap.get(addr);

                            if (res == null) {
                                try {
                                    res = pingNode(addr, null).get1() != null;
                                }
                                catch (IgniteCheckedException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to ping node [addr=" + addr +
                                            ", err=" + e.getMessage() + ']');

                                    res = false;
                                }
                                finally {
                                    pingResMap.put(addr, res);
                                }
                            }

                            return !res;
                        }
                    }
                );

                // Unregister dead-nodes addresses.
                if (!rmvAddrs.isEmpty()) {
                    ipFinder.unregisterAddresses(rmvAddrs);

                    if (log.isDebugEnabled())
                        log.debug("Unregistered addresses from IP finder: " + rmvAddrs);
                }

                // Addresses that were removed by mistake (e.g. on segmentation).
                Collection<InetSocketAddress> missingAddrs = F.view(
                    currAddrs,
                    F.notContains(regAddrs)
                );

                // Re-register missing addresses.
                if (!missingAddrs.isEmpty()) {
                    ipFinder.registerAddresses(missingAddrs);

                    if (log.isDebugEnabled())
                        log.debug("Registered missing addresses in IP finder: " + missingAddrs);
                }
            }
            catch (IgniteSpiException e) {
                LT.error(log, e, "Failed to clean IP finder up.");
            }
        }
    }

    /**
     * Pending messages container.
     */
    private static class PendingMessages {
        /** */
        private static final int MAX = 1024;

        /** Pending messages. */
        private final Queue<TcpDiscoveryAbstractMessage> msgs = new ArrayDeque<>(MAX * 2);

        /** Discarded message ID. */
        private IgniteUuid discardId;

        /**
         * Adds pending message and shrinks queue if it exceeds limit
         * (messages that were not discarded yet are never removed).
         *
         * @param msg Message to add.
         */
        void add(TcpDiscoveryAbstractMessage msg) {
            msgs.add(msg);

            while (msgs.size() > MAX) {
                TcpDiscoveryAbstractMessage polled = msgs.poll();

                assert polled != null;

                if (polled.id().equals(discardId))
                    break;
            }
        }

        /**
         * Gets messages starting from provided ID (exclusive). If such
         * message is not found, {@code null} is returned (this indicates
         * a failure condition when it was already removed from queue).
         *
         * @param lastMsgId Last message ID.
         * @return Collection of messages.
         */
        @Nullable Collection<TcpDiscoveryAbstractMessage> messages(IgniteUuid lastMsgId) {
            assert lastMsgId != null;

            Collection<TcpDiscoveryAbstractMessage> copy = new ArrayList<>(msgs.size());

            boolean skip = true;

            for (TcpDiscoveryAbstractMessage msg : msgs) {
                if (skip) {
                    if (msg.id().equals(lastMsgId))
                        skip = false;
                }
                else
                    copy.add(msg);
            }

            return !skip ? copy : null;
        }

        /**
         * Resets pending messages.
         *
         * @param msgs Message.
         * @param discardId Discarded message ID.
         */
        void reset(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs, @Nullable IgniteUuid discardId) {
            this.msgs.clear();

            if (msgs != null)
                this.msgs.addAll(msgs);

            this.discardId = discardId;
        }

        /**
         * Clears pending messages.
         */
        void clear() {
            msgs.clear();

            discardId = null;
        }

        /**
         * Discards message with provided ID and all before it.
         *
         * @param id Discarded message ID.
         */
        void discard(IgniteUuid id) {
            discardId = id;
        }
    }

    /**
     * Message worker thread for messages processing.
     */
    private class RingMessageWorker extends MessageWorkerAdapter {
        /** Next node. */
        @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
        private TcpDiscoveryNode next;

        /** Pending messages. */
        private final PendingMessages pendingMsgs = new PendingMessages();

        /** Last message that updated topology. */
        private TcpDiscoveryAbstractMessage lastMsg;

        /** Force pending messages send. */
        private boolean forceSndPending;

        /** Socket. */
        private Socket sock;

        /**
         */
        protected RingMessageWorker() {
            super("tcp-disco-msg-worker");
        }

        /**
         * @param msg Message to process.
         */
        @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
            if (log.isDebugEnabled())
                log.debug("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            if (debugMode)
                debugLog("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            stats.onMessageProcessingStarted(msg);

            if (msg instanceof TcpDiscoveryJoinRequestMessage)
                processJoinRequestMessage((TcpDiscoveryJoinRequestMessage)msg);

            else if (msg instanceof TcpDiscoveryClientReconnectMessage)
                processClientReconnectMessage((TcpDiscoveryClientReconnectMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((TcpDiscoveryNodeAddedMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((TcpDiscoveryNodeAddFinishedMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeLeftMessage)
                processNodeLeftMessage((TcpDiscoveryNodeLeftMessage)msg);

            else if (msg instanceof TcpDiscoveryNodeFailedMessage)
                processNodeFailedMessage((TcpDiscoveryNodeFailedMessage)msg);

            else if (msg instanceof TcpDiscoveryHeartbeatMessage) {
                if (msg.client()) {
                    ClientMessageWorker wrk = clientMsgWorkers.get(msg.creatorNodeId());

                    if (wrk != null) {
                        msg.verify(ignite.configuration().getNodeId());

                        wrk.addMessage(msg);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Received heartbeat message from unknown client node: " + msg);
                }
                else
                    processHeartbeatMessage((TcpDiscoveryHeartbeatMessage)msg);
            }
            else if (msg instanceof TcpDiscoveryStatusCheckMessage)
                processStatusCheckMessage((TcpDiscoveryStatusCheckMessage)msg);

            else if (msg instanceof TcpDiscoveryDiscardMessage)
                processDiscardMessage((TcpDiscoveryDiscardMessage)msg);

            else if (msg instanceof TcpDiscoveryCustomEventMessage)
                processCustomMessage((TcpDiscoveryCustomEventMessage)msg);

            else
                assert false : "Unknown message type: " + msg.getClass().getSimpleName();

            stats.onMessageProcessingFinished(msg);
        }

        /**
         * Sends message across the ring.
         *
         * @param msg Message to send
         */
        @SuppressWarnings({"BreakStatementWithLabel", "LabeledStatement", "ContinueStatementWithLabel"})
        private void sendMessageAcrossRing(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            assert ring.hasRemoteNodes();

            onBeforeMessageSentAcrossRing(msg);

            if (redirectToClients(msg)) {
                for (ClientMessageWorker clientMsgWorker : clientMsgWorkers.values())
                    clientMsgWorker.addMessage(msg);
            }

            Collection<TcpDiscoveryNode> failedNodes;

            TcpDiscoverySpiState state;

            synchronized (mux) {
                failedNodes = U.arrayList(TcpDiscoverySpi.this.failedNodes);

                state = spiState;
            }

            Collection<Throwable> errs = null;

            boolean sent = false;

            boolean searchNext = true;

            UUID locNodeId = ignite.configuration().getNodeId();

            while (true) {
                if (searchNext) {
                    TcpDiscoveryNode newNext = ring.nextNode(failedNodes);

                    if (newNext == null) {
                        if (log.isDebugEnabled())
                            log.debug("No next node in topology.");

                        if (debugMode)
                            debugLog("No next node in topology.");

                        if (ring.hasRemoteNodes()) {
                            msg.senderNodeId(locNodeId);

                            addMessage(msg);
                        }

                        break;
                    }

                    if (!newNext.equals(next)) {
                        if (log.isDebugEnabled())
                            log.debug("New next node [newNext=" + newNext + ", formerNext=" + next +
                                ", ring=" + ring + ", failedNodes=" + failedNodes + ']');

                        if (debugMode)
                            debugLog("New next node [newNext=" + newNext + ", formerNext=" + next +
                                ", ring=" + ring + ", failedNodes=" + failedNodes + ']');

                        U.closeQuiet(sock);

                        sock = null;

                        next = newNext;
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Next node remains the same [nextId=" + next.id() +
                            ", nextOrder=" + next.internalOrder() + ']');
                }

                // Flag that shows whether next node exists and accepts incoming connections.
                boolean nextNodeExists = sock != null;

                final boolean sameHost = U.sameMacs(locNode, next);

                addr: for (InetSocketAddress addr : getNodeAddresses(next, sameHost)) {
                    long ackTimeout0 = ackTimeout;

                    for (int i = 0; i < reconCnt; i++) {
                        if (sock == null) {
                            nextNodeExists = false;

                            boolean success = false;

                            boolean openSock = false;

                            // Restore ring.
                            try {
                                long tstamp = U.currentTimeMillis();

                                sock = openSocket(addr);

                                openSock = true;

                                // Handshake.
                                writeToSocket(sock, new TcpDiscoveryHandshakeRequest(locNodeId));

                                TcpDiscoveryHandshakeResponse res = readMessage(sock, null, ackTimeout0);

                                if (locNodeId.equals(res.creatorNodeId())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Handshake response from local node: " + res);

                                    U.closeQuiet(sock);

                                    sock = null;

                                    break;
                                }

                                stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                                UUID nextId = res.creatorNodeId();

                                long nextOrder = res.order();

                                if (!next.id().equals(nextId)) {
                                    // Node with different ID has bounded to the same port.
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to restore ring because next node ID received is not as " +
                                            "expected [expectedId=" + next.id() + ", rcvdId=" + nextId + ']');

                                    if (debugMode)
                                        debugLog("Failed to restore ring because next node ID received is not as " +
                                            "expected [expectedId=" + next.id() + ", rcvdId=" + nextId + ']');

                                    break;
                                }
                                else {
                                    // ID is as expected. Check node order.
                                    if (nextOrder != next.internalOrder()) {
                                        // Is next currently being added?
                                        boolean nextNew = (msg instanceof TcpDiscoveryNodeAddedMessage &&
                                            ((TcpDiscoveryNodeAddedMessage)msg).node().id().equals(nextId));

                                        if (!nextNew) {
                                            if (log.isDebugEnabled())
                                                log.debug("Failed to restore ring because next node order received " +
                                                    "is not as expected [expected=" + next.internalOrder() +
                                                    ", rcvd=" + nextOrder + ", id=" + next.id() + ']');

                                            if (debugMode)
                                                debugLog("Failed to restore ring because next node order received " +
                                                    "is not as expected [expected=" + next.internalOrder() +
                                                    ", rcvd=" + nextOrder + ", id=" + next.id() + ']');

                                            break;
                                        }
                                    }

                                    if (log.isDebugEnabled())
                                        log.debug("Initialized connection with next node: " + next.id());

                                    if (debugMode)
                                        debugLog("Initialized connection with next node: " + next.id());

                                    errs = null;

                                    success = true;
                                }
                            }
                            catch (IOException | IgniteCheckedException e) {
                                if (errs == null)
                                    errs = new ArrayList<>();

                                errs.add(e);

                                if (log.isDebugEnabled())
                                    log.debug("Failed to connect to next node [msg=" + msg + ", err=" + e + ']');

                                onException("Failed to connect to next node [msg=" + msg + ", err=" + e + ']', e);

                                if (!openSock)
                                    break; // Don't retry if we can not establish connection.

                                if (e instanceof SocketTimeoutException ||
                                    X.hasCause(e, SocketTimeoutException.class)) {
                                    ackTimeout0 *= 2;

                                    if (!checkAckTimeout(ackTimeout0))
                                        break;
                                }

                                continue;
                            }
                            finally {
                                if (!success) {
                                    U.closeQuiet(sock);

                                    sock = null;
                                }
                                else
                                    // Next node exists and accepts incoming messages.
                                    nextNodeExists = true;
                            }
                        }

                        try {
                            boolean failure;

                            synchronized (mux) {
                                failure = TcpDiscoverySpi.this.failedNodes.size() < failedNodes.size();
                            }

                            assert !forceSndPending || msg instanceof TcpDiscoveryNodeLeftMessage;

                            if (failure || forceSndPending) {
                                if (log.isDebugEnabled())
                                    log.debug("Pending messages will be sent [failure=" + failure +
                                        ", forceSndPending=" + forceSndPending + ']');

                                if (debugMode)
                                    debugLog("Pending messages will be sent [failure=" + failure +
                                        ", forceSndPending=" + forceSndPending + ']');

                                boolean skip = pendingMsgs.discardId != null;

                                for (TcpDiscoveryAbstractMessage pendingMsg : pendingMsgs.msgs) {
                                    if (skip) {
                                        if (pendingMsg.id().equals(pendingMsgs.discardId))
                                            skip = false;

                                        continue;
                                    }

                                    long tstamp = U.currentTimeMillis();

                                    prepareNodeAddedMessage(pendingMsg, next.id(), pendingMsgs.msgs,
                                        pendingMsgs.discardId);

                                    try {
                                        writeToSocket(sock, pendingMsg);
                                    }
                                    finally {
                                        clearNodeAddedMessage(pendingMsg);
                                    }

                                    stats.onMessageSent(pendingMsg, U.currentTimeMillis() - tstamp);

                                    int res = readReceipt(sock, ackTimeout0);

                                    if (log.isDebugEnabled())
                                        log.debug("Pending message has been sent to next node [msg=" + msg.id() +
                                            ", pendingMsgId=" + pendingMsg + ", next=" + next.id() +
                                            ", res=" + res + ']');

                                    if (debugMode)
                                        debugLog("Pending message has been sent to next node [msg=" + msg.id() +
                                            ", pendingMsgId=" + pendingMsg + ", next=" + next.id() +
                                            ", res=" + res + ']');
                                }
                            }

                            prepareNodeAddedMessage(msg, next.id(), pendingMsgs.msgs, pendingMsgs.discardId);

                            try {
                                long tstamp = U.currentTimeMillis();

                                writeToSocket(sock, msg);

                                stats.onMessageSent(msg, U.currentTimeMillis() - tstamp);

                                int res = readReceipt(sock, ackTimeout0);

                                if (log.isDebugEnabled())
                                    log.debug("Message has been sent to next node [msg=" + msg +
                                        ", next=" + next.id() +
                                        ", res=" + res + ']');

                                if (debugMode)
                                    debugLog("Message has been sent to next node [msg=" + msg +
                                        ", next=" + next.id() +
                                        ", res=" + res + ']');
                            }
                            finally {
                                clearNodeAddedMessage(msg);
                            }

                            registerPendingMessage(msg);

                            sent = true;

                            break addr;
                        }
                        catch (IOException | IgniteCheckedException e) {
                            if (errs == null)
                                errs = new ArrayList<>();

                            errs.add(e);

                            if (log.isDebugEnabled())
                                U.error(log, "Failed to send message to next node [next=" + next.id() + ", msg=" + msg +
                                    ", err=" + e + ']', e);

                            onException("Failed to send message to next node [next=" + next.id() + ", msg=" + msg + ']',
                                e);

                            if (e instanceof SocketTimeoutException || X.hasCause(e, SocketTimeoutException.class)) {
                                ackTimeout0 *= 2;

                                if (!checkAckTimeout(ackTimeout0))
                                    break;
                            }
                        }
                        finally {
                            forceSndPending = false;

                            if (!sent) {
                                U.closeQuiet(sock);

                                sock = null;

                                if (log.isDebugEnabled())
                                    log.debug("Message has not been sent [next=" + next.id() + ", msg=" + msg +
                                        ", i=" + i + ']');
                            }
                        }
                    } // Try to reconnect.
                } // Iterating node's addresses.

                if (!sent) {
                    if (!failedNodes.contains(next)) {
                        failedNodes.add(next);

                        if (state == CONNECTED) {
                            Exception err = errs != null ?
                                U.exceptionWithSuppressed("Failed to send message to next node [msg=" + msg +
                                    ", next=" + U.toShortString(next) + ']', errs) :
                                null;

                            // If node existed on connection initialization we should check
                            // whether it has not gone yet.
                            if (nextNodeExists && pingNode(next))
                                U.error(log, "Failed to send message to next node [msg=" + msg +
                                    ", next=" + next + ']', err);
                            else if (log.isDebugEnabled())
                                log.debug("Failed to send message to next node [msg=" + msg + ", next=" + next +
                                    ", errMsg=" + (err != null ? err.getMessage() : "N/A") + ']');
                        }
                    }

                    if (msg instanceof TcpDiscoveryStatusCheckMessage) {
                        TcpDiscoveryStatusCheckMessage msg0 = (TcpDiscoveryStatusCheckMessage)msg;

                        if (next.id().equals(msg0.failedNodeId())) {
                            next = null;

                            if (log.isDebugEnabled())
                                log.debug("Discarding status check since next node has indeed failed [next=" + next +
                                    ", msg=" + msg + ']');

                            // Discard status check message by exiting loop and handle failure.
                            break;
                        }
                    }

                    next = null;

                    searchNext = true;

                    errs = null;
                }
                else
                    break;
            }

            synchronized (mux) {
                failedNodes.removeAll(TcpDiscoverySpi.this.failedNodes);
            }

            if (!failedNodes.isEmpty()) {
                if (state == CONNECTED) {
                    if (!sent && log.isDebugEnabled())
                        // Message has not been sent due to some problems.
                        log.debug("Message has not been sent: " + msg);

                    if (log.isDebugEnabled())
                        log.debug("Detected failed nodes: " + failedNodes);
                }

                synchronized (mux) {
                    TcpDiscoverySpi.this.failedNodes.addAll(failedNodes);
                }

                for (TcpDiscoveryNode n : failedNodes)
                    msgWorker.addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, n.id(), n.internalOrder()));
            }
        }

        /**
         * @param msg Message.
         * @return Whether to redirect message to client nodes.
         */
        private boolean redirectToClients(TcpDiscoveryAbstractMessage msg) {
            return msg.verified() && U.getAnnotation(msg.getClass(), TcpDiscoveryRedirectToClient.class) != null;
        }

        /**
         * Registers pending message.
         *
         * @param msg Message to register.
         */
        private void registerPendingMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            if (ensured(msg)) {
                pendingMsgs.add(msg);

                stats.onPendingMessageRegistered();

                if (log.isDebugEnabled())
                    log.debug("Pending message has been registered: " + msg.id());
            }
        }

        /**
         * Processes join request message.
         *
         * @param msg Join request message.
         */
        private void processJoinRequestMessage(TcpDiscoveryJoinRequestMessage msg) {
            assert msg != null;

            TcpDiscoveryNode node = msg.node();

            UUID locNodeId = ignite.configuration().getNodeId();

            if (!msg.client()) {
                boolean rmtHostLoopback = node.socketAddresses().size() == 1 &&
                    node.socketAddresses().iterator().next().getAddress().isLoopbackAddress();

                // This check is performed by the node joining node is connected to, but not by coordinator
                // because loopback problem message is sent directly to the joining node which may be unavailable
                // if coordinator resides on another host.
                if (locHost.isLoopbackAddress() != rmtHostLoopback) {
                    String firstNode = rmtHostLoopback ? "remote" : "local";

                    String secondNode = rmtHostLoopback ? "local" : "remote";

                    String errMsg = "Failed to add node to topology because " + firstNode +
                        " node is configured to use loopback address, but " + secondNode + " node is not " +
                        "(consider changing 'localAddress' configuration parameter) " +
                        "[locNodeAddrs=" + U.addressesAsString(locNode) +
                        ", rmtNodeAddrs=" + U.addressesAsString(node) + ']';

                    LT.warn(log, null, errMsg);

                    // Always output in debug.
                    if (log.isDebugEnabled())
                        log.debug(errMsg);

                    try {
                        trySendMessageDirectly(node, new TcpDiscoveryLoopbackProblemMessage(
                            locNodeId, locNode.addresses(), locNode.hostNames()));
                    }
                    catch (IgniteSpiException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send loopback problem message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']');

                        onException("Failed to send loopback problem message to node " +
                            "[node=" + node + ", err=" + e.getMessage() + ']', e);
                    }

                    // Ignore join request.
                    return;
                }
            }

            if (isLocalNodeCoordinator()) {
                TcpDiscoveryNode existingNode = ring.node(node.id());

                if (existingNode != null) {
                    if (!node.socketAddresses().equals(existingNode.socketAddresses())) {
                        if (!pingNode(existingNode)) {
                            addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                                existingNode.id(), existingNode.internalOrder()));

                            // Ignore this join request since existing node is about to fail
                            // and new node can continue.
                            return;
                        }

                        try {
                            trySendMessageDirectly(node, new TcpDiscoveryDuplicateIdMessage(locNodeId,
                                existingNode));
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send duplicate ID message to node " +
                                    "[node=" + node + ", existingNode=" + existingNode +
                                    ", err=" + e.getMessage() + ']');

                            onException("Failed to send duplicate ID message to node " +
                                "[node=" + node + ", existingNode=" + existingNode + ']', e);
                        }

                        // Output warning.
                        LT.warn(log, null, "Ignoring join request from node (duplicate ID) [node=" + node +
                            ", existingNode=" + existingNode + ']');

                        // Ignore join request.
                        return;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Ignoring join request message since node is already in topology: " + msg);

                    return;
                }

                if (nodeAuth != null) {
                    // Authenticate node first.
                    try {
                        GridSecurityCredentials cred = unmarshalCredentials(node);

                        SecurityContext subj = nodeAuth.authenticateNode(node, cred);

                        if (subj == null) {
                            // Node has not pass authentication.
                            LT.warn(log, null,
                                "Authentication failed [nodeId=" + node.id() +
                                    ", addrs=" + U.addressesAsString(node) + ']',
                                "Authentication failed [nodeId=" + U.id8(node.id()) + ", addrs=" +
                                    U.addressesAsString(node) + ']');

                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug("Authentication failed [nodeId=" + node.id() + ", addrs=" +
                                    U.addressesAsString(node));

                            try {
                                trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId, locHost));
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send unauthenticated message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');

                                onException("Failed to send unauthenticated message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
                            }

                            // Ignore join request.
                            return;
                        }
                        else {
                            if (!(subj instanceof Serializable)) {
                                // Node has not pass authentication.
                                LT.warn(log, null,
                                    "Authentication subject is not Serializable [nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node) + ']',
                                    "Authentication subject is not Serializable [nodeId=" + U.id8(node.id()) +
                                        ", addrs=" +
                                        U.addressesAsString(node) + ']');

                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug("Authentication subject is not serializable [nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node));

                                try {
                                    trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId, locHost));
                                }
                                catch (IgniteSpiException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send unauthenticated message to node " +
                                            "[node=" + node + ", err=" + e.getMessage() + ']');
                                }

                                // Ignore join request.
                                return;
                            }

                            // Stick in authentication subject to node (use security-safe attributes for copy).
                            Map<String, Object> attrs = new HashMap<>(node.getAttributes());

                            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT,
                                ignite.configuration().getMarshaller().marshal(subj));

                            node.setAttributes(attrs);
                        }
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        LT.error(log, e, "Authentication failed [nodeId=" + node.id() + ", addrs=" +
                            U.addressesAsString(node) + ']');

                        if (log.isDebugEnabled())
                            log.debug("Failed to authenticate node (will ignore join request) [node=" + node +
                                ", err=" + e + ']');

                        onException("Failed to authenticate node (will ignore join request) [node=" + node +
                            ", err=" + e + ']', e);

                        // Ignore join request.
                        return;
                    }
                }

                IgniteSpiNodeValidationResult err = getSpiContext().validateNode(node);

                if (err != null) {
                    boolean ping = node.id().equals(err.nodeId()) ? pingNode(node) : pingNode(err.nodeId());

                    if (!ping) {
                        if (log.isDebugEnabled())
                            log.debug("Conflicting node has already left, need to wait for event. " +
                                "Will ignore join request for now since it will be recent [req=" + msg +
                                ", err=" + err.message() + ']');

                        // Ignore join request.
                        return;
                    }

                    LT.warn(log, null, err.message());

                    // Always output in debug.
                    if (log.isDebugEnabled())
                        log.debug(err.message());

                    try {
                        trySendMessageDirectly(node,
                            new TcpDiscoveryCheckFailedMessage(locNodeId, err.sendMessage()));
                    }
                    catch (IgniteSpiException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send hash ID resolver validation failed message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']');

                        onException("Failed to send hash ID resolver validation failed message to node " +
                            "[node=" + node + ", err=" + e.getMessage() + ']', e);
                    }

                    // Ignore join request.
                    return;
                }

                // Check version.
                String locBuildVer = locNode.attribute(ATTR_BUILD_VER);
                String rmtBuildVer = node.attribute(ATTR_BUILD_VER);

                if (!F.eq(rmtBuildVer, locBuildVer)) {
                    // OS nodes don't support rolling updates.
                    if (!locBuildVer.equals(rmtBuildVer)) {
                        String errMsg = "Local node and remote node have different version numbers " +
                            "(node will not join, Ignite does not support rolling updates, " +
                            "so versions must be exactly the same) " +
                            "[locBuildVer=" + locBuildVer + ", rmtBuildVer=" + rmtBuildVer +
                            ", locNodeAddrs=" + U.addressesAsString(locNode) +
                            ", rmtNodeAddrs=" + U.addressesAsString(node) +
                            ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                        LT.warn(log, null, errMsg);

                        // Always output in debug.
                        if (log.isDebugEnabled())
                            log.debug(errMsg);

                        try {
                            String sndMsg = "Local node and remote node have different version numbers " +
                                "(node will not join, Ignite does not support rolling updates, " +
                                "so versions must be exactly the same) " +
                                "[locBuildVer=" + rmtBuildVer + ", rmtBuildVer=" + locBuildVer +
                                ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                ", rmtNodeId=" + locNode.id() + ']';

                            trySendMessageDirectly(node,
                                    new TcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send version check failed message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']');

                            onException("Failed to send version check failed message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']', e);
                        }

                        // Ignore join request.
                        return;
                    }

                    Collection<String> locCompatibleVers = locNode.attribute(ATTR_COMPATIBLE_VERS);
                    Collection<String> rmtCompatibleVers = node.attribute(ATTR_COMPATIBLE_VERS);

                    if (F.contains(rmtCompatibleVers, locBuildVer) || F.contains(locCompatibleVers, rmtBuildVer)) {
                        String errMsg = "Local node's build version differs from remote node's, " +
                            "but they are compatible (will continue join process) " +
                            "[locBuildVer=" + locBuildVer + ", rmtBuildVer=" + rmtBuildVer +
                            ", locNodeAddrs=" + U.addressesAsString(locNode) +
                            ", rmtNodeAddrs=" + U.addressesAsString(node) +
                            ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                        LT.warn(log, null, errMsg);

                        // Always output in debug.
                        if (log.isDebugEnabled())
                            log.debug(errMsg);
                    }
                    else {
                        String errMsg = "Local node's and remote node's build versions are not compatible " +
                            "(topologies built with different Ignite versions " +
                            "are supported in Enterprise version only) "  +
                            "[locBuildVer=" + locBuildVer + ", rmtBuildVer=" + rmtBuildVer +
                            ", locNodeAddrs=" + U.addressesAsString(locNode) +
                            ", rmtNodeAddrs=" + U.addressesAsString(node) +
                            ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                        LT.warn(log, null, errMsg);

                        // Always output in debug.
                        if (log.isDebugEnabled())
                            log.debug(errMsg);

                        try {
                            String sndMsg = "Local node's and remote node's build versions are not compatible " +
                                "(topologies built with different Ignite versions " +
                                "are supported in Enterprise version only) " +
                                " [locBuildVer=" + rmtBuildVer + ", rmtBuildVer=" + locBuildVer +
                                ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                ", rmtNodeId=" + locNode.id() + ']';

                            trySendMessageDirectly(node,
                                new TcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send version check failed message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']');

                            onException("Failed to send version check failed message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']', e);
                        }

                        // Ignore join request.
                        return;
                    }
                }

                String locMarsh = locNode.attribute(ATTR_MARSHALLER);
                String rmtMarsh = node.attribute(ATTR_MARSHALLER);

                if (!F.eq(locMarsh, rmtMarsh)) {
                    String errMsg = "Local node's marshaller differs from remote node's marshaller " +
                        "(to make sure all nodes in topology have identical marshaller, " +
                        "configure marshaller explicitly in configuration) " +
                        "[locMarshaller=" + locMarsh + ", rmtMarshaller=" + rmtMarsh +
                        ", locNodeAddrs=" + U.addressesAsString(locNode) +
                        ", rmtNodeAddrs=" + U.addressesAsString(node) +
                        ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                    LT.warn(log, null, errMsg);

                    // Always output in debug.
                    if (log.isDebugEnabled())
                        log.debug(errMsg);

                    try {
                        String sndMsg = "Local node's marshaller differs from remote node's marshaller " +
                            "(to make sure all nodes in topology have identical marshaller, " +
                            "configure marshaller explicitly in configuration) " +
                            "[locMarshaller=" + rmtMarsh + ", rmtMarshaller=" + locMarsh +
                            ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                            ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                            ", rmtNodeId=" + locNode.id() + ']';

                        trySendMessageDirectly(node,
                            new TcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                    }
                    catch (IgniteSpiException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send marshaller check failed message to node " +
                                "[node=" + node + ", err=" + e.getMessage() + ']');

                        onException("Failed to send marshaller check failed message to node " +
                            "[node=" + node + ", err=" + e.getMessage() + ']', e);
                    }

                    // Ignore join request.
                    return;
                }

                // Handle join.
                node.internalOrder(ring.nextNodeOrder());

                if (log.isDebugEnabled())
                    log.debug("Internal order has been assigned to node: " + node);

                TcpDiscoveryNodeAddedMessage nodeAddedMsg = new TcpDiscoveryNodeAddedMessage(locNodeId,
                    node, msg.discoveryData(), gridStartTime);

                nodeAddedMsg.client(msg.client());

                processNodeAddedMessage(nodeAddedMsg);
            }
            else if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Tries to send a message to all node's available addresses.
         *
         * @param node Node to send message to.
         * @param msg Message.
         * @throws IgniteSpiException Last failure if all attempts failed.
         */
        private void trySendMessageDirectly(TcpDiscoveryNode node, TcpDiscoveryAbstractMessage msg)
            throws IgniteSpiException {
            if (node.isClient()) {
                TcpDiscoveryNode routerNode = ring.node(node.clientRouterNodeId());

                if (routerNode == null)
                    throw new IgniteSpiException("Router node for client does not exist: " + node);

                assert !routerNode.isClient();

                trySendMessageDirectly(routerNode, msg);

                return;
            }

            IgniteSpiException ex = null;

            for (InetSocketAddress addr : getNodeAddresses(node, U.sameMacs(locNode, node))) {
                try {
                    sendMessageDirectly(msg, addr, null);

                    ex = null;

                    break;
                }
                catch (IgniteSpiException e) {
                    ex = e;
                }
            }

            if (ex != null)
                throw ex;
        }

        /**
         * Processes client reconnect message.
         *
         * @param msg Client reconnect message.
         */
        private void processClientReconnectMessage(TcpDiscoveryClientReconnectMessage msg) {
            UUID locNodeId = ignite.configuration().getNodeId();

            boolean isLocalNodeRouter = locNodeId.equals(msg.routerNodeId());

            if (!msg.verified()) {
                assert isLocalNodeRouter;

                msg.verify(locNodeId);
            }
            else {
                UUID nodeId = msg.creatorNodeId();

                TcpDiscoveryNode node = ring.node(nodeId);

                assert node == null || node.isClient();

                if (node != null) {
                    assert node.isClient();

                    node.clientRouterNodeId(msg.routerNodeId());
                    node.aliveCheck(maxMissedClientHbs);

                    if (isLocalNodeCoordinator()) {
                        Collection<TcpDiscoveryAbstractMessage> pending =
                            pendingMsgs.messages(msg.lastMessageId());

                        if (pending != null) {
                            msg.pendingMessages(pending);
                            msg.success(true);
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Failing reconnecting client node because failed to restore pending " +
                                    "messages [locNodeId=" + locNodeId + ", clientNodeId=" + nodeId + ']');

                            processNodeFailedMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                                node.id(), node.order()));
                        }
                    }
                }
                else if (log.isDebugEnabled())
                    log.debug("Reconnecting client node is already failed [nodeId=" + nodeId + ']');

                if (isLocalNodeRouter) {
                    ClientMessageWorker wrk = clientMsgWorkers.get(nodeId);

                    if (wrk != null)
                        wrk.addMessage(msg);
                    else if (log.isDebugEnabled())
                        log.debug("Failed to reconnect client node (disconnected during the process) [locNodeId=" +
                            locNodeId + ", clientNodeId=" + nodeId + ']');
                }
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Processes node added message.
         *
         * @param msg Node added message.
         * @deprecated Due to current protocol node add process cannot be dropped in the middle of the ring,
         *      if new node auth fails due to config inconsistency. So, we need to finish add
         *      and only then initiate failure.
         */
        @Deprecated
        private void processNodeAddedMessage(TcpDiscoveryNodeAddedMessage msg) {
            assert msg != null;

            TcpDiscoveryNode node = msg.node();

            assert node != null;

            if (node.internalOrder() < locNode.internalOrder()) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node added message since local node's order is greater " +
                        "[node=" + node + ", locNode=" + locNode + ", msg=" + msg + ']');

                return;
            }

            UUID locNodeId = ignite.configuration().getNodeId();

            if (isLocalNodeCoordinator()) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    processNodeAddFinishedMessage(new TcpDiscoveryNodeAddFinishedMessage(locNodeId, node.id()));

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified() && !locNodeId.equals(node.id())) {
                if (node.internalOrder() <= ring.maxInternalOrder()) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

                    if (debugMode)
                        debugLog("Discarding node added message since new node's order is less than " +
                            "max order in ring [ring=" + ring + ", node=" + node + ", locNode=" + locNode +
                            ", msg=" + msg + ']');

                    return;
                }

                if (!isLocalNodeCoordinator() && nodeAuth != null && nodeAuth.isGlobalNodeAuthentication()) {
                    boolean authFailed = true;

                    try {
                        GridSecurityCredentials cred = unmarshalCredentials(node);

                        if (cred == null) {
                            if (log.isDebugEnabled())
                                log.debug(
                                    "Skipping global authentication for node (security credentials not found, " +
                                        "probably, due to coordinator has older version) " +
                                        "[nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node) +
                                        ", coord=" + ring.coordinator() + ']');

                            authFailed = false;
                        }
                        else {
                            SecurityContext subj = nodeAuth.authenticateNode(node, cred);

                            SecurityContext coordSubj = ignite.configuration().getMarshaller().unmarshal(
                                node.<byte[]>attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT),
                                U.gridClassLoader());

                            if (!permissionsEqual(coordSubj.subject().permissions(), subj.subject().permissions())) {
                                // Node has not pass authentication.
                                LT.warn(log, null,
                                    "Authentication failed [nodeId=" + node.id() +
                                        ", addrs=" + U.addressesAsString(node) + ']',
                                    "Authentication failed [nodeId=" + U.id8(node.id()) + ", addrs=" +
                                        U.addressesAsString(node) + ']');

                                // Always output in debug.
                                if (log.isDebugEnabled())
                                    log.debug("Authentication failed [nodeId=" + node.id() + ", addrs=" +
                                        U.addressesAsString(node));
                            }
                            else
                                // Node will not be kicked out.
                                authFailed = false;
                        }
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        U.error(log, "Failed to verify node permissions consistency (will drop the node): " + node, e);
                    }
                    finally {
                        if (authFailed) {
                            try {
                                trySendMessageDirectly(node, new TcpDiscoveryAuthFailedMessage(locNodeId, locHost));
                            }
                            catch (IgniteSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send unauthenticated message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');

                                onException("Failed to send unauthenticated message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']', e);
                            }

                            addMessage(new TcpDiscoveryNodeFailedMessage(locNodeId, node.id(),
                                node.internalOrder()));
                        }
                    }
                }

                if (msg.client())
                    node.aliveCheck(maxMissedClientHbs);

                boolean topChanged = ring.add(node);

                if (topChanged) {
                    assert !node.visible() : "Added visible node [node=" + node + ", locNode=" + locNode + ']';

                    Map<Integer, Object> data = msg.newNodeDiscoveryData();

                    if (data != null)
                        exchange.onExchange(node.id(), node.id(), data);

                    msg.addDiscoveryData(locNodeId, exchange.collect(node.id()));
                }

                if (log.isDebugEnabled())
                    log.debug("Added node to local ring [added=" + topChanged + ", node=" + node +
                        ", ring=" + ring + ']');
            }

            if (msg.verified() && locNodeId.equals(node.id())) {
                // Discovery data.
                Map<UUID, Map<Integer, Object>> dataMap;

                synchronized (mux) {
                    if (spiState == CONNECTING && locNode.internalOrder() != node.internalOrder()) {
                        // Initialize topology.
                        Collection<TcpDiscoveryNode> top = msg.topology();

                        if (top != null && !top.isEmpty()) {
                            gridStartTime = msg.gridStartTime();

                            for (TcpDiscoveryNode n : top) {
                                // Make all preceding nodes and local node visible.
                                n.visible(true);
                            }

                            locNode.setAttributes(node.attributes());

                            locNode.visible(true);

                            // Restore topology with all nodes visible.
                            ring.restoreTopology(top, node.internalOrder());

                            if (log.isDebugEnabled())
                                log.debug("Restored topology from node added message: " + ring);

                            dataMap = msg.oldNodesDiscoveryData();

                            topHist.clear();
                            topHist.putAll(msg.topologyHistory());

                            // Restore pending messages.
                            pendingMsgs.reset(msg.messages(), msg.discardedMessageId());

                            // Clear data to minimize message size.
                            msg.messages(null, null);
                            msg.topology(null);
                            msg.topologyHistory(null);
                            msg.clearDiscoveryData();
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Discarding node added message with empty topology: " + msg);

                            return;
                        }
                    }
                    else  {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node added message (this message has already been processed) " +
                                "[spiState=" + spiState +
                                ", msg=" + msg +
                                ", locNode=" + locNode + ']');

                        return;
                    }
                }

                // Notify outside of synchronized block.
                if (dataMap != null) {
                    for (Map.Entry<UUID, Map<Integer, Object>> entry : dataMap.entrySet())
                        exchange.onExchange(node.id(), entry.getKey(), entry.getValue());
                }
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Processes node add finished message.
         *
         * @param msg Node add finished message.
         */
        private void processNodeAddFinishedMessage(TcpDiscoveryNodeAddFinishedMessage msg) {
            assert msg != null;

            UUID nodeId = msg.nodeId();

            assert nodeId != null;

            TcpDiscoveryNode node = ring.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message since node is not found " +
                        "[msg=" + msg + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Node to finish add: " + node);

            boolean locNodeCoord = isLocalNodeCoordinator();

            UUID locNodeId = ignite.configuration().getNodeId();

            if (locNodeCoord) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                if (node.visible() && node.order() != 0) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node add finished message since node has already been added " +
                            "[node=" + node + ", msg=" + msg + ']');

                    return;
                }
                else
                    msg.topologyVersion(ring.incrementTopologyVersion());

                msg.verify(locNodeId);
            }

            long topVer = msg.topologyVersion();

            boolean fireEvt = false;

            if (msg.verified()) {
                assert topVer > 0 : "Invalid topology version: " + msg;

                if (node.order() == 0)
                    node.order(topVer);

                if (!node.visible()) {
                    node.visible(true);

                    fireEvt = true;
                }
            }

            if (msg.verified() && !locNodeId.equals(nodeId) && spiStateCopy() == CONNECTED && fireEvt) {
                stats.onNodeJoined();

                // Make sure that node with greater order will never get EVT_NODE_JOINED
                // on node with less order.
                assert node.internalOrder() > locNode.internalOrder() : "Invalid order [node=" + node +
                    ", locNode=" + locNode + ", msg=" + msg + ", ring=" + ring + ']';

                if (locNodeVer.equals(node.version()))
                    node.version(locNodeVer);

                if (!locNodeCoord) {
                    boolean b = ring.topologyVersion(topVer);

                    assert b : "Topology version has not been updated: [ring=" + ring + ", msg=" + msg +
                        ", lastMsg=" + lastMsg + ", spiState=" + spiStateCopy() + ']';

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                notifyDiscovery(EVT_NODE_JOINED, topVer, node);

                try {
                    if (ipFinder.isShared() && locNodeCoord)
                        ipFinder.registerAddresses(node.socketAddresses());
                }
                catch (IgniteSpiException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to register new node address [node=" + node +
                            ", err=" + e.getMessage() + ']');

                    onException("Failed to register new node address [node=" + node +
                        ", err=" + e.getMessage() + ']', e);
                }
            }

            if (msg.verified() && locNodeId.equals(nodeId) && spiStateCopy() == CONNECTING) {
                assert node != null;

                ring.topologyVersion(topVer);

                node.order(topVer);

                synchronized (mux) {
                    spiState = CONNECTED;

                    mux.notifyAll();
                }

                // Discovery manager must create local joined event before spiStart completes.
                notifyDiscovery(EVT_NODE_JOINED, topVer, locNode);
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Processes node left message.
         *
         * @param msg Node left message.
         */
        private void processNodeLeftMessage(TcpDiscoveryNodeLeftMessage msg) {
            assert msg != null;

            UUID locNodeId = ignite.configuration().getNodeId();

            UUID leavingNodeId = msg.creatorNodeId();

            if (locNodeId.equals(leavingNodeId)) {
                if (msg.senderNodeId() == null) {
                    synchronized (mux) {
                        if (log.isDebugEnabled())
                            log.debug("Starting local node stop procedure.");

                        spiState = STOPPING;

                        mux.notifyAll();
                    }
                }

                if (msg.verified() || !ring.hasRemoteNodes() || msg.senderNodeId() != null) {
                    if (ipFinder.isShared() && !ring.hasRemoteNodes()) {
                        try {
                            ipFinder.unregisterAddresses(locNode.socketAddresses());
                        }
                        catch (IgniteSpiException e) {
                            U.error(log, "Failed to unregister local node address from IP finder.", e);
                        }
                    }

                    synchronized (mux) {
                        if (spiState == STOPPING) {
                            spiState = LEFT;

                            mux.notifyAll();
                        }
                    }

                    return;
                }

                sendMessageAcrossRing(msg);

                return;
            }

            if (ring.node(msg.senderNodeId()) == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node left message since sender node is not in topology: " + msg);

                return;
            }

            TcpDiscoveryNode leavingNode = ring.node(leavingNodeId);

            if (leavingNode != null) {
                synchronized (mux) {
                    leavingNodes.add(leavingNode);
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Discarding node left message since node was not found: " + msg);

                return;
            }

            boolean locNodeCoord = isLocalNodeCoordinator();

            if (locNodeCoord) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified() && !locNodeId.equals(leavingNodeId)) {
                TcpDiscoveryNode leftNode = ring.removeNode(leavingNodeId);

                assert leftNode != null;

                if (log.isDebugEnabled())
                    log.debug("Removed node from topology: " + leftNode);

                // Clear pending messages map.
                if (!ring.hasRemoteNodes())
                    pendingMsgs.clear();

                long topVer;

                if (locNodeCoord) {
                    if (!msg.client() && ipFinder.isShared()) {
                        try {
                            ipFinder.unregisterAddresses(leftNode.socketAddresses());
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to unregister left node address: " + leftNode);

                            onException("Failed to unregister left node address: " + leftNode, e);
                        }
                    }

                    topVer = ring.incrementTopologyVersion();

                    msg.topologyVersion(topVer);
                }
                else {
                    topVer = msg.topologyVersion();

                    assert topVer > 0 : "Topology version is empty for message: " + msg;

                    boolean b = ring.topologyVersion(topVer);

                    assert b : "Topology version has not been updated: [ring=" + ring + ", msg=" + msg +
                        ", lastMsg=" + lastMsg + ", spiState=" + spiStateCopy() + ']';

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                if (msg.client()) {
                    ClientMessageWorker wrk = clientMsgWorkers.remove(leavingNodeId);

                    if (wrk != null)
                        wrk.addMessage(msg);
                }
                else if (leftNode.equals(next) && sock != null) {
                    try {
                        writeToSocket(sock, msg);

                        if (log.isDebugEnabled())
                            log.debug("Sent verified node left message to leaving node: " + msg);
                    }
                    catch (IgniteCheckedException | IOException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send verified node left message to leaving node [msg=" + msg +
                                ", err=" + e.getMessage() + ']');

                        onException("Failed to send verified node left message to leaving node [msg=" + msg +
                            ", err=" + e.getMessage() + ']', e);
                    }
                    finally {
                        forceSndPending = true;

                        next = null;

                        U.closeQuiet(sock);
                    }
                }

                stats.onNodeLeft();

                notifyDiscovery(EVT_NODE_LEFT, topVer, leftNode);

                synchronized (mux) {
                    failedNodes.remove(leftNode);

                    leavingNodes.remove(leftNode);
                }
            }

            if (ring.hasRemoteNodes()) {
                try {
                    sendMessageAcrossRing(msg);
                }
                finally {
                    forceSndPending = false;
                }
            }
            else {
                forceSndPending = false;

                if (log.isDebugEnabled())
                    log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

                U.closeQuiet(sock);
            }
        }

        /**
         * Processes node failed message.
         *
         * @param msg Node failed message.
         */
        private void processNodeFailedMessage(TcpDiscoveryNodeFailedMessage msg) {
            assert msg != null;

            UUID sndId = msg.senderNodeId();

            if (sndId != null) {
                TcpDiscoveryNode sndNode = ring.node(sndId);

                if (sndNode == null) {
                    if (log.isDebugEnabled())
                        log.debug("Discarding node failed message sent from unknown node: " + msg);

                    return;
                }
                else {
                    boolean contains;

                    synchronized (mux) {
                        contains = failedNodes.contains(sndNode);
                    }

                    if (contains) {
                        if (log.isDebugEnabled())
                            log.debug("Discarding node failed message sent from node which is about to fail: " + msg);

                        return;
                    }
                }
            }

            UUID nodeId = msg.failedNodeId();
            long order = msg.order();

            TcpDiscoveryNode node = ring.node(nodeId);

            if (node != null && node.internalOrder() != order) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring node failed message since node internal order does not match " +
                        "[msg=" + msg + ", node=" + node + ']');

                return;
            }

            if (node != null) {
                synchronized (mux) {
                    failedNodes.add(node);
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Discarding node failed message since node was not found: " + msg);

                return;
            }

            boolean locNodeCoord = isLocalNodeCoordinator();

            UUID locNodeId = ignite.configuration().getNodeId();

            if (locNodeCoord) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    addMessage(new TcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified()) {
                node = ring.removeNode(nodeId);

                assert node != null;

                // Clear pending messages map.
                if (!ring.hasRemoteNodes())
                    pendingMsgs.clear();

                long topVer;

                if (locNodeCoord) {
                    if (!node.isClient() && ipFinder.isShared()) {
                        try {
                            ipFinder.unregisterAddresses(node.socketAddresses());
                        }
                        catch (IgniteSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to unregister failed node address [node=" + node +
                                    ", err=" + e.getMessage() + ']');

                            onException("Failed to unregister failed node address [node=" + node +
                                ", err=" + e.getMessage() + ']', e);
                        }
                    }

                    topVer = ring.incrementTopologyVersion();

                    msg.topologyVersion(topVer);
                }
                else {
                    topVer = msg.topologyVersion();

                    assert topVer > 0 : "Topology version is empty for message: " + msg;

                    boolean b = ring.topologyVersion(topVer);

                    assert b : "Topology version has not been updated: [ring=" + ring + ", msg=" + msg +
                        ", lastMsg=" + lastMsg + ", spiState=" + spiStateCopy() + ']';

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                synchronized (mux) {
                    failedNodes.remove(node);

                    leavingNodes.remove(node);
                }

                notifyDiscovery(EVT_NODE_FAILED, topVer, node);

                stats.onNodeFailed();
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
            else {
                if (log.isDebugEnabled())
                    log.debug("Unable to send message across the ring (topology has no remote nodes): " + msg);

                U.closeQuiet(sock);
            }
        }

        /**
         * Processes status check message.
         *
         * @param msg Status check message.
         */
        private void processStatusCheckMessage(TcpDiscoveryStatusCheckMessage msg) {
            assert msg != null;

            UUID locNodeId = ignite.configuration().getNodeId();

            if (msg.failedNodeId() != null) {
                if (locNodeId.equals(msg.failedNodeId())) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (suspect node is local node).");

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() != null) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (local node is the sender of the status message).");

                    return;
                }

                if (isLocalNodeCoordinator() && ring.node(msg.creatorNodeId()) == null) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (creator node is not in topology).");

                    return;
                }
            }
            else {
                if (isLocalNodeCoordinator() && !locNodeId.equals(msg.creatorNodeId())) {
                    // Local node is real coordinator, it should respond and discard message.
                    if (ring.node(msg.creatorNodeId()) != null) {
                        // Sender is in topology, send message via ring.
                        msg.status(STATUS_OK);

                        sendMessageAcrossRing(msg);
                    }
                    else {
                        // Sender is not in topology, it should reconnect.
                        msg.status(STATUS_RECON);

                        try {
                            trySendMessageDirectly(msg.creatorNode(), msg);

                            if (log.isDebugEnabled())
                                log.debug("Responded to status check message " +
                                    "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']');
                        }
                        catch (IgniteSpiException e) {
                            if (e.hasCause(SocketException.class)) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Failed to respond to status check message (connection refused) " +
                                        "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']');
                                }

                                onException("Failed to respond to status check message (connection refused) " +
                                    "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']', e);
                            }
                            else {
                                if (pingNode(msg.creatorNode())) {
                                    // Node exists and accepts incoming connections.
                                    U.error(log, "Failed to respond to status check message " +
                                        "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']', e);
                                }
                                else if (log.isDebugEnabled()) {
                                    log.debug("Failed to respond to status check message (did the node stop?) " +
                                        "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']');
                                }
                            }
                        }
                    }

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null &&
                    U.currentTimeMillis() - locNode.lastUpdateTime() < hbFreq) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (local node receives updates).");

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null &&
                    spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Status check message discarded (local node is not connected to topology).");

                    return;
                }

                if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() != null) {
                    if (spiStateCopy() != CONNECTED)
                        return;

                    if (msg.status() == STATUS_OK) {
                        if (log.isDebugEnabled())
                            log.debug("Received OK status response from coordinator: " + msg);
                    }
                    else if (msg.status() == STATUS_RECON) {
                        U.warn(log, "Node is out of topology (probably, due to short-time network problems).");

                        notifyDiscovery(EVT_NODE_SEGMENTED, ring.topologyVersion(), locNode);

                        return;
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Status value was not updated in status response: " + msg);

                    // Discard the message.
                    return;
                }
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Processes regular heartbeat message.
         *
         * @param msg Heartbeat message.
         */
        private void processHeartbeatMessage(TcpDiscoveryHeartbeatMessage msg) {
            assert msg != null;

            UUID locNodeId = ignite.configuration().getNodeId();

            if (ring.node(msg.creatorNodeId()) == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message issued by unknown node [msg=" + msg +
                        ", ring=" + ring + ']');

                return;
            }

            if (isLocalNodeCoordinator() && !locNodeId.equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message issued by non-coordinator node: " + msg);

                return;
            }

            if (!isLocalNodeCoordinator() && locNodeId.equals(msg.creatorNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message issued by local node (node is no more coordinator): " +
                        msg);

                return;
            }

            if (locNodeId.equals(msg.creatorNodeId()) && !msg.hasMetrics(locNodeId) && msg.senderNodeId() != null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message that has made two passes: " + msg);

                return;
            }

            long tstamp = U.currentTimeMillis();

            if (spiStateCopy() == CONNECTED) {
                if (msg.hasMetrics()) {
                    for (Map.Entry<UUID, MetricsSet> e : msg.metrics().entrySet()) {
                        MetricsSet metricsSet = e.getValue();

                        updateMetrics(e.getKey(), metricsSet.metrics(), tstamp);

                        for (T2<UUID, ClusterMetrics> t : metricsSet.clientMetrics())
                            updateMetrics(t.get1(), t.get2(), tstamp);
                    }
                }
            }

            if (ring.hasRemoteNodes()) {
                if ((locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null ||
                    !msg.hasMetrics(locNodeId)) && spiStateCopy() == CONNECTED) {
                    // Message is on its first ring or just created on coordinator.
                    msg.setMetrics(locNodeId, metricsProvider.metrics());

                    for (Map.Entry<UUID, ClientMessageWorker> e : clientMsgWorkers.entrySet()) {
                        UUID nodeId = e.getKey();
                        ClusterMetrics metrics = e.getValue().metrics();

                        if (metrics != null)
                            msg.setClientMetrics(locNodeId, nodeId, metrics);

                        msg.addClientNodeId(nodeId);
                    }
                }
                else {
                    // Message is on its second ring.
                    msg.removeMetrics(locNodeId);

                    Collection<UUID> clientNodeIds = msg.clientNodeIds();

                    for (TcpDiscoveryNode clientNode : ring.clientNodes()) {
                        if (clientNode.visible()) {
                            if (clientNodeIds.contains(clientNode.id()))
                                clientNode.aliveCheck(maxMissedClientHbs);
                            else {
                                int aliveCheck = clientNode.decrementAliveCheck();

                                if (aliveCheck == 0 && isLocalNodeCoordinator()) {
                                    processNodeFailedMessage(new TcpDiscoveryNodeFailedMessage(locNodeId,
                                        clientNode.id(), clientNode.order()));
                                }
                            }
                        }
                    }
                }

                if (ring.hasRemoteNodes())
                    sendMessageAcrossRing(msg);
            }
            else {
                locNode.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), locNode);
            }
        }

        /**
         * @param nodeId Node ID.
         * @param metrics Metrics.
         * @param tstamp Timestamp.
         */
        private void updateMetrics(UUID nodeId, ClusterMetrics metrics, long tstamp) {
            assert nodeId != null;
            assert metrics != null;

            TcpDiscoveryNode node = ring.node(nodeId);

            if (node != null) {
                node.setMetrics(metrics);

                node.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), node);
            }
            else if (log.isDebugEnabled())
                log.debug("Received metrics from unknown node: " + nodeId);
        }

        /**
         * Processes discard message and discards previously registered pending messages.
         *
         * @param msg Discard message.
         */
        @SuppressWarnings("StatementWithEmptyBody")
        private void processDiscardMessage(TcpDiscoveryDiscardMessage msg) {
            assert msg != null;

            IgniteUuid msgId = msg.msgId();

            assert msgId != null;

            if (isLocalNodeCoordinator()) {
                if (!ignite.configuration().getNodeId().equals(msg.verifierNodeId()))
                    // Message is not verified or verified by former coordinator.
                    msg.verify(ignite.configuration().getNodeId());
                else
                    // Discard the message.
                    return;
            }

            if (msg.verified())
                pendingMsgs.discard(msgId);

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * @param msg Message.
         */
        private void processCustomMessage(TcpDiscoveryCustomEventMessage msg) {
            if (isLocalNodeCoordinator()) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    addMessage(new TcpDiscoveryDiscardMessage(getLocalNodeId(), msg.id()));

                    return;
                }

                msg.verify(getLocalNodeId());
                msg.topologyVersion(ring.topologyVersion());
            }

            if (msg.verified()) {
                DiscoverySpiListener lsnr = TcpDiscoverySpi.this.lsnr;

                TcpDiscoverySpiState spiState = spiStateCopy();

                Map<Long, Collection<ClusterNode>> hist;

                synchronized (mux) {
                    hist = new TreeMap<>(topHist);
                }

                Collection<ClusterNode> snapshot = hist.get(msg.topologyVersion());

                if (lsnr != null && (spiState == CONNECTED || spiState == DISCONNECTING))
                    lsnr.onDiscovery(DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT,
                        msg.topologyVersion(),
                        ring.node(msg.creatorNodeId()),
                        snapshot,
                        hist,
                        msg.message());
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }
    }

    /**
     * Thread that accepts incoming TCP connections.
     * <p>
     * Tcp server will call provided closure when accepts incoming connection.
     * From that moment server is no more responsible for the socket.
     */
    private class TcpServer extends IgniteSpiThread {
        /** Socket TCP server listens to. */
        private ServerSocket srvrSock;

        /** Port to listen. */
        private int port;

        /**
         * Constructor.
         *
         * @throws IgniteSpiException In case of error.
         */
        TcpServer() throws IgniteSpiException {
            super(ignite.name(), "tcp-disco-srvr", log);

            setPriority(threadPri);

            for (port = locPort; port < locPort + locPortRange; port++) {
                try {
                    srvrSock = new ServerSocket(port, 0, locHost);

                    break;
                }
                catch (IOException e) {
                    if (port < locPort + locPortRange - 1) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to bind to local port (will try next port within range) " +
                                "[port=" + port + ", localHost=" + locHost + ']');

                        onException("Failed to bind to local port. " +
                            "[port=" + port + ", localHost=" + locHost + ']', e);
                    }
                    else {
                        throw new IgniteSpiException("Failed to bind TCP server socket (possibly all ports in range " +
                            "are in use) [firstPort=" + locPort + ", lastPort=" + (locPort + locPortRange - 1) +
                            ", addr=" + locHost + ']', e);
                    }
                }
            }

            if (log.isInfoEnabled())
                log.info("Successfully bound to TCP port [port=" + port + ", localHost=" + locHost + ']');
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                while (!isInterrupted()) {
                    Socket sock = srvrSock.accept();

                    long tstamp = U.currentTimeMillis();

                    if (log.isDebugEnabled())
                        log.debug("Accepted incoming connection from addr: " + sock.getInetAddress());

                    SocketReader reader = new SocketReader(sock);

                    synchronized (mux) {
                        readers.add(reader);

                        reader.start();
                    }

                    stats.onServerSocketInitialized(U.currentTimeMillis() - tstamp);
                }
            }
            catch (IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Failed to accept TCP connection.", e);

                onException("Failed to accept TCP connection.", e);

                if (!isInterrupted()) {
                    if (U.isMacInvalidArgumentError(e))
                        U.error(log, "Failed to accept TCP connection\n\t" + U.MAC_INVALID_ARG_MSG, e);
                    else
                        U.error(log, "Failed to accept TCP connection.", e);
                }
            }
            finally {
                U.closeQuiet(srvrSock);
            }
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            super.interrupt();

            U.close(srvrSock, log);
        }
    }

    /**
     * Thread that reads messages from the socket created for incoming connections.
     */
    private class SocketReader extends IgniteSpiThread {
        /** Socket to read data from. */
        private final Socket sock;

        /** */
        private volatile UUID nodeId;

        /** */
        private volatile boolean client;

        /**
         * Constructor.
         *
         * @param sock Socket to read data from.
         */
        SocketReader(Socket sock) {
            super(ignite.name(), "tcp-disco-sock-reader", log);

            this.sock = sock;

            setPriority(threadPri);

            stats.onSocketReaderCreated();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            UUID locNodeId = ignite.configuration().getNodeId();

            try {
                InputStream in;

                try {
                    // Set socket options.
                    sock.setKeepAlive(true);
                    sock.setTcpNoDelay(true);

                    int timeout = sock.getSoTimeout();

                    sock.setSoTimeout((int)netTimeout);

                    in = new BufferedInputStream(sock.getInputStream());

                    byte[] buf = new byte[4];
                    int read = 0;

                    while (read < buf.length) {
                        int r = in.read(buf, read, buf.length - read);

                        if (r >= 0)
                            read += r;
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Failed to read magic header (too few bytes received) " +
                                    "[rmtAddr=" + sock.getRemoteSocketAddress() +
                                    ", locAddr=" + sock.getLocalSocketAddress() + ']');

                            LT.warn(log, null, "Failed to read magic header (too few bytes received) [rmtAddr=" +
                                sock.getRemoteSocketAddress() + ", locAddr=" + sock.getLocalSocketAddress() + ']');

                            return;
                        }
                    }

                    if (!Arrays.equals(buf, U.IGNITE_HEADER)) {
                        if (log.isDebugEnabled())
                            log.debug("Unknown connection detected (is some other software connecting to " +
                                "this Ignite port?) " +
                                "[rmtAddr=" + sock.getRemoteSocketAddress() +
                                ", locAddr=" + sock.getLocalSocketAddress() + ']');

                        LT.warn(log, null, "Unknown connection detected (is some other software connecting to " +
                            "this Ignite port?) [rmtAddr=" + sock.getRemoteSocketAddress() +
                            ", locAddr=" + sock.getLocalSocketAddress() + ']');

                        return;
                    }

                    // Restore timeout.
                    sock.setSoTimeout(timeout);

                    TcpDiscoveryAbstractMessage msg = readMessage(sock, in, netTimeout);

                    // Ping.
                    if (msg instanceof TcpDiscoveryPingRequest) {
                        TcpDiscoveryPingRequest req = (TcpDiscoveryPingRequest)msg;

                        TcpDiscoveryPingResponse res = new TcpDiscoveryPingResponse(locNodeId);

                        if (req.clientNodeId() != null)
                            res.clientExists(clientMsgWorkers.containsKey(req.clientNodeId()));

                        writeToSocket(sock, res);

                        return;
                    }

                    // Handshake.
                    TcpDiscoveryHandshakeRequest req = (TcpDiscoveryHandshakeRequest)msg;

                    UUID nodeId = req.creatorNodeId();
                    boolean client = req.client();

                    this.nodeId = nodeId;
                    this.client = client;

                    TcpDiscoveryHandshakeResponse res =
                        new TcpDiscoveryHandshakeResponse(locNodeId, locNode.internalOrder());

                    writeToSocket(sock, res);

                    // It can happen if a remote node is stopped and it has a loopback address in the list of addresses,
                    // the local node sends a handshake request message on the loopback address, so we get here.
                    if (locNodeId.equals(nodeId)) {
                        assert !client;

                        if (log.isDebugEnabled())
                            log.debug("Handshake request from local node: " + req);

                        return;
                    }

                    if (client) {
                        if (log.isDebugEnabled())
                            log.debug("Created client message worker [locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ", sock=" + sock + ']');

                        ClientMessageWorker clientMsgWrk = new ClientMessageWorker(sock, nodeId);

                        clientMsgWrk.start();

                        clientMsgWorkers.put(nodeId, clientMsgWrk);
                    }

                    if (log.isDebugEnabled())
                        log.debug("Initialized connection with remote node [nodeId=" + nodeId +
                            ", client=" + client + ']');

                    if (debugMode)
                        debugLog("Initialized connection with remote node [nodeId=" + nodeId +
                            ", client=" + client + ']');
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

                    if (X.hasCause(e, ObjectStreamException.class) || !sock.isClosed()) {
                        if (U.isMacInvalidArgumentError(e))
                            LT.error(log, e, "Failed to initialize connection [sock=" + sock + "]\n\t" +
                                U.MAC_INVALID_ARG_MSG);
                        else
                            LT.error(log, e, "Failed to initialize connection [sock=" + sock + ']');
                    }

                    onException("Caught exception on handshake [err=" + e + ", sock=" + sock + ']', e);

                    return;
                }
                catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

                    onException("Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

                    if (e.hasCause(SocketTimeoutException.class))
                        LT.warn(log, null, "Socket operation timed out on handshake " +
                            "(consider increasing 'networkTimeout' configuration property) " +
                            "[netTimeout=" + netTimeout + ']');

                    else if (e.hasCause(ClassNotFoundException.class))
                        LT.warn(log, null, "Failed to read message due to ClassNotFoundException " +
                            "(make sure same versions of all classes are available on all nodes) " +
                            "[rmtAddr=" + sock.getRemoteSocketAddress() +
                            ", err=" + X.cause(e, ClassNotFoundException.class).getMessage() + ']');

                    // Always report marshalling problems.
                    else if (e.hasCause(ObjectStreamException.class) ||
                        (!sock.isClosed() && !e.hasCause(IOException.class)))
                        LT.error(log, e, "Failed to initialize connection [sock=" + sock + ']');

                    return;
                }

                while (!isInterrupted()) {
                    try {
                        TcpDiscoveryAbstractMessage msg = marsh.unmarshal(in, U.gridClassLoader());

                        UUID destClientNodeId = msg.destinationClientNodeId();

                        if (destClientNodeId != null) {
                            ClientMessageWorker wrk = clientMsgWorkers.get(destClientNodeId);

                            if (wrk != null) {
                                msg.senderNodeId(locNodeId);

                                wrk.addMessage(msg);

                                writeToSocket(sock, RES_OK);
                            }
                            else if (log.isDebugEnabled())
                                log.debug("Discarding routed message because client has already left: " + msg);

                            continue;
                        }

                        msg.senderNodeId(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Message has been received: " + msg);

                        stats.onMessageReceived(msg);

                        if (debugMode && recordable(msg))
                            debugLog("Message has been received: " + msg);

                        if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                            TcpDiscoveryJoinRequestMessage req = (TcpDiscoveryJoinRequestMessage)msg;

                            if (!req.responded()) {
                                boolean ok = processJoinRequestMessage(req);

                                if (client && ok)
                                    continue;
                                else
                                    // Direct join request - no need to handle this socket anymore.
                                    break;
                            }
                        }
                        else if (msg instanceof TcpDiscoveryClientReconnectMessage) {
                            if (client) {
                                TcpDiscoverySpiState state = spiStateCopy();

                                if (state == CONNECTED) {
                                    writeToSocket(sock, RES_OK);

                                    msgWorker.addMessage(msg);

                                    continue;
                                }
                                else {
                                    writeToSocket(sock, RES_CONTINUE_JOIN);

                                    break;
                                }
                            }
                        }
                        else if (msg instanceof TcpDiscoveryDuplicateIdMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = DUPLICATE_ID;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Duplicate ID message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryAuthFailedMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = AUTH_FAILED;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Auth failed message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryCheckFailedMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = CHECK_FAILED;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Check failed message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

                            continue;
                        }
                        else if (msg instanceof TcpDiscoveryLoopbackProblemMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            TcpDiscoverySpiState state = null;

                            synchronized (mux) {
                                if (spiState == CONNECTING) {
                                    joinRes.set(msg);

                                    spiState = LOOPBACK_PROBLEM;

                                    mux.notifyAll();
                                }
                                else {
                                    ignored = true;

                                    state = spiState;
                                }
                            }

                            if (ignored && log.isDebugEnabled())
                                log.debug("Loopback problem message has been ignored [msg=" + msg +
                                    ", spiState=" + state + ']');

                            continue;
                        }

                        msgWorker.addMessage(msg);

                        // Send receipt back.
                        if (!client)
                            writeToSocket(sock, RES_OK);
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Caught exception on message read [sock=" + sock +
                                ", locNodeId=" + locNodeId + ", rmtNodeId=" + nodeId + ']', e);

                        onException("Caught exception on message read [sock=" + sock +
                            ", locNodeId=" + locNodeId + ", rmtNodeId=" + nodeId + ']', e);

                        if (isInterrupted() || sock.isClosed())
                            return;

                        if (e.hasCause(ClassNotFoundException.class))
                            LT.warn(log, null, "Failed to read message due to ClassNotFoundException " +
                                "(make sure same versions of all classes are available on all nodes) " +
                                "[rmtNodeId=" + nodeId +
                                ", err=" + X.cause(e, ClassNotFoundException.class).getMessage() + ']');

                        // Always report marshalling errors.
                        boolean err = e.hasCause(ObjectStreamException.class) ||
                            (nodeAlive(nodeId) && spiStateCopy() == CONNECTED && !X.hasCause(e, IOException.class));

                        if (err)
                            LT.error(log, e, "Failed to read message [sock=" + sock + ", locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ']');

                        return;
                    }
                    catch (IOException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Caught exception on message read [sock=" + sock + ", locNodeId=" + locNodeId +
                                ", rmtNodeId=" + nodeId + ']', e);

                        if (isInterrupted() || sock.isClosed())
                            return;

                        // Always report marshalling errors (although it is strange here).
                        boolean err = X.hasCause(e, ObjectStreamException.class) ||
                            (nodeAlive(nodeId) && spiStateCopy() == CONNECTED);

                        if (err)
                            LT.error(log, e, "Failed to send receipt on message [sock=" + sock +
                                ", locNodeId=" + locNodeId + ", rmtNodeId=" + nodeId + ']');

                        onException("Caught exception on message read [sock=" + sock + ", locNodeId=" + locNodeId +
                            ", rmtNodeId=" + nodeId + ']', e);

                        return;
                    }
                }
            }
            finally {
                if (client) {
                    if (log.isDebugEnabled())
                        log.debug("Client connection failed [sock=" + sock + ", locNodeId=" + locNodeId +
                            ", rmtNodeId=" + nodeId + ']');

                    U.interrupt(clientMsgWorkers.remove(nodeId));
                }

                U.closeQuiet(sock);
            }
        }

        /**
         * @param nodeId Node ID.
         * @return {@code True} if node is in the ring and is not being removed from.
         */
        private boolean nodeAlive(UUID nodeId) {
            // Is node alive or about to be removed from the ring?
            TcpDiscoveryNode node = ring.node(nodeId);

            boolean nodeAlive = node != null && node.visible();

            if (nodeAlive) {
                synchronized (mux) {
                    nodeAlive = !F.transform(failedNodes, F.node2id()).contains(nodeId) &&
                        !F.transform(leavingNodes, F.node2id()).contains(nodeId);
                }
            }

            return nodeAlive;
        }

        /**
         * @param msg Join request message.
         * @return Whether connection was successful.
         * @throws IOException If IO failed.
         */
        @SuppressWarnings({"IfMayBeConditional"})
        private boolean processJoinRequestMessage(TcpDiscoveryJoinRequestMessage msg) throws IOException {
            assert msg != null;
            assert !msg.responded();

            TcpDiscoverySpiState state = spiStateCopy();

            if (state == CONNECTED) {
                writeToSocket(sock, RES_OK);

                if (log.isDebugEnabled())
                    log.debug("Responded to join request message [msg=" + msg + ", res=" + RES_OK + ']');

                msg.responded(true);

                msgWorker.addMessage(msg);

                return true;
            }
            else {
                stats.onMessageProcessingStarted(msg);

                Integer res;

                SocketAddress rmtAddr = sock.getRemoteSocketAddress();

                if (state == CONNECTING) {
                    if (noResAddrs.contains(rmtAddr) ||
                        ignite.configuration().getNodeId().compareTo(msg.creatorNodeId()) < 0)
                        // Remote node node has not responded to join request or loses UUID race.
                        res = RES_WAIT;
                    else
                        // Remote node responded to join request and wins UUID race.
                        res = RES_CONTINUE_JOIN;
                }
                else
                    // Local node is stopping. Remote node should try next one.
                    res = RES_CONTINUE_JOIN;

                writeToSocket(sock, res);

                if (log.isDebugEnabled())
                    log.debug("Responded to join request message [msg=" + msg + ", res=" + res + ']');

                fromAddrs.addAll(msg.node().socketAddresses());

                stats.onMessageProcessingFinished(msg);

                return false;
            }
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            super.interrupt();

            U.closeQuiet(sock);
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            super.cleanup();

            U.closeQuiet(sock);

            synchronized (mux) {
                readers.remove(this);
            }

            stats.onSocketReaderRemoved();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Socket reader [id=" + getId() + ", name=" + getName() + ", nodeId=" + nodeId + ']';
        }
    }

    /**
     * SPI Statistics printer.
     */
    private class StatisticsPrinter extends IgniteSpiThread {
        /**
         * Constructor.
         */
        StatisticsPrinter() {
            super(ignite.name(), "tcp-disco-stats-printer", log);

            assert statsPrintFreq > 0;

            assert log.isInfoEnabled();

            setPriority(threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Statistics printer has been started.");

            while (!isInterrupted()) {
                Thread.sleep(statsPrintFreq);

                printStatistics();
            }
        }
    }

    /**
     */
    private class ClientMessageWorker extends MessageWorkerAdapter {
        /** Node ID. */
        private final UUID nodeId;

        /** Socket. */
        private final Socket sock;

        /** Current client metrics. */
        private volatile ClusterMetrics metrics;

        /**
         * @param sock Socket.
         * @param nodeId Node ID.
         */
        protected ClientMessageWorker(Socket sock, UUID nodeId) {
            super("tcp-disco-client-message-worker");

            this.sock = sock;
            this.nodeId = nodeId;
        }

        /**
         * @return Current client metrics.
         */
        ClusterMetrics metrics() {
            return metrics;
        }

        /** {@inheritDoc} */
        @Override void addMessage(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryHeartbeatMessage) {
                TcpDiscoveryHeartbeatMessage hbMsg = (TcpDiscoveryHeartbeatMessage)msg;

                if (hbMsg.creatorNodeId().equals(nodeId)) {
                    metrics = hbMsg.metrics().get(nodeId).metrics();

                    hbMsg.removeMetrics(nodeId);

                    assert !hbMsg.hasMetrics();
                }
            }

            super.addMessage(msg);
        }

        /** {@inheritDoc} */
        @Override protected void processMessage(TcpDiscoveryAbstractMessage msg) {
            try {
                assert msg.verified() : msg;

                if (log.isDebugEnabled())
                    log.debug("Redirecting message to client [sock=" + sock + ", locNodeId="
                        + ignite.configuration().getNodeId() + ", rmtNodeId=" + nodeId + ", msg=" + msg + ']');

                try {
                    prepareNodeAddedMessage(msg, nodeId, null, null);

                    writeToSocket(sock, msg);
                }
                finally {
                    clearNodeAddedMessage(msg);
                }
            }
            catch (IgniteCheckedException | IOException e) {
                if (log.isDebugEnabled())
                    U.error(log, "Client connection failed [sock=" + sock + ", locNodeId="
                        + ignite.configuration().getNodeId() + ", rmtNodeId=" + nodeId + ", msg=" + msg + ']', e);

                onException("Client connection failed [sock=" + sock + ", locNodeId="
                    + ignite.configuration().getNodeId() + ", rmtNodeId=" + nodeId + ", msg=" + msg + ']', e);

                U.interrupt(clientMsgWorkers.remove(nodeId));

                U.closeQuiet(sock);
            }
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            super.cleanup();

            U.closeQuiet(sock);
        }
    }
}
