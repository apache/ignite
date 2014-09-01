/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.jdk.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.internal.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.jdbc.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.multicast.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.sharedfs.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.discovery.tcp.messages.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.jdbc.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.sharedfs.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.vm.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.gridgain.grid.spi.GridPortProtocol.*;
import static org.gridgain.grid.spi.discovery.tcp.internal.GridTcpDiscoverySpiState.*;
import static org.gridgain.grid.spi.discovery.tcp.messages.GridTcpDiscoveryStatusCheckMessage.*;

/**
 * Discovery SPI implementation that uses TCP/IP for node discovery.
 * <p>
 * Nodes are organized in ring. So almost all network exchange (except few cases) is
 * done across it.
 * <p>
 * At startup SPI tries to send messages to random IP taken from
 * {@link GridTcpDiscoveryIpFinder} about self start (stops when send succeeds)
 * and then this info goes to coordinator. When coordinator processes join request
 * and issues node added messages and all other nodes then receive info about new node.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>IP finder to share info about nodes IP addresses
 * (see {@link #setIpFinder(GridTcpDiscoveryIpFinder)}).
 * See the following IP finder implementations for details on configuration:
 * <ul>
 * <li>{@link GridTcpDiscoverySharedFsIpFinder}</li>
 * <li>{@gglink org.gridgain.grid.spi.discovery.tcp.ipfinder.s3.GridTcpDiscoveryS3IpFinder}</li>
 * <li>{@link GridTcpDiscoveryJdbcIpFinder}</li>
 * <li>{@link GridTcpDiscoveryVmIpFinder}</li>
 * <li>{@link GridTcpDiscoveryMulticastIpFinder} - default</li>
 * </ul>
 * </li>
 * </ul>
 * <ul>
 * <li>Metrics store (see {@link #setMetricsStore(GridTcpDiscoveryMetricsStore)})</li>
 * See the following metrics store implementations for details on configuration:
 * <ul>
 * <li>{@link GridTcpDiscoverySharedFsMetricsStore}</li>
 * <li>{@gglink org.gridgain.grid.spi.discovery.tcp.metricsstore.s3.GridTcpDiscoveryS3MetricsStore}</li>
 * <li>{@link GridTcpDiscoveryJdbcMetricsStore}</li>
 * <li>{@link GridTcpDiscoveryVmMetricsStore}</li>
 * </ul>
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
 * <li>IP finder and Metrics Store clean frequency (see {@link #setStoresCleanFrequency(long)})</li>
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
 * GridGain.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridTcpDiscoverySpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="discoverySpi"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi"&gt;
 *                 &lt;property name="ipFinder"&gt;
 *                     &lt;bean class="org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder" /&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridDiscoverySpi
 */
@GridSpiMultipleInstancesSupport(true)
@GridDiscoverySpiOrderSupport(true)
@GridDiscoverySpiReconnectSupport(true)
@GridDiscoverySpiHistorySupport(true)
public class GridTcpDiscoverySpi extends GridSpiAdapter implements GridDiscoverySpi, GridTcpDiscoverySpiMBean {
    /** Default port to listen (value is <tt>47500</tt>). */
    public static final int DFLT_PORT = 47500;

    /** Default local port range (value is <tt>100</tt>). */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default network timeout in milliseconds (value is <tt>5,000ms</tt>). */
    public static final long DFLT_NETWORK_TIMEOUT = 5000;

    /** Default socket operations timeout in milliseconds (value is <tt>2,000ms</tt>). */
    public static final long DFLT_SOCK_TIMEOUT = 2000;

    /** Default timeout for receiving message acknowledgement in milliseconds (value is <tt>5,000ms</tt>). */
    public static final long DFLT_ACK_TIMEOUT = 5000;

    /** Default timeout for joining topology (value is <tt>0</tt>). */
    public static final long DFLT_JOIN_TIMEOUT = 0;

    /** Default reconnect attempts count (value is <tt>10</tt>). */
    public static final int DFLT_RECONNECT_CNT = 10;

    /** Default heartbeat messages issuing frequency (value is <tt>2,000ms</tt>). */
    public static final long DFLT_HEARTBEAT_FREQ = 2000;

    /** Default max heartbeats count node can miss without initiating status check (value is <tt>1</tt>). */
    public static final int DFLT_MAX_MISSED_HEARTBEATS = 1;

    /** Default value for thread priority (value is <tt>10</tt>). */
    public static final int DFLT_THREAD_PRI = 10;

    /** Default stores (IP finder clean and metrics store) frequency in milliseconds (value is <tt>60,000ms</tt>). */
    public static final long DFLT_STORES_CLEAN_FREQ = 60 * 1000;

    /** Default statistics print frequency in milliseconds (value is <tt>0ms</tt>). */
    public static final long DFLT_STATS_PRINT_FREQ = 0;

    /** Maximum ack timeout value for receiving message acknowledgement in milliseconds (value is <tt>600,000ms</tt>). */
    public static final long DFLT_MAX_ACK_TIMEOUT = 10 * 60 * 1000;

    /** Default size of topology snapshots history. */
    public static final int DFLT_TOP_HISTORY_SIZE = 1000;

    /** Response OK. */
    private static final int RES_OK = 1;

    /** Response CONTINUE JOIN. */
    private static final int RES_CONTINUE_JOIN = 100;

    /** Response WAIT. */
    private static final int RES_WAIT = 200;

    /** Predicate to filter visible nodes. */
    private static final GridPredicate<GridTcpDiscoveryNode> VISIBLE_NODES = new P1<GridTcpDiscoveryNode>() {
        @Override public boolean apply(GridTcpDiscoveryNode node) {
            return node.visible();
        }
    };

    /** Node attribute that is mapped to node's external addresses (value is <tt>disc.tcp.ext-addrs</tt>). */
    public static final String ATTR_EXT_ADDRS = "disc.tcp.ext-addrs";

    /** Address resolver. */
    private GridAddressResolver addrRslvr;

    /** Local port which node uses. */
    private int locPort = DFLT_PORT;

    /** Local port range. */
    private int locPortRange = DFLT_PORT_RANGE;

    /** Statistics print frequency. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized", "RedundantFieldInitialization"})
    private long statsPrintFreq = DFLT_STATS_PRINT_FREQ;

    /** Network timeout. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private long netTimeout = DFLT_NETWORK_TIMEOUT;

    /** Socket operations timeout. */
    private long sockTimeout = DFLT_SOCK_TIMEOUT;

    /** Message acknowledgement timeout. */
    private long ackTimeout = DFLT_ACK_TIMEOUT;

    /** Maximum message acknowledgement timeout. */
    private long maxAckTimeout = DFLT_MAX_ACK_TIMEOUT;

    /** Join timeout. */
    @SuppressWarnings("RedundantFieldInitialization")
    private long joinTimeout = DFLT_JOIN_TIMEOUT;

    /** Heartbeat messages issuing frequency. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private long hbFreq = DFLT_HEARTBEAT_FREQ;

    /** Max heartbeats count node can miss without initiating status check. */
    private int maxMissedHbs = DFLT_MAX_MISSED_HEARTBEATS;

    /** Thread priority for all threads started by SPI. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private int threadPri = DFLT_THREAD_PRI;

    /** Stores clean frequency. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private long storesCleanFreq = DFLT_STORES_CLEAN_FREQ;

    /** Reconnect attempts count. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private int reconCnt = DFLT_RECONNECT_CNT;

    /** Size of topology snapshots history. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private int topHistSize = DFLT_TOP_HISTORY_SIZE;

    /** Name of the grid. */
    @GridNameResource
    private String gridName;

    /** Grid logger. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridLoggerResource
    private GridLogger log;

    /** Local node Id. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridLocalNodeIdResource
    private UUID locNodeId;

    /** Marshaller. */
    private final GridMarshaller marsh = new GridJdkMarshaller();

    /** Grid marshaller. */
    @GridMarshallerResource
    private GridMarshaller gridMarsh;

    /**
     * Local node (although, it may be reassigned on segmentation, it may be non-volatile,
     * since all internal threads are restarted).
     */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private GridTcpDiscoveryNode locNode;

    /** Internal and external addresses of local node. */
    private Collection<InetSocketAddress> locNodeAddrs;

    /** Local IP address. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private String locAddr;

    /** Complex variable that represents this node IP address. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private InetAddress locHost;

    /** Grid discovery listener. */
    @GridToStringExclude
    private volatile GridDiscoverySpiListener lsnr;

    /** Discovery data exchange handler. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    @GridToStringExclude
    private GridDiscoverySpiDataExchange exchange;

    /** Metrics provider. */
    private GridDiscoveryMetricsProvider metricsProvider;

    /** Local node attributes. */
    private Map<String, Object> nodeAttrs;

    /** Local node version. */
    @GridToStringExclude
    private GridProductVersion nodeVer;

    /** IP finder. */
    private GridTcpDiscoveryIpFinder ipFinder;

    /** Metrics store. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private GridTcpDiscoveryMetricsStore metricsStore;

    /** Nodes ring. */
    @GridToStringExclude
    private final GridTcpDiscoveryNodesRing ring = new GridTcpDiscoveryNodesRing();

    /** Topology snapshots history. */
    private final SortedMap<Long, Collection<GridNode>> topHist = new TreeMap<>();

    /** Discovery state. */
    private GridTcpDiscoverySpiState spiState = DISCONNECTED;

    /** Socket readers. */
    private final Collection<SocketReader> readers = new LinkedList<>();

    /** TCP server for discovery SPI. */
    private TcpServer tcpSrvr;

    /** Message worker. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private MessageWorker msgWorker;

    /** Metrics sender. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private HeartbeatsSender hbsSnd;

    /** Status checker. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private CheckStatusSender chkStatusSnd;

    /** Metrics update notifier. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private MetricsUpdateNotifier metricsUpdateNtf;

    /** Stores cleaner. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private StoresCleaner storesCleaner;

    /** Statistics printer thread. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private StatisticsPrinter statsPrinter;

    /** Socket timeout worker. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private SocketTimeoutWorker sockTimeoutWorker;

    /** Failed nodes (but still in topology). */
    private Collection<GridTcpDiscoveryNode> failedNodes = new HashSet<>();

    /** Leaving nodes (but still in topology). */
    private Collection<GridTcpDiscoveryNode> leavingNodes = new HashSet<>();

    /** Statistics. */
    @GridToStringExclude
    private final GridTcpDiscoveryStatistics stats = new GridTcpDiscoveryStatistics();

    /** If non-shared IP finder is used this flag shows whether IP finder contains local address. */
    private boolean ipFinderHasLocAddr;

    /** Addresses that do not respond during join requests send (for resolving concurrent start). */
    private final Collection<SocketAddress> noResAddrs = new GridConcurrentHashSet<>();

    /** Addresses that incoming join requests send were send from (for resolving concurrent start). */
    private final Collection<SocketAddress> fromAddrs = new GridConcurrentHashSet<>();

    /** SPI reconnect flag to filter initial node connected event. */
    private volatile boolean recon;

    /** Response on join request from coordinator (in case of duplicate ID or auth failure). */
    private final GridTuple<GridTcpDiscoveryAbstractMessage> joinRes = F.t1();

    /** Context initialization latch. */
    @GridToStringExclude
    private final CountDownLatch ctxInitLatch = new CountDownLatch(1);

    /** Node authenticator. */
    private GridDiscoverySpiNodeAuthenticator nodeAuth;

    /** Mutex. */
    private final Object mux = new Object();

    /** Start time of the very first grid node. */
    private volatile long gridStartTime;

    /** Debug mode. */
    private boolean debugMode;

    /** Debug messages history. */
    private int debugMsgHist = 128;

    /** Received messages. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private ConcurrentLinkedDeque<String> debugLog;

    /**
     * Sets local host IP address that discovery SPI uses.
     * <p>
     * If not provided, by default a first found non-loopback address
     * will be used. If there is no non-loopback address available,
     * then {@link InetAddress#getLocalHost()} will be used.
     *
     * @param locAddr IP address.
     */
    @GridSpiConfiguration(optional = true)
    @GridLocalHostResource
    public void setLocalAddress(String locAddr) {
        // Injection should not override value already set by Spring or user.
        if (this.locAddr == null)
            this.locAddr = locAddr;
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
    @GridSpiConfiguration(optional = true)
    @GridAddressResolverResource
    public void setAddressResolver(GridAddressResolver addrRslvr) {
        // Injection should not override value already set by Spring or user.
        if (this.addrRslvr == null)
            this.addrRslvr = addrRslvr;
    }

    /**
     * Gets address resolver.
     *
     * @return Address resolver.
     */
    public GridAddressResolver getAddressResolver() {
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
    @GridSpiConfiguration(optional = true)
    public void setReconnectCount(int reconCnt) {
        this.reconCnt = reconCnt;
    }

    /** {@inheritDoc} */
    @Override public long getNetworkTimeout() {
        return netTimeout;
    }

    /**
     * Sets size of topology snapshots history. Specified size should be greater than or equal to default size
     * {@link #DFLT_TOP_HISTORY_SIZE}.
     *
     * @param topHistSize Size of topology snapshots history.
     */
    @GridSpiConfiguration(optional = true)
    public void setTopHistorySize(int topHistSize) {
        if (topHistSize < DFLT_TOP_HISTORY_SIZE) {
            U.warn(log, "Topology history size should be greater than or equal to default size. " +
                "Specified size will not be set [curSize=" + this.topHistSize + ", specifiedSize=" + topHistSize +
                ", defaultSize=" + DFLT_TOP_HISTORY_SIZE + ']');

            return;
        }

        this.topHistSize = topHistSize;
    }

    /**
     * @return Size of topology snapshots history.
     */
    public long getTopHistorySize() {
        return topHistSize;
    }

    /**
     * Sets maximum network timeout to use for network operations.
     * <p>
     * If not specified, default is {@link #DFLT_NETWORK_TIMEOUT}.
     *
     * @param netTimeout Network timeout.
     */
    @GridSpiConfiguration(optional = true)
    public void setNetworkTimeout(long netTimeout) {
        this.netTimeout = netTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getAckTimeout() {
        return ackTimeout;
    }

    /**
     * Sets timeout for receiving acknowledgement for sent message.
     * <p>
     * If acknowledgement is not received within this timeout, sending is considered as failed
     * and SPI tries to repeat message sending.
     * <p>
     * If not specified, default is {@link #DFLT_ACK_TIMEOUT}.
     *
     * @param ackTimeout Acknowledgement timeout.
     */
    @GridSpiConfiguration(optional = true)
    public void setAckTimeout(long ackTimeout) {
        this.ackTimeout = ackTimeout;
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
    @GridSpiConfiguration(optional = true)
    public void setMaxAckTimeout(long maxAckTimeout) {
        this.maxAckTimeout = maxAckTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getSocketTimeout() {
        return sockTimeout;
    }

    /**
     * Sets socket operations timeout. This timeout is used to limit connection time and
     * write-to-socket time.
     * <p>
     * Note that when running GridGain on Amazon EC2, socket timeout must be set to a value
     * significantly greater than the default (e.g. to {@code 30000}).
     * <p>
     * If not specified, default is {@link #DFLT_SOCK_TIMEOUT}.
     *
     * @param sockTimeout Socket connection timeout.
     */
    @GridSpiConfiguration(optional = true)
    public void setSocketTimeout(long sockTimeout) {
        this.sockTimeout = sockTimeout;
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
     * @see GridTcpDiscoveryIpFinder#isShared()
     */
    @GridSpiConfiguration(optional = true)
    public void setJoinTimeout(long joinTimeout) {
        this.joinTimeout = joinTimeout;
    }

    /** {@inheritDoc} */
    @Override public int getLocalPort() {
        GridTcpDiscoveryNode locNode0 = locNode;

        return locNode0 != null ? locNode0.discoveryPort() : 0;
    }

    /**
     * Sets local port to listen to.
     * <p>
     * If not specified, default is {@link #DFLT_PORT}.
     *
     * @param locPort Local port to bind.
     */
    @GridSpiConfiguration(optional = true)
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
    @GridSpiConfiguration(optional = true)
    public void setLocalPortRange(int locPortRange) {
        this.locPortRange = locPortRange;
    }

    /** {@inheritDoc} */
    @Override public long getHeartbeatFrequency() {
        return hbFreq;
    }

    /**
     * Sets delay between issuing of heartbeat messages. SPI sends heartbeat messages
     * in configurable time interval to other nodes to notify them about its state.
     * <p>
     * If not provided, default value is {@link #DFLT_HEARTBEAT_FREQ}.
     *
     * @param hbFreq Heartbeat frequency in milliseconds.
     */
    @GridSpiConfiguration(optional = true)
    public void setHeartbeatFrequency(long hbFreq) {
        this.hbFreq = hbFreq;
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
    @GridSpiConfiguration(optional = true)
    public void setMaxMissedHeartbeats(int maxMissedHbs) {
        this.maxMissedHbs = maxMissedHbs;
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
    @GridSpiConfiguration(optional = true)
    public void setStatisticsPrintFrequency(long statsPrintFreq) {
        this.statsPrintFreq = statsPrintFreq;
    }

    /**
     * Gets IP finder for IP addresses sharing and storing.
     *
     * @return IP finder for IP addresses sharing and storing.
     */
    public GridTcpDiscoveryIpFinder getIpFinder() {
        return ipFinder;
    }

    /**
     * Sets IP finder for IP addresses sharing and storing.
     * <p>
     * If not provided {@link GridTcpDiscoveryMulticastIpFinder} will be used by default.
     *
     * @param ipFinder IP finder.
     */
    @GridSpiConfiguration(optional = true)
    public void setIpFinder(GridTcpDiscoveryIpFinder ipFinder) {
        this.ipFinder = ipFinder;
    }

    /** {@inheritDoc} */
    @Override public int getThreadPriority() {
        return threadPri;
    }

    /**
     * Sets thread priority. All threads within SPI will be started with it.
     * <p>
     * If not provided, default value is {@link #DFLT_THREAD_PRI}
     *
     * @param threadPri Thread priority.
     */
    @GridSpiConfiguration(optional = true)
    public void setThreadPriority(int threadPri) {
        this.threadPri = threadPri;
    }

    /** {@inheritDoc} */
    @Override public long getStoresCleanFrequency() {
        return storesCleanFreq;
    }

    /**
     * Sets stores (IP finder and metrics store) clean frequency in milliseconds.
     * <p>
     * If not provided, default value is {@link #DFLT_STORES_CLEAN_FREQ}
     *
     * @param storesCleanFreq Stores clean frequency.
     */
    @GridSpiConfiguration(optional = true)
    public void setStoresCleanFrequency(long storesCleanFreq) {
        this.storesCleanFreq = storesCleanFreq;
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
    @Override public String getIpFinderFormatted() {
        return ipFinder.toString();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String getMetricsStoreFormatted() {
        return metricsStore != null ? metricsStore.toString() : null;
    }

    /** {@inheritDoc} */
    @Override public int getMessageWorkerQueueSize() {
        return msgWorker.queue.size();
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
        GridTcpDiscoveryNode crd = resolveCoordinator();

        return crd != null ? crd.id() : null;
    }

    /**
     * Sets metrics store.
     * <p>
     * If provided, SPI does not send metrics across the ring and uses metrics
     * store to exchange metrics. It is recommended to provide metrics store when
     * working with large topologies.
     *
     * @param metricsStore Metrics store.
     */
    @GridSpiConfiguration(optional = true)
    public void setMetricsStore(GridTcpDiscoveryMetricsStore metricsStore) {
        this.metricsStore = metricsStore;
    }

    /** {@inheritDoc} */
    @Override public GridNode getLocalNode() {
        return locNode;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNode getNode(UUID nodeId) {
        assert nodeId != null;

        UUID locNodeId0 = locNodeId;

        if (locNodeId0 != null && locNodeId0.equals(nodeId))
            // Return local node directly.
            return locNode;

        GridTcpDiscoveryNode node = ring.node(nodeId);

        if (node != null && !node.visible())
            return null;

        return node;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> getRemoteNodes() {
        return new ArrayList<GridNode>(F.view(ring.remoteNodes(), VISIBLE_NODES));
    }

    /** {@inheritDoc} */
    @Override public void setListener(GridDiscoverySpiListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public void setDataExchange(GridDiscoverySpiDataExchange exchange) {
        this.exchange = exchange;
    }

    /** {@inheritDoc} */
    @Override public void setMetricsProvider(GridDiscoveryMetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    /** {@inheritDoc} */
    @Override public void setNodeAttributes(Map<String, Object> attrs, GridProductVersion ver) {
        assert nodeAttrs == null;
        assert nodeVer == null;

        if (log.isDebugEnabled()) {
            log.debug("Node attributes to set: " + attrs);
            log.debug("Node version to set: " + ver);
        }

        nodeAttrs = attrs;
        nodeVer = ver;
    }

    /** {@inheritDoc} */
    @Override public Collection<Object> injectables() {
        Collection<Object> res = new LinkedList<>();

        if (metricsStore != null)
            res.add(metricsStore);

        if (ipFinder != null)
            res.add(ipFinder);

        return res;
    }

    private static volatile GridTcpDiscoverySpi spi1;
    private static volatile GridTcpDiscoverySpi spi2;

    private static final GridBoundedConcurrentLinkedHashSet<GridTcpDiscoverySpi> spis =
        new GridBoundedConcurrentLinkedHashSet<>(24);

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        if (spi1 == null)
            spi1 = this;
        else if (spi2 == null)
            spi2 = this;
        else
            spis.add(this);

        spiStart0(false);
    }

    /**
     * Starts or restarts SPI after stop (to reconnect).
     *
     * @param restart {@code True} if SPI is restarted after stop.
     * @throws GridSpiException If failed.
     */
    private void spiStart0(boolean restart) throws GridSpiException {
        if (!restart)
            // It is initial start.
            onSpiStart();

        synchronized (mux) {
            spiState = DISCONNECTED;
        }

        if (debugMode) {
            if (!log.isInfoEnabled())
                throw new GridSpiException("Info log level should be enabled for TCP discovery to work " +
                    "in debug mode.");

            debugLog = new ConcurrentLinkedDeque<>();
        }

        // Clear addresses collections.
        fromAddrs.clear();
        noResAddrs.clear();

        sockTimeoutWorker = new SocketTimeoutWorker();
        sockTimeoutWorker.start();

        msgWorker = new MessageWorker();
        msgWorker.start();

        tcpSrvr = new TcpServer();

        // Init local node.
        GridBiTuple<Collection<String>, Collection<String>> addrs;

        try {
            addrs = U.resolveLocalAddresses(locHost);
        }
        catch (IOException | GridException e) {
            throw new GridSpiException("Failed to resolve local host to set of external addresses: " + locHost, e);
        }

        locNode = new GridTcpDiscoveryNode(
            locNodeId,
            addrs.get1(),
            addrs.get2(),
            tcpSrvr.port,
            metricsProvider,
            nodeVer);

        try {
            Collection<InetSocketAddress> extAddrs = addrRslvr == null ? null :
                U.resolveAddresses(addrRslvr, F.flat(Arrays.asList(addrs.get1(), addrs.get2())),
                    locNode.discoveryPort());

            if (extAddrs != null)
                nodeAttrs.put(createSpiAttributeName(ATTR_EXT_ADDRS), extAddrs);
        }
        catch (GridException e) {
            throw new GridSpiException("Failed to resolve local host to addresses: " + locHost, e);
        }

        locNode.setAttributes(nodeAttrs);

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
                throw new GridSpiException("Non-shared IP finder must have IP addresses specified in " +
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

        if (metricsStore != null) {
            metricsUpdateNtf = new MetricsUpdateNotifier();
            metricsUpdateNtf.start();
        }

        if (ipFinder.isShared() || metricsStore != null) {
            storesCleaner = new StoresCleaner();
            storesCleaner.start();
        }

        if (log.isDebugEnabled() && !restart)
            log.debug(startInfo());

        if (restart)
            getSpiContext().registerPort(tcpSrvr.port, TCP);
    }

    /**
     * @throws GridSpiException If failed.
     */
    @SuppressWarnings("BusyWait")
    private void registerLocalNodeAddress() throws GridSpiException {
        // Make sure address registration succeeded.
        while (true) {
            try {
                ipFinder.initializeLocalAddresses(locNode.socketAddresses());

                // Success.
                break;
            }
            catch (IllegalStateException e) {
                throw new GridSpiException("Failed to register local node address with IP finder: " +
                    locNode.socketAddresses(), e);
            }
            catch (GridSpiException e) {
                LT.error(log, e, "Failed to register local node address in IP finder on start " +
                    "(retrying every 2000 ms).");
            }

            try {
                U.sleep(2000);
            }
            catch (GridInterruptedException e) {
                throw new GridSpiException("Thread has been interrupted.", e);
            }
        }
    }

    /**
     * @throws GridSpiException If failed.
     */
    private void onSpiStart() throws GridSpiException {
        startStopwatch();

        assertParameter(ipFinder != null, "ipFinder != null");
        assertParameter(storesCleanFreq > 0, "ipFinderCleanFreq > 0");
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
        assertParameter(threadPri > 0, "threadPri > 0");
        assertParameter(statsPrintFreq >= 0, "statsPrintFreq >= 0");

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new GridSpiException("Unknown local address: " + locAddr, e);
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
            log.debug(configInfo("ipFinderCleanFreq", storesCleanFreq));
            log.debug(configInfo("heartbeatFreq", hbFreq));
            log.debug(configInfo("maxMissedHeartbeats", maxMissedHbs));
            log.debug(configInfo("metricsStore", metricsStore));
            log.debug(configInfo("statsPrintFreq", statsPrintFreq));
        }

        // Warn on odd network timeout.
        if (netTimeout < 3000)
            U.warn(log, "Network timeout is too low (at least 3000 ms recommended): " + netTimeout);

        // Warn on odd heartbeat frequency.
        if (hbFreq < 2000)
            U.warn(log, "Heartbeat frequency is too high (at least 2000 ms recommended): " + hbFreq);

        registerMBean(gridName, this, GridTcpDiscoverySpiMBean.class);

        if (ipFinder instanceof GridTcpDiscoveryMulticastIpFinder) {
            GridTcpDiscoveryMulticastIpFinder mcastIpFinder = ((GridTcpDiscoveryMulticastIpFinder)ipFinder);

            if (mcastIpFinder.getLocalAddress() == null)
                mcastIpFinder.setLocalAddress(locAddr);
        }
    }

    /** {@inheritDoc} */
    @Override public void onContextInitialized0(GridSpiContext spiCtx) throws GridSpiException {
        ctxInitLatch.countDown();

        spiCtx.registerPort(tcpSrvr.port, TCP);

        ipFinder.onSpiContextInitialized(spiCtx);

        if (metricsStore != null)
            metricsStore.onSpiContextInitialized(spiCtx);
    }

    /** {@inheritDoc} */
    @Override protected GridSpiContext getSpiContext() {
        if (ctxInitLatch.getCount() > 0) {
            if (log.isDebugEnabled())
                log.debug("Waiting for context initialization.");

            try {
                U.await(ctxInitLatch);

                if (log.isDebugEnabled())
                    log.debug("Context has been initialized.");
            }
            catch (GridInterruptedException e) {
                U.warn(log, "Thread has been interrupted while waiting for SPI context initialization.", e);
            }
        }

        return super.getSpiContext();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        spiStop0(false);
    }

    /**
     * Stops SPI finally or stops SPI for restart.
     *
     * @param disconnect {@code True} if SPI is being disconnected.
     * @throws GridSpiException If failed.
     */
    private void spiStop0(boolean disconnect) throws GridSpiException {
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
            msgWorker.addMessage(new GridTcpDiscoveryNodeLeftMessage(locNodeId));

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
            tmp = new ArrayList<>(readers);
        }

        U.interrupt(tmp);
        U.joinThreads(tmp, log);

        U.interrupt(hbsSnd);
        U.join(hbsSnd, log);

        U.interrupt(chkStatusSnd);
        U.join(chkStatusSnd, log);

        U.interrupt(storesCleaner);
        U.join(storesCleaner, log);

        U.interrupt(metricsUpdateNtf);
        U.join(metricsUpdateNtf, log);

        U.interrupt(msgWorker);
        U.join(msgWorker, log);

        U.interrupt(sockTimeoutWorker);
        U.join(sockTimeoutWorker, log);

        U.interrupt(statsPrinter);
        U.join(statsPrinter, log);

        if (ipFinder != null)
            ipFinder.close();

        Collection<GridTcpDiscoveryNode> rmts = null;

        if (!disconnect) {
            // This is final stop.
            unregisterMBean();

            if (log.isDebugEnabled())
                log.debug(stopInfo());
        }
        else {
            getSpiContext().deregisterPorts();

            rmts = ring.remoteNodes();
        }

        long topVer = ring.topologyVersion();

        ring.clear();

        if (rmts != null && !rmts.isEmpty()) {
            // This is restart/disconnection and remote nodes are not empty.
            // We need to fire FAIL event for each.
            GridDiscoverySpiListener lsnr = this.lsnr;

            if (lsnr != null) {
                Collection<GridNode> processed = new LinkedList<>();

                for (GridTcpDiscoveryNode n : rmts) {
                    processed.add(n);

                    if (n.visible()) {
                        Collection<GridNode> top = F.viewReadOnly(rmts, F.<GridNode>identity(),
                            F.and(F.notIn(processed), VISIBLE_NODES));

                        topVer++;

                        Map<Long, Collection<GridNode>> hist = updateTopologyHistory(topVer, top);

                        lsnr.onDiscovery(EVT_NODE_FAILED, topVer, n, top, hist);
                    }
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
        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        getSpiContext().deregisterPorts();

        ipFinder.onSpiContextDestroyed();

        if (metricsStore != null)
            metricsStore.onSpiContextDestroyed();
    }

    /**
     * @throws GridSpiException If any error occurs.
     * @return {@code true} if IP finder contains local address.
     */
    private boolean ipFinderHasLocalAddress() throws GridSpiException {
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
                catch (UnknownHostException ignored) {
                    // No-op.
                }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        assert nodeId != null;

        if (nodeId == locNodeId)
            return true;

        GridTcpDiscoveryNode node = ring.node(nodeId);

        if (node == null || !node.visible())
            return false;

        boolean res = pingNode(node);

        if (!res) {
            LT.warn(log, null, "Failed to ping node (status check will be initiated): " + nodeId);

            msgWorker.addMessage(new GridTcpDiscoveryStatusCheckMessage(locNode, node.id()));
        }

        return res;
    }

    /**
     * Pings the remote node to see if it's alive.
     *
     * @param node Node.
     * @return {@code True} if ping succeeds.
     */
    private boolean pingNode(GridTcpDiscoveryNode node) {
        assert node != null;

        if (node.id().equals(locNodeId))
            return true;

        for (InetSocketAddress addr : getNodeAddresses(node, U.sameMacs(locNode, node)) ) {
            try {
                // ID returned by the node should be the same as ID of the parameter for ping to succeed.
                return node.id().equals(pingNode(addr));
            }
            catch (GridSpiException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to ping node [node=" + node + ", err=" + e.getMessage() + ']');

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
     * @throws GridSpiException If an error occurs.
     */
    private UUID pingNode(InetSocketAddress addr) throws GridSpiException {
        assert addr != null;

        if (F.contains(locNodeAddrs, addr))
            return locNodeId;

        Collection<Throwable> errs = null;

        Socket sock = null;

        for (int i = 0; i < reconCnt; i++) {
            try {
                if (addr.isUnresolved())
                    addr = new InetSocketAddress(InetAddress.getByName(addr.getHostName()), addr.getPort());

                long tstamp = U.currentTimeMillis();

                sock = openSocket(addr);

                // Handshake response will act as ping response.
                writeToSocket(sock, new GridTcpDiscoveryHandshakeRequest(locNodeId));

                GridTcpDiscoveryHandshakeResponse res = readMessage(sock, null, netTimeout);

                if (locNodeId.equals(res.creatorNodeId())) {
                    if (log.isDebugEnabled())
                        log.debug("Handshake response from local node: " + res);

                    break;
                }

                stats.onClientSocketInitialized(U.currentTimeMillis() - tstamp);

                return res.creatorNodeId();
            }
            catch (IOException | GridException e) {
                if (errs == null)
                    errs = new ArrayList<>();

                errs.add(e);
            }
            finally {
                U.closeQuiet(sock);
            }
        }

        throw new GridSpiException("Failed to ping node by address: " + addr,
            U.exceptionWithSuppressed("Failed to ping node by address: " + addr, errs));
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws GridSpiException {
        spiStop0(true);
    }

    /** {@inheritDoc} */
    @Override public void reconnect() throws GridSpiException {
        spiStart0(true);
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(GridDiscoverySpiNodeAuthenticator nodeAuth) {
        this.nodeAuth = nodeAuth;
    }

    /**
     * Tries to join this node to topology.
     *
     * @throws GridSpiException If any error occurs.
     */
    private void joinTopology() throws GridSpiException {
        synchronized (mux) {
            assert spiState == CONNECTING || spiState == DISCONNECTED;

            spiState = CONNECTING;
        }

        GridSecurityCredentials locCred = (GridSecurityCredentials)locNode.getAttributes()
            .get(GridNodeAttributes.ATTR_SECURITY_CREDENTIALS);

        // Marshal credentials for backward compatibility and security.
        marshalCredentials(locNode);

        while (true) {
            if (!sendJoinRequestMessage()) {
                if (log.isDebugEnabled())
                    log.debug("Join request message has not been sent (local node is the first in the topology).");

                // Authenticate local node.
                try {
                    GridSecurityContext subj = nodeAuth.authenticateNode(locNode, locCred);

                    if (subj == null)
                        throw new GridSpiException("Authentication failed for local node: " + locNode.id());

                    Map<String, Object> attrs = new HashMap<>(locNode.attributes());

                    attrs.put(GridNodeAttributes.ATTR_SECURITY_SUBJECT, gridMarsh.marshal(subj));
                    attrs.remove(GridNodeAttributes.ATTR_SECURITY_CREDENTIALS);

                    locNode.setAttributes(attrs);
                }
                catch (GridException e) {
                    throw new GridSpiException("Failed to authenticate local node (will shutdown local node).", e);
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

                // Alter flag here and fire event here, since it has not been done in msgWorker.
                if (recon)
                    // Node has reconnected and it is the first.
                    notifyDiscovery(EVT_NODE_RECONNECTED, 1, locNode);
                else {
                    // This is initial start, node is the first.
                    recon = true;

                    notifyDiscovery(EVT_NODE_JOINED, 1, locNode);
                }

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

                        throw new GridSpiException("Thread has been interrupted.");
                    }
                }

                if (spiState == CONNECTED)
                    break;
                else if (spiState == DUPLICATE_ID) {
                    GridTcpDiscoveryDuplicateIdMessage msg = (GridTcpDiscoveryDuplicateIdMessage)joinRes.get();

                    throw new GridSpiException("Local node has the same ID as existing node in topology " +
                        "(fix configuration and restart local node) " +
                        "[localNode=" + locNode + ", existingNode=" + msg.node() + ']');
                }
                else if (spiState == AUTH_FAILED) {
                    GridTcpDiscoveryAuthFailedMessage msg =
                        (GridTcpDiscoveryAuthFailedMessage)joinRes.get();

                    throw new GridSpiException(new GridAuthenticationException("Authentication failed [nodeId=" +
                        msg.creatorNodeId() + ", addr=" + msg.address().getHostAddress() + ']'));
                }
                else if (spiState == CHECK_FAILED) {
                    GridTcpDiscoveryCheckFailedMessage msg = (GridTcpDiscoveryCheckFailedMessage)joinRes.get();

                    if (versionCheckFailed(msg))
                        throw new GridSpiVersionCheckException(msg.error());
                    else
                        throw new GridSpiException(msg.error());
                }
                else if (spiState == LOOPBACK_PROBLEM) {
                    GridTcpDiscoveryLoopbackProblemMessage msg = (GridTcpDiscoveryLoopbackProblemMessage)joinRes.get();

                    boolean locHostLoopback = locHost.isLoopbackAddress();

                    String firstNode = locHostLoopback ? "local" : "remote";

                    String secondNode = locHostLoopback ? "remote" : "local";

                    throw new GridSpiException("Failed to add node to topology because " + firstNode +
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
     * @param msg Failed message.
     * @return {@code True} if specified failed message relates to version incompatibility, {@code false} otherwise.
     * @deprecated Parsing of error message was used for preserving backward compatibility. We should remove it
     *      and create separate message for failed version check with next major release.
     */
    @Deprecated
    private static boolean versionCheckFailed(GridTcpDiscoveryCheckFailedMessage msg) {
        return msg.error().contains("versions are not compatible");
    }

    /**
     * @param msg Error message.
     * @return Remote grid version parsed from error message.
     * @deprecated This method was created for preserving backward compatibility. During major version update
     *      parsing of error message should be replaced with new {@link GridTcpDiscoveryCheckFailedMessage}
     *      which contains all necessary information.
     */
    @Deprecated
    @Nullable private String parseRemoteVersion(String msg) {
        msg = msg.replaceAll("\\s", "");

        final String verPrefix = "rmtBuildVer=";

        int startIdx = msg.indexOf(verPrefix);
        int endIdx = msg.indexOf(',', startIdx);

        if (endIdx < 0)
            endIdx = msg.indexOf(']', startIdx);

        if (startIdx < 0 || endIdx < 0)
            return null;

        return msg.substring(startIdx + verPrefix.length() - 1, endIdx);
    }

    /**
     * Tries to send join request message to a random node presenting in topology.
     * Address is provided by {@link GridTcpDiscoveryIpFinder} and message is
     * sent to first node connection succeeded to.
     *
     * @return {@code true} if send succeeded.
     * @throws GridSpiException If any error occurs.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean sendJoinRequestMessage() throws GridSpiException {
        GridTcpDiscoveryAbstractMessage joinReq = new GridTcpDiscoveryJoinRequestMessage(locNode,
            exchange.collect(locNodeId));

        // Time when it has been detected, that addresses from IP finder do not respond.
        long noResStart = 0;

        while (true) {
            Collection<InetSocketAddress> addrs = resolvedAddresses();

            if (addrs.isEmpty())
                return false;

            List<InetSocketAddress> shuffled = new ArrayList<>(addrs);

            // Shuffle addresses to send join request to different nodes.
            Collections.shuffle(shuffled);

            boolean retry = false;
            GridException errs = null;

            for (InetSocketAddress addr : shuffled) {
                try {
                    Integer res = sendMessageDirectly(joinReq, addr);

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
                catch (GridSpiException e) {
                    if (errs == null)
                        errs = new GridException("Multiple connection attempts failed.");

                    errs.addSuppressed(e);

                    if (log.isDebugEnabled()) {
                        IOException ioe = X.cause(e, IOException.class);

                        log.debug("Failed to send join request message [addr=" + addr +
                            ", msg=" + ioe != null ? ioe.getMessage() : e.getMessage() + ']');
                    }

                    noResAddrs.add(addr);
                }
            }

            if (retry) {
                if (log.isDebugEnabled())
                    log.debug("Concurrent discovery SPI start has been detected (local node should wait).");

                try {
                    U.sleep(2000);
                }
                catch (GridInterruptedException e) {
                    throw new GridSpiException("Thread has been interrupted.", e);
                }
            }
            else if (!ipFinder.isShared() && !ipFinderHasLocAddr) {
                if (errs != null && X.hasCause(errs, ConnectException.class))
                    LT.warn(log, null, "Failed to connect to any address from IP finder " +
                        "(make sure IP finder addresses are correct and firewalls are disabled on all host machines): " +
                        addrs);

                if (joinTimeout > 0) {
                    if (noResStart == 0)
                        noResStart = U.currentTimeMillis();
                    else if (U.currentTimeMillis() - noResStart > joinTimeout)
                        throw new GridSpiException(
                            "Failed to connect to any address from IP finder within join timeout " +
                                "(make sure IP finder addresses are correct, and operating system firewalls are disabled " +
                                "on all host machines, or consider increasing 'joinTimeout' configuration property): " +
                                addrs, errs);
                }

                try {
                    U.sleep(2000);
                }
                catch (GridInterruptedException e) {
                    throw new GridSpiException("Thread has been interrupted.", e);
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
     * @throws GridSpiException If an error occurs.
     */
    @Nullable private Integer sendMessageDirectly(GridTcpDiscoveryAbstractMessage msg, InetSocketAddress addr)
        throws GridSpiException {
        assert msg != null;
        assert addr != null;

        Collection<Throwable> errs = null;

        Socket sock = null;

        long ackTimeout0 = ackTimeout;

        int connectAttempts = 1;

        boolean joinReqSent = false;

        for (int i = 0; i < reconCnt; i++) {
            // Need to set to false on each new iteration,
            // since remote node may leave in the middle of the first iteration.
            joinReqSent = false;

            boolean openSock = false;

            try {
                long tstamp = U.currentTimeMillis();

                sock = openSocket(addr);

                openSock = true;

                // Handshake.
                writeToSocket(sock, new GridTcpDiscoveryHandshakeRequest(locNodeId));

                GridTcpDiscoveryHandshakeResponse res = readMessage(sock, null, ackTimeout0);

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
                joinReqSent = msg instanceof GridTcpDiscoveryJoinRequestMessage;

                return readReceipt(sock, ackTimeout0);
            }
            catch (ClassCastException e) {
                // This issue is rarely reproducible on AmazonEC2, but never
                // on dedicated machines.
                if (log.isDebugEnabled())
                    U.error(log, "Class cast exception on direct send: " + addr, e);

                if (errs == null)
                    errs = new ArrayList<>();

                errs.add(e);
            }
            catch (IOException | GridException e) {
                if (log.isDebugEnabled())
                    log.error("Exception on direct send: " + e.getMessage(), e);

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
            }
        }

        if (joinReqSent) {
            if (log.isDebugEnabled())
                log.debug("Join request has been sent, but receipt has not been read (returning RES_WAIT).");

            // Topology will not include this node,
            // however, warning on timed out join will be output.
            return RES_OK;
        }

        throw new GridSpiException(
            "Failed to send message to address [addr=" + addr + ", msg=" + msg + ']',
            U.exceptionWithSuppressed("Failed to send message to address " +
                "[addr=" + addr + ", msg=" + msg + ']', errs));
    }

    /**
     * Marshalls credentials with discovery SPI marshaller (will replace attribute value).
     *
     * @param node Node to marshall credentials for.
     * @throws GridSpiException If marshalling failed.
     */
    private void marshalCredentials(GridTcpDiscoveryNode node) throws GridSpiException {
        try {
            // Use security-unsafe getter.
            Map<String, Object> attrs = new HashMap<>(node.getAttributes());

            attrs.put(GridNodeAttributes.ATTR_SECURITY_CREDENTIALS,
                marsh.marshal(attrs.get(GridNodeAttributes.ATTR_SECURITY_CREDENTIALS)));

            node.setAttributes(attrs);
        }
        catch (GridException e) {
            throw new GridSpiException("Failed to marshal node security credentials: " + node.id(), e);
        }
    }

    /**
     * Unmarshalls credentials with discovery SPI marshaller (will not replace attribute value).
     *
     * @param node Node to unmarshall credentials for.
     * @return Security credentials.
     * @throws GridSpiException If unmarshal fails.
     */
    private GridSecurityCredentials unmarshalCredentials(GridTcpDiscoveryNode node) throws GridSpiException {
        try {
            byte[] credBytes = (byte[])node.getAttributes().get(GridNodeAttributes.ATTR_SECURITY_CREDENTIALS);

            if (credBytes == null)
                return null;

            return marsh.unmarshal(credBytes, null);
        }
        catch (GridException e) {
            throw new GridSpiException("Failed to unmarshal node security credentials: " + node.id(), e);
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
     * @param sockAddr Remote address.
     * @return Opened socket.
     * @throws IOException If failed.
     */
    private Socket openSocket(InetSocketAddress sockAddr) throws IOException {
        assert sockAddr != null;

        InetSocketAddress resolved = sockAddr.isUnresolved() ?
            new InetSocketAddress(InetAddress.getByName(sockAddr.getHostName()), sockAddr.getPort()) : sockAddr;

        InetAddress addr = resolved.getAddress();

        assert addr != null;

        Socket sock = new Socket();

        sock.bind(new InetSocketAddress(locHost, 0));

        sock.setTcpNoDelay(true);

        sock.connect(resolved, (int)sockTimeout);

        writeToSocket(sock, U.GG_HEADER);

        return sock;
    }

    /**
     * Writes message to the socket limiting write time to {@link #getSocketTimeout()}.
     *
     * @param sock Socket.
     * @param msg Message.
     * @throws IOException If IO failed or write timed out.
     * @throws GridException If marshalling failed.
     */
    private void writeToSocket(Socket sock, GridTcpDiscoveryAbstractMessage msg) throws IOException, GridException {
        writeToSocket(sock, msg, new GridByteArrayOutputStream(8 * 1024)); // 8K.
    }

    /**
     * Writes message to the socket limiting write time to {@link #getSocketTimeout()}.
     *
     * @param sock Socket.
     * @param msg Message.
     * @param bout Byte array output stream.
     * @throws IOException If IO failed or write timed out.
     * @throws GridException If marshalling failed.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void writeToSocket(Socket sock, GridTcpDiscoveryAbstractMessage msg, GridByteArrayOutputStream bout)
        throws IOException, GridException {
        assert sock != null;
        assert msg != null;
        assert bout != null;

        // Marshall message first to perform only write after.
        marsh.marshal(msg, bout);

        SocketTimeoutObject obj = new SocketTimeoutObject(sock, U.currentTimeMillis() + sockTimeout);

        sockTimeoutWorker.addTimeoutObject(obj);

        IOException err = null;

        try {
            OutputStream out = sock.getOutputStream();

            bout.writeTo(out);

            out.flush();
        }
        catch (IOException e) {
            err = e;
        }
        finally {
            boolean cancelled = obj.cancel();

            if (cancelled)
                sockTimeoutWorker.removeTimeoutObject(obj);

            // Throw original exception.
            if (err != null)
                throw err;

            if (!cancelled)
                throw new SocketTimeoutException("Write timed out (socket was concurrently closed).");
        }
    }

    /**
     * Writes response to the socket limiting write time to {@link #getSocketTimeout()}.
     *
     * @param sock Socket.
     * @param res Integer response.
     * @throws IOException If IO failed or write timed out.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void writeToSocket(Socket sock, int res) throws IOException {
        assert sock != null;

        SocketTimeoutObject obj = new SocketTimeoutObject(sock, U.currentTimeMillis() + sockTimeout);

        sockTimeoutWorker.addTimeoutObject(obj);

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
                sockTimeoutWorker.removeTimeoutObject(obj);

            // Throw original exception.
            if (err != null)
                throw err;

            if (!cancelled)
                throw new SocketTimeoutException("Write timed out (socket was concurrently closed).");
        }
    }

    /**
     * Writes message to the socket limiting write time to {@link #getSocketTimeout()}.
     *
     * @param sock Socket.
     * @param data Raw data to write.
     * @throws IOException If IO failed or write timed out.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void writeToSocket(Socket sock, byte[] data) throws IOException {
        assert sock != null;
        assert data != null;

        SocketTimeoutObject obj = new SocketTimeoutObject(sock, U.currentTimeMillis() + sockTimeout);

        sockTimeoutWorker.addTimeoutObject(obj);

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
                sockTimeoutWorker.removeTimeoutObject(obj);

            // Throw original exception.
            if (err != null)
                throw err;

            if (!cancelled)
                throw new SocketTimeoutException("Write timed out (socket was concurrently closed).");
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
    private int readReceipt(Socket sock, long timeout) throws IOException {
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
            LT.warn(log, null, "Timed out waiting for message delivery receipt (most probably, the reason is " +
                "in long GC pauses on remote node; consider tuning GC and increasing 'ackTimeout' " +
                "configuration property). Will retry to send message with increased timeout. " +
                "Current timeout: " + timeout + '.');

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
     * Reads message from the socket limiting read time.
     *
     * @param sock Socket.
     * @param in Input stream (in case socket stream was wrapped).
     * @param timeout Socket timeout for this operation.
     * @return Message.
     * @throws IOException If IO failed or read timed out.
     * @throws GridException If unmarshalling failed.
     */
    private <T> T readMessage(Socket sock, @Nullable InputStream in, long timeout) throws IOException, GridException {
        assert sock != null;

        int oldTimeout = sock.getSoTimeout();

        try {
            sock.setSoTimeout((int)timeout);

            return marsh.unmarshal(in == null ? sock.getInputStream() : in, U.gridClassLoader());
        }
        catch (IOException | GridException e) {
            if (X.hasCause(e, SocketTimeoutException.class))
                LT.warn(log, null, "Timed out waiting for message to be read (most probably, the reason is " +
                    "in long GC pauses on remote node. Current timeout: " + timeout + '.');

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
     * Notify external listener on discovery event.
     *
     * @param type Discovery event type. See {@link GridDiscoveryEvent} for more details.
     * @param topVer Topology version.
     * @param node Remote node this event is connected with.
     */
    private void notifyDiscovery(int type, long topVer, GridTcpDiscoveryNode node) {
        assert type > 0;
        assert node != null;

        GridDiscoverySpiListener lsnr = this.lsnr;

        GridTcpDiscoverySpiState spiState = spiStateCopy();

        try {
            if (lsnr != null && node.visible() && (spiState == CONNECTED || spiState == DISCONNECTING)) {
                Collection<GridNode> top = new ArrayList<GridNode>(F.view(ring.allNodes(), VISIBLE_NODES));

                Map<Long, Collection<GridNode>> hist = updateTopologyHistory(topVer, top);

                lsnr.onDiscovery(type, topVer, node, top, hist);
            }
            else if (log.isDebugEnabled())
                log.debug("Skipped discovery notification [node=" + node + ", spiState=" + spiState +
                    ", type=" + U.gridEventName(type) + ", topVer=" + topVer + ']');
        }
        catch (Throwable t) {
            spi1.dumpDebugInfo(log);
            spi2.dumpDebugInfo(log);

            for (GridTcpDiscoverySpi spi : spis)
                spi.dumpDebugInfo(log);

            throw new RuntimeException(t);
        }
    }

    /**
     * Update topology history with new topology snapshots.
     *
     * @param topVer Topology version.
     * @param top Topology snapshot.
     * @return Copy of updated topology history.
     */
    @Nullable private Map<Long, Collection<GridNode>> updateTopologyHistory(long topVer, Collection<GridNode> top) {
        synchronized (mux) {
            if (topHist.containsKey(topVer))
                return null;

            topHist.put(topVer, top);

            while (topHist.size() > topHistSize)
                topHist.remove(topHist.firstKey());

            if (log.isDebugEnabled())
                log.debug("Added topology snapshot to history, topVer=" + topVer + ", historySize=" + topHist);

            return new TreeMap<>(topHist);
        }
    }

    /**
     * Resolves addresses registered in the IP finder, removes duplicates and local host
     * address and returns the collection of.
     *
     * @return Resolved addresses without duplicates and local address (potentially
     * empty but never null).
     * @throws GridSpiException If an error occurs.
     */
    @SuppressWarnings("BusyWait")
    private Collection<InetSocketAddress> resolvedAddresses() throws GridSpiException {
        Collection<InetSocketAddress> res = new LinkedHashSet<>();

        Collection<InetSocketAddress> addrs;

        // Get consistent addresses collection.
        while (true) {
            try {
                addrs = registeredAddresses();

                break;
            }
            catch (GridSpiException e) {
                LT.error(log, e, "Failed to get registered addresses from IP finder on start " +
                    "(retrying every 2000 ms).");
            }

            try {
                U.sleep(2000);
            }
            catch (GridInterruptedException e) {
                throw new GridSpiException("Thread has been interrupted.", e);
            }
        }

        for (InetSocketAddress addr : addrs) {
            assert addr != null;

            try {
                InetSocketAddress resolved = addr.isUnresolved() ?
                    new InetSocketAddress(InetAddress.getByName(addr.getHostName()), addr.getPort()) : addr;

                if (!locNodeAddrs.contains(resolved))
                    res.add(resolved);
            }
            catch (UnknownHostException ignored) {
                LT.warn(log, null, "Failed to resolve address from IP finder (host is unknown): " + addr);

                // Add address in any case.
                res.add(addr);
            }
        }

        return res;
    }

    /**
     * @param node Node.
     * @return {@link LinkedHashSet} of internal and external addresses of provided node.
     *      Internal addresses placed before external addresses.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private LinkedHashSet<InetSocketAddress> getNodeAddresses(GridTcpDiscoveryNode node) {
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
    private LinkedHashSet<InetSocketAddress> getNodeAddresses(GridTcpDiscoveryNode node, boolean sameHost) {
        ArrayList<InetSocketAddress> addrs = new ArrayList<>(node.socketAddresses());

        Collections.sort(addrs, inetAddressesComparator(sameHost));

        LinkedHashSet<InetSocketAddress> res = new LinkedHashSet<>(addrs);

        Collection<InetSocketAddress> extAddrs = node.attribute(createSpiAttributeName(ATTR_EXT_ADDRS));

        if (extAddrs != null)
            res.addAll(extAddrs);

        return res;
    }

    /**
     * Gets addresses registered in the IP finder, initializes addresses having no
     * port (or 0 port) with {@link #DFLT_PORT}.
     *
     * @return Registered addresses.
     * @throws GridSpiException If an error occurs.
     */
    private Collection<InetSocketAddress> registeredAddresses() throws GridSpiException {
        Collection<InetSocketAddress> res = new LinkedList<>();

        for (InetSocketAddress addr : ipFinder.getRegisteredAddresses()) {
            if (addr.getPort() == 0)
                addr = addr.isUnresolved() ? new InetSocketAddress(addr.getHostName(), DFLT_PORT) :
                    new InetSocketAddress(addr.getAddress(), DFLT_PORT);

            res.add(addr);
        }

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
    private GridTcpDiscoverySpiState spiStateCopy() {
        GridTcpDiscoverySpiState state;

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
    @Nullable private GridTcpDiscoveryNode resolveCoordinator() {
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
    @Nullable private GridTcpDiscoveryNode resolveCoordinator(
        @Nullable Collection<GridTcpDiscoveryNode> filter) {
        synchronized (mux) {
            Collection<GridTcpDiscoveryNode> excluded = F.concat(false, failedNodes, leavingNodes);

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

            GridTcpDiscoveryNode coord = resolveCoordinator();

            log.info("Discovery SPI statistics [statistics=" + stats + ", spiState=" + spiStateCopy() +
                ", coord=" + coord +
                ", topSize=" + ring.allNodes().size() +
                ", leavingNodesSize=" + leavingNodesSize + ", failedNodesSize=" + failedNodesSize +
                ", msgWorker.queue.size=" + (msgWorker != null ? msgWorker.queue.size() : "N/A") +
                ", lastUpdate=" + (locNode != null ? U.format(locNode.lastUpdateTime()) : "N/A") +
                ", heapFree=" + runtime.freeMemory() / (1024 * 1024) +
                "M, heapTotal=" + runtime.maxMemory() / (1024 * 1024) + "M]");
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
        U.warn(log, "Simulating node failure: " + locNodeId);

        U.interrupt(tcpSrvr);
        U.join(tcpSrvr, log);

        U.interrupt(hbsSnd);
        U.join(hbsSnd, log);

        U.interrupt(chkStatusSnd);
        U.join(chkStatusSnd, log);

        U.interrupt(storesCleaner);
        U.join(storesCleaner, log);

        U.interrupt(metricsUpdateNtf);
        U.join(metricsUpdateNtf, log);

        Collection<SocketReader> tmp;

        synchronized (mux) {
            tmp = new ArrayList<>(readers);
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

        GridTcpDiscoveryNode next;

        synchronized (mux) {
            next = ring.nextNode(failedNodes);
        }

        if (next != null)
            msgWorker.addMessage(new GridTcpDiscoveryNodeFailedMessage(locNodeId, next.id(), next.order()));
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
    GridTcpDiscoveryNodesRing ring() {
        return ring;
    }

    /** {@inheritDoc} */
    @Override public long getGridStartTime() {
        assert gridStartTime != 0;

        return gridStartTime;
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo() {
        dumpDebugInfo(log);
    }

    public void dumpDebugInfo(GridLogger log) {
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

            b.append("Local node ID: ").append(locNodeId).append(U.nl()).append(U.nl());
            b.append("Local node: ").append(locNode).append(U.nl()).append(U.nl());
            b.append("SPI state: ").append(spiState).append(U.nl()).append(U.nl());

            b.append("Internal threads: ").append(U.nl());

            b.append("    Message worker: ").append(threadStatus(msgWorker)).append(U.nl());
            b.append("    Check status sender: ").append(threadStatus(chkStatusSnd)).append(U.nl());
            b.append("    HB sender: ").append(threadStatus(hbsSnd)).append(U.nl());
            b.append("    Metrics update notifier: ").append(threadStatus(metricsUpdateNtf)).append(U.nl());
            b.append("    Socket timeout worker: ").append(threadStatus(sockTimeoutWorker)).append(U.nl());
            b.append("    Stores cleaner: ").append(threadStatus(storesCleaner)).append(U.nl());
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

            for (GridTcpDiscoveryNode node : leavingNodes)
                b.append("    ").append(node.id()).append(U.nl());

            b.append(U.nl());

            b.append("Failed nodes: ").append(U.nl());

            for (GridTcpDiscoveryNode node : failedNodes)
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
            '[' + Thread.currentThread().getName() + "] " +
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
    private boolean recordable(GridTcpDiscoveryAbstractMessage msg) {
        return !(msg instanceof GridTcpDiscoveryHeartbeatMessage) &&
            !(msg instanceof GridTcpDiscoveryStatusCheckMessage) &&
            !(msg instanceof GridTcpDiscoveryDiscardMessage);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoverySpi.class, this);
    }

    /**
     * Thread that sends heartbeats.
     */
    private class HeartbeatsSender extends GridSpiThread {
        /**
         * Constructor.
         */
        private HeartbeatsSender() {
            super(gridName, "tcp-disco-hb-sender", log);

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

                GridTcpDiscoveryHeartbeatMessage msg = new GridTcpDiscoveryHeartbeatMessage(locNodeId);

                msgWorker.addMessage(msg);

                Thread.sleep(hbFreq);
            }
        }
    }

    /**
     * Thread that sends status check messages to next node if local node has not
     * been receiving heartbeats ({@link GridTcpDiscoveryHeartbeatMessage})
     * for {@link GridTcpDiscoverySpi#getMaxMissedHeartbeats()} *
     * {@link GridTcpDiscoverySpi#getHeartbeatFrequency()}.
     */
    private class CheckStatusSender extends GridSpiThread {
        /**
         * Constructor.
         */
        private CheckStatusSender() {
            super(gridName, "tcp-disco-status-check-sender", log);

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

                msgWorker.addMessage(new GridTcpDiscoveryStatusCheckMessage(locNode, null));
            }
        }
    }

    /**
     * Thread that cleans SPI stores (IP finder and metrics store) and keeps them in
     * the correct state, unregistering addresses and metrics of the nodes that has
     * left the topology.
     * <p>
     * This thread should run only on coordinator node and will clean IP finder
     * if and only if {@link GridTcpDiscoveryIpFinder#isShared()} is {@code true}.
     */
    private class StoresCleaner extends GridSpiThread {
        /**
         * Constructor.
         */
        private StoresCleaner() {
            super(gridName, "tcp-disco-stores-cleaner", log);

            setPriority(threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Stores cleaner has been started.");

            while (!isInterrupted()) {
                Thread.sleep(storesCleanFreq);

                if (!isLocalNodeCoordinator())
                    continue;

                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping stores cleaner (SPI is not connected to topology).");

                    return;
                }

                if (ipFinder.isShared())
                    cleanIpFinder();

                if (metricsStore != null)
                    cleanMetricsStore();
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
                        new C1<GridTcpDiscoveryNode, Collection<InetSocketAddress>>() {
                            @Override public Collection<InetSocketAddress> apply(GridTcpDiscoveryNode node) {
                                return getNodeAddresses(node);
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

                            if (res == null)
                                try {
                                    res = pingNode(addr) != null;
                                }
                                catch (GridSpiException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to ping node [addr=" + addr +
                                            ", err=" + e.getMessage() + ']');

                                    res = false;
                                }
                                finally {
                                    pingResMap.put(addr, res);
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
            catch (GridSpiException e) {
                LT.error(log, e, "Failed to clean IP finder up.");
            }
        }

        /**
         * Cleans metrics store.
         */
        private void cleanMetricsStore() {
            assert metricsStore != null;

            try {
                Collection<UUID> ids = F.view(metricsStore.allNodeIds(), F.notContains(
                    F.viewReadOnly(ring.allNodes(), F.node2id())));

                if (!ids.isEmpty())
                    metricsStore.removeMetrics(ids);
            }
            catch (GridSpiException e) {
                LT.error(log, e, "Failed to clean metrics store up.");
            }
        }
    }

    /**
     * Message worker thread for messages processing.
     */
    private class MessageWorker extends GridSpiThread {
        /** Socket to next node. */
        private Socket nextNodeSock;

        /** Next node. */
        @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
        private GridTcpDiscoveryNode next;

        /** First queue message gets in. */
        private final BlockingDeque<GridTcpDiscoveryAbstractMessage> queue =
            new LinkedBlockingDeque<>();

        /** Pending messages. */
        private final Queue<GridTcpDiscoveryAbstractMessage> pendingMsgs =
            new LinkedList<>();

        /** Backed interrupted flag. */
        private volatile boolean interrupted;

        /** Pre-allocated output stream (100K). */
        private final GridByteArrayOutputStream bout = new GridByteArrayOutputStream(100 * 1024);

        /** Last message that updated topology. */
        private GridTcpDiscoveryAbstractMessage lastMsg;

        /** Force pending messages send. */
        private boolean forceSndPending;

        /** Constructor. */
        private MessageWorker() {
            super(gridName, "tcp-disco-msg-worker", log);

            setPriority(threadPri);
        }

        /**
         * Adds message to queue.
         *
         * @param msg Message to add.
         */
        void addMessage(GridTcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            if (msg instanceof GridTcpDiscoveryHeartbeatMessage)
                queue.addFirst(msg);

            else
                queue.add(msg);

            if (log.isDebugEnabled())
                log.debug("Message has been added to queue: " + msg);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Message worker started [locNodeId=" + locNodeId + ']');

            while (!isInterrupted()) {
                GridTcpDiscoveryAbstractMessage msg = queue.poll(2000, TimeUnit.MILLISECONDS);

                if (msg == null)
                    continue;

                processMessage(msg);
            }
        }

        /**
         * @param msg Message to process.
         */
        private void processMessage(GridTcpDiscoveryAbstractMessage msg) {
            if (log.isDebugEnabled())
                log.debug("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            if (debugMode)
                debugLog("Processing message [cls=" + msg.getClass().getSimpleName() + ", id=" + msg.id() + ']');

            stats.onMessageProcessingStarted(msg);

            if (msg instanceof GridTcpDiscoveryJoinRequestMessage)
                processJoinRequestMessage((GridTcpDiscoveryJoinRequestMessage)msg);

            else if (msg instanceof GridTcpDiscoveryNodeAddedMessage)
                processNodeAddedMessage((GridTcpDiscoveryNodeAddedMessage)msg);

            else if (msg instanceof GridTcpDiscoveryNodeAddFinishedMessage)
                processNodeAddFinishedMessage((GridTcpDiscoveryNodeAddFinishedMessage)msg);

            else if (msg instanceof GridTcpDiscoveryNodeLeftMessage)
                processNodeLeftMessage((GridTcpDiscoveryNodeLeftMessage)msg);

            else if (msg instanceof GridTcpDiscoveryNodeFailedMessage)
                processNodeFailedMessage((GridTcpDiscoveryNodeFailedMessage)msg);

            else if (msg instanceof GridTcpDiscoveryHeartbeatMessage) {
                if (metricsStore != null)
                    processHeartbeatMessageMetricsStore((GridTcpDiscoveryHeartbeatMessage)msg);
                else
                    processHeartbeatMessage((GridTcpDiscoveryHeartbeatMessage)msg);
            }
            else if (msg instanceof GridTcpDiscoveryStatusCheckMessage)
                processStatusCheckMessage((GridTcpDiscoveryStatusCheckMessage)msg);

            else if (msg instanceof GridTcpDiscoveryDiscardMessage)
                processDiscardMessage((GridTcpDiscoveryDiscardMessage)msg);

            else
                assert false : "Unknown message type: " + msg.getClass().getSimpleName();

            stats.onMessageProcessingFinished(msg);
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            super.cleanup();

            U.closeQuiet(nextNodeSock);
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            interrupted = true;

            super.interrupt();
        }

        /** {@inheritDoc} */
        @Override public boolean isInterrupted() {
            return interrupted || super.isInterrupted();
        }

        /**
         * @param sock Socket.
         * @param msg Message.
         * @throws IOException If IO failed.
         * @throws GridException If marshalling failed.
         */
        private void writeToSocket(Socket sock, GridTcpDiscoveryAbstractMessage msg) throws IOException, GridException {
            bout.reset();

            GridTcpDiscoverySpi.this.writeToSocket(sock, msg, bout);
        }

        /**
         * Sends message across the ring.
         *
         * @param msg Message to send
         */
        @SuppressWarnings({"BreakStatementWithLabel", "LabeledStatement", "ContinueStatementWithLabel"})
        private void sendMessageAcrossRing(GridTcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            assert ring.hasRemoteNodes();

            onBeforeMessageSentAcrossRing(msg);

            Collection<GridTcpDiscoveryNode> failedNodes;

            GridTcpDiscoverySpiState state;

            synchronized (mux) {
                failedNodes = new ArrayList<>(GridTcpDiscoverySpi.this.failedNodes);

                state = spiState;
            }

            Collection<Throwable> errs = null;

            boolean sent = false;

            boolean searchNext = true;

            while (true) {
                if (searchNext) {
                    GridTcpDiscoveryNode newNext = ring.nextNode(failedNodes);

                    if (newNext == null) {
                        if (log.isDebugEnabled())
                            log.debug("No next node in topology.");

                        if (debugMode)
                            debugLog("No next node in topology.");

                        break;
                    }

                    if (!newNext.equals(next)) {
                        if (log.isDebugEnabled())
                            log.debug("New next node [newNext=" + newNext + ", formerNext=" + next +
                                ", ring=" + ring + ", failedNodes=" + failedNodes + ']');

                        if (debugMode)
                            debugLog("New next node [newNext=" + newNext + ", formerNext=" + next +
                                ", ring=" + ring + ", failedNodes=" + failedNodes + ']');

                        U.closeQuiet(nextNodeSock);

                        nextNodeSock = null;

                        next = newNext;
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Next node remains the same [nextId=" + next.id() +
                            ", nextOrder=" + next.internalOrder() + ']');
                }

                // Flag that shows whether next node exists and accepts incoming connections.
                boolean nextNodeExists = nextNodeSock != null;

                final boolean sameHost = U.sameMacs(locNode, next);

                addr: for (InetSocketAddress addr : getNodeAddresses(next, sameHost)) {
                    long ackTimeout0 = ackTimeout;

                    for (int i = 0; i < reconCnt; i++) {
                        if (nextNodeSock == null) {
                            nextNodeExists = false;

                            boolean success = false;

                            boolean openSock = false;

                            // Restore ring.
                            try {
                                long tstamp = U.currentTimeMillis();

                                nextNodeSock = openSocket(addr);

                                openSock = true;

                                // Handshake.
                                writeToSocket(nextNodeSock, new GridTcpDiscoveryHandshakeRequest(locNodeId));

                                GridTcpDiscoveryHandshakeResponse res = readMessage(nextNodeSock, null, ackTimeout0);

                                if (locNodeId.equals(res.creatorNodeId())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Handshake response from local node: " + res);

                                    U.closeQuiet(nextNodeSock);

                                    nextNodeSock = null;

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
                                        boolean nextNew = (msg instanceof GridTcpDiscoveryNodeAddedMessage &&
                                            ((GridTcpDiscoveryNodeAddedMessage)msg).node().id().equals(nextId));

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
                            catch (IOException | GridException e) {
                                if (errs == null)
                                    errs = new ArrayList<>();

                                errs.add(e);

                                if (log.isDebugEnabled())
                                    log.debug("Failed to connect to next node [msg=" + msg + ", err=" + e + ']');

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
                                    U.closeQuiet(nextNodeSock);

                                    nextNodeSock = null;
                                }
                                else
                                    // Next node exists and accepts incoming messages.
                                    nextNodeExists = true;
                            }
                        }

                        try {
                            boolean failure;

                            synchronized (mux) {
                                failure = GridTcpDiscoverySpi.this.failedNodes.size() < failedNodes.size();
                            }

                            assert !forceSndPending || msg instanceof GridTcpDiscoveryNodeLeftMessage;

                            if (failure || forceSndPending) {
                                if (log.isDebugEnabled())
                                    log.debug("Pending messages will be sent [failure=" + failure +
                                        ", forceSndPending=" + forceSndPending + ']');

                                if (debugMode)
                                    debugLog("Pending messages will be sent [failure=" + failure +
                                        ", forceSndPending=" + forceSndPending + ']');

                                for (GridTcpDiscoveryAbstractMessage pendingMsg : pendingMsgs) {
                                    long tstamp = U.currentTimeMillis();

                                    prepareNodeAddedMessage(pendingMsg);

                                    try {
                                        writeToSocket(nextNodeSock, pendingMsg);
                                    }
                                    finally {
                                        clearNodeAddedMessage(pendingMsg);
                                    }

                                    stats.onMessageSent(pendingMsg, U.currentTimeMillis() - tstamp);

                                    int res = readReceipt(nextNodeSock, ackTimeout0);

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

                            prepareNodeAddedMessage(msg);

                            try {
                                long tstamp = U.currentTimeMillis();

                                writeToSocket(nextNodeSock, msg);

                                stats.onMessageSent(msg, U.currentTimeMillis() - tstamp);

                                int res = readReceipt(nextNodeSock, ackTimeout0);

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
                        catch (IOException | GridException e) {
                            if (errs == null)
                                errs = new ArrayList<>();

                            errs.add(e);

                            if (log.isDebugEnabled())
                                U.error(log, "Failed to send message to next node [next=" + next.id() + ", msg=" + msg +
                                    ", err=" + e + ']', e);

                            if (e instanceof SocketTimeoutException || X.hasCause(e, SocketTimeoutException.class)) {
                                ackTimeout0 *= 2;

                                if (!checkAckTimeout(ackTimeout0))
                                    break;
                            }
                        }
                        finally {
                            forceSndPending = false;

                            if (!sent) {
                                U.closeQuiet(nextNodeSock);

                                nextNodeSock = null;

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

                    if (msg instanceof GridTcpDiscoveryStatusCheckMessage) {
                        GridTcpDiscoveryStatusCheckMessage msg0 = (GridTcpDiscoveryStatusCheckMessage)msg;

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
                failedNodes.removeAll(GridTcpDiscoverySpi.this.failedNodes);
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
                    GridTcpDiscoverySpi.this.failedNodes.addAll(failedNodes);
                }

                for (GridTcpDiscoveryNode n : failedNodes)
                    msgWorker.addMessage(new GridTcpDiscoveryNodeFailedMessage(locNodeId, n.id(), n.internalOrder()));
            }
        }

        /**
         * @param msg Message to clear.
         */
        private void clearNodeAddedMessage(GridTcpDiscoveryAbstractMessage msg) {
            if (msg instanceof GridTcpDiscoveryNodeAddedMessage) {
                // Nullify topology before registration.
                GridTcpDiscoveryNodeAddedMessage nodeAddedMsg = (GridTcpDiscoveryNodeAddedMessage)msg;

                nodeAddedMsg.topology(null);
                nodeAddedMsg.topologyHistory(null);
            }
        }

        /**
         * @param msg Message to prepare.
         */
        private void prepareNodeAddedMessage(GridTcpDiscoveryAbstractMessage msg) {
            if (msg instanceof GridTcpDiscoveryNodeAddedMessage) {
                GridTcpDiscoveryNodeAddedMessage nodeAddedMsg =
                    (GridTcpDiscoveryNodeAddedMessage)msg;

                // If new node is next, then send topology to and all pending messages
                // as a part of message.
                if (nodeAddedMsg.node().equals(next)) {
                    Collection<GridTcpDiscoveryNode> allNodes = ring.allNodes();
                    Collection<GridTcpDiscoveryNode> topToSend = new ArrayList<>(allNodes.size());

                    for (GridTcpDiscoveryNode n0 : allNodes) {
                        assert n0.internalOrder() != 0 : n0;

                        // Skip next node and nodes added after next
                        // in case this message is resent due to failures/leaves.
                        // There will be separate messages for nodes with greater
                        // internal order.
                        if (n0.internalOrder() < nodeAddedMsg.node().internalOrder())
                            topToSend.add(n0);
                    }

                    nodeAddedMsg.topology(topToSend);

                    Map<Long, Collection<GridNode>> hist;

                    synchronized (mux) {
                        hist = new TreeMap<>(topHist);
                    }

                    nodeAddedMsg.topologyHistory(hist);
                }
            }
        }

        /**
         * Registers pending message.
         *
         * @param msg Message to register.
         */
        private void registerPendingMessage(GridTcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            if (U.getAnnotation(msg.getClass(), GridTcpDiscoveryEnsureDelivery.class) != null) {
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
        private void processJoinRequestMessage(GridTcpDiscoveryJoinRequestMessage msg) {
            assert msg != null;

            GridTcpDiscoveryNode node = msg.node();

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
                    trySendMessageDirectly(node,
                        new GridTcpDiscoveryLoopbackProblemMessage(locNodeId, locNode.addresses(), locNode.hostNames()));
                }
                catch (GridSpiException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send loopback problem message to node " +
                            "[node=" + node + ", err=" + e.getMessage() + ']');
                }

                // Ignore join request.
                return;
            }

            if (isLocalNodeCoordinator()) {
                GridTcpDiscoveryNode existingNode = ring.node(node.id());

                if (existingNode != null) {
                    if (!node.socketAddresses().equals(existingNode.socketAddresses())) {
                        if (!pingNode(existingNode)) {
                            addMessage(new GridTcpDiscoveryNodeFailedMessage(locNodeId, existingNode.id(),
                                existingNode.internalOrder()));

                            // Ignore this join request since existing node is about to fail
                            // and new node can continue.
                            return;
                        }

                        try {
                            trySendMessageDirectly(node,
                                new GridTcpDiscoveryDuplicateIdMessage(locNodeId, existingNode));
                        }
                        catch (GridSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send duplicate ID message to node " +
                                    "[node=" + node + ", existingNode=" + existingNode +
                                    ", err=" + e.getMessage() + ']');
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
                else {
                    // Authenticate node first.
                    try {
                        GridSecurityCredentials cred = unmarshalCredentials(node);

                        GridSecurityContext subj = nodeAuth.authenticateNode(node, cred);

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
                                trySendMessageDirectly(node, new GridTcpDiscoveryAuthFailedMessage(locNodeId, locHost));
                            }
                            catch (GridSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send unauthenticated message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');
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
                                    trySendMessageDirectly(node, new GridTcpDiscoveryAuthFailedMessage(locNodeId, locHost));
                                }
                                catch (GridSpiException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send unauthenticated message to node " +
                                            "[node=" + node + ", err=" + e.getMessage() + ']');
                                }

                                // Ignore join request.
                                return;
                            }

                            // Stick in authentication subject to node (use security-safe attributes for copy).
                            Map<String, Object> attrs = new HashMap<>(node.attributes());

                            attrs.put(GridNodeAttributes.ATTR_SECURITY_SUBJECT, gridMarsh.marshal(subj));

                            node.setAttributes(attrs);
                        }
                    }
                    catch (GridException e) {
                        LT.error(log, e, "Authentication failed [nodeId=" + node.id() + ", addrs=" +
                            U.addressesAsString(node) + ']');

                        if (log.isDebugEnabled())
                            log.debug("Failed to authenticate node (will ignore join request) [node=" + node +
                                ", err=" + e + ']');

                        // Ignore join request.
                        return;
                    }

                    GridNodeValidationResult err = getSpiContext().validateNode(node);

                    if (err != null) {
                        if (!pingNode(err.nodeId())) {
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
                                new GridTcpDiscoveryCheckFailedMessage(locNodeId, err.sendMessage()));
                        }
                        catch (GridSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send hash ID resolver validation failed message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']');
                        }

                        // Ignore join request.
                        return;
                    }

                    // Check version.
                    String locBuildVer = locNode.attribute(ATTR_BUILD_VER);
                    String rmtBuildVer = node.attribute(ATTR_BUILD_VER);

                    if (!F.eq(rmtBuildVer, locBuildVer)) {
                        final String osFlag = "-os";
                        final String entFlag = "-ent";

                        assert locBuildVer.contains(osFlag) || locBuildVer.contains(entFlag);
                        assert rmtBuildVer.contains(osFlag) || rmtBuildVer.contains(entFlag);

                        // OS and ENT nodes cannot join one topology.
                        if (locBuildVer.contains(entFlag) && rmtBuildVer.contains(osFlag) ||
                            locBuildVer.contains(osFlag) && rmtBuildVer.contains(entFlag)) {
                            String errMsg = "Topology cannot contain nodes of both enterprise and open source " +
                                "versions (node will not join, all nodes in topology should be of either " +
                                "enterprise or open source version) " +
                                "[locBuildVer=" + locBuildVer + ", rmtBuildVer=" + rmtBuildVer +
                                ", locNodeAddrs=" + U.addressesAsString(locNode) +
                                ", rmtNodeAddrs=" + U.addressesAsString(node) +
                                ", locNodeId=" + locNode.id() + ", rmtNodeId=" + msg.creatorNodeId() + ']';

                            LT.warn(log, null, errMsg);

                            // Always output in debug.
                            if (log.isDebugEnabled())
                                log.debug(errMsg);

                            try {
                                String sndMsg = "Topology cannot contain nodes of both enterprise and open source " +
                                    "versions (node will not join, all nodes in topology should be of either " +
                                    "enterprise or open source version) " +
                                    "[locBuildVer=" + rmtBuildVer + ", rmtBuildVer=" + locBuildVer +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + locNode.id() + ']';

                                trySendMessageDirectly(node,
                                    new GridTcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                            }
                            catch (GridSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send version check failed message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');
                            }

                            // Ignore join request.
                            return;
                        }

                        // OS nodes don't support rolling updates.
                        if (locBuildVer.contains(osFlag) && rmtBuildVer.contains(osFlag) &&
                            !locBuildVer.equals(rmtBuildVer)) {
                            String errMsg = "Local node and remote node have different version numbers " +
                                "(node will not join, open source version does not support rolling updates, " +
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
                                    "(node will not join, open source version does not support rolling updates, " +
                                    "so versions must be exactly the same) " +
                                    "[locBuildVer=" + rmtBuildVer + ", rmtBuildVer=" + locBuildVer +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + locNode.id() + ']';

                                trySendMessageDirectly(node,
                                    new GridTcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                            }
                            catch (GridSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send version check failed message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');
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
                                (rmtBuildVer.contains("-os") && locBuildVer.contains("-os") ?
                                    "(topologies built with different GridGain versions " +
                                        "are supported in Enterprise version only) " :
                                    "(node will not join, all nodes in topology should have " +
                                        "compatible build versions) ") +
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
                                    (rmtBuildVer.contains("-os") && locBuildVer.contains("-os") ?
                                        "(topologies built with different GridGain versions " +
                                            "are supported in Enterprise version only) " :
                                        "(node will not join, all nodes in topology should have " +
                                            "compatible build versions) ") +
                                    "[locBuildVer=" + rmtBuildVer + ", rmtBuildVer=" + locBuildVer +
                                    ", locNodeAddrs=" + U.addressesAsString(node) + ", locPort=" + node.discoveryPort() +
                                    ", rmtNodeAddr=" + U.addressesAsString(locNode) + ", locNodeId=" + node.id() +
                                    ", rmtNodeId=" + locNode.id() + ']';

                                trySendMessageDirectly(node,
                                    new GridTcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                            }
                            catch (GridSpiException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send version check failed message to node " +
                                        "[node=" + node + ", err=" + e.getMessage() + ']');
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
                                new GridTcpDiscoveryCheckFailedMessage(locNodeId, sndMsg));
                        }
                        catch (GridSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send marshaller check failed message to node " +
                                    "[node=" + node + ", err=" + e.getMessage() + ']');
                        }

                        // Ignore join request.
                        return;
                    }

                    // Handle join.
                    node.internalOrder(ring.nextNodeOrder());

                    if (log.isDebugEnabled())
                        log.debug("Internal order has been assigned to node: " + node);
                }

                processNodeAddedMessage(new GridTcpDiscoveryNodeAddedMessage(locNodeId, node, msg.discoveryData(),
                    gridStartTime));
            }
            else if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Tries to send a message to all node's available addresses.
         *
         * @param node Node to send message to.
         * @param msg Message.
         * @throws GridSpiException Last failure if all attempts failed.
         */
        private void trySendMessageDirectly(GridTcpDiscoveryNode node, GridTcpDiscoveryAbstractMessage msg)
            throws GridSpiException {
            GridSpiException ex = null;

            for (InetSocketAddress addr : getNodeAddresses(node, U.sameMacs(locNode, node))) {
                try {
                    sendMessageDirectly(msg, addr);

                    ex = null;

                    break;
                }
                catch (GridSpiException e) {
                    ex = e;
                }
            }

            if (ex != null)
                throw ex;
        }

        /**
         * Processes node added message.
         *
         * @param msg Node added message.
         */
        private void processNodeAddedMessage(GridTcpDiscoveryNodeAddedMessage msg) {
            assert msg != null;

            GridTcpDiscoveryNode node = msg.node();

            assert node != null;

            if (node.internalOrder() < locNode.internalOrder()) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node added message since local node's order is greater " +
                        "[node=" + node + ", locNode=" + locNode + ", msg=" + msg + ']');

                return;
            }

            if (isLocalNodeCoordinator()) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    processNodeAddFinishedMessage(new GridTcpDiscoveryNodeAddFinishedMessage(locNodeId, node.id()));

                    addMessage(new GridTcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified() && !locNodeId.equals(node.id())) {
                if (metricsStore != null) {
                    node.metricsStore(metricsStore);

                    node.logger(log);
                }

                boolean topChanged = ring.add(node);

                if (topChanged) {
                    assert !node.visible() : "Added visible node [node=" + node + ", locNode=" + locNode + ']';

                    List<Object> data = msg.newNodeDiscoveryData();

                    if (data != null)
                        exchange.onExchange(data);

                    msg.addDiscoveryData(exchange.collect(node.id()));
                }

                if (log.isDebugEnabled())
                    log.debug("Added node to local ring [added=" + topChanged + ", node=" + node +
                        ", ring=" + ring + ']');
            }

            if (msg.verified() && locNodeId.equals(node.id())) {
                // Discovery data.
                Collection<List<Object>> dataList;

                synchronized (mux) {
                    if (spiState == CONNECTING && locNode.internalOrder() != node.internalOrder()) {
                        // Initialize topology.
                        Collection<GridTcpDiscoveryNode> top = msg.topology();

                        if (top != null && !top.isEmpty()) {
                            gridStartTime = msg.gridStartTime();

                            for (GridTcpDiscoveryNode n : top) {
                                if (metricsStore != null) {
                                    n.metricsStore(metricsStore);

                                    n.logger(log);
                                }

                                // Make all preceding nodes and local node visible.
                                n.visible(true);
                            }

                            locNode.setAttributes(node.attributes());

                            locNode.visible(true);

                            // Restore topology with all nodes visible.
                            ring.restoreTopology(top, node.internalOrder());

                            if (log.isDebugEnabled())
                                log.debug("Restored topology from node added message: " + ring);

                            dataList = msg.oldNodesDiscoveryData();

                            topHist.clear();
                            topHist.putAll(msg.topologyHistory());

                            // Clear data to minimize message size.
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
                if (dataList != null) {
                    for (List<Object> discoData : dataList)
                        exchange.onExchange(discoData);
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
        private void processNodeAddFinishedMessage(GridTcpDiscoveryNodeAddFinishedMessage msg) {
            assert msg != null;

            UUID nodeId = msg.nodeId();

            assert nodeId != null;

            GridTcpDiscoveryNode node = ring.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding node add finished message since node is not found " +
                        "[msg=" + msg + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Node to finish add: " + node);

            boolean locNodeCoord = isLocalNodeCoordinator();

            if (locNodeCoord) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    addMessage(new GridTcpDiscoveryDiscardMessage(locNodeId, msg.id()));

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

            if (node != null && msg.verified()) {
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

                if (nodeVer.equals(node.version()))
                    node.version(nodeVer);

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
                catch (GridSpiException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to register new node address [node=" + node +
                            ", err=" + e.getMessage() + ']');
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

                if (recon)
                    notifyDiscovery(EVT_NODE_RECONNECTED, topVer, locNode);
                else {
                    recon = true;

                    // Discovery manager must create local joined event before spiStart completes.
                    notifyDiscovery(EVT_NODE_JOINED, topVer, locNode);
                }
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Processes node left message.
         *
         * @param msg Node left message.
         */
        private void processNodeLeftMessage(GridTcpDiscoveryNodeLeftMessage msg) {
            assert msg != null;

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

                if (msg.verified() || !ring.hasRemoteNodes()) {
                    if (ipFinder.isShared() && !ring.hasRemoteNodes()) {
                        try {
                            ipFinder.unregisterAddresses(locNode.socketAddresses());
                        }
                        catch (GridSpiException e) {
                            U.error(log, "Failed to unregister local node address from IP finder.", e);
                        }
                    }

                    if (metricsStore != null && !ring.hasRemoteNodes()) {
                        try {
                            metricsStore.removeMetrics(Collections.singletonList(locNodeId));
                        }
                        catch (GridSpiException e) {
                            U.error(log, "Failed to remove local node metrics from metrics store.", e);
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

            GridTcpDiscoveryNode leavingNode = ring.node(leavingNodeId);

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

                    addMessage(new GridTcpDiscoveryDiscardMessage(locNodeId, msg.id()));

                    return;
                }

                msg.verify(locNodeId);
            }

            if (msg.verified() && !locNodeId.equals(leavingNodeId)) {
                GridTcpDiscoveryNode leftNode = ring.removeNode(leavingNodeId);

                assert leftNode != null;

                // Clear pending messages map.
                if (!ring.hasRemoteNodes())
                    pendingMsgs.clear();

                long topVer;

                if (locNodeCoord) {
                    if (ipFinder.isShared()) {
                        try {
                            ipFinder.unregisterAddresses(leftNode.socketAddresses());
                        }
                        catch (GridSpiException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to unregister left node address: " + leftNode);
                        }
                    }

                    if (metricsStore != null) {
                        try {
                            metricsStore.removeMetrics(Collections.singletonList(leftNode.id()));
                        }
                        catch (GridSpiException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to remove left node metrics from store: " + leftNode.id());
                        }
                    }

                    topVer = ring.incrementTopologyVersion();

                    msg.topologyVersion(topVer);
                }
                else {
                    topVer = msg.topologyVersion();

                    assert topVer > 0 : "Topology version is empty for message: " + msg;

                    boolean b = ring.topologyVersion(topVer);

                    try {
                        assert b : "Topology version has not been updated: [ring=" + ring + ", msg=" + msg +
                            ", lastMsg=" + lastMsg + ", spiState=" + spiStateCopy() + ']';
                    }
                    catch (Throwable t) {
                        spi1.dumpDebugInfo(log);
                        spi2.dumpDebugInfo(log);

                        for (GridTcpDiscoverySpi spi : spis)
                            spi.dumpDebugInfo(log);

                        throw new RuntimeException(t);
                    }

                    if (log.isDebugEnabled())
                        log.debug("Topology version has been updated: [ring=" + ring + ", msg=" + msg + ']');

                    lastMsg = msg;
                }

                if (leftNode.equals(next) && nextNodeSock != null) {
                    try {
                        writeToSocket(nextNodeSock, msg);

                        if (log.isDebugEnabled())
                            log.debug("Sent verified node left message to leaving node: " + msg);
                    }
                    catch (GridException | IOException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send verified node left message to leaving node [msg=" + msg +
                                ", err=" + e.getMessage() + ']');
                    }
                    finally {
                        forceSndPending = true;

                        next = null;

                        U.closeQuiet(nextNodeSock);
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

                U.closeQuiet(nextNodeSock);
            }
        }

        /**
         * Processes node failed message.
         *
         * @param msg Node failed message.
         */
        private void processNodeFailedMessage(GridTcpDiscoveryNodeFailedMessage msg) {
            assert msg != null;

            UUID sndId = msg.senderNodeId();

            if (sndId != null) {
                GridTcpDiscoveryNode sndNode = ring.node(sndId);

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

            GridTcpDiscoveryNode node = ring.node(nodeId);

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

            if (locNodeCoord) {
                if (msg.verified()) {
                    stats.onRingMessageReceived(msg);

                    addMessage(new GridTcpDiscoveryDiscardMessage(locNodeId, msg.id()));

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
                    if (ipFinder.isShared()) {
                        try {
                            ipFinder.unregisterAddresses(node.socketAddresses());
                        }
                        catch (GridSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to unregister failed node address [node=" + node +
                                    ", err=" + e.getMessage() + ']');
                        }
                    }

                    if (metricsStore != null) {
                        Collection<UUID> ids = Collections.singletonList(node.id());

                        try {
                            metricsStore.removeMetrics(ids);
                        }
                        catch (GridSpiException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to remove failed node metrics from store [node=" + node +
                                    ", err=" + e.getMessage() + ']');
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

                U.closeQuiet(nextNodeSock);
            }
        }

        /**
         * Processes status check message.
         *
         * @param msg Status check message.
         */
        private void processStatusCheckMessage(GridTcpDiscoveryStatusCheckMessage msg) {
            assert msg != null;

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
                        catch (GridSpiException e) {
                            if (e.hasCause(SocketException.class)) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Failed to respond to status check message (connection refused) " +
                                        "[recipient=" + msg.creatorNodeId() + ", status=" + msg.status() + ']');
                                }
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
        private void processHeartbeatMessage(GridTcpDiscoveryHeartbeatMessage msg) {
            assert msg != null;

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

            if (msg.hasMetrics() && spiStateCopy() == CONNECTED)
                for (Map.Entry<UUID, GridNodeMetrics> e : msg.metrics().entrySet()) {
                    GridTcpDiscoveryNode node = ring.node(e.getKey());

                    if (node != null) {
                        node.setMetrics(e.getValue());

                        node.lastUpdateTime(tstamp);

                        notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), node);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Received metrics from unknown node: " + e.getKey());
                }

            if (ring.hasRemoteNodes()) {
                if ((locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() == null ||
                    !msg.hasMetrics(locNodeId)) && spiStateCopy() == CONNECTED)

                    // Message is on its first ring or just created on coordinator.
                    msg.setMetrics(locNodeId, metricsProvider.getMetrics());
                else
                    // Message is on its second ring.
                    msg.removeMetrics(locNodeId);

                sendMessageAcrossRing(msg);
            }
            else {
                locNode.lastUpdateTime(tstamp);

                notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), locNode);
            }
        }

        /**
         * Processes heartbeat message when working with metrics store.
         *
         * @param msg Heartbeat message.
         */
        private void processHeartbeatMessageMetricsStore(GridTcpDiscoveryHeartbeatMessage msg) {
            assert msg != null;
            assert metricsStore != null;

            assert !msg.hasMetrics();

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

            if (locNodeId.equals(msg.creatorNodeId()) && msg.senderNodeId() != null) {
                if (log.isDebugEnabled())
                    log.debug("Discarding heartbeat message that has made full ring pass: " + msg);

                return;
            }

            long tstamp = U.currentTimeMillis();

            try {
                if (spiStateCopy() == CONNECTED) {
                    // Cache metrics in node.
                    GridNodeMetrics metrics = locNode.metrics();

                    if (ring.hasRemoteNodes())
                        // Send metrics to store only if there are remote nodes.
                        metricsStore.updateLocalMetrics(locNodeId, metrics);

                    locNode.lastUpdateTime(tstamp);

                    notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), locNode);
                }
            }
            catch (GridSpiException e) {
                U.error(log, "Failed to update local node metrics in metrics store.", e);
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }

        /**
         * Processes discard message and discards previously registered pending messages.
         *
         * @param msg Discard message.
         */
        private void processDiscardMessage(GridTcpDiscoveryDiscardMessage msg) {
            assert msg != null;

            GridUuid msgId = msg.msgId();

            assert msgId != null;

            if (isLocalNodeCoordinator()) {
                if (!locNodeId.equals(msg.verifierNodeId()))
                    // Message is not verified or verified by former coordinator.
                    msg.verify(locNodeId);
                else
                    // Discard the message.
                    return;
            }

            if (msg.verified()) {
                for (GridTcpDiscoveryAbstractMessage m = pendingMsgs.poll();
                     m != null && !msgId.equals(m.id());
                     m = pendingMsgs.poll())
                    ;
            }

            if (ring.hasRemoteNodes())
                sendMessageAcrossRing(msg);
        }
    }

    /**
     * Returns comparator that sorts remote node addresses. If remote node resides on the same host, then put
     * loopback addresses first, last otherwise.
     *
     * @param sameHost {@code True} if remote node resides on the same host, {@code false} otherwise.
     * @return Comparator.
     */
    private static Comparator<InetSocketAddress> inetAddressesComparator(final boolean sameHost) {
        return new Comparator<InetSocketAddress>() {
            @Override public int compare(InetSocketAddress addr1, InetSocketAddress addr2) {
                boolean addr1Loopback = addr1.getAddress().isLoopbackAddress();

                boolean addr2Loopback = addr2.getAddress().isLoopbackAddress();

                // No need to reorder.
                if (addr1Loopback == addr2Loopback)
                    return 0;

                if (sameHost)
                    return addr1Loopback ? -1 : 1;
                else
                    return addr1Loopback ? 1 : -1;
            }
        };
    }

    /**
     * Thread that accepts incoming TCP connections.
     * <p>
     * Tcp server will call provided closure when accepts incoming connection.
     * From that moment server is no more responsible for the socket.
     */
    private class TcpServer extends GridSpiThread {
        /** Socket TCP server listens to. */
        private ServerSocket srvrSock;

        /** Port to listen. */
        private int port;

        /**
         * Constructor.
         *
         * @throws GridSpiException In case of error.
         */
        TcpServer() throws GridSpiException {
            super(gridName, "tcp-disco-srvr", log);

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
                    }
                    else {
                        throw new GridSpiException("Failed to bind TCP server socket (possibly all ports in range " +
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
    private class SocketReader extends GridSpiThread {
        /** Socket to read data from. */
        private final Socket sock;

        /** */
        private volatile UUID nodeId;

        /**
         * Constructor.
         *
         * @param sock Socket to read data from.
         */
        SocketReader(Socket sock) {
            super(gridName, "tcp-disco-sock-reader", log);

            this.sock = sock;

            setPriority(threadPri);

            stats.onSocketReaderCreated();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
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

                    if (!U.bytesEqual(buf, 0, U.GG_HEADER, 0, 4)) {
                        if (log.isDebugEnabled())
                            log.debug("Unknown connection detected (is some other software connecting to " +
                                "this GridGain port?) " +
                                "[rmtAddr=" + sock.getRemoteSocketAddress() +
                                ", locAddr=" + sock.getLocalSocketAddress() + ']');

                        LT.warn(log, null, "Unknown connection detected (is some other software connecting to " +
                            "this GridGain port?) [rmtAddr=" + sock.getRemoteSocketAddress() +
                            ", locAddr=" + sock.getLocalSocketAddress() + ']');

                        return;
                    }

                    // Restore timeout.
                    sock.setSoTimeout(timeout);

                    // Handshake.
                    GridTcpDiscoveryHandshakeRequest req = readMessage(sock, in, netTimeout);

                    UUID nodeId = req.creatorNodeId();

                    this.nodeId = nodeId;

                    GridTcpDiscoveryHandshakeResponse res =
                        new GridTcpDiscoveryHandshakeResponse(locNodeId, locNode.internalOrder());

                    writeToSocket(sock, res);

                    // It can happen if a remote node is stopped and it has a loopback address in the list of addresses,
                    // the local node sends a handshake request message on the loopback address, so we get here.
                    if (locNodeId.equals(nodeId)) {
                        if (log.isDebugEnabled())
                            log.debug("Handshake request from local node: " + req);

                        return;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Initialized connection with remote node: " + nodeId);

                    if (debugMode)
                        debugLog("Initialized connection with remote node: " + nodeId);
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

                    return;
                }
                catch (GridException e) {
                    if (log.isDebugEnabled())
                        U.error(log, "Caught exception on handshake [err=" + e +", sock=" + sock + ']', e);

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
                        GridTcpDiscoveryAbstractMessage msg = marsh.unmarshal(in, U.gridClassLoader());

                        msg.senderNodeId(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Message has been received: " + msg);

                        stats.onMessageReceived(msg);

                        if (debugMode && recordable(msg))
                            debugLog("Message has been received: " + msg);

                        if (msg instanceof GridTcpDiscoveryJoinRequestMessage) {
                            GridTcpDiscoveryJoinRequestMessage req = (GridTcpDiscoveryJoinRequestMessage)msg;

                            // Direct join request requires special processing.
                            if (!req.responded()) {
                                processJoinRequestMessage(req);

                                continue;
                            }
                        }
                        else if (msg instanceof GridTcpDiscoveryDuplicateIdMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            GridTcpDiscoverySpiState state = null;

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
                        else if (msg instanceof GridTcpDiscoveryAuthFailedMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            GridTcpDiscoverySpiState state = null;

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
                        else if (msg instanceof GridTcpDiscoveryCheckFailedMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            GridTcpDiscoverySpiState state = null;

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
                        else if (msg instanceof GridTcpDiscoveryLoopbackProblemMessage) {
                            // Send receipt back.
                            writeToSocket(sock, RES_OK);

                            boolean ignored = false;

                            GridTcpDiscoverySpiState state = null;

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
                        writeToSocket(sock, RES_OK);
                    }
                    catch (GridException e) {
                        if (log.isDebugEnabled())
                            U.error(log, "Caught exception on message read [sock=" + sock +
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

                        return;
                    }
                }
            }
            finally {
                U.closeQuiet(sock);
            }
        }

        /**
         * @param nodeId Node ID.
         * @return {@code True} if node is in the ring and is not being removed from.
         */
        private boolean nodeAlive(UUID nodeId) {
            // Is node alive or about to be removed from the ring?
            GridTcpDiscoveryNode node = ring.node(nodeId);

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
         * @throws IOException If IO failed.
         */
        @SuppressWarnings({"IfMayBeConditional"})
        private void processJoinRequestMessage(GridTcpDiscoveryJoinRequestMessage msg) throws IOException {
            assert msg != null;
            assert !msg.responded();

            GridTcpDiscoverySpiState state = spiStateCopy();

            if (state == CONNECTED) {
                // Direct join request - socket should be closed after handling.
                try {
                    writeToSocket(sock, RES_OK);

                    if (log.isDebugEnabled())
                        log.debug("Responded to join request message [msg=" + msg + ", res=" + RES_OK + ']');

                    msg.responded(true);

                    msgWorker.addMessage(msg);
                }
                finally {
                    U.closeQuiet(sock);
                }
            }
            else {
                // Direct join request - socket should be closed after handling.
                try {
                    stats.onMessageProcessingStarted(msg);

                    Integer res;

                    SocketAddress rmtAddr = sock.getRemoteSocketAddress();

                    if (state == CONNECTING) {
                        if (noResAddrs.contains(rmtAddr) || locNodeId.compareTo(msg.creatorNodeId()) < 0)
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
                }
                finally {
                    U.closeQuiet(sock);
                }
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
     * Metrics update notifier.
     */
    private class MetricsUpdateNotifier extends GridSpiThread {
        /** Constructor. */
        MetricsUpdateNotifier() {
            super(gridName, "tcp-disco-metrics-update-notifier", log);

            assert metricsStore != null;

            setPriority(threadPri);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Metrics update notifier has been started.");

            while (!isInterrupted()) {
                Thread.sleep(metricsStore.getMetricsExpireTime());

                if (spiStateCopy() != CONNECTED) {
                    if (log.isDebugEnabled())
                        log.debug("Stopping metrics update notifier (SPI is not connected to topology).");

                    return;
                }

                long tstamp = U.currentTimeMillis();

                // Event is fired for all nodes in the topology since all alive nodes should update their metrics
                // on time. If it is not so, most probably, nodes have failed and failure will be detected by common
                // failure detection logic.
                for (GridTcpDiscoveryNode node : ring.remoteNodes()) {
                    node.lastUpdateTime(tstamp);

                    notifyDiscovery(EVT_NODE_METRICS_UPDATED, ring.topologyVersion(), node);
                }
            }
        }
    }

    /**
     * SPI Statistics printer.
     */
    private class StatisticsPrinter extends GridSpiThread {
        /**
         * Constructor.
         */
        StatisticsPrinter() {
            super(gridName, "tcp-disco-stats-printer", log);

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
     * Handles sockets timeouts.
     */
    private class SocketTimeoutWorker extends GridSpiThread {
        /** Time-based sorted set for timeout objects. */
        private final GridConcurrentSkipListSet<SocketTimeoutObject> timeoutObjs =
            new GridConcurrentSkipListSet<>(new Comparator<SocketTimeoutObject>() {
                @Override public int compare(SocketTimeoutObject o1, SocketTimeoutObject o2) {
                    long time1 = o1.endTime();
                    long time2 = o2.endTime();

                    long id1 = o1.id();
                    long id2 = o2.id();

                    return time1 < time2 ? -1 : time1 > time2 ? 1 :
                        id1 < id2 ? -1 : id1 > id2 ? 1 : 0;
                }
            });

        /** Mutex. */
        private final Object mux0 = new Object();

        /**
         *
         */
        SocketTimeoutWorker() {
            super(gridName, "tcp-disco-sock-timeout-worker", log);

            setPriority(threadPri);
        }

        /**
         * @param timeoutObj Timeout object to add.
         */
        @SuppressWarnings({"NakedNotify"})
        public void addTimeoutObject(SocketTimeoutObject timeoutObj) {
            assert timeoutObj != null && timeoutObj.endTime() > 0 && timeoutObj.endTime() != Long.MAX_VALUE;

            timeoutObjs.add(timeoutObj);

            if (timeoutObjs.firstx() == timeoutObj) {
                synchronized (mux0) {
                    mux0.notifyAll();
                }
            }
        }

        /**
         * @param timeoutObj Timeout object to remove.
         */
        public void removeTimeoutObject(SocketTimeoutObject timeoutObj) {
            assert timeoutObj != null;

            timeoutObjs.remove(timeoutObj);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Socket timeout worker has been started.");

            while (!isInterrupted()) {
                long now = U.currentTimeMillis();

                for (Iterator<SocketTimeoutObject> iter = timeoutObjs.iterator(); iter.hasNext(); ) {
                    SocketTimeoutObject timeoutObj = iter.next();

                    if (timeoutObj.endTime() <= now) {
                        iter.remove();

                        if (timeoutObj.onTimeout()) {
                            LT.warn(log, null, "Socket write has timed out (consider increasing " +
                                "'sockTimeout' configuration property) [sockTimeout=" + sockTimeout + ']');

                            stats.onSocketTimeout();
                        }
                    }
                    else
                        break;
                }

                synchronized (mux0) {
                    while (true) {
                        // Access of the first element must be inside of
                        // synchronization block, so we don't miss out
                        // on thread notification events sent from
                        // 'addTimeoutObject(..)' method.
                        SocketTimeoutObject first = timeoutObjs.firstx();

                        if (first != null) {
                            long waitTime = first.endTime() - U.currentTimeMillis();

                            if (waitTime > 0)
                                mux0.wait(waitTime);
                            else
                                break;
                        }
                        else
                            mux0.wait(5000);
                    }
                }
            }
        }
    }

    /**
     *
     */
    private static class SocketTimeoutObject {
        /** */
        private static final AtomicLong idGen = new AtomicLong();

        /** */
        private final long id = idGen.incrementAndGet();

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

        /**
         * @return {@code True} if object has not yet been canceled.
         */
        boolean onTimeout() {
            if (done.compareAndSet(false, true)) {
                // Close socket - timeout occurred.
                U.closeQuiet(sock);

                return true;
            }

            return false;
        }

        /**
         * @return End time.
         */
        long endTime() {
            return endTime;
        }

        /**
         * @return ID.
         */
        long id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SocketTimeoutObject.class, this);
        }
    }
}
