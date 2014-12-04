/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.communication.tcp;

import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.GridSystemProperties.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * <tt>GridTcpCommunicationSpi</tt> is default communication SPI which uses
 * TCP/IP protocol and Java NIO to communicate with other nodes.
 * <p>
 * To enable communication with other nodes, this SPI adds {@link #ATTR_ADDRS}
 * and {@link #ATTR_PORT} local node attributes (see {@link org.apache.ignite.cluster.ClusterNode#attributes()}.
 * <p>
 * At startup, this SPI tries to start listening to local port specified by
 * {@link #setLocalPort(int)} method. If local port is occupied, then SPI will
 * automatically increment the port number until it can successfully bind for
 * listening. {@link #setLocalPortRange(int)} configuration parameter controls
 * maximum number of ports that SPI will try before it fails. Port range comes
 * very handy when starting multiple grid nodes on the same machine or even
 * in the same VM. In this case all nodes can be brought up without a single
 * change in configuration.
 * <p>
 * This SPI caches connections to remote nodes so it does not have to reconnect every
 * time a message is sent. By default, idle connections are kept active for
 * {@link #DFLT_IDLE_CONN_TIMEOUT} period and then are closed. Use
 * {@link #setIdleConnectionTimeout(long)} configuration parameter to configure
 * you own idle connection timeout.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>Node local IP address (see {@link #setLocalAddress(String)})</li>
 * <li>Node local port number (see {@link #setLocalPort(int)})</li>
 * <li>Local port range (see {@link #setLocalPortRange(int)}</li>
 * <li>Connection buffer flush frequency (see {@link #setConnectionBufferFlushFrequency(long)})</li>
 * <li>Connection buffer size (see {@link #setConnectionBufferSize(int)})</li>
 * <li>Idle connection timeout (see {@link #setIdleConnectionTimeout(long)})</li>
 * <li>Direct or heap buffer allocation (see {@link #setDirectBuffer(boolean)})</li>
 * <li>Direct or heap buffer allocation for sending (see {@link #setDirectSendBuffer(boolean)})</li>
 * <li>Count of selectors and selector threads for NIO server (see {@link #setSelectorsCount(int)})</li>
 * <li>{@code TCP_NODELAY} socket option for sockets (see {@link #setTcpNoDelay(boolean)})</li>
 * <li>Async message sending (see {@link #setAsyncSend(boolean)})</li>
 * <li>Message queue limit (see {@link #setMessageQueueLimit(int)})</li>
 * <li>Dual socket connection (see {@link #setDualSocketConnection(boolean)})</li>
 * <li>Minimum buffered message count (see {@link #setMinimumBufferedMessageCount(int)})</li>
 * <li>Buffer size ratio (see {@link #setBufferSizeRatio(double)})</li>
 * <li>Connect timeout (see {@link #setConnectTimeout(long)})</li>
 * <li>Maximum connect timeout (see {@link #setMaxConnectTimeout(long)})</li>
 * <li>Reconnect attempts count (see {@link #setReconnectCount(int)})</li>
 * <li>Local port to accept shared memory connections (see {@link #setSharedMemoryPort(int)})</li>
 * <li>Socket receive buffer size (see {@link #setSocketReceiveBuffer(int)})</li>
 * <li>Socket send buffer size (see {@link #setSocketSendBuffer(int)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * GridTcpCommunicationSpi is used by default and should be explicitly configured
 * only if some SPI configuration parameters need to be overridden.
 * <pre name="code" class="java">
 * GridTcpCommunicationSpi commSpi = new GridTcpCommunicationSpi();
 *
 * // Override local port.
 * commSpi.setLocalPort(4321);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default communication SPI.
 * cfg.setCommunicationSpi(commSpi);
 *
 * // Start grid.
 * GridGain.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridTcpCommunicationSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="communicationSpi"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.communication.tcp.GridTcpCommunicationSpi"&gt;
 *                 &lt;!-- Override local port. --&gt;
 *                 &lt;property name="localPort" value="4321"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridCommunicationSpi
 */
@GridSpiMultipleInstancesSupport(true)
@GridSpiConsistencyChecked(optional = false)
public class GridTcpCommunicationSpi extends GridSpiAdapter
    implements GridCommunicationSpi<GridTcpCommunicationMessageAdapter>, GridTcpCommunicationSpiMBean {
    /** IPC error message. */
    public static final String OUT_OF_RESOURCES_TCP_MSG = "Failed to allocate shared memory segment " +
        "(switching to TCP, may be slower). For troubleshooting see " +
        GridIpcSharedMemoryServerEndpoint.TROUBLESHOOTING_URL;

    /** Node attribute that is mapped to node IP addresses (value is <tt>comm.tcp.addrs</tt>). */
    public static final String ATTR_ADDRS = "comm.tcp.addrs";

    /** Node attribute that is mapped to node host names (value is <tt>comm.tcp.host.names</tt>). */
    public static final String ATTR_HOST_NAMES = "comm.tcp.host.names";

    /** Node attribute that is mapped to node port number (value is <tt>comm.tcp.port</tt>). */
    public static final String ATTR_PORT = "comm.tcp.port";

    /** Node attribute that is mapped to node port number (value is <tt>comm.shmem.tcp.port</tt>). */
    public static final String ATTR_SHMEM_PORT = "comm.shmem.tcp.port";

    /** Node attribute that is mapped to node's external addresses (value is <tt>comm.tcp.ext-addrs</tt>). */
    public static final String ATTR_EXT_ADDRS = "comm.tcp.ext-addrs";

    /** Default port which node sets listener to (value is <tt>47100</tt>). */
    public static final int DFLT_PORT = 47100;

    /** Default port which node sets listener for shared memory connections (value is <tt>48100</tt>). */
    public static final int DFLT_SHMEM_PORT = 48100;

    /** Default idle connection timeout (value is <tt>30000</tt>ms). */
    public static final long DFLT_IDLE_CONN_TIMEOUT = 30000;

    /** Default value for connection buffer flush frequency (value is <tt>100</tt> ms). */
    public static final long DFLT_CONN_BUF_FLUSH_FREQ = 100;

    /** Default value for connection buffer size (value is <tt>0</tt>). */
    public static final int DFLT_CONN_BUF_SIZE = 0;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

    /** Default connection timeout (value is <tt>1000</tt>ms). */
    public static final long DFLT_CONN_TIMEOUT = 1000;

    /** Default Maximum connection timeout (value is <tt>600,000</tt>ms). */
    public static final long DFLT_MAX_CONN_TIMEOUT = 10 * 60 * 1000;

    /** Default reconnect attempts count (value is <tt>10</tt>). */
    public static final int DFLT_RECONNECT_CNT = 10;

    /** Default message queue limit per connection (for incoming and outgoing . */
    public static final int DFLT_MSG_QUEUE_LIMIT = GridNioServer.DFLT_SEND_QUEUE_LIMIT;

    /** Default value for dualSocketConnection flag. */
    public static final boolean DFLT_DUAL_SOCKET_CONNECTION = false;

    /**
     * Default count of selectors for TCP server equals to
     * {@code "Math.min(4, Runtime.getRuntime().availableProcessors())"}.
     */
    public static final int DFLT_SELECTORS_CNT = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Node ID meta for session. */
    private static final int NODE_ID_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Message tracker meta for session. */
    private static final int TRACKER_META = GridNioSessionMetaKey.nextUniqueKey();

    /**
     * Default local port range (value is <tt>100</tt>).
     * See {@link #setLocalPortRange(int)} for details.
     */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default value for {@code TCP_NODELAY} socket option (value is <tt>true</tt>). */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** No-op runnable. */
    private static final IgniteRunnable NOOP = new IgniteRunnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** Node ID message type. */
    public static final byte NODE_ID_MSG_TYPE = -1;

    /** Server listener. */
    private final GridNioServerListener<GridTcpCommunicationMessageAdapter> srvLsnr =
        new GridNioServerListenerAdapter<GridTcpCommunicationMessageAdapter>() {
            @Override public void onConnected(GridNioSession ses) {
                if (ses.accepted()) {
                    if (log.isDebugEnabled())
                        log.debug("Sending local node ID to newly accepted session: " + ses);

                    ses.send(nodeIdMsg);
                }
                else
                    assert asyncSnd;
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                UUID id = ses.meta(NODE_ID_META);

                if (id != null) {
                    GridCommunicationClient rmv = clients.get(id);

                    if (rmv instanceof GridTcpNioCommunicationClient &&
                        ((GridTcpNioCommunicationClient)rmv).session() == ses &&
                        clients.remove(id, rmv))
                        rmv.forceClose();

                    GridCommunicationListener<GridTcpCommunicationMessageAdapter> lsnr0 = lsnr;

                    if (lsnr0 != null)
                        lsnr0.onDisconnected(id);
                }
            }

            @Override public void onMessage(GridNioSession ses, GridTcpCommunicationMessageAdapter msg) {
                UUID sndId = ses.meta(NODE_ID_META);

                if (sndId == null) {
                    assert ses.accepted();

                    assert msg instanceof NodeIdMessage;

                    sndId = U.bytesToUuid(((NodeIdMessage)msg).nodeIdBytes, 0);

                    if (log.isDebugEnabled())
                        log.debug("Remote node ID received: " + sndId);

                    UUID old = ses.addMeta(NODE_ID_META, sndId);

                    assert old == null;

                    GridProductVersion locVer = getSpiContext().localNode().version();

                    ClusterNode rmtNode = getSpiContext().node(sndId);

                    if (rmtNode == null) {
                        ses.close();

                        return;
                    }

                    GridProductVersion rmtVer = rmtNode.version();

                    if (!locVer.equals(rmtVer))
                        ses.addMeta(GridNioServer.DIFF_VER_NODE_ID_META_KEY, sndId);

                    if (asyncSnd && ses.remoteAddress() != null && !dualSockConn) {
                        Object sync = locks.tryLock(sndId);

                        if (sync != null) {
                            try {
                                if (clients.get(sndId) == null) {
                                    if (log.isDebugEnabled())
                                        log.debug("Will reuse session for node: " + sndId);

                                    clients.put(sndId, new GridTcpNioCommunicationClient(ses));
                                }
                            }
                            finally {
                                locks.unlock(sndId, sync);
                            }
                        }
                    }
                }
                else {
                    rcvdMsgsCnt.increment();

                    IgniteRunnable c;

                    if (msgQueueLimit > 0) {
                        GridNioMessageTracker tracker = ses.meta(TRACKER_META);

                        if (tracker == null) {
                            GridNioMessageTracker old = ses.addMeta(TRACKER_META, tracker =
                                new GridNioMessageTracker(ses, msgQueueLimit));

                            assert old == null;
                        }

                        tracker.onMessageReceived();

                        c = tracker;
                    }
                    else
                        c = NOOP;

                    notifyListener(sndId, msg, c);
                }
            }
        };

    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Node ID. */
    @GridLocalNodeIdResource
    private UUID locNodeId;

    /** Marshaller. */
    @GridMarshallerResource
    private GridMarshaller marsh;

    /** Local IP address. */
    private String locAddr;

    /** Complex variable that represents this node IP address. */
    private volatile InetAddress locHost;

    /** Local port which node uses. */
    private int locPort = DFLT_PORT;

    /** Local port range. */
    private int locPortRange = DFLT_PORT_RANGE;

    /** Local port which node uses to accept shared memory connections. */
    private int shmemPort = DFLT_SHMEM_PORT;

    /** Grid name. */
    @GridNameResource
    private String gridName;

    /** Allocate direct buffer or heap buffer. */
    private boolean directBuf = true;

    /** Allocate direct buffer or heap buffer. */
    private boolean directSndBuf;

    /** Idle connection timeout. */
    private long idleConnTimeout = DFLT_IDLE_CONN_TIMEOUT;

    /** Connection buffer flush frequency. */
    private volatile long connBufFlushFreq = DFLT_CONN_BUF_FLUSH_FREQ;

    /** Connection buffer size. */
    @SuppressWarnings("RedundantFieldInitialization")
    private int connBufSize = DFLT_CONN_BUF_SIZE;

    /** Connect timeout. */
    private long connTimeout = DFLT_CONN_TIMEOUT;

    /** Maximum connect timeout. */
    private long maxConnTimeout = DFLT_MAX_CONN_TIMEOUT;

    /** Reconnect attempts count. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private int reconCnt = DFLT_RECONNECT_CNT;

    /** Socket send buffer. */
    private int sockSndBuf = DFLT_SOCK_BUF_SIZE;

    /** Socket receive buffer. */
    private int sockRcvBuf = DFLT_SOCK_BUF_SIZE;

    /** Message queue limit. */
    private int msgQueueLimit = DFLT_MSG_QUEUE_LIMIT;

    /** Min buffered message count. */
    private int minBufferedMsgCnt = Integer.getInteger(GG_MIN_BUFFERED_COMMUNICATION_MSG_CNT, 512);

    /** Buffer size ratio. */
    private double bufSizeRatio = GridSystemProperties.getDouble(GG_COMMUNICATION_BUF_RESIZE_RATIO, 0.8);

    /** Dual socket connection flag. */
    private boolean dualSockConn = DFLT_DUAL_SOCKET_CONNECTION;

    /** NIO server. */
    private GridNioServer<GridTcpCommunicationMessageAdapter> nioSrvr;

    /** Shared memory server. */
    private GridIpcSharedMemoryServerEndpoint shmemSrv;

    /** {@code TCP_NODELAY} option value for created sockets. */
    private boolean tcpNoDelay = DFLT_TCP_NODELAY;

    /** Use async client flag. */
    private boolean asyncSnd = true;

    /** Shared memory accept worker. */
    private ShmemAcceptWorker shmemAcceptWorker;

    /** Idle client worker. */
    private IdleClientWorker idleClientWorker;

    /** Flush client worker. */
    private ClientFlushWorker clientFlushWorker;

    /** Socket timeout worker. */
    private SocketTimeoutWorker sockTimeoutWorker;

    /** Shared memory workers. */
    private final Collection<ShmemWorker> shmemWorkers = new ConcurrentLinkedDeque8<>();

    /** Clients. */
    private final ConcurrentMap<UUID, GridCommunicationClient> clients = GridConcurrentFactory.newMap();

    /** SPI listener. */
    private volatile GridCommunicationListener<GridTcpCommunicationMessageAdapter> lsnr;

    /** Bound port. */
    private int boundTcpPort = -1;

    /** Bound port for shared memory server. */
    private int boundTcpShmemPort = -1;

    /** Count of selectors to use in TCP server. */
    private int selectorsCnt = DFLT_SELECTORS_CNT;

    /** Address resolver. */
    private GridAddressResolver addrRslvr;

    /** Local node ID message. */
    private NodeIdMessage nodeIdMsg;

    /** Received messages count. */
    private final LongAdder rcvdMsgsCnt = new LongAdder();

    /** Sent messages count.*/
    private final LongAdder sentMsgsCnt = new LongAdder();

    /** Received bytes count. */
    private final LongAdder rcvdBytesCnt = new LongAdder();

    /** Sent bytes count.*/
    private final LongAdder sentBytesCnt = new LongAdder();

    /** Context initialization latch. */
    private final CountDownLatch ctxInitLatch = new CountDownLatch(1);

    /** metrics listener. */
    private final GridNioMetricsListener metricsLsnr = new GridNioMetricsListener() {
        @Override public void onBytesSent(int bytesCnt) {
            sentBytesCnt.add(bytesCnt);
        }

        @Override public void onBytesReceived(int bytesCnt) {
            rcvdBytesCnt.add(bytesCnt);
        }
    };

    /** Locks. */
    private final GridKeyLock locks = new GridKeyLock();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(IgniteEvent evt) {
            assert evt instanceof IgniteDiscoveryEvent;
            assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

            onNodeLeft(((IgniteDiscoveryEvent)evt).eventNode().id());
        }
    };

    /** Message reader. */
    private final GridNioMessageReader msgReader = new GridNioMessageReader() {
        /** */
        private GridTcpMessageFactory msgFactory;

        @Override public boolean read(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, ByteBuffer buf) {
            assert msg != null;
            assert buf != null;

            msg.messageReader(this, nodeId);

            boolean finished = msg.readFrom(buf);

            if (finished && nodeId != null)
                finished = getSpiContext().readDelta(nodeId, msg.getClass(), buf);

            return finished;
        }

        @Nullable @Override public GridTcpMessageFactory messageFactory() {
            if (msgFactory == null)
                msgFactory = getSpiContext().messageFactory();

            return msgFactory;
        }
    };

    /** Message writer. */
    private final GridNioMessageWriter msgWriter = new GridNioMessageWriter() {
        @Override public boolean write(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, ByteBuffer buf) {
            assert msg != null;
            assert buf != null;

            msg.messageWriter(this, nodeId);

            boolean finished = msg.writeTo(buf);

            if (finished && nodeId != null)
                finished = getSpiContext().writeDelta(nodeId, msg, buf);

            return finished;
        }

        @Override public int writeFully(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, OutputStream out,
            ByteBuffer buf) throws IOException {
            assert msg != null;
            assert out != null;
            assert buf != null;
            assert buf.hasArray();

            msg.messageWriter(this, nodeId);

            boolean finished = false;
            int cnt = 0;

            while (!finished) {
                finished = msg.writeTo(buf);

                out.write(buf.array(), 0, buf.position());

                cnt += buf.position();

                buf.clear();
            }

            if (nodeId != null) {
                while (!finished) {
                    finished = getSpiContext().writeDelta(nodeId, msg.getClass(), buf);

                    out.write(buf.array(), 0, buf.position());

                    cnt += buf.position();

                    buf.clear();
                }
            }

            return cnt;
        }
    };

    /**
     * Sets address resolver.
     *
     * @param addrRslvr Address resolver.
     */
    @GridSpiConfiguration(optional = true)
    @IgniteAddressResolverResource
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

    /**
     * Sets local host address for socket binding. Note that one node could have
     * additional addresses beside the loopback one. This configuration
     * parameter is optional.
     *
     * @param locAddr IP address. Default value is any available local
     *      IP address.
     */
    @GridSpiConfiguration(optional = true)
    @GridLocalHostResource
    public void setLocalAddress(String locAddr) {
        // Injection should not override value already set by Spring or user.
        if (this.locAddr == null)
            this.locAddr = locAddr;
    }

    /** {@inheritDoc} */
    @Override public String getLocalAddress() {
        return locAddr;
    }

    /**
     * Sets local port for socket binding.
     * <p>
     * If not provided, default value is {@link #DFLT_PORT}.
     *
     * @param locPort Port number.
     */
    @GridSpiConfiguration(optional = true)
    public void setLocalPort(int locPort) {
        this.locPort = locPort;
    }

    /** {@inheritDoc} */
    @Override public int getLocalPort() {
        return locPort;
    }

    /**
     * Sets local port range for local host ports (value must greater than or equal to <tt>0</tt>).
     * If provided local port (see {@link #setLocalPort(int)}} is occupied,
     * implementation will try to increment the port number for as long as it is less than
     * initial value plus this range.
     * <p>
     * If port range value is <tt>0</tt>, then implementation will try bind only to the port provided by
     * {@link #setLocalPort(int)} method and fail if binding to this port did not succeed.
     * <p>
     * Local port range is very useful during development when more than one grid nodes need to run
     * on the same physical machine.
     * <p>
     * If not provided, default value is {@link #DFLT_PORT_RANGE}.
     *
     * @param locPortRange New local port range.
     */
    @GridSpiConfiguration(optional = true)
    public void setLocalPortRange(int locPortRange) {
        this.locPortRange = locPortRange;
    }

    /** {@inheritDoc} */
    @Override public int getLocalPortRange() {
        return locPortRange;
    }

    /**
     * Sets local port to accept shared memory connections.
     * <p>
     * If set to {@code -1} shared memory communication will be disabled.
     * <p>
     * If not provided, default value is {@link #DFLT_SHMEM_PORT}.
     *
     * @param shmemPort Port number.
     */
    @GridSpiConfiguration(optional = true)
    public void setSharedMemoryPort(int shmemPort) {
        this.shmemPort = shmemPort;
    }

    /** {@inheritDoc} */
    @Override public int getSharedMemoryPort() {
        return shmemPort;
    }

    /**
     * Sets maximum idle connection timeout upon which a connection
     * to client will be closed.
     * <p>
     * If not provided, default value is {@link #DFLT_IDLE_CONN_TIMEOUT}.
     *
     * @param idleConnTimeout Maximum idle connection time.
     */
    @GridSpiConfiguration(optional = true)
    public void setIdleConnectionTimeout(long idleConnTimeout) {
        this.idleConnTimeout = idleConnTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getIdleConnectionTimeout() {
        return idleConnTimeout;
    }

    /**
     * Sets connection buffer size. If set to {@code 0} connection buffer is disabled.
     * <p>
     * If not provided, default value is {@link #DFLT_CONN_BUF_SIZE}.
     *
     * @param connBufSize Connection buffer size.
     * @see #setConnectionBufferFlushFrequency(long)
     */
    @GridSpiConfiguration(optional = true)
    public void setConnectionBufferSize(int connBufSize) {
        this.connBufSize = connBufSize;
    }

    /** {@inheritDoc} */
    @Override public int getConnectionBufferSize() {
        return connBufSize;
    }

    /** {@inheritDoc} */
    @GridSpiConfiguration(optional = true)
    @Override public void setConnectionBufferFlushFrequency(long connBufFlushFreq) {
        this.connBufFlushFreq = connBufFlushFreq;
    }

    /** {@inheritDoc} */
    @Override public long getConnectionBufferFlushFrequency() {
        return connBufFlushFreq;
    }

    /**
     * Sets connect timeout used when establishing connection
     * with remote nodes.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link #DFLT_CONN_TIMEOUT}.
     *
     * @param connTimeout Connect timeout.
     */
    @GridSpiConfiguration(optional = true)
    public void setConnectTimeout(long connTimeout) {
        this.connTimeout = connTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getConnectTimeout() {
        return connTimeout;
    }

    /**
     * Sets maximum connect timeout. If handshake is not established within connect timeout,
     * then SPI tries to repeat handshake procedure with increased connect timeout.
     * Connect timeout can grow till maximum timeout value,
     * if maximum timeout value is reached then the handshake is considered as failed.
     * <p>
     * {@code 0} is interpreted as infinite timeout.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_CONN_TIMEOUT}.
     *
     * @param maxConnTimeout Maximum connect timeout.
     */
    @GridSpiConfiguration(optional = true)
    public void setMaxConnectTimeout(long maxConnTimeout) {
        this.maxConnTimeout = maxConnTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getMaxConnectTimeout() {
        return maxConnTimeout;
    }

    /**
     * Sets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     * <p>
     * If not provided, default value is {@link #DFLT_RECONNECT_CNT}.
     *
     * @param reconCnt Maximum number of reconnection attempts.
     */
    @GridSpiConfiguration(optional = true)
    public void setReconnectCount(int reconCnt) {
        this.reconCnt = reconCnt;
    }

    /** {@inheritDoc} */
    @Override public int getReconnectCount() {
        return reconCnt;
    }

    /**
     * Sets flag to allocate direct or heap buffer in SPI.
     * If value is {@code true}, then SPI will use {@link ByteBuffer#allocateDirect(int)} call.
     * Otherwise, SPI will use {@link ByteBuffer#allocate(int)} call.
     * <p>
     * If not provided, default value is {@code true}.
     *
     * @param directBuf Flag indicates to allocate direct or heap buffer in SPI.
     */
    @GridSpiConfiguration(optional = true)
    public void setDirectBuffer(boolean directBuf) {
        this.directBuf = directBuf;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectBuffer() {
        return directBuf;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectSendBuffer() {
        return directSndBuf;
    }

    /**
     * Sets whether to use direct buffer for sending.
     * <p>
     * If not provided default is {@code false}.
     *
     * @param directSndBuf {@code True} to use direct buffers for send.
     */
    @GridSpiConfiguration(optional = true)
    public void setDirectSendBuffer(boolean directSndBuf) {
        this.directSndBuf = directSndBuf;
    }

    /**
     * Sets the count of selectors te be used in TCP server.
     * <p/>
     * If not provided, default value is {@link #DFLT_SELECTORS_CNT}.
     *
     * @param selectorsCnt Selectors count.
     */
    @GridSpiConfiguration(optional = true)
    public void setSelectorsCount(int selectorsCnt) {
        this.selectorsCnt = selectorsCnt;
    }

    /** {@inheritDoc} */
    @Override public int getSelectorsCount() {
        return selectorsCnt;
    }

    /** {@inheritDoc} */
    @Override public boolean isAsyncSend() {
        return asyncSnd;
    }

    /**
     * Sets flag defining whether asynchronous (NIO) or synchronous (blocking) IO
     * should be used to send messages.
     * <p>
     * If not provided, default value is {@code true}.
     *
     * @param asyncSnd {@code True} if asynchronous IO should be used to send messages.
     */
    @GridSpiConfiguration(optional = true)
    public void setAsyncSend(boolean asyncSnd) {
        this.asyncSnd = asyncSnd;
    }

    /**
     * Sets value for {@code TCP_NODELAY} socket option. Each
     * socket will be opened using provided value.
     * <p>
     * Setting this option to {@code true} disables Nagle's algorithm
     * for socket decreasing latency and delivery time for small messages.
     * <p>
     * For systems that work under heavy network load it is advisable to
     * set this value to {@code false}.
     * <p>
     * If not provided, default value is {@link #DFLT_TCP_NODELAY}.
     *
     * @param tcpNoDelay {@code True} to disable TCP delay.
     */
    @GridSpiConfiguration(optional = true)
    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /** {@inheritDoc} */
    @Override public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Sets receive buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockRcvBuf Socket receive buffer size.
     */
    @GridSpiConfiguration(optional = true)
    public void setSocketReceiveBuffer(int sockRcvBuf) {
        this.sockRcvBuf = sockRcvBuf;
    }

    /** {@inheritDoc} */
    @Override public int getSocketReceiveBuffer() {
        return sockRcvBuf;
    }

    /**
     * Sets send buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @param sockSndBuf Socket send buffer size.
     */
    @GridSpiConfiguration(optional = true)
    public void setSocketSendBuffer(int sockSndBuf) {
        this.sockSndBuf = sockSndBuf;
    }

    /** {@inheritDoc} */
    @Override public int getSocketSendBuffer() {
        return sockSndBuf;
    }

    /**
     * Sets flag indicating whether dual-socket connection between nodes should be enforced. If set to
     * {@code true}, two separate connections will be established between communicating nodes: one for outgoing
     * messages, and one for incoming. When set to {@code false}, single {@code TCP} connection will be used
     * for both directions.
     * <p>
     * This flag is useful on some operating systems, when {@code TCP_NODELAY} flag is disabled and
     * messages take too long to get delivered.
     * <p>
     * If not provided, default is {@code false}.
     *
     * @param dualSockConn Whether dual-socket connection should be enforced.
     */
    @GridSpiConfiguration(optional = true)
    public void setDualSocketConnection(boolean dualSockConn) {
        this.dualSockConn = dualSockConn;
    }

    /** {@inheritDoc} */
    @Override public boolean isDualSocketConnection() {
        return dualSockConn;
    }

    /**
     * Sets message queue limit for incoming and outgoing messages.
     * <p>
     * This parameter only used when {@link #isAsyncSend()} set to {@code true}.
     * <p>
     * When set to positive number send queue is limited to the configured value.
     * {@code 0} disables the size limitations.
     * <p>
     * If not provided, default is {@link #DFLT_MSG_QUEUE_LIMIT}.
     *
     * @param msgQueueLimit Send queue size limit.
     */
    @GridSpiConfiguration(optional = true)
    public void setMessageQueueLimit(int msgQueueLimit) {
        this.msgQueueLimit = msgQueueLimit;
    }

    /** {@inheritDoc} */
    @Override public int getMessageQueueLimit() {
        return msgQueueLimit;
    }

    /**
     * Sets the minimum number of messages for this SPI, that are buffered
     * prior to sending.
     * <p>
     * Defaults to either {@code 512} or {@link GridSystemProperties#GG_MIN_BUFFERED_COMMUNICATION_MSG_CNT}
     * system property (if specified).
     *
     * @param minBufferedMsgCnt Minimum buffered message count.
     */
    @GridSpiConfiguration(optional = true)
    public void setMinimumBufferedMessageCount(int minBufferedMsgCnt) {
        this.minBufferedMsgCnt = minBufferedMsgCnt;
    }

    /** {@inheritDoc} */
    @Override public int getMinimumBufferedMessageCount() {
        return minBufferedMsgCnt;
    }

    /**
     * Sets the buffer size ratio for this SPI. As messages are sent,
     * the buffer size is adjusted using this ratio.
     * <p>
     * Defaults to either {@code 0.8} or {@link GridSystemProperties#GG_COMMUNICATION_BUF_RESIZE_RATIO}
     * system property (if specified).
     *
     * @param bufSizeRatio Buffer size ratio.
     */
    @GridSpiConfiguration(optional = true)
    public void setBufferSizeRatio(double bufSizeRatio) {
        this.bufSizeRatio = bufSizeRatio;
    }

    /** {@inheritDoc} */
    @Override public double getBufferSizeRatio() {
        return bufSizeRatio;
    }

    /** {@inheritDoc} */
    @Override public void setListener(GridCommunicationListener<GridTcpCommunicationMessageAdapter> lsnr) {
        this.lsnr = lsnr;
    }

    /**
     * @return Listener.
     */
    public GridCommunicationListener getListener() {
        return lsnr;
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return sentMsgsCnt.intValue();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return sentBytesCnt.longValue();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return rcvdMsgsCnt.intValue();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return rcvdBytesCnt.longValue();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return nioSrvr.outboundMessagesQueueSize();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        // Can't use 'reset' method because it is not thread-safe
        // according to javadoc.
        sentMsgsCnt.add(-sentMsgsCnt.sum());
        rcvdMsgsCnt.add(-rcvdMsgsCnt.sum());
        sentBytesCnt.add(-sentBytesCnt.sum());
        rcvdBytesCnt.add(-rcvdBytesCnt.sum());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws GridSpiException {
        nodeIdMsg = new NodeIdMessage(locNodeId);

        assertParameter(locPort > 1023, "locPort > 1023");
        assertParameter(locPort <= 0xffff, "locPort < 0xffff");
        assertParameter(locPortRange >= 0, "locPortRange >= 0");
        assertParameter(idleConnTimeout > 0, "idleConnTimeout > 0");
        assertParameter(connBufFlushFreq > 0, "connBufFlushFreq > 0");
        assertParameter(connBufSize >= 0, "connBufSize >= 0");
        assertParameter(sockRcvBuf >= 0, "sockRcvBuf >= 0");
        assertParameter(sockSndBuf >= 0, "sockSndBuf >= 0");
        assertParameter(msgQueueLimit >= 0, "msgQueueLimit >= 0");
        assertParameter(shmemPort > 0 || shmemPort == -1, "shmemPort > 0 || shmemPort == -1");
        assertParameter(reconCnt > 0, "reconnectCnt > 0");
        assertParameter(selectorsCnt > 0, "selectorsCnt > 0");
        assertParameter(minBufferedMsgCnt >= 0, "minBufferedMsgCnt >= 0");
        assertParameter(bufSizeRatio > 0 && bufSizeRatio < 1, "bufSizeRatio > 0 && bufSizeRatio < 1");
        assertParameter(connTimeout >= 0, "connTimeout >= 0");
        assertParameter(maxConnTimeout >= connTimeout, "maxConnTimeout >= connTimeout");

        try {
            locHost = U.resolveLocalHost(locAddr);
        }
        catch (IOException e) {
            throw new GridSpiException("Failed to initialize local address: " + locAddr, e);
        }

        try {
            shmemSrv = resetShmemServer();
        }
        catch (GridException e) {
            U.warn(log, "Failed to start shared memory communication server.", e);
        }

        try {
            // This method potentially resets local port to the value
            // local node was bound to.
            nioSrvr = resetNioServer();
        }
        catch (GridException e) {
            throw new GridSpiException("Failed to initialize TCP server: " + locHost, e);
        }

        // Set local node attributes.
        try {
            IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(locHost);

            Collection<InetSocketAddress> extAddrs = addrRslvr == null ? null :
                U.resolveAddresses(addrRslvr, F.flat(Arrays.asList(addrs.get1(), addrs.get2())), boundTcpPort);

            return F.asMap(
                createSpiAttributeName(ATTR_ADDRS), addrs.get1(),
                createSpiAttributeName(ATTR_HOST_NAMES), addrs.get2(),
                createSpiAttributeName(ATTR_PORT), boundTcpPort,
                createSpiAttributeName(ATTR_SHMEM_PORT), boundTcpShmemPort >= 0 ? boundTcpShmemPort : null,
                createSpiAttributeName(ATTR_EXT_ADDRS), extAddrs);
        }
        catch (IOException | GridException e) {
            throw new GridSpiException("Failed to resolve local host to addresses: " + locHost, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        assert locHost != null;

        // Start SPI start stopwatch.
        startStopwatch();

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("locAddr", locAddr));
            log.debug(configInfo("locPort", locPort));
            log.debug(configInfo("locPortRange", locPortRange));
            log.debug(configInfo("idleConnTimeout", idleConnTimeout));
            log.debug(configInfo("directBuf", directBuf));
            log.debug(configInfo("directSendBuf", directSndBuf));
            log.debug(configInfo("connBufSize", connBufSize));
            log.debug(configInfo("connBufFlushFreq", connBufFlushFreq));
            log.debug(configInfo("selectorsCnt", selectorsCnt));
            log.debug(configInfo("asyncSend", asyncSnd));
            log.debug(configInfo("tcpNoDelay", tcpNoDelay));
            log.debug(configInfo("sockSndBuf", sockSndBuf));
            log.debug(configInfo("sockRcvBuf", sockRcvBuf));
            log.debug(configInfo("shmemPort", shmemPort));
            log.debug(configInfo("msgQueueLimit", msgQueueLimit));
            log.debug(configInfo("dualSockConn", dualSockConn));
            log.debug(configInfo("minBufferedMsgCnt", minBufferedMsgCnt));
            log.debug(configInfo("bufSizeRatio", bufSizeRatio));
            log.debug(configInfo("connTimeout", connTimeout));
            log.debug(configInfo("maxConnTimeout", maxConnTimeout));
            log.debug(configInfo("reconCnt", reconCnt));
        }

        if (connBufSize > 8192)
            U.warn(log, "Specified communication IO buffer size is larger than recommended (ignore if done " +
                "intentionally) [specified=" + connBufSize + ", recommended=8192]",
                "Specified communication IO buffer size is larger than recommended (ignore if done intentionally).");

        if (!tcpNoDelay)
            U.quietAndWarn(log, "'TCP_NO_DELAY' for communication is off, which should be used with caution " +
                "since may produce significant delays with some scenarios.");

        registerMBean(gridName, this, GridTcpCommunicationSpiMBean.class);

        if (shmemSrv != null) {
            shmemAcceptWorker = new ShmemAcceptWorker(shmemSrv);

            new GridThread(shmemAcceptWorker).start();
        }

        nioSrvr.start();

        idleClientWorker = new IdleClientWorker();

        idleClientWorker.start();

        if (connBufSize > 0) {
            clientFlushWorker = new ClientFlushWorker();

            clientFlushWorker.start();
        }

        sockTimeoutWorker = new SocketTimeoutWorker();

        sockTimeoutWorker.start();

        // Ack start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} }*/
    @Override public void onContextInitialized0(GridSpiContext spiCtx) throws GridSpiException {
        spiCtx.registerPort(boundTcpPort, GridPortProtocol.TCP);

        // SPI can start without shmem port.
        if (boundTcpShmemPort > 0)
            spiCtx.registerPort(boundTcpShmemPort, GridPortProtocol.TCP);

        spiCtx.addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ctxInitLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public GridSpiContext getSpiContext() {
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

    /**
     * Recreates tpcSrvr socket instance.
     *
     * @return Server instance.
     * @throws GridException Thrown if it's not possible to create server.
     */
    private GridNioServer<GridTcpCommunicationMessageAdapter> resetNioServer() throws GridException {
        if (boundTcpPort >= 0)
            throw new GridException("Tcp NIO server was already created on port " + boundTcpPort);

        GridException lastEx = null;

        // If configured TCP port is busy, find first available in range.
        for (int port = locPort; port < locPort + locPortRange; port++) {
            try {
                GridNioServer<GridTcpCommunicationMessageAdapter> srvr =
                    GridNioServer.<GridTcpCommunicationMessageAdapter>builder()
                        .address(locHost)
                        .port(port)
                        .listener(srvLsnr)
                        .logger(log)
                        .selectorCount(selectorsCnt)
                        .gridName(gridName)
                        .tcpNoDelay(tcpNoDelay)
                        .directBuffer(directBuf)
                        .byteOrder(ByteOrder.nativeOrder())
                        .socketSendBufferSize(sockSndBuf)
                        .socketReceiveBufferSize(sockRcvBuf)
                        .sendQueueLimit(msgQueueLimit)
                        .directMode(true)
                        .metricsListener(metricsLsnr)
                        .messageWriter(msgWriter)
                        .filters(new GridNioCodecFilter(new GridDirectParser(msgReader, this), log, true),
                            new GridConnectionBytesVerifyFilter(log))
                        .build();

                boundTcpPort = port;

                // Ack Port the TCP server was bound to.
                if (log.isInfoEnabled())
                    log.info("Successfully bound to TCP port [port=" + boundTcpPort +
                        ", locHost=" + locHost + ']');

                srvr.idleTimeout(idleConnTimeout);

                return srvr;
            }
            catch (GridException e) {
                lastEx = e;

                if (log.isDebugEnabled())
                    log.debug("Failed to bind to local port (will try next port within range) [port=" + port +
                        ", locHost=" + locHost + ']');
            }
        }

        // If free port wasn't found.
        throw new GridException("Failed to bind to any port within range [startPort=" + locPort +
            ", portRange=" + locPortRange + ", locHost=" + locHost + ']', lastEx);
    }

    /**
     * Creates new shared memory communication server.
     * @return Server.
     * @throws GridException If failed.
     */
    @Nullable private GridIpcSharedMemoryServerEndpoint resetShmemServer() throws GridException {
        if (boundTcpShmemPort >= 0)
            throw new GridException("Shared memory server was already created on port " + boundTcpShmemPort);

        if (shmemPort == -1 || U.isWindows())
            return null;

        GridException lastEx = null;

        // If configured TCP port is busy, find first available in range.
        for (int port = shmemPort; port < shmemPort + locPortRange; port++) {
            try {
                GridIpcSharedMemoryServerEndpoint srv = new GridIpcSharedMemoryServerEndpoint(log, locNodeId, gridName);

                srv.setPort(port);

                srv.omitOutOfResourcesWarning(true);

                srv.start();

                boundTcpShmemPort = port;

                // Ack Port the TCP server was bound to.
                if (log.isInfoEnabled())
                    log.info("Successfully bound shared memory communication to TCP port [port=" + boundTcpShmemPort +
                        ", locHost=" + locHost + ']');

                return srv;
            }
            catch (GridException e) {
                lastEx = e;

                if (log.isDebugEnabled())
                    log.debug("Failed to bind to local port (will try next port within range) [port=" + port +
                        ", locHost=" + locHost + ']');
            }
        }

        // If free port wasn't found.
        throw new GridException("Failed to bind shared memory communication to any port within range [startPort=" +
            locPort + ", portRange=" + locPortRange + ", locHost=" + locHost + ']', lastEx);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        unregisterMBean();

        // Stop TCP server.
        if (nioSrvr != null)
            nioSrvr.stop();

        U.cancel(shmemAcceptWorker);
        U.join(shmemAcceptWorker, log);

        U.interrupt(idleClientWorker);
        U.interrupt(clientFlushWorker);
        U.interrupt(sockTimeoutWorker);

        U.join(idleClientWorker, log);
        U.join(clientFlushWorker, log);
        U.join(sockTimeoutWorker, log);

        U.cancel(shmemWorkers);
        U.join(shmemWorkers, log);

        shmemWorkers.clear();

        // Force closing on stop (safety).
        for (GridCommunicationClient client : clients.values())
            client.forceClose();

        // Clear resources.
        nioSrvr = null;
        idleClientWorker = null;

        boundTcpPort = -1;

        // Ack stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        if (ctxInitLatch.getCount() > 0)
            // Safety.
            ctxInitLatch.countDown();

        // Force closing.
        for (GridCommunicationClient client : clients.values())
            client.forceClose();

        getSpiContext().deregisterPorts();

        getSpiContext().removeLocalEventListener(discoLsnr);
    }

    /**
     * @param nodeId Left node ID.
     */
    void onNodeLeft(UUID nodeId) {
        assert nodeId != null;

        GridCommunicationClient client = clients.get(nodeId);

        if (client != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing NIO client close since node has left [nodeId=" + nodeId +
                    ", client=" + client + ']');

            client.forceClose();

            clients.remove(nodeId, client);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkConfigurationConsistency0(GridSpiContext spiCtx, ClusterNode node, boolean starting)
        throws GridSpiException {
        // These attributes are set on node startup in any case, so we MUST receive them.
        checkAttributePresence(node, createSpiAttributeName(ATTR_ADDRS));
        checkAttributePresence(node, createSpiAttributeName(ATTR_HOST_NAMES));
        checkAttributePresence(node, createSpiAttributeName(ATTR_PORT));
    }

    /**
     * Checks that node has specified attribute and prints warning if it does not.
     *
     * @param node Node to check.
     * @param attrName Name of the attribute.
     */
    private void checkAttributePresence(ClusterNode node, String attrName) {
        if (node.attribute(attrName) == null)
            U.warn(log, "Remote node has inconsistent configuration (required attribute was not found) " +
                "[attrName=" + attrName + ", nodeId=" + node.id() +
                "spiCls=" + U.getSimpleName(GridTcpCommunicationSpi.class) + ']');
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg) throws GridSpiException {
        assert node != null;
        assert msg != null;

        if (log.isTraceEnabled())
            log.trace("Sending message to node [node=" + node + ", msg=" + msg + ']');

        if (node.id().equals(locNodeId))
            notifyListener(locNodeId, msg, NOOP);
        else {
            GridCommunicationClient client = null;

            try {
                client = reserveClient(node);

                UUID nodeId = null;

                if (!client.async() && !getSpiContext().localNode().version().equals(node.version()))
                    nodeId = node.id();

                client.sendMessage(nodeId, msg);

                client.release();

                client = null;

                sentMsgsCnt.increment();
            }
            catch (GridException e) {
                throw new GridSpiException("Failed to send message to remote node: " + node, e);
            }
            finally {
                if (client != null && clients.remove(node.id(), client))
                    client.forceClose();
            }
        }
    }

    /**
     * Returns existing or just created client to node.
     *
     * @param node Node to which client should be open.
     * @return The existing or just created client.
     * @throws GridException Thrown if any exception occurs.
     */
    private GridCommunicationClient reserveClient(ClusterNode node) throws GridException {
        assert node != null;

        UUID nodeId = node.id();

        while (true) {
            GridCommunicationClient client = clients.get(nodeId);

            if (client == null) {
                // Do not allow concurrent connects.
                Object sync = locks.lock(nodeId);

                try {
                    client = clients.get(nodeId);

                    if (client == null) {
                        GridCommunicationClient old = clients.put(nodeId, client = createNioClient(node));

                        assert old == null;
                    }
                }
                finally {
                    locks.unlock(nodeId, sync);
                }

                assert client != null;

                if (getSpiContext().node(nodeId) == null) {
                    if (clients.remove(nodeId, client))
                        client.forceClose();

                    throw new GridSpiException("Destination node is not in topology: " + node.id());
                }
            }

            if (client.reserve())
                return client;
            else
                // Client has just been closed by idle worker. Help it and try again.
                clients.remove(nodeId, client);
        }
    }

    /**
     * @param node Node to create client for.
     * @return Client.
     * @throws GridException If failed.
     */
    @Nullable protected GridCommunicationClient createNioClient(ClusterNode node) throws GridException {
        assert node != null;

        Integer shmemPort = node.attribute(createSpiAttributeName(ATTR_SHMEM_PORT));

        ClusterNode locNode = getSpiContext().localNode();

        if (locNode == null)
            throw new GridException("Failed to create NIO client (local node is stopping)");

        // If remote node has shared memory server enabled and has the same set of MACs
        // then we are likely to run on the same host and shared memory communication could be tried.
        if (shmemPort != null && U.sameMacs(locNode, node)) {
            try {
                return createShmemClient(node, shmemPort);
            }
            catch (GridException e) {
                if (e.hasCause(GridIpcOutOfSystemResourcesException.class))
                    // Has cause or is itself the GridIpcOutOfSystemResourcesException.
                    LT.warn(log, null, OUT_OF_RESOURCES_TCP_MSG);
                else if (getSpiContext().node(node.id()) != null)
                    LT.warn(log, null, e.getMessage());
                else if (log.isDebugEnabled())
                    log.debug("Failed to establish shared memory connection with local node (node has left): " +
                        node.id());
            }
        }

        return createTcpClient(node);
    }

    /**
     * @param node Node.
     * @param port Port.
     * @return Client.
     * @throws GridException If failed.
     */
    @Nullable protected GridCommunicationClient createShmemClient(ClusterNode node, Integer port) throws GridException {
        int attempt = 1;

        int connectAttempts = 1;

        long connTimeout0 = connTimeout;

        while (true) {
            GridCommunicationClient client;

            try {
                client = new GridShmemCommunicationClient(metricsLsnr, port, connTimeout, log, msgWriter);
            }
            catch (GridException e) {
                // Reconnect for the second time, if connection is not established.
                if (connectAttempts < 2 && X.hasCause(e, ConnectException.class)) {
                    connectAttempts++;

                    continue;
                }

                throw e;
            }

            try {
                safeHandshake(client, node.id(), connTimeout0);
            }
            catch (HandshakeTimeoutException e) {
                if (log.isDebugEnabled())
                    log.debug("Handshake timedout (will retry with increased timeout) [timeout=" + connTimeout0 +
                        ", err=" + e.getMessage() + ", client=" + client + ']');

                client.forceClose();

                if (attempt == reconCnt || connTimeout0 > maxConnTimeout) {
                    if (log.isDebugEnabled())
                        log.debug("Handshake timedout (will stop attempts to perform the handshake) " +
                            "[timeout=" + connTimeout0 + ", maxConnTimeout=" + maxConnTimeout +
                            ", attempt=" + attempt + ", reconCnt=" + reconCnt +
                            ", err=" + e.getMessage() + ", client=" + client + ']');

                    throw e;
                }
                else {
                    attempt++;

                    connTimeout0 *= 2;

                    continue;
                }
            }
            catch (GridException | RuntimeException | Error e) {
                if (log.isDebugEnabled())
                    log.debug(
                        "Caught exception (will close client) [err=" + e.getMessage() + ", client=" + client + ']');

                client.forceClose();

                throw e;
            }

            return client;
        }
    }

    /**
     * Establish TCP connection to remote node and returns client.
     *
     * @param node Remote node.
     * @return Client.
     * @throws GridException If failed.
     */
    protected GridCommunicationClient createTcpClient(ClusterNode node) throws GridException {
        Collection<String> rmtAddrs0 = node.attribute(createSpiAttributeName(ATTR_ADDRS));
        Collection<String> rmtHostNames0 = node.attribute(createSpiAttributeName(ATTR_HOST_NAMES));
        Integer boundPort = node.attribute(createSpiAttributeName(ATTR_PORT));
        Collection<InetSocketAddress> extAddrs = node.attribute(createSpiAttributeName(ATTR_EXT_ADDRS));

        boolean isRmtAddrsExist = (!F.isEmpty(rmtAddrs0) && boundPort != null);
        boolean isExtAddrsExist = !F.isEmpty(extAddrs);

        if (!isRmtAddrsExist && !isExtAddrsExist)
            throw new GridException("Failed to send message to the destination node. Node doesn't have any " +
                "TCP communication addresses or mapped external addresses. Check configuration and make sure " +
                "that you use the same communication SPI on all nodes. Remote node id: " + node.id());

        List<InetSocketAddress> addrs;

        // Try to connect first on bound addresses.
        if (isRmtAddrsExist) {
            addrs = new ArrayList<>(U.toSocketAddresses(rmtAddrs0, rmtHostNames0, boundPort));

            boolean sameHost = U.sameMacs(getSpiContext().localNode(), node);

            Collections.sort(addrs, U.inetAddressesComparator(sameHost));
        }
        else
            addrs = new ArrayList<>();

        // Then on mapped external addresses.
        if (isExtAddrsExist)
            addrs.addAll(extAddrs);

        boolean conn = false;
        GridCommunicationClient client = null;
        GridException errs = null;

        int connectAttempts = 1;

        for (InetSocketAddress addr : addrs) {
            long connTimeout0 = connTimeout;

            int attempt = 1;

            while (!conn) { // Reconnection on handshake timeout.
                try {
                    if (asyncSnd) {
                        SocketChannel ch = SocketChannel.open();

                        ch.configureBlocking(true);

                        ch.socket().setTcpNoDelay(tcpNoDelay);
                        ch.socket().setKeepAlive(true);

                        if (sockRcvBuf > 0)
                            ch.socket().setReceiveBufferSize(sockRcvBuf);

                        if (sockSndBuf > 0)
                            ch.socket().setSendBufferSize(sockSndBuf);

                        ch.socket().connect(addr, (int)connTimeout);

                        safeHandshake(ch, node.id(), connTimeout0);

                        UUID diffVerNodeId = null;

                        GridProductVersion locVer = getSpiContext().localNode().version();
                        GridProductVersion rmtVer = node.version();

                        if (!locVer.equals(rmtVer))
                            diffVerNodeId = node.id();

                        GridNioSession ses = nioSrvr.createSession(
                            ch,
                            F.asMap(
                                NODE_ID_META, node.id(),
                                GridNioServer.DIFF_VER_NODE_ID_META_KEY, diffVerNodeId)
                        ).get();

                        client = new GridTcpNioCommunicationClient(ses);
                    }
                    else {
                        client = new GridTcpCommunicationClient(
                            metricsLsnr,
                            msgWriter,
                            addr,
                            locHost,
                            connTimeout,
                            tcpNoDelay,
                            sockRcvBuf,
                            sockSndBuf,
                            connBufSize,
                            minBufferedMsgCnt,
                            bufSizeRatio);

                        safeHandshake(client, node.id(), connTimeout0);
                    }

                    conn = true;
                }
                catch (HandshakeTimeoutException e) {
                    if (client != null) {
                        client.forceClose();

                        client = null;
                    }

                    if (log.isDebugEnabled())
                        log.debug(
                            "Handshake timedout (will retry with increased timeout) [timeout=" + connTimeout0 +
                                ", addr=" + addr + ", err=" + e + ']');

                    if (attempt == reconCnt || connTimeout0 > maxConnTimeout) {
                        if (log.isDebugEnabled())
                            log.debug("Handshake timedout (will stop attempts to perform the handshake) " +
                                "[timeout=" + connTimeout0 + ", maxConnTimeout=" + maxConnTimeout +
                                ", attempt=" + attempt + ", reconCnt=" + reconCnt +
                                ", err=" + e.getMessage() + ", addr=" + addr + ']');

                        if (errs == null)
                            errs = new GridException("Failed to connect to node (is node still alive?). " +
                                "Make sure that each GridComputeTask and GridCacheTransaction has a timeout set " +
                                "in order to prevent parties from waiting forever in case of network issues " +
                                "[nodeId=" + node.id() + ", addrs=" + addrs + ']');

                        errs.addSuppressed(new GridException("Failed to connect to address: " + addr, e));

                        break;
                    }
                    else {
                        attempt++;

                        connTimeout0 *= 2;

                        // Continue loop.
                    }
                }
                catch (Exception e) {
                    if (client != null) {
                        client.forceClose();

                        client = null;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Client creation failed [addr=" + addr + ", err=" + e + ']');

                    if (X.hasCause(e, SocketTimeoutException.class))
                        LT.warn(log, null, "Connect timed out (consider increasing 'connTimeout' " +
                            "configuration property) [addr=" + addr + ']');

                    if (errs == null)
                        errs = new GridException("Failed to connect to node (is node still alive?). " +
                            "Make sure that each GridComputeTask and GridCacheTransaction has a timeout set " +
                            "in order to prevent parties from waiting forever in case of network issues " +
                            "[nodeId=" + node.id() + ", addrs=" + addrs + ']');

                    errs.addSuppressed(new GridException("Failed to connect to address: " + addr, e));

                    // Reconnect for the second time, if connection is not established.
                    if (connectAttempts < 2 &&
                        (e instanceof ConnectException || X.hasCause(e, ConnectException.class))) {
                        connectAttempts++;

                        continue;
                    }

                    break;
                }
            }

            if (conn)
                break;
        }

        if (client == null) {
            assert errs != null;

            if (X.hasCause(errs, ConnectException.class))
                LT.warn(log, null, "Failed to connect to a remote node " +
                    "(make sure that destination node is alive and " +
                    "operating system firewall is disabled on local and remote hosts) " +
                    "[addrs=" + addrs + ']');

            throw errs;
        }

        if (log.isDebugEnabled())
            log.debug("Created client: " + client);

        return client;
    }

    /**
     * Performs handshake in timeout-safe way.
     *
     * @param client Client.
     * @param rmtNodeId Remote node.
     * @param timeout Timeout for handshake.
     * @throws GridException If handshake failed or wasn't completed withing timeout.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private <T> void safeHandshake(T client, UUID rmtNodeId, long timeout) throws GridException {
        HandshakeTimeoutObject<T> obj = new HandshakeTimeoutObject<>(client, U.currentTimeMillis() + timeout);

        sockTimeoutWorker.addTimeoutObject(obj);

        try {
            if (client instanceof GridCommunicationClient)
                ((GridCommunicationClient)client).doHandshake(new HandshakeClosure(rmtNodeId));
            else {
                SocketChannel ch = (SocketChannel)client;

                boolean success = false;

                try {
                    ByteBuffer buf = ByteBuffer.allocate(17);

                    for (int i = 0; i < 17; ) {
                        int read = ch.read(buf);

                        if (read == -1)
                            throw new GridException("Failed to read remote node ID (connection closed).");

                        i += read;
                    }

                    UUID rmtNodeId0 = U.bytesToUuid(buf.array(), 1);

                    if (!rmtNodeId.equals(rmtNodeId0))
                        throw new GridException("Remote node ID is not as expected [expected=" + rmtNodeId +
                            ", rcvd=" + rmtNodeId0 + ']');
                    else if (log.isDebugEnabled())
                        log.debug("Received remote node ID: " + rmtNodeId0);

                    ch.write(ByteBuffer.wrap(U.GG_HEADER));
                    ch.write(ByteBuffer.wrap(nodeIdMsg.nodeIdBytesWithType));

                    success = true;
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to read from channel: " + e);

                    throw new GridException("Failed to read from channel.", e);
                }
                finally {
                    if (!success)
                        U.closeQuiet(ch);
                }
            }
        }
        finally {
            boolean cancelled = obj.cancel();

            if (cancelled)
                sockTimeoutWorker.removeTimeoutObject(obj);

            // Ignoring whatever happened after timeout - reporting only timeout event.
            if (!cancelled)
                throw new HandshakeTimeoutException("Failed to perform handshake due to timeout (consider increasing " +
                    "'connectionTimeout' configuration property).");
        }
    }

    /**
     * @param sndId Sender ID.
     * @param msg Communication message.
     * @param msgC Closure to call when message processing finished.
     */
    protected void notifyListener(UUID sndId, GridTcpCommunicationMessageAdapter msg, IgniteRunnable msgC) {
        GridCommunicationListener<GridTcpCommunicationMessageAdapter> lsnr = this.lsnr;

        if (lsnr != null)
            // Notify listener of a new message.
            lsnr.onMessage(sndId, msg, msgC);
        else if (log.isDebugEnabled())
            log.debug("Received communication message without any registered listeners (will ignore, " +
                "is node stopping?) [senderNodeId=" + sndId + ", msg=" + msg + ']');
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpCommunicationSpi.class, this);
    }

    /** Internal exception class for proper timeout handling. */
    private static class HandshakeTimeoutException extends GridException {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param msg Message.
         */
        HandshakeTimeoutException(String msg) {
            super(msg);
        }
    }

    /**
     * This worker takes responsibility to shut the server down when stopping,
     * No other thread shall stop passed server.
     */
    private class ShmemAcceptWorker extends GridWorker {
        /** */
        private final GridIpcSharedMemoryServerEndpoint srv;

        /**
         * @param srv Server.
         */
        ShmemAcceptWorker(GridIpcSharedMemoryServerEndpoint srv) {
            super(gridName, "shmem-communication-acceptor", log);

            this.srv = srv;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                while (!Thread.interrupted()) {
                    ShmemWorker e = new ShmemWorker(srv.accept());

                    shmemWorkers.add(e);

                    new GridThread(e).start();
                }
            }
            catch (GridException e) {
                if (!isCancelled())
                    U.error(log, "Shmem server failed.", e);
            }
            finally {
                srv.close();
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            super.cancel();

            srv.close();
        }
    }

    /**
     *
     */
    private class ShmemWorker extends GridWorker {
        /** */
        private final GridIpcEndpoint endpoint;

        /**
         * @param endpoint Endpoint.
         */
        private ShmemWorker(GridIpcEndpoint endpoint) {
            super(gridName, "shmem-worker", log);

            this.endpoint = endpoint;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                GridIpcToNioAdapter<GridTcpCommunicationMessageAdapter> adapter = new GridIpcToNioAdapter<>(
                    metricsLsnr,
                    log,
                    endpoint,
                    msgWriter,
                    srvLsnr,
                    new GridNioCodecFilter(new GridDirectParser(msgReader, GridTcpCommunicationSpi.this), log, true),
                    new GridConnectionBytesVerifyFilter(log)
                );

                adapter.serve();
            }
            finally {
                shmemWorkers.remove(this);

                endpoint.close();
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            super.cancel();

            endpoint.close();
        }

        /** @{@inheritDoc} */
        @Override protected void cleanup() {
            super.cleanup();

            endpoint.close();
        }

        /** @{@inheritDoc} */
        @Override public String toString() {
            return S.toString(ShmemWorker.class, this);
        }
    }

    /**
     *
     */
    private class IdleClientWorker extends GridSpiThread {
        /**
         *
         */
        IdleClientWorker() {
            super(gridName, "nio-idle-client-collector", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override protected void body() throws InterruptedException {
            while (!isInterrupted()) {
                for (Map.Entry<UUID, GridCommunicationClient> e : clients.entrySet()) {
                    UUID nodeId = e.getKey();

                    GridCommunicationClient client = e.getValue();

                    if (getSpiContext().node(nodeId) == null) {
                        if (log.isDebugEnabled())
                            log.debug("Forcing close of non-existent node connection: " + nodeId);

                        client.forceClose();

                        clients.remove(nodeId, client);

                        continue;
                    }

                    long idleTime = client.getIdleTime();

                    if (idleTime >= idleConnTimeout) {
                        if (log.isDebugEnabled())
                            log.debug("Closing idle node connection: " + nodeId);

                        if (client.close() || client.closed())
                            clients.remove(nodeId, client);
                    }
                }

                Thread.sleep(idleConnTimeout);
            }
        }
    }

    /**
     *
     */
    private class ClientFlushWorker extends GridSpiThread {
        /**
         *
         */
        ClientFlushWorker() {
            super(gridName, "nio-client-flusher", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override protected void body() throws InterruptedException {
            while (!isInterrupted()) {
                long connBufFlushFreq0 = connBufFlushFreq;

                for (Map.Entry<UUID, GridCommunicationClient> entry : clients.entrySet()) {
                    GridCommunicationClient client = entry.getValue();

                    if (client.reserve()) {
                        boolean err = true;

                        try {
                            client.flushIfNeeded(connBufFlushFreq0);

                            err = false;
                        }
                        catch (IOException e) {
                            if (getSpiContext().pingNode(entry.getKey()))
                                U.error(log, "Failed to flush client: " + client, e);
                            else if (log.isDebugEnabled())
                                log.debug("Failed to flush client (node left): " + client);
                        }
                        finally {
                            if (err)
                                client.forceClose();
                            else
                                client.release();
                        }
                    }
                }

                Thread.sleep(connBufFlushFreq0);
            }
        }
    }

    /**
     * Handles sockets timeouts.
     */
    private class SocketTimeoutWorker extends GridSpiThread {
        /** Time-based sorted set for timeout objects. */
        private final GridConcurrentSkipListSet<HandshakeTimeoutObject> timeoutObjs =
            new GridConcurrentSkipListSet<>(new Comparator<HandshakeTimeoutObject>() {
                @Override public int compare(HandshakeTimeoutObject o1, HandshakeTimeoutObject o2) {
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
            super(gridName, "tcp-comm-sock-timeout-worker", log);
        }

        /**
         * @param timeoutObj Timeout object to add.
         */
        @SuppressWarnings({"NakedNotify"})
        public void addTimeoutObject(HandshakeTimeoutObject timeoutObj) {
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
        public void removeTimeoutObject(HandshakeTimeoutObject timeoutObj) {
            assert timeoutObj != null;

            timeoutObjs.remove(timeoutObj);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug("Socket timeout worker has been started.");

            while (!isInterrupted()) {
                long now = U.currentTimeMillis();

                for (Iterator<HandshakeTimeoutObject> iter = timeoutObjs.iterator(); iter.hasNext(); ) {
                    HandshakeTimeoutObject timeoutObj = iter.next();

                    if (timeoutObj.endTime() <= now) {
                        iter.remove();

                        timeoutObj.onTimeout();
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
                        HandshakeTimeoutObject first = timeoutObjs.firstx();

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
    private static class HandshakeTimeoutObject<T> {
        /** */
        private static final AtomicLong idGen = new AtomicLong();

        /** */
        private final long id = idGen.incrementAndGet();

        /** */
        private final T obj;

        /** */
        private final long endTime;

        /** */
        private final AtomicBoolean done = new AtomicBoolean();

        /**
         * @param obj Client.
         * @param endTime End time.
         */
        private HandshakeTimeoutObject(T obj, long endTime) {
            assert obj != null;
            assert obj instanceof GridCommunicationClient || obj instanceof SelectableChannel;
            assert endTime > 0;

            this.obj = obj;
            this.endTime = endTime;
        }

        /**
         * @return {@code True} if object has not yet been timed out.
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
                if (obj instanceof GridCommunicationClient)
                    ((GridCommunicationClient)obj).forceClose();
                else
                    U.closeQuiet((AbstractInterruptibleChannel)obj);

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
            return S.toString(HandshakeTimeoutObject.class, this);
        }
    }

    /**
     *
     */
    private class HandshakeClosure extends IgniteInClosure2X<InputStream, OutputStream> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final UUID rmtNodeId;

        /**
         * @param rmtNodeId Remote node ID.
         */
        private HandshakeClosure(UUID rmtNodeId) {
            this.rmtNodeId = rmtNodeId;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ThrowFromFinallyBlock")
        @Override public void applyx(InputStream in, OutputStream out) throws GridException {
            try {
                // Handshake.
                byte[] b = new byte[17];

                int n = 0;

                while (n < 17) {
                    int cnt = in.read(b, n, 17 - n);

                    if (cnt < 0)
                        throw new GridException("Failed to get remote node ID (end of stream reached)");

                    n += cnt;
                }

                // First 4 bytes are for length.
                UUID id = U.bytesToUuid(b, 1);

                if (!rmtNodeId.equals(id))
                    throw new GridException("Remote node ID is not as expected [expected=" + rmtNodeId +
                        ", rcvd=" + id + ']');
                else if (log.isDebugEnabled())
                    log.debug("Received remote node ID: " + id);
            }
            catch (SocketTimeoutException e) {
                throw new GridException("Failed to perform handshake due to timeout (consider increasing " +
                    "'connectionTimeout' configuration property).", e);
            }
            catch (IOException e) {
                throw new GridException("Failed to perform handshake.", e);
            }

            try {
                out.write(U.GG_HEADER);
                out.write(NODE_ID_MSG_TYPE);
                out.write(nodeIdMsg.nodeIdBytes);

                out.flush();

                if (log.isDebugEnabled())
                    log.debug("Sent local node ID [locNodeId=" + locNodeId + ", rmtNodeId=" + rmtNodeId + ']');
            }
            catch (IOException e) {
                throw new GridException("Failed to perform handshake.", e);
            }
        }
    }

    /**
     * Node ID message.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class NodeIdMessage extends GridTcpCommunicationMessageAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private byte[] nodeIdBytes;

        /** */
        private byte[] nodeIdBytesWithType;

        /** */
        public NodeIdMessage() {
            // No-op.
        }

        /**
         * @param nodeId Node ID.
         */
        private NodeIdMessage(UUID nodeId) {
            nodeIdBytes = U.uuidToBytes(nodeId);

            nodeIdBytesWithType = new byte[nodeIdBytes.length + 1];

            nodeIdBytesWithType[0] = NODE_ID_MSG_TYPE;

            System.arraycopy(nodeIdBytes, 0, nodeIdBytesWithType, 1, nodeIdBytes.length);
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            assert nodeIdBytes.length == 16;

            if (buf.remaining() < 17)
                return false;

            buf.put(NODE_ID_MSG_TYPE);
            buf.put(nodeIdBytes);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            if (buf.remaining() < 16)
                return false;

            nodeIdBytes = new byte[16];

            buf.get(nodeIdBytes);

            return true;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return NODE_ID_MSG_TYPE;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("CloneDoesntCallSuperClone")
        @Override public GridTcpCommunicationMessageAdapter clone() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
            // No-op.
        }
    }
}
