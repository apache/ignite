/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.jdk.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.internal.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.multicast.*;
import org.gridgain.grid.spi.discovery.tcp.messages.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.spi.discovery.tcp.internal.GridTcpDiscoverySpiState.*;

/**
 * TODO
 */
abstract class GridTcpDiscoverySpiAdapter extends GridSpiAdapter implements GridDiscoverySpi {
    /** Default port to listen (value is <tt>47500</tt>). */
    public static final int DFLT_PORT = 47500;

    /** Default socket operations timeout in milliseconds (value is <tt>2,000ms</tt>). */
    public static final long DFLT_SOCK_TIMEOUT = 2000;

    /** Default timeout for receiving message acknowledgement in milliseconds (value is <tt>5,000ms</tt>). */
    public static final long DFLT_ACK_TIMEOUT = 5000;

    /** Default network timeout in milliseconds (value is <tt>5,000ms</tt>). */
    public static final long DFLT_NETWORK_TIMEOUT = 5000;

    /** Default value for thread priority (value is <tt>10</tt>). */
    public static final int DFLT_THREAD_PRI = 10;

    /** Default heartbeat messages issuing frequency (value is <tt>2,000ms</tt>). */
    public static final long DFLT_HEARTBEAT_FREQ = 2000;

    /** Default size of topology snapshots history. */
    public static final int DFLT_TOP_HISTORY_SIZE = 1000;

    /** Response OK. */
    protected static final int RES_OK = 1;

    /** Response CONTINUE JOIN. */
    protected static final int RES_CONTINUE_JOIN = 100;

    /** Response WAIT. */
    protected static final int RES_WAIT = 200;

    /** Predicate to filter visible nodes. */
    protected static final GridPredicate<GridTcpDiscoveryNode> VISIBLE_NODES = new P1<GridTcpDiscoveryNode>() {
        @Override public boolean apply(GridTcpDiscoveryNode node) {
            return node.visible();
        }
    };

    /** Local address. */
    protected String locAddr;

    /** IP finder. */
    protected GridTcpDiscoveryIpFinder ipFinder;

    /** Socket operations timeout. */
    protected long sockTimeout = DFLT_SOCK_TIMEOUT;

    /** Message acknowledgement timeout. */
    protected long ackTimeout = DFLT_ACK_TIMEOUT;

    /** Network timeout. */
    protected long netTimeout = DFLT_NETWORK_TIMEOUT;

    /** Thread priority for all threads started by SPI. */
    protected int threadPri = DFLT_THREAD_PRI;

    /** Heartbeat messages issuing frequency. */
    protected long hbFreq = DFLT_HEARTBEAT_FREQ;

    /** Size of topology snapshots history. */
    protected int topHistSize = DFLT_TOP_HISTORY_SIZE;

    /** Grid discovery listener. */
    protected volatile GridDiscoverySpiListener lsnr;

    /** Data exchange. */
    protected GridDiscoverySpiDataExchange exchange;

    /** Metrics provider. */
    protected GridDiscoveryMetricsProvider metricsProvider;

    /** Local node attributes. */
    protected Map<String, Object> locNodeAttrs;

    /** Local node version. */
    protected GridProductVersion locNodeVer;

    /** Local node. */
    protected GridTcpDiscoveryNode locNode;

    /** Local host. */
    protected InetAddress locHost;

    /** Internal and external addresses of local node. */
    protected Collection<InetSocketAddress> locNodeAddrs;

    /** Socket timeout worker. */
    protected SocketTimeoutWorker sockTimeoutWorker;

    /** Discovery state. */
    protected GridTcpDiscoverySpiState spiState = DISCONNECTED;

    /** Start time of the very first grid node. */
    protected volatile long gridStartTime;

    /** Marshaller. */
    protected final GridMarshaller marsh = new GridJdkMarshaller();

    /** Statistics. */
    protected final GridTcpDiscoveryStatistics stats = new GridTcpDiscoveryStatistics();

    /** Local node ID. */
    @GridLocalNodeIdResource
    protected UUID locNodeId;

    /** Name of the grid. */
    @GridNameResource
    protected String gridName;

    /** Logger. */
    @GridLoggerResource
    protected GridLogger log;

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
    public long getSocketTimeout() {
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
    public long getAckTimeout() {
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
    public long getNetworkTimeout() {
        return netTimeout;
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
    public int getThreadPriority() {
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
    public long getHeartbeatFrequency() {
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

    /** {@inheritDoc} */
    @Override public void setNodeAttributes(Map<String, Object> attrs, GridProductVersion ver) {
        assert locNodeAttrs == null;
        assert locNodeVer == null;

        if (log.isDebugEnabled()) {
            log.debug("Node attributes to set: " + attrs);
            log.debug("Node version to set: " + ver);
        }

        locNodeAttrs = attrs;
        locNodeVer = ver;
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(GridSpiContext spiCtx) throws GridSpiException {
        super.onContextInitialized0(spiCtx);

        ipFinder.onSpiContextInitialized(spiCtx);
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        super.onContextDestroyed0();

        ipFinder.onSpiContextDestroyed();
    }

    /** {@inheritDoc} */
    @Override public GridNode getLocalNode() {
        return locNode;
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable GridDiscoverySpiListener lsnr) {
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
    @Override public long getGridStartTime() {
        assert gridStartTime != 0;

        return gridStartTime;
    }

    /**
     * @param sockAddr Remote address.
     * @return Opened socket.
     * @throws IOException If failed.
     */
    protected Socket openSocket(InetSocketAddress sockAddr) throws IOException {
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
     * @param data Raw data to write.
     * @throws IOException If IO failed or write timed out.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    protected void writeToSocket(Socket sock, byte[] data) throws IOException {
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
     * Writes message to the socket limiting write time to {@link #getSocketTimeout()}.
     *
     * @param sock Socket.
     * @param msg Message.
     * @throws IOException If IO failed or write timed out.
     * @throws GridException If marshalling failed.
     */
    protected void writeToSocket(Socket sock, GridTcpDiscoveryAbstractMessage msg) throws IOException, GridException {
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
    protected void writeToSocket(Socket sock, GridTcpDiscoveryAbstractMessage msg, GridByteArrayOutputStream bout)
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
    protected void writeToSocket(Socket sock, int res) throws IOException {
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
     * Reads message from the socket limiting read time.
     *
     * @param sock Socket.
     * @param in Input stream (in case socket stream was wrapped).
     * @param timeout Socket timeout for this operation.
     * @return Message.
     * @throws IOException If IO failed or read timed out.
     * @throws GridException If unmarshalling failed.
     */
    protected <T> T readMessage(Socket sock, @Nullable InputStream in, long timeout) throws IOException, GridException {
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
     * Resolves addresses registered in the IP finder, removes duplicates and local host
     * address and returns the collection of.
     *
     * @return Resolved addresses without duplicates and local address (potentially
     *      empty but never null).
     * @throws GridSpiException If an error occurs.
     */
    @SuppressWarnings("BusyWait")
    protected Collection<InetSocketAddress> resolvedAddresses() throws GridSpiException {
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

                if (locNodeAddrs == null || !locNodeAddrs.contains(resolved))
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
     * Gets addresses registered in the IP finder, initializes addresses having no
     * port (or 0 port) with {@link #DFLT_PORT}.
     *
     * @return Registered addresses.
     * @throws GridSpiException If an error occurs.
     */
    protected Collection<InetSocketAddress> registeredAddresses() throws GridSpiException {
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
     * @param msg Message.
     * @return Error.
     */
    protected GridSpiException duplicateIdError(GridTcpDiscoveryDuplicateIdMessage msg) {
        assert msg != null;

        return new GridSpiException("Local node has the same ID as existing node in topology " +
            "(fix configuration and restart local node) [localNode=" + locNode +
            ", existingNode=" + msg.node() + ']');
    }

    /**
     * @param msg Message.
     * @return Error.
     */
    protected GridSpiException authenticationFailedError(GridTcpDiscoveryAuthFailedMessage msg) {
        assert msg != null;

        return new GridSpiException(new GridAuthenticationException("Authentication failed [nodeId=" +
            msg.creatorNodeId() + ", addr=" + msg.address().getHostAddress() + ']'));
    }

    /**
     * @param msg Message.
     * @return Error.
     */
    protected GridSpiException checkFailedError(GridTcpDiscoveryCheckFailedMessage msg) {
        assert msg != null;

        return versionCheckFailed(msg) ? new GridSpiVersionCheckException(msg.error()) :
            new GridSpiException(msg.error());
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
     * Handles sockets timeouts.
     */
    protected class SocketTimeoutWorker extends GridSpiThread {
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
     * Socket timeout object.
     */
    protected static class SocketTimeoutObject {
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

    /**
     * Base class for message workers.
     */
    protected abstract class MessageWorkerAdapter extends GridSpiThread {
        /** Pre-allocated output stream (100K). */
        private final GridByteArrayOutputStream bout = new GridByteArrayOutputStream(100 * 1024);

        /** Message queue. */
        private final BlockingDeque<GridTcpDiscoveryAbstractMessage> queue = new LinkedBlockingDeque<>();

        /** Backed interrupted flag. */
        private volatile boolean interrupted;

        /**
         * @param name Thread name.
         */
        protected MessageWorkerAdapter(String name) {
            super(gridName, name, log);

            setPriority(threadPri);
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
         * @return Current queue size.
         */
        int queueSize() {
            return queue.size();
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

        protected abstract void processMessage(GridTcpDiscoveryAbstractMessage msg);

        /**
         * @param sock Socket.
         * @param msg Message.
         * @throws IOException If IO failed.
         * @throws GridException If marshalling failed.
         */
        protected final void writeToSocket(Socket sock, GridTcpDiscoveryAbstractMessage msg)
            throws IOException, GridException {
            bout.reset();

            GridTcpDiscoverySpiAdapter.this.writeToSocket(sock, msg, bout);
        }
    }
}
