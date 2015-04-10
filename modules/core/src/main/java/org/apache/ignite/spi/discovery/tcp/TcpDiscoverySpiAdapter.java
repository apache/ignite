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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.messages.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState.*;

/**
 * Base class for TCP discovery SPIs.
 */
abstract class TcpDiscoverySpiAdapter extends IgniteSpiAdapter implements DiscoverySpi {
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

    /** Local address. */
    protected String locAddr;

    /** IP finder. */
    protected TcpDiscoveryIpFinder ipFinder;

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

    /** Local host. */
    protected InetAddress locHost;

    /** Internal and external addresses of local node. */
    protected Collection<InetSocketAddress> locNodeAddrs;

    /** Socket timeout worker. */
    protected SocketTimeoutWorker sockTimeoutWorker;

    /** Discovery state. */
    protected TcpDiscoverySpiState spiState = DISCONNECTED;

    /** Start time of the very first grid node. */
    protected volatile long gridStartTime;

    /** Marshaller. */
    protected final Marshaller marsh = new JdkMarshaller();

    /** Statistics. */
    protected final TcpDiscoveryStatistics stats = new TcpDiscoveryStatistics();

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    /**
     * Inject resources
     *
     * @param ignite Ignite.
     */
    @IgniteInstanceResource
    @Override protected void injectResources(Ignite ignite) {
        super.injectResources(ignite);

        // Inject resource.
        if (ignite != null)
            setLocalAddress(ignite.configuration().getLocalHost());
    }

    /**
     * Sets local host IP address that discovery SPI uses.
     * <p>
     * If not provided, by default a first found non-loopback address
     * will be used. If there is no non-loopback address available,
     * then {@link InetAddress#getLocalHost()} will be used.
     *
     * @param locAddr IP address.
     */
    @IgniteSpiConfiguration(optional = true)
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
    public TcpDiscoveryIpFinder getIpFinder() {
        return ipFinder;
    }

    /**
     * Sets IP finder for IP addresses sharing and storing.
     * <p>
     * If not provided {@link org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder} will be used by default.
     *
     * @param ipFinder IP finder.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setIpFinder(TcpDiscoveryIpFinder ipFinder) {
        this.ipFinder = ipFinder;
    }

    /**
     * Sets socket operations timeout. This timeout is used to limit connection time and
     * write-to-socket time.
     * <p>
     * Note that when running Ignite on Amazon EC2, socket timeout must be set to a value
     * significantly greater than the default (e.g. to {@code 30000}).
     * <p>
     * If not specified, default is {@link #DFLT_SOCK_TIMEOUT}.
     *
     * @param sockTimeout Socket connection timeout.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setSocketTimeout(long sockTimeout) {
        this.sockTimeout = sockTimeout;
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
    @IgniteSpiConfiguration(optional = true)
    public void setAckTimeout(long ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    /**
     * Sets maximum network timeout to use for network operations.
     * <p>
     * If not specified, default is {@link #DFLT_NETWORK_TIMEOUT}.
     *
     * @param netTimeout Network timeout.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setNetworkTimeout(long netTimeout) {
        this.netTimeout = netTimeout;
    }

    /**
     * Sets thread priority. All threads within SPI will be started with it.
     * <p>
     * If not provided, default value is {@link #DFLT_THREAD_PRI}
     *
     * @param threadPri Thread priority.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setThreadPriority(int threadPri) {
        this.threadPri = threadPri;
    }

    /**
     * Sets delay between issuing of heartbeat messages. SPI sends heartbeat messages
     * in configurable time interval to other nodes to notify them about its state.
     * <p>
     * If not provided, default value is {@link #DFLT_HEARTBEAT_FREQ}.
     *
     * @param hbFreq Heartbeat frequency in milliseconds.
     */
    @IgniteSpiConfiguration(optional = true)
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
    @IgniteSpiConfiguration(optional = true)
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

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        super.onContextInitialized0(spiCtx);

        ipFinder.onSpiContextInitialized(spiCtx);
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        super.onContextDestroyed0();

        if (ipFinder != null)
            ipFinder.onSpiContextDestroyed();
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

        writeToSocket(sock, U.IGNITE_HEADER);

        return sock;
    }

    /**
     * Writes message to the socket.
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
     * Writes message to the socket.
     *
     * @param sock Socket.
     * @param msg Message.
     * @throws IOException If IO failed or write timed out.
     * @throws IgniteCheckedException If marshalling failed.
     */
    protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg) throws IOException, IgniteCheckedException {
        writeToSocket(sock, msg, new GridByteArrayOutputStream(8 * 1024)); // 8K.
    }

    /**
     * Writes message to the socket.
     *
     * @param sock Socket.
     * @param msg Message.
     * @param bout Byte array output stream.
     * @throws IOException If IO failed or write timed out.
     * @throws IgniteCheckedException If marshalling failed.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, GridByteArrayOutputStream bout)
        throws IOException, IgniteCheckedException {
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
     * Writes response to the socket.
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
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    protected <T> T readMessage(Socket sock, @Nullable InputStream in, long timeout) throws IOException, IgniteCheckedException {
        assert sock != null;

        int oldTimeout = sock.getSoTimeout();

        try {
            sock.setSoTimeout((int)timeout);

            return marsh.unmarshal(in == null ? sock.getInputStream() : in, U.gridClassLoader());
        }
        catch (IOException | IgniteCheckedException e) {
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
                    "(retrying every 2000 ms).");
            }

            try {
                U.sleep(2000);
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
                LT.warn(log, null, "Failed to resolve address from IP finder (host is unknown): " + addr);

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
     * Handles sockets timeouts.
     */
    protected class SocketTimeoutWorker extends IgniteSpiThread {
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

    /**
     * Base class for message workers.
     */
    protected abstract class MessageWorkerAdapter extends IgniteSpiThread {
        /** Pre-allocated output stream (100K). */
        private final GridByteArrayOutputStream bout = new GridByteArrayOutputStream(100 * 1024);

        /** Message queue. */
        private final BlockingDeque<TcpDiscoveryAbstractMessage> queue = new LinkedBlockingDeque<>();

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
                log.debug("Message worker started [locNodeId=" + getLocalNodeId() + ']');

            while (!isInterrupted()) {
                TcpDiscoveryAbstractMessage msg = queue.poll(2000, TimeUnit.MILLISECONDS);

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
        void addMessage(TcpDiscoveryAbstractMessage msg) {
            assert msg != null;

            if (msg instanceof TcpDiscoveryHeartbeatMessage)
                queue.addFirst(msg);
            else
                queue.add(msg);

            if (log.isDebugEnabled())
                log.debug("Message has been added to queue: " + msg);
        }

        /**
         * @param msg Message.
         */
        protected abstract void processMessage(TcpDiscoveryAbstractMessage msg);

        /**
         * @param sock Socket.
         * @param msg Message.
         * @throws IOException If IO failed.
         * @throws IgniteCheckedException If marshalling failed.
         */
        protected final void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg)
            throws IOException, IgniteCheckedException {
            bout.reset();

            TcpDiscoverySpiAdapter.this.writeToSocket(sock, msg, bout);
        }
    }

    /**
     *
     */
    protected class SocketMultiConnector implements AutoCloseable {
        /** */
        private int connInProgress;

        /** */
        private final ExecutorService executor;

        /** */
        private final CompletionService<GridTuple3<InetSocketAddress, Socket, Exception>> completionSrvc;

        /**
         * @param addrs Addresses.
         * @param retryCnt Retry count.
         */
        public SocketMultiConnector(Collection<InetSocketAddress> addrs, final int retryCnt) {
            connInProgress = addrs.size();

            executor = Executors.newFixedThreadPool(Math.min(1, addrs.size()));

            completionSrvc = new ExecutorCompletionService<>(executor);

            for (final InetSocketAddress addr : addrs) {
                completionSrvc.submit(new Callable<GridTuple3<InetSocketAddress, Socket, Exception>>() {
                    @Override public GridTuple3<InetSocketAddress, Socket, Exception> call() {
                        Exception ex = null;
                        Socket sock = null;

                        for (int i = 0; i < retryCnt; i++) {
                            if (Thread.currentThread().isInterrupted())
                                return null; // Executor is shutdown.

                            try {
                                sock = openSocket(addr);

                                break;
                            }
                            catch (Exception e) {
                                ex = e;
                            }
                        }

                        return new GridTuple3<>(addr, sock, ex);
                    }
                });
            }
        }

        /**
         *
         */
        @Nullable public GridTuple3<InetSocketAddress, Socket, Exception> next() {
            if (connInProgress == 0)
                return null;

            try {
                Future<GridTuple3<InetSocketAddress, Socket, Exception>> fut = completionSrvc.take();

                connInProgress--;

                return fut.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteSpiException("Thread has been interrupted.", e);
            }
            catch (ExecutionException e) {
                throw new IgniteSpiException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            List<Runnable> unstartedTasks = executor.shutdownNow();

            connInProgress -= unstartedTasks.size();

            if (connInProgress > 0) {
                Thread thread = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            executor.awaitTermination(5, TimeUnit.MINUTES);

                            Future<GridTuple3<InetSocketAddress, Socket, Exception>> fut;

                            while ((fut = completionSrvc.poll()) != null) {
                                try {
                                    GridTuple3<InetSocketAddress, Socket, Exception> tuple3 = fut.get();

                                    if (tuple3 != null)
                                        IgniteUtils.closeQuiet(tuple3.get2());
                                }
                                catch (ExecutionException ignore) {

                                }
                            }
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();

                            throw new RuntimeException(e);
                        }
                    }
                });

                thread.setDaemon(true);

                thread.start();
            }
        }
    }
}
