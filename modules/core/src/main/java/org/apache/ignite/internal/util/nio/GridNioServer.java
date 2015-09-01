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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;
import sun.nio.ch.DirectBuffer;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.ACK_CLOSURE;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MSG_WRITER;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.NIO_OPERATION;

/**
 * TCP NIO server. Due to asynchronous nature of connections processing
 * network events such as client connection, disconnection and message receiving are passed to
 * the server listener. Once client connected, an associated {@link GridNioSession} object is
 * created and can be used in communication.
 * <p>
 * This implementation supports several selectors and several reading threads.
 *
 * @param <T> Message type.
 *
 */
public class GridNioServer<T> {
    /** Default session write timeout. */
    public static final int DFLT_SES_WRITE_TIMEOUT = 5000;

    /** Default send queue limit. */
    public static final int DFLT_SEND_QUEUE_LIMIT = 1024;

    /** Time, which server will wait before retry operation. */
    private static final long ERR_WAIT_TIME = 2000;

    /** Buffer metadata key. */
    private static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL system data buffer metadata key. */
    private static final int BUF_SSL_SYSTEM_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL write buf limit. */
    private static final int WRITE_BUF_LIMIT = GridNioSessionMetaKey.nextUniqueKey();

    /** Accept worker thread. */
    @GridToStringExclude
    private final IgniteThread acceptThread;

    /** Read worker threads. */
    private final IgniteThread[] clientThreads;

    /** Read workers. */
    private final List<AbstractNioClientWorker> clientWorkers;

    /** Filter chain to use. */
    private final GridNioFilterChain<T> filterChain;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Closed flag. */
    private volatile boolean closed;

    /** Flag indicating if this server should use direct buffers. */
    private final boolean directBuf;

    /** Index to select which thread will serve next socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int balanceIdx;

    /** Tcp no delay flag. */
    private final boolean tcpNoDelay;

    /** Socket send buffer. */
    private final int sockSndBuf;

    /** Socket receive buffer. */
    private final int sockRcvBuf;

    /** Write timeout */
    private volatile long writeTimeout = DFLT_SES_WRITE_TIMEOUT;

    /** Idle timeout. */
    private volatile long idleTimeout = ConnectorConfiguration.DFLT_IDLE_TIMEOUT;

    /** For test purposes only. */
    @SuppressWarnings("UnusedDeclaration")
    private boolean skipWrite;

    /** For test purposes only. */
    @SuppressWarnings("UnusedDeclaration")
    private boolean skipRead;

    /** Local address. */
    private final InetSocketAddress locAddr;

    /** Order. */
    private final ByteOrder order;

    /** Send queue limit. */
    private final int sndQueueLimit;

    /** Whether direct mode is used. */
    private final boolean directMode;

    /** Metrics listener. */
    private final GridNioMetricsListener metricsLsnr;

    /** Sessions. */
    private final GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions = new GridConcurrentHashSet<>();

    /** */
    private GridNioSslFilter sslFilter;

    /** */
    @GridToStringExclude
    private MessageFormatter formatter;

    /** */
    @GridToStringExclude
    private IgnitePredicate<Message> skipRecoveryPred;

    /** Optional listener to monitor outbound message queue size. */
    private IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr;

    /** Static initializer ensures single-threaded execution of workaround. */
    static {
        // This is a workaround for JDK bug (NPE in Selector.open()).
        // http://bugs.sun.com/view_bug.do?bug_id=6427854
        try {
            Selector.open().close();
        }
        catch (IOException ignored) {
        }
    }

    /**
     * @param addr Address.
     * @param port Port.
     * @param log Log.
     * @param selectorCnt Count of selectors and selecting threads.
     * @param gridName Grid name.
     * @param tcpNoDelay If TCP_NODELAY option should be set to accepted sockets.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param lsnr Listener.
     * @param sockSndBuf Socket send buffer.
     * @param sockRcvBuf Socket receive buffer.
     * @param sndQueueLimit Send queue limit.
     * @param directMode Whether direct mode is used.
     * @param daemon Daemon flag to create threads.
     * @param metricsLsnr Metrics listener.
     * @param formatter Message formatter.
     * @param skipRecoveryPred Skip recovery predicate.
     * @param msgQueueLsnr Message queue size listener.
     * @param filters Filters for this server.
     * @throws IgniteCheckedException If failed.
     */
    private GridNioServer(
        InetAddress addr,
        int port,
        IgniteLogger log,
        int selectorCnt,
        @Nullable String gridName,
        boolean tcpNoDelay,
        boolean directBuf,
        ByteOrder order,
        GridNioServerListener<T> lsnr,
        int sockSndBuf,
        int sockRcvBuf,
        int sndQueueLimit,
        boolean directMode,
        boolean daemon,
        GridNioMetricsListener metricsLsnr,
        MessageFormatter formatter,
        IgnitePredicate<Message> skipRecoveryPred,
        IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr,
        GridNioFilter... filters
    ) throws IgniteCheckedException {
        if (port != -1)
            A.notNull(addr, "addr");

        A.notNull(lsnr, "lsnr");
        A.notNull(log, "log");
        A.notNull(order, "order");

        A.ensure(port == -1 || (port > 0 && port < 0xffff), "port");
        A.ensure(selectorCnt > 0, "selectorCnt");
        A.ensure(sockRcvBuf >= 0, "sockRcvBuf");
        A.ensure(sockSndBuf >= 0, "sockSndBuf");
        A.ensure(sndQueueLimit >= 0, "sndQueueLimit");

        this.log = log;
        this.directBuf = directBuf;
        this.order = order;
        this.tcpNoDelay = tcpNoDelay;
        this.sockRcvBuf = sockRcvBuf;
        this.sockSndBuf = sockSndBuf;
        this.sndQueueLimit = sndQueueLimit;
        this.msgQueueLsnr = msgQueueLsnr;

        filterChain = new GridNioFilterChain<>(log, lsnr, new HeadFilter(), filters);

        if (directMode) {
            for (GridNioFilter filter : filters) {
                if (filter instanceof GridNioSslFilter) {
                    sslFilter = (GridNioSslFilter)filter;

                    assert sslFilter.directMode();
                }
            }
        }

        if (port != -1) {
            // Once bind, we will not change the port in future.
            locAddr = new InetSocketAddress(addr, port);

            // This method will throw exception if address already in use.
            Selector acceptSelector = createSelector(locAddr);

            acceptThread = new IgniteThread(new GridNioAcceptWorker(gridName, "nio-acceptor", log, acceptSelector));
        }
        else {
            locAddr = null;
            acceptThread = null;
        }

        clientWorkers = new ArrayList<>(selectorCnt);
        clientThreads = new IgniteThread[selectorCnt];

        for (int i = 0; i < selectorCnt; i++) {
            AbstractNioClientWorker worker = directMode ?
                new DirectNioClientWorker(i, gridName, "grid-nio-worker-" + i, log) :
                new ByteBufferNioClientWorker(i, gridName, "grid-nio-worker-" + i, log);

            clientWorkers.add(worker);

            clientThreads[i] = new IgniteThread(worker);

            clientThreads[i].setDaemon(daemon);
        }

        this.directMode = directMode;
        this.metricsLsnr = metricsLsnr;
        this.formatter = formatter;

        this.skipRecoveryPred = skipRecoveryPred != null ? skipRecoveryPred : F.<Message>alwaysFalse();
    }

    /**
     * Creates and returns a builder for a new instance of this class.
     *
     * @return Builder for new instance.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Starts all associated threads to perform accept and read activities.
     */
    public void start() {
        filterChain.start();

        if (acceptThread != null)
            acceptThread.start();

        for (IgniteThread thread : clientThreads)
            thread.start();
    }

    /**
     * Stops all threads and releases all resources.
     */
    public void stop() {
        if (!closed) {
            closed = true;

            // Make sure to entirely stop acceptor if any.
            U.interrupt(acceptThread);
            U.join(acceptThread, log);

            U.cancel(clientWorkers);
            U.join(clientWorkers, log);

            filterChain.stop();

            for (GridSelectorNioSessionImpl ses : sessions)
                ses.onServerStopped();
        }
    }

    /**
     * Gets the address server is bound to.
     *
     * @return Address server is bound to.
     */
    public InetSocketAddress localAddress() {
        return locAddr;
    }

    /**
     * @param ses Session to close.
     * @return Future for operation.
     */
    public GridNioFuture<Boolean> close(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture<>(false);

        NioOperationFuture<Boolean> fut = new NioOperationFuture<>(impl, NioOperation.CLOSE);

        clientWorkers.get(impl.selectorIndex()).offer(fut);

        return fut;
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @return Future for operation.
     */
    GridNioFuture<?> send(GridNioSession ses, ByteBuffer msg) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, NioOperation.REQUIRE_WRITE, msg);

        send0(impl, fut, false);

        return fut;
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @return Future for operation.
     */
    GridNioFuture<?> send(GridNioSession ses, Message msg) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, NioOperation.REQUIRE_WRITE, msg,
            skipRecoveryPred.apply(msg));

        send0(impl, fut, false);

        return fut;
    }

    /**
     * @param ses Session.
     * @param fut Future.
     * @param sys System message flag.
     */
    private void send0(GridSelectorNioSessionImpl ses, NioOperationFuture<?> fut, boolean sys) {
        assert ses != null;
        assert fut != null;

        int msgCnt = sys ? ses.offerSystemFuture(fut) : ses.offerFuture(fut);

        IgniteInClosure<IgniteException> ackClosure;

        if (!sys && (ackClosure = ses.removeMeta(ACK_CLOSURE.ordinal())) != null)
            fut.ackClosure(ackClosure);

        if (ses.closed()) {
            if (ses.removeFuture(fut))
                fut.connectionClosed();
        }
        else if (msgCnt == 1)
            // Change from 0 to 1 means that worker thread should be waken up.
            clientWorkers.get(ses.selectorIndex()).offer(fut);

        IgniteBiInClosure<GridNioSession, Integer> lsnr0 = msgQueueLsnr;

        if (lsnr0 != null)
            lsnr0.apply(ses, msgCnt);
    }

    /**
     * Adds message at the front of the queue without acquiring back pressure semaphore.
     *
     * @param ses Session.
     * @param msg Message.
     * @return Future.
     */
    public GridNioFuture<?> sendSystem(GridNioSession ses, Message msg) {
        return sendSystem(ses, msg, null);
    }

    /**
     * Adds message at the front of the queue without acquiring back pressure semaphore.
     *
     * @param ses Session.
     * @param msg Message.
     * @param lsnr Future listener notified from the session thread.
     * @return Future.
     */
    public GridNioFuture<?> sendSystem(GridNioSession ses,
        Message msg,
        @Nullable IgniteInClosure<? super IgniteInternalFuture<?>> lsnr) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, NioOperation.REQUIRE_WRITE, msg,
            skipRecoveryPred.apply(msg));

        if (lsnr != null) {
            fut.listen(lsnr);

            assert !fut.isDone();
        }

        send0(impl, fut, true);

        return fut;
    }

    /**
     * @param ses Session.
     */
    public void resend(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridNioRecoveryDescriptor recoveryDesc = ses.recoveryDescriptor();

        if (recoveryDesc != null && !recoveryDesc.messagesFutures().isEmpty()) {
            Deque<GridNioFuture<?>> futs = recoveryDesc.messagesFutures();

            if (log.isDebugEnabled())
                log.debug("Resend messages [rmtNode=" + recoveryDesc.node().id() + ", msgCnt=" + futs.size() + ']');

            GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

            GridNioFuture<?> fut0 = futs.iterator().next();

            for (GridNioFuture<?> fut : futs) {
                fut.messageThread(true);

                ((NioOperationFuture)fut).resetSession(ses0);
            }

            ses0.resend(futs);

            // Wake up worker.
            clientWorkers.get(ses0.selectorIndex()).offer(((NioOperationFuture)fut0));
        }
    }

    /**
     * @param ses Session.
     * @param op Operation.
     * @return Future for operation.
     */
    GridNioFuture<?> pauseResumeReads(GridNioSession ses, NioOperation op) {
        assert ses instanceof GridSelectorNioSessionImpl;
        assert op == NioOperation.PAUSE_READ || op == NioOperation.RESUME_READ;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture(new IOException("Failed to pause/resume reads " +
                "(connection was closed): " + ses));

        NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, op);

        clientWorkers.get(impl.selectorIndex()).offer(fut);

        return fut;
    }

    /**
     * Establishes a session.
     *
     * @param ch Channel to register within the server and create session for.
     * @param meta Optional meta for new session.
     * @return Future to get session.
     */
    public GridNioFuture<GridNioSession> createSession(final SocketChannel ch,
        @Nullable Map<Integer, ?> meta) {
        try {
            if (!closed) {
                ch.configureBlocking(false);

                NioOperationFuture<GridNioSession> req = new NioOperationFuture<>(ch, false, meta);

                offerBalanced(req);

                return req;
            }
            else
                return new GridNioFinishedFuture<>(
                    new IgniteCheckedException("Failed to create session, server is stopped."));
        }
        catch (IOException e) {
            return new GridNioFinishedFuture<>(e);
        }
    }

    /**
     * Gets configurable write timeout for this session. If not set, default value is {@link #DFLT_SES_WRITE_TIMEOUT}.
     *
     * @return Write timeout in milliseconds.
     */
    public long writeTimeout() {
        return writeTimeout;
    }

    /**
     * Sets configurable write timeout for session.
     *
     * @param writeTimeout Write timeout in milliseconds.
     */
    public void writeTimeout(long writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /**
     * Gets configurable idle timeout for this session. If not set, default value is
     * {@link ConnectorConfiguration#DFLT_IDLE_TIMEOUT}.
     *
     * @return Idle timeout in milliseconds.
     */
    public long idleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets configurable idle timeout for session.
     *
     * @param idleTimeout Idle timeout in milliseconds.
     */
    public void idleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Creates selector and binds server socket to a given address and port. If address is null
     * then will not bind any address and just creates a selector.
     *
     * @param addr Local address to listen on.
     * @return Created selector.
     * @throws IgniteCheckedException If selector could not be created or port is already in use.
     */
    private Selector createSelector(@Nullable SocketAddress addr) throws IgniteCheckedException {
        Selector selector = null;

        ServerSocketChannel srvrCh = null;

        try {
            // Create a new selector
            selector = SelectorProvider.provider().openSelector();

            if (addr != null) {
                // Create a new non-blocking server socket channel
                srvrCh = ServerSocketChannel.open();

                srvrCh.configureBlocking(false);

                if (sockRcvBuf > 0)
                    srvrCh.socket().setReceiveBufferSize(sockRcvBuf);

                // Bind the server socket to the specified address and port
                srvrCh.socket().bind(addr);

                // Register the server socket channel, indicating an interest in
                // accepting new connections
                srvrCh.register(selector, SelectionKey.OP_ACCEPT);
            }

            return selector;
        }
        catch (Throwable e) {
            U.close(srvrCh, log);
            U.close(selector, log);

            if (e instanceof Error)
                throw (Error)e;

            throw new IgniteCheckedException("Failed to initialize NIO selector.", e);
        }
    }

    /**
     * @param req Request to balance.
     */
    private synchronized void offerBalanced(NioOperationFuture req) {
        clientWorkers.get(balanceIdx).offer(req);

        balanceIdx++;

        if (balanceIdx == clientWorkers.size())
            balanceIdx = 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioServer.class, this);
    }

    /**
     * Client worker for byte buffer mode.
     */
    private class ByteBufferNioClientWorker extends AbstractNioClientWorker {
        /** Read buffer. */
        private final ByteBuffer readBuf;

        /**
         * @param idx Index of this worker in server's array.
         * @param gridName Grid name.
         * @param name Worker name.
         * @param log Logger.
         * @throws IgniteCheckedException If selector could not be created.
         */
        protected ByteBufferNioClientWorker(int idx, @Nullable String gridName, String name, IgniteLogger log)
            throws IgniteCheckedException {
            super(idx, gridName, name, log);

            readBuf = directBuf ? ByteBuffer.allocateDirect(8 << 10) : ByteBuffer.allocate(8 << 10);

            readBuf.order(order);
        }

        /**
        * Processes read-available event on the key.
        *
        * @param key Key that is ready to be read.
        * @throws IOException If key read failed.
        */
        @Override protected void processRead(SelectionKey key) throws IOException {
            if (skipRead) {
                try {
                    U.sleep(50);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    U.warn(log, "Sleep has been interrupted.");
                }

                return;
            }

            ReadableByteChannel sockCh = (ReadableByteChannel)key.channel();

            final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            // Reset buffer to read bytes up to its capacity.
            readBuf.clear();

            // Attempt to read off the channel
            int cnt = sockCh.read(readBuf);

            if (cnt == -1) {
                if (log.isDebugEnabled())
                    log.debug("Remote client closed connection: " + ses);

                close(ses, null);

                return;
            }
            else if (cnt == 0)
                return;

            if (log.isTraceEnabled())
                log.trace("Bytes received [sockCh=" + sockCh + ", cnt=" + cnt + ']');

            if (metricsLsnr != null)
                metricsLsnr.onBytesReceived(cnt);

            ses.bytesReceived(cnt);

            // Sets limit to current position and
            // resets position to 0.
            readBuf.flip();

            try {
                assert readBuf.hasRemaining();

                filterChain.onMessageReceived(ses, readBuf);

                if (readBuf.remaining() > 0) {
                    LT.warn(log, null, "Read buffer contains data after filter chain processing (will discard " +
                        "remaining bytes) [ses=" + ses + ", remainingCnt=" + readBuf.remaining() + ']');

                    readBuf.clear();
                }
            }
            catch (IgniteCheckedException e) {
                close(ses, e);
            }
        }

        /**
        * Processes write-ready event on the key.
        *
        * @param key Key that is ready to be written.
        * @throws IOException If write failed.
        */
        @Override protected void processWrite(SelectionKey key) throws IOException {
            WritableByteChannel sockCh = (WritableByteChannel)key.channel();

            final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            while (true) {
                ByteBuffer buf = ses.removeMeta(BUF_META_KEY);
                NioOperationFuture<?> req = ses.removeMeta(NIO_OPERATION.ordinal());

                // Check if there were any pending data from previous writes.
                if (buf == null) {
                    assert req == null;

                    req = (NioOperationFuture<?>)ses.pollFuture();

                    if (req == null) {
                        key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));

                        break;
                    }

                    buf = req.message();
                }

                if (!skipWrite) {
                    int cnt = sockCh.write(buf);

                    if (log.isTraceEnabled())
                        log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                    if (metricsLsnr != null)
                        metricsLsnr.onBytesSent(cnt);

                    ses.bytesSent(cnt);
                }
                else {
                    // For test purposes only (skipWrite is set to true in tests only).
                    try {
                        U.sleep(50);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IOException("Thread has been interrupted.", e);
                    }
                }

                if (buf.remaining() > 0) {
                    // Not all data was written.
                    ses.addMeta(BUF_META_KEY, buf);
                    ses.addMeta(NIO_OPERATION.ordinal(), req);

                    break;
                }
                else {
                    // Message was successfully written.
                    assert req != null;

                    req.onDone();
                }
            }
        }
    }

    /**
     * Client worker for direct mode.
     */
    private class DirectNioClientWorker extends AbstractNioClientWorker {
        /**
         * @param idx Index of this worker in server's array.
         * @param gridName Grid name.
         * @param name Worker name.
         * @param log Logger.
         * @throws IgniteCheckedException If selector could not be created.
         */
        protected DirectNioClientWorker(int idx, @Nullable String gridName, String name, IgniteLogger log)
            throws IgniteCheckedException {
            super(idx, gridName, name, log);
        }

        /**
         * Processes read-available event on the key.
         *
         * @param key Key that is ready to be read.
         * @throws IOException If key read failed.
         */
        @Override protected void processRead(SelectionKey key) throws IOException {
            if (skipRead) {
                try {
                    U.sleep(50);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    U.warn(log, "Sleep has been interrupted.");
                }

                return;
            }

            ReadableByteChannel sockCh = (ReadableByteChannel)key.channel();

            final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            ByteBuffer readBuf = ses.readBuffer();

            // Attempt to read off the channel.
            int cnt = sockCh.read(readBuf);

            if (cnt == -1) {
                if (log.isDebugEnabled())
                    log.debug("Remote client closed connection: " + ses);

                close(ses, null);

                return;
            }
            else if (cnt == 0 && !readBuf.hasRemaining())
                return;

            if (log.isTraceEnabled())
                log.trace("Bytes received [sockCh=" + sockCh + ", cnt=" + cnt + ']');

            if (metricsLsnr != null)
                metricsLsnr.onBytesReceived(cnt);

            ses.bytesReceived(cnt);

            // Sets limit to current position and
            // resets position to 0.
            readBuf.flip();

            try {
                assert readBuf.hasRemaining();

                filterChain.onMessageReceived(ses, readBuf);

                if (readBuf.hasRemaining())
                    readBuf.compact();
                else
                    readBuf.clear();
            }
            catch (IgniteCheckedException e) {
                close(ses, e);
            }
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        @Override protected void processWrite(SelectionKey key) throws IOException {
            if (sslFilter != null)
                processWriteSsl(key);
            else
                processWrite0(key);
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void processWriteSsl(SelectionKey key) throws IOException {
            WritableByteChannel sockCh = (WritableByteChannel)key.channel();

            GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            MessageWriter writer = ses.meta(MSG_WRITER.ordinal());

            if (writer == null)
                ses.addMeta(MSG_WRITER.ordinal(), writer = formatter.writer());

            boolean handshakeFinished = sslFilter.lock(ses);

            try {
                writeSslSystem(ses, sockCh);

                if (!handshakeFinished)
                    return;

                ByteBuffer sslNetBuf = ses.removeMeta(BUF_META_KEY);

                if (sslNetBuf != null) {
                    int cnt = sockCh.write(sslNetBuf);

                    if (metricsLsnr != null)
                        metricsLsnr.onBytesSent(cnt);

                    ses.bytesSent(cnt);

                    if (sslNetBuf.hasRemaining()) {
                        ses.addMeta(BUF_META_KEY, sslNetBuf);

                        return;
                    }
                }

                ByteBuffer buf = ses.writeBuffer();

                if (ses.meta(WRITE_BUF_LIMIT) != null)
                    buf.limit((int)ses.meta(WRITE_BUF_LIMIT));

                NioOperationFuture<?> req = ses.removeMeta(NIO_OPERATION.ordinal());

                List<NioOperationFuture<?>> doneFuts = null;

                while (true) {
                    if (req == null) {
                        req = (NioOperationFuture<?>)ses.pollFuture();

                        if (req == null && buf.position() == 0) {
                            key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));

                            break;
                        }
                    }

                    Message msg;
                    boolean finished = false;

                    if (req != null) {
                        msg = req.directMessage();

                        assert msg != null;

                        finished = msg.writeTo(buf, writer);

                        if (finished)
                            writer.reset();
                    }

                    // Fill up as many messages as possible to write buffer.
                    while (finished) {
                        if (doneFuts == null)
                            doneFuts = new ArrayList<>();

                        doneFuts.add(req);

                        req = (NioOperationFuture<?>)ses.pollFuture();

                        if (req == null)
                            break;

                        msg = req.directMessage();

                        assert msg != null;

                        finished = msg.writeTo(buf, writer);

                        if (finished)
                            writer.reset();
                    }

                    int sesBufLimit = buf.limit();
                    int sesCap = buf.capacity();

                    buf.flip();

                    buf = sslFilter.encrypt(ses, buf);

                    ByteBuffer sesBuf = ses.writeBuffer();

                    sesBuf.clear();

                    if (sesCap - buf.limit() < 0) {
                        int limit = sesBufLimit + (sesCap - buf.limit()) - 100;

                        ses.addMeta(WRITE_BUF_LIMIT, limit);

                        sesBuf.limit(limit);
                    }

                    assert buf.hasRemaining();

                    if (!skipWrite) {
                        int cnt = sockCh.write(buf);

                        if (!F.isEmpty(doneFuts)) {
                            for (int i = 0; i < doneFuts.size(); i++)
                                doneFuts.get(i).onDone();

                            doneFuts.clear();
                        }

                        if (log.isTraceEnabled())
                            log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                        if (metricsLsnr != null)
                            metricsLsnr.onBytesSent(cnt);

                        ses.bytesSent(cnt);
                    }
                    else {
                        // For test purposes only (skipWrite is set to true in tests only).
                        try {
                            U.sleep(50);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IOException("Thread has been interrupted.", e);
                        }
                    }

                    ses.addMeta(NIO_OPERATION.ordinal(), req);

                    if (buf.hasRemaining()) {
                        ses.addMeta(BUF_META_KEY, buf);

                        break;
                    }
                    else {
                        buf = ses.writeBuffer();

                        if (ses.meta(WRITE_BUF_LIMIT) != null)
                            buf.limit((int)ses.meta(WRITE_BUF_LIMIT));
                    }
                }
            }
            finally {
                sslFilter.unlock(ses);
            }
        }

        /**
         * @param ses NIO session.
         * @param sockCh Socket channel.
         * @throws IOException If failed.
         */
        private void writeSslSystem(GridSelectorNioSessionImpl ses, WritableByteChannel sockCh)
            throws IOException {
            ConcurrentLinkedDeque8<ByteBuffer> queue = ses.meta(BUF_SSL_SYSTEM_META_KEY);

            ByteBuffer buf;

            while ((buf = queue.peek()) != null) {
                int cnt = sockCh.write(buf);

                if (metricsLsnr != null)
                    metricsLsnr.onBytesSent(cnt);

                ses.bytesSent(cnt);

                if (!buf.hasRemaining())
                    queue.remove(buf);
                else
                    break;
            }
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void processWrite0(SelectionKey key) throws IOException {
            WritableByteChannel sockCh = (WritableByteChannel)key.channel();

            GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();
            ByteBuffer buf = ses.writeBuffer();
            NioOperationFuture<?> req = ses.removeMeta(NIO_OPERATION.ordinal());

            MessageWriter writer = ses.meta(MSG_WRITER.ordinal());

            if (writer == null)
                ses.addMeta(MSG_WRITER.ordinal(), writer = formatter.writer());

            if (req == null) {
                req = (NioOperationFuture<?>)ses.pollFuture();

                if (req == null && buf.position() == 0) {
                    key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));

                    return;
                }
            }

            Message msg;
            boolean finished = false;

            if (req != null) {
                msg = req.directMessage();

                assert msg != null;

                finished = msg.writeTo(buf, writer);

                if (finished)
                    writer.reset();
            }

            // Fill up as many messages as possible to write buffer.
            List<NioOperationFuture<?>> doneFuts = null;

            while (finished) {
                if (doneFuts == null)
                    doneFuts = new ArrayList<>();

                doneFuts.add(req);

                req = (NioOperationFuture<?>)ses.pollFuture();

                if (req == null)
                    break;

                msg = req.directMessage();

                assert msg != null;

                finished = msg.writeTo(buf, writer);

                if (finished)
                    writer.reset();
            }

            buf.flip();

            assert buf.hasRemaining();

            if (!skipWrite) {
                int cnt = sockCh.write(buf);

                if (!F.isEmpty(doneFuts)) {
                    for (int i = 0; i < doneFuts.size(); i++)
                        doneFuts.get(i).onDone();

                    doneFuts.clear();
                }

                if (log.isTraceEnabled())
                    log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                if (metricsLsnr != null)
                    metricsLsnr.onBytesSent(cnt);

                ses.bytesSent(cnt);
            }
            else {
                // For test purposes only (skipWrite is set to true in tests only).
                try {
                    U.sleep(50);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IOException("Thread has been interrupted.", e);
                }
            }

            if (buf.hasRemaining() || !finished) {
                buf.compact();

                ses.addMeta(NIO_OPERATION.ordinal(), req);
            }
            else
                buf.clear();
        }
    }

    /**
     * Thread performing only read operations from the channel.
     */
    private abstract class AbstractNioClientWorker extends GridWorker {
        /** Queue of change requests on this selector. */
        private final Queue<NioOperationFuture> changeReqs = new ConcurrentLinkedDeque8<>();

        /** Selector to select read events. */
        private Selector selector;

        /** Worker index. */
        private final int idx;

        /**
         * @param idx Index of this worker in server's array.
         * @param gridName Grid name.
         * @param name Worker name.
         * @param log Logger.
         * @throws IgniteCheckedException If selector could not be created.
         */
        protected AbstractNioClientWorker(int idx, @Nullable String gridName, String name, IgniteLogger log)
            throws IgniteCheckedException {
            super(gridName, name, log);

            selector = createSelector(null);

            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                boolean reset = false;
                while (!closed) {
                    try {
                        if (reset)
                            selector = createSelector(null);

                        bodyInternal();
                    }
                    catch (IgniteCheckedException e) {
                        if (!Thread.currentThread().isInterrupted()) {
                            U.error(log, "Failed to read data from remote connection (will wait for " +
                                ERR_WAIT_TIME + "ms).", e);

                            U.sleep(ERR_WAIT_TIME);

                            reset = true;
                        }
                    }
                }
            }
            catch (Throwable e) {
                U.error(log, "Caught unhandled exception in NIO worker thread (restart the node).", e);

                if (e instanceof Error)
                    throw e;
            }
        }

        /**
         * Adds socket channel to the registration queue and wakes up reading thread.
         *
         * @param req Change request.
         */
        private void offer(NioOperationFuture req) {
            changeReqs.offer(req);

            selector.wakeup();
        }

        /**
         * Processes read and write events and registration requests.
         *
         * @throws IgniteCheckedException If IOException occurred or thread was unable to add worker to workers pool.
         */
        @SuppressWarnings("unchecked")
        private void bodyInternal() throws IgniteCheckedException {
            try {
                while (!closed && selector.isOpen()) {
                    NioOperationFuture req;

                    while ((req = changeReqs.poll()) != null) {
                        switch (req.operation()) {
                            case REGISTER: {
                                register(req);

                                break;
                            }

                            case REQUIRE_WRITE: {
                                //Just register write key.
                                SelectionKey key = req.session().key();

                                if (key.isValid()) {
                                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

                                    // Update timestamp to protected against false write timeout.
                                    ((GridNioSessionImpl)key.attachment()).bytesSent(0);
                                }

                                break;
                            }

                            case CLOSE: {
                                if (close(req.session(), null))
                                    req.onDone(true);
                                else
                                    req.onDone(false);

                                break;
                            }

                            case PAUSE_READ: {
                                SelectionKey key = req.session().key();

                                if (key.isValid()) {
                                    key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));

                                    GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

                                    ses.readsPaused(true);

                                    req.onDone(true);
                                }
                                else
                                    req.onDone(false);

                                break;
                            }

                            case RESUME_READ: {
                                SelectionKey key = req.session().key();

                                if (key.isValid()) {
                                    key.interestOps(key.interestOps() | SelectionKey.OP_READ);

                                    GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

                                    ses.readsPaused(false);

                                    req.onDone(true);
                                }
                                else
                                    req.onDone(false);

                                break;
                            }
                        }
                    }

                    // Wake up every 2 seconds to check if closed.
                    if (selector.select(2000) > 0)
                        // Walk through the ready keys collection and process network events.
                        processSelectedKeys(selector.selectedKeys());

                    checkIdle(selector.keys());
                }
            }
            // Ignore this exception as thread interruption is equal to 'close' call.
            catch (ClosedByInterruptException e) {
                if (log.isDebugEnabled())
                    log.debug("Closing selector due to thread interruption: " + e.getMessage());
            }
            catch (ClosedSelectorException e) {
                throw new IgniteCheckedException("Selector got closed while active.", e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to select events on selector.", e);
            }
            finally {
                if (selector.isOpen()) {
                    if (log.isDebugEnabled())
                        log.debug("Closing all connected client sockets.");

                    // Close all channels registered with selector.
                    for (SelectionKey key : selector.keys())
                        close((GridSelectorNioSessionImpl)key.attachment(), null);

                    if (log.isDebugEnabled())
                        log.debug("Closing NIO selector.");

                    U.close(selector, log);
                }
            }
        }

        /**
         * Processes keys selected by a selector.
         *
         * @param keys Selected keys.
         * @throws ClosedByInterruptException If this thread was interrupted while reading data.
         */
        private void processSelectedKeys(Set<SelectionKey> keys) throws ClosedByInterruptException {
            if (log.isTraceEnabled())
                log.trace("Processing keys in client worker: " + keys.size());

            for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext(); ) {
                SelectionKey key = iter.next();

                iter.remove();

                // Was key closed?
                if (!key.isValid())
                    continue;

                GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

                assert ses != null;

                try {
                    if (key.isReadable())
                        processRead(key);

                    if (key.isValid() && key.isWritable())
                        processWrite(key);
                }
                catch (ClosedByInterruptException e) {
                    // This exception will be handled in bodyInternal() method.
                    throw e;
                }
                catch (IOException e) {
                    if (!closed)
                        U.warn(log, "Failed to process selector key (will close): " + ses, e);

                    close(ses, new GridNioException(e));
                }
            }
        }

        /**
         * Checks sessions assigned to a selector for timeouts.
         *
         * @param keys Keys registered to selector.
         */
        private void checkIdle(Iterable<SelectionKey> keys) {
            long now = U.currentTimeMillis();

            for (SelectionKey key : keys) {
                GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

                try {
                    long writeTimeout0 = writeTimeout;

                    // If we are writing and timeout passed.
                    if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0 &&
                        now - ses.lastSendTime() > writeTimeout0) {
                        filterChain.onSessionWriteTimeout(ses);

                        // Update timestamp to avoid multiple notifications within one timeout interval.
                        ses.bytesSent(0);

                        continue;
                    }

                    long idleTimeout0 = idleTimeout;

                    if (now - ses.lastReceiveTime() > idleTimeout0 && now - ses.lastSendScheduleTime() > idleTimeout0) {
                        filterChain.onSessionIdleTimeout(ses);

                        // Update timestamp to avoid multiple notifications within one timeout interval.
                        ses.resetSendScheduleTime();
                        ses.bytesReceived(0);
                    }
                }
                catch (IgniteCheckedException e) {
                    close(ses,  e);
                }
            }
        }

        /**
         * Registers given socket channel to the selector, creates a session and notifies the listener.
         *
         * @param req Registration request.
         */
        private void register(NioOperationFuture<GridNioSession> req) {
            assert req != null;

            SocketChannel sockCh = req.socketChannel();

            assert sockCh != null;

            Socket sock = sockCh.socket();

            try {
                ByteBuffer writeBuf = null;
                ByteBuffer readBuf = null;

                if (directMode) {
                    writeBuf = directBuf ? ByteBuffer.allocateDirect(sock.getSendBufferSize()) :
                        ByteBuffer.allocate(sock.getSendBufferSize());
                    readBuf = directBuf ? ByteBuffer.allocateDirect(sock.getReceiveBufferSize()) :
                        ByteBuffer.allocate(sock.getReceiveBufferSize());

                    writeBuf.order(order);
                    readBuf.order(order);
                }

                final GridSelectorNioSessionImpl ses = new GridSelectorNioSessionImpl(
                    log,
                    idx,
                    filterChain,
                    (InetSocketAddress)sockCh.getLocalAddress(),
                    (InetSocketAddress)sockCh.getRemoteAddress(),
                    req.accepted(),
                    sndQueueLimit,
                    writeBuf,
                    readBuf);

                Map<Integer, ?> meta = req.meta();

                if (meta != null) {
                    for (Entry<Integer, ?> e : meta.entrySet())
                        ses.addMeta(e.getKey(), e.getValue());
                }

                SelectionKey key = sockCh.register(selector, SelectionKey.OP_READ, ses);

                ses.key(key);

                if (!ses.accepted())
                    resend(ses);

                sessions.add(ses);

                try {
                    filterChain.onSessionOpened(ses);

                    req.onDone(ses);
                }
                catch (IgniteCheckedException e) {
                    close(ses, e);

                    req.onDone(e);
                }

                if (closed)
                    ses.onServerStopped();
            }
            catch (ClosedChannelException e) {
                U.warn(log, "Failed to register accepted socket channel to selector (channel was closed): "
                    + sock.getRemoteSocketAddress(), e);
            }
            catch (IOException e) {
                U.error(log, "Failed to get socket addresses.", e);
            }
        }

        /**
         * Closes the ses and all associated resources, then notifies the listener.
         *
         * @param ses Session to be closed.
         * @param e Exception to be passed to the listener, if any.
         * @return {@code True} if this call closed the ses.
         */
        protected boolean close(final GridSelectorNioSessionImpl ses, @Nullable final IgniteCheckedException e) {
            if (e != null) {
                // Print stack trace only if has runtime exception in it's cause.
                if (e.hasCause(IOException.class))
                    U.warn(log, "Closing NIO session because of unhandled exception [cls=" + e.getClass() +
                        ", msg=" + e.getMessage() + ']');
                else
                    U.error(log, "Closing NIO session because of unhandled exception.", e);
            }

            sessions.remove(ses);

            if (closed)
                ses.onServerStopped();

            SelectionKey key = ses.key();

            // Shutdown input and output so that remote client will see correct socket close.
            Socket sock = ((SocketChannel)key.channel()).socket();

            if (ses.setClosed()) {
                if (directBuf) {
                    if (ses.writeBuffer() != null)
                        ((DirectBuffer)ses.writeBuffer()).cleaner().clean();

                    if (ses.readBuffer() != null)
                        ((DirectBuffer)ses.readBuffer()).cleaner().clean();
                }

                try {
                    try {
                        sock.shutdownInput();
                    }
                    catch (IOException ignored) {
                        // No-op.
                    }

                    try {
                        sock.shutdownOutput();
                    }
                    catch (IOException ignored) {
                        // No-op.
                    }
                }
                finally {
                    U.close(key, log);
                    U.close(sock, log);
                }

                if (e != null)
                    filterChain.onExceptionCaught(ses, e);

                try {
                    filterChain.onSessionClosed(ses);
                }
                catch (IgniteCheckedException e1) {
                    filterChain.onExceptionCaught(ses, e1);
                }

                ses.removeMeta(BUF_META_KEY);

                // Since ses is in closed state, no write requests will be added.
                NioOperationFuture<?> fut = ses.removeMeta(NIO_OPERATION.ordinal());

                GridNioRecoveryDescriptor recovery = ses.recoveryDescriptor();

                if (recovery != null) {
                    try {
                        // Poll will update recovery data.
                        while ((fut = (NioOperationFuture<?>)ses.pollFuture()) != null) {
                            if (fut.skipRecovery())
                                fut.connectionClosed();
                        }
                    }
                    finally {
                        recovery.release();
                    }
                }
                else {
                    if (fut != null)
                        fut.connectionClosed();

                    while ((fut = (NioOperationFuture<?>)ses.pollFuture()) != null)
                        fut.connectionClosed();
                }

                return true;
            }

            return false;
        }

        /**
         * Processes read-available event on the key.
         *
         * @param key Key that is ready to be read.
         * @throws IOException If key read failed.
         */
        protected abstract void processRead(SelectionKey key) throws IOException;

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        protected abstract void processWrite(SelectionKey key) throws IOException;
    }

    /**
     * Gets outbound messages queue size.
     *
     * @return Write queue size.
     */
    public int outboundMessagesQueueSize() {
        int res = 0;

        for (GridSelectorNioSessionImpl ses : sessions)
            res += ses.writeQueueSize();

        return res;
    }

    /**
     * A separate thread that will accept incoming connections and schedule read to some worker.
     */
    private class GridNioAcceptWorker extends GridWorker {
        /** Selector for this thread. */
        private Selector selector;

        /**
         * @param gridName Grid name.
         * @param name Thread name.
         * @param log Log.
         * @param selector Which will accept incoming connections.
         */
        protected GridNioAcceptWorker(@Nullable String gridName, String name, IgniteLogger log, Selector selector) {
            super(gridName, name, log);

            this.selector = selector;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                boolean reset = false;

                while (!closed && !Thread.currentThread().isInterrupted()) {
                    try {
                        if (reset)
                            selector = createSelector(locAddr);

                        accept();
                    }
                    catch (IgniteCheckedException e) {
                        if (!Thread.currentThread().isInterrupted()) {
                            U.error(log, "Failed to accept remote connection (will wait for " + ERR_WAIT_TIME + "ms).",
                                e);

                            U.sleep(ERR_WAIT_TIME);

                            reset = true;
                        }
                    }
                }
            }
            finally {
                closeSelector(); // Safety.
            }
        }

        /**
         * Accepts connections and schedules them for processing by one of read workers.
         *
         * @throws IgniteCheckedException If failed.
         */
        private void accept() throws IgniteCheckedException {
            try {
                while (!closed && selector.isOpen() && !Thread.currentThread().isInterrupted()) {
                    // Wake up every 2 seconds to check if closed.
                    if (selector.select(2000) > 0)
                        // Walk through the ready keys collection and process date requests.
                        processSelectedKeys(selector.selectedKeys());
                }
            }
            // Ignore this exception as thread interruption is equal to 'close' call.
            catch (ClosedByInterruptException e) {
                if (log.isDebugEnabled())
                    log.debug("Closing selector due to thread interruption [srvr=" + this +
                        ", err=" + e.getMessage() + ']');
            }
            catch (ClosedSelectorException e) {
                throw new IgniteCheckedException("Selector got closed while active: " + this, e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to accept connection: " + this, e);
            }
            finally {
                closeSelector();
            }
        }

        /**
         * Close selector if needed.
         */
        private void closeSelector() {
            if (selector.isOpen()) {
                if (log.isDebugEnabled())
                    log.debug("Closing all listening sockets.");

                // Close all channels registered with selector.
                for (SelectionKey key : selector.keys())
                    U.close(key.channel(), log);

                if (log.isDebugEnabled())
                    log.debug("Closing NIO selector.");

                U.close(selector, log);
            }
        }

        /**
         * Processes selected accept requests for server socket.
         *
         * @param keys Selected keys from acceptor.
         * @throws IOException If accept failed or IOException occurred while configuring channel.
         */
        private void processSelectedKeys(Set<SelectionKey> keys) throws IOException {
            if (log.isDebugEnabled())
                log.debug("Processing keys in accept worker: " + keys.size());

            for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext();) {
                SelectionKey key = iter.next();

                iter.remove();

                // Was key closed?
                if (!key.isValid())
                    continue;

                if (key.isAcceptable()) {
                    // The key indexes into the selector so we
                    // can retrieve the socket that's ready for I/O
                    ServerSocketChannel srvrCh = (ServerSocketChannel)key.channel();

                    SocketChannel sockCh = srvrCh.accept();

                    sockCh.configureBlocking(false);
                    sockCh.socket().setTcpNoDelay(tcpNoDelay);
                    sockCh.socket().setKeepAlive(true);

                    if (sockSndBuf > 0)
                        sockCh.socket().setSendBufferSize(sockSndBuf);

                    if (sockRcvBuf > 0)
                        sockCh.socket().setReceiveBufferSize(sockRcvBuf);

                    if (log.isDebugEnabled())
                        log.debug("Accepted new client connection: " + sockCh.socket().getRemoteSocketAddress());

                    addRegistrationReq(sockCh);
                }
            }
        }

        /**
         * Adds registration request for a given socket channel to the next selector. Next selector
         * is selected according to a round-robin algorithm.
         *
         * @param sockCh Socket channel to be registered on one of the selectors.
         */
        private void addRegistrationReq(SocketChannel sockCh) {
            offerBalanced(new NioOperationFuture(sockCh));
        }
    }

    /**
     * Asynchronous operation that may be requested on selector.
     */
    private enum NioOperation {
        /** Register read key selection. */
        REGISTER,

        /** Register write key selection. */
        REQUIRE_WRITE,

        /** Close key. */
        CLOSE,

        /** Pause read. */
        PAUSE_READ,

        /** Resume read. */
        RESUME_READ
    }

    /**
     * Class for requesting write and session close operations.
     */
    private static class NioOperationFuture<R> extends GridNioFutureImpl<R> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Socket channel in register request. */
        @GridToStringExclude
        private SocketChannel sockCh;

        /** Session to perform operation on. */
        private GridSelectorNioSessionImpl ses;

        /** Is it a close request or a write request. */
        private NioOperation op;

        /** Message. */
        private ByteBuffer msg;

        /** Direct message. */
        private Message commMsg;

        /** */
        private boolean accepted;

        /** */
        private Map<Integer, ?> meta;

        /** */
        private boolean skipRecovery;

        /**
         * Creates registration request for a given socket channel.
         *
         * @param sockCh Socket channel to register on selector.
         */
        NioOperationFuture(SocketChannel sockCh) {
            this(sockCh, true, null);
        }

        /**
         * @param sockCh Socket channel.
         * @param accepted {@code True} if socket has been accepted.
         * @param meta Optional meta.
         */
        NioOperationFuture(
            SocketChannel sockCh,
            boolean accepted,
            @Nullable Map<Integer, ?> meta
        ) {
            op = NioOperation.REGISTER;

            this.sockCh = sockCh;
            this.accepted = accepted;
            this.meta = meta;
        }

        /**
         * Creates change request.
         *
         * @param ses Session to change.
         * @param op Requested operation.
         */
        NioOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op) {
            assert ses != null;
            assert op != null;
            assert op != NioOperation.REGISTER;

            this.ses = ses;
            this.op = op;
        }

        /**
         * Creates change request.
         *
         * @param ses Session to change.
         * @param op Requested operation.
         * @param msg Message.
         */
        NioOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op,
            ByteBuffer msg) {
            assert ses != null;
            assert op != null;
            assert op != NioOperation.REGISTER;
            assert msg != null;

            this.ses = ses;
            this.op = op;
            this.msg = msg;
        }

        /**
         * Creates change request.
         *
         * @param ses Session to change.
         * @param op Requested operation.
         * @param commMsg Direct message.
         * @param skipRecovery Skip recovery flag.
         */
        NioOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op,
            Message commMsg, boolean skipRecovery) {
            assert ses != null;
            assert op != null;
            assert op != NioOperation.REGISTER;
            assert commMsg != null;

            this.ses = ses;
            this.op = op;
            this.commMsg = commMsg;
            this.skipRecovery = skipRecovery;
        }

        /**
         * @return Requested change operation.
         */
        private NioOperation operation() {
            return op;
        }

        /**
         * @return Message.
         */
        private ByteBuffer message() {
            return msg;
        }

        /**
         * @return Direct message.
         */
        private Message directMessage() {
            return commMsg;
        }

        /**
         * @param ses New session instance.
         */
        private void resetSession(GridSelectorNioSessionImpl ses) {
            assert commMsg != null;

            this.ses = ses;
        }

        /**
         * @return Socket channel for register request.
         */
        private SocketChannel socketChannel() {
            return sockCh;
        }

        /**
         * @return Session for this change request.
         */
        private GridSelectorNioSessionImpl session() {
            return ses;
        }

        /**
         * @return {@code True} if connection has been accepted.
         */
        boolean accepted() {
            return accepted;
        }

        /**
         * @return Meta.
         */
        public Map<Integer, ?> meta() {
            return meta;
        }

        /**
         * Applicable to write futures only. Fails future with corresponding IOException.
         */
        private void connectionClosed() {
            assert op == NioOperation.REQUIRE_WRITE;
            assert ses != null;

            onDone(new IOException("Failed to send message (connection was closed): " + ses));
        }

        /** {@inheritDoc} */
        @Override public boolean skipRecovery() {
            return skipRecovery;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NioOperationFuture.class, this);
        }
    }

    /**
     * Filter forwarding messages from chain's head to this server.
     */
    private class HeadFilter extends GridNioFilterAdapter {
        /**
         * Assigns filter name.
         */
        protected HeadFilter() {
            super("HeadFilter");
        }

        /** {@inheritDoc} */
        @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
            if (directMode && sslFilter != null)
                ses.addMeta(BUF_SSL_SYSTEM_META_KEY, new ConcurrentLinkedDeque8<>());

            proceedSessionOpened(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionClosed(ses);
        }

        /** {@inheritDoc} */
        @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
            proceedExceptionCaught(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) {
            if (directMode) {
                boolean sslSys = sslFilter != null && msg instanceof ByteBuffer;

                if (sslSys) {
                    ConcurrentLinkedDeque8<ByteBuffer> queue = ses.meta(BUF_SSL_SYSTEM_META_KEY);

                    queue.offer((ByteBuffer)msg);

                    SelectionKey key = ((GridSelectorNioSessionImpl)ses).key();

                    if (key.isValid())
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

                    return null;
                }
                else
                    return send(ses, (Message)msg);
            }
            else
                return send(ses, (ByteBuffer)msg);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
            proceedMessageReceived(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) {
            return close(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionIdleTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionWriteTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws IgniteCheckedException {
            return pauseResumeReads(ses, NioOperation.PAUSE_READ);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws IgniteCheckedException {
            return pauseResumeReads(ses, NioOperation.RESUME_READ);
        }
    }

    /**
     * Constructs a new instance of {@link GridNioServer}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder<T> {
        /** Empty filters. */
        private static final GridNioFilter[] EMPTY_FILTERS = new GridNioFilter[0];

        /** Local address. */
        private InetAddress addr;

        /** Local port. */
        private int port;

        /** Logger. */
        private IgniteLogger log;

        /** Selector count. */
        private int selectorCnt;

        /** Grid name. */
        private String gridName;

        /** TCP_NO_DELAY flag. */
        private boolean tcpNoDelay;

        /** Direct buffer flag. */
        private boolean directBuf;

        /** Byte order. */
        private ByteOrder byteOrder = ByteOrder.nativeOrder();

        /** NIO server listener. */
        private GridNioServerListener<T> lsnr;

        /** Send buffer size. */
        private int sockSndBufSize;

        /** Receive buffer size. */
        private int sockRcvBufSize;

        /** Send queue limit. */
        private int sndQueueLimit = DFLT_SEND_QUEUE_LIMIT;

        /** Whether direct mode is used. */
        private boolean directMode;

        /** Metrics listener. */
        private GridNioMetricsListener metricsLsnr;

        /** NIO filters. */
        private GridNioFilter[] filters;

        /** Idle timeout. */
        private long idleTimeout = -1;

        /** Write timeout. */
        private long writeTimeout = -1;

        /** Daemon flag. */
        private boolean daemon;

        /** Message formatter. */
        private MessageFormatter formatter;

        /** Skip recovery predicate. */
        private IgnitePredicate<Message> skipRecoveryPred;

        /** Message queue size listener. */
        private IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr;

        /**
         * Finishes building the instance.
         *
         * @return Final instance of {@link GridNioServer}.
         * @throws IgniteCheckedException If NIO client worker creation failed or address is already in use.
         */
        public GridNioServer<T> build() throws IgniteCheckedException {
            GridNioServer<T> ret = new GridNioServer<>(
                addr,
                port,
                log,
                selectorCnt,
                gridName,
                tcpNoDelay,
                directBuf,
                byteOrder,
                lsnr,
                sockSndBufSize,
                sockRcvBufSize,
                sndQueueLimit,
                directMode,
                daemon,
                metricsLsnr,
                formatter,
                skipRecoveryPred,
                msgQueueLsnr,
                filters != null ? Arrays.copyOf(filters, filters.length) : EMPTY_FILTERS
            );

            if (idleTimeout >= 0)
                ret.idleTimeout(idleTimeout);

            if (writeTimeout >= 0)
                ret.writeTimeout(writeTimeout);

            return ret;
        }

        /**
         * @param addr Local address.
         * @return This for chaining.
         */
        public Builder<T> address(InetAddress addr) {
            this.addr = addr;

            return this;
        }

        /**
         * @param port Local port. If {@code -1} passed then server will not be
         *      accepting connections and only outgoing connections will be possible.
         * @return This for chaining.
         */
        public Builder<T> port(int port) {
            this.port = port;

            return this;
        }

        /**
         * @param log Logger.
         * @return This for chaining.
         */
        public Builder<T> logger(IgniteLogger log) {
            this.log = log;

            return this;
        }

        /**
         * @param selectorCnt Selector count.
         * @return This for chaining.
         */
        public Builder<T> selectorCount(int selectorCnt) {
            this.selectorCnt = selectorCnt;

            return this;
        }

        /**
         * @param gridName Grid name.
         * @return This for chaining.
         */
        public Builder<T> gridName(@Nullable String gridName) {
            this.gridName = gridName;

            return this;
        }

        /**
         * @param tcpNoDelay If TCP_NODELAY option should be set to accepted sockets.
         * @return This for chaining.
         */
        public Builder<T> tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;

            return this;
        }

        /**
         * @param directBuf Whether to use direct buffer.
         * @return This for chaining.
         */
        public Builder<T> directBuffer(boolean directBuf) {
            this.directBuf = directBuf;

            return this;
        }

        /**
         * @param byteOrder Byte order to use.
         * @return This for chaining.
         */
        public Builder<T> byteOrder(ByteOrder byteOrder) {
            this.byteOrder = byteOrder;

            return this;
        }

        /**
         * @param lsnr NIO server listener.
         * @return This for chaining.
         */
        public Builder<T> listener(GridNioServerListener<T> lsnr) {
            this.lsnr = lsnr;

            return this;
        }

        /**
         * @param sockSndBufSize Socket send buffer size.
         * @return This for chaining.
         */
        public Builder<T> socketSendBufferSize(int sockSndBufSize) {
            this.sockSndBufSize = sockSndBufSize;

            return this;
        }

        /**
         * @param sockRcvBufSize Socket receive buffer size.
         * @return This for chaining.
         */
        public Builder<T> socketReceiveBufferSize(int sockRcvBufSize) {
            this.sockRcvBufSize = sockRcvBufSize;

            return this;
        }

        /**
         * @param sndQueueLimit Send queue limit.
         * @return This for chaining.
         */
        public Builder<T> sendQueueLimit(int sndQueueLimit) {
            this.sndQueueLimit = sndQueueLimit;

            return this;
        }

        /**
         * @param directMode Whether direct mode is used.
         * @return This for chaining.
         */
        public Builder<T> directMode(boolean directMode) {
            this.directMode = directMode;

            return this;
        }

        /**
         * @param metricsLsnr Metrics listener.
         * @return This for chaining.
         */
        public Builder<T> metricsListener(GridNioMetricsListener metricsLsnr) {
            this.metricsLsnr = metricsLsnr;

            return this;
        }

        /**
         * @param filters NIO filters.
         * @return This for chaining.
         */
        public Builder<T> filters(GridNioFilter... filters) {
            this.filters = filters;

            return this;
        }

        /**
         * @param idleTimeout Idle timeout.
         * @return This for chaining.
         */
        public Builder<T> idleTimeout(long idleTimeout) {
            this.idleTimeout = idleTimeout;

            return this;
        }

        /**
         * @param writeTimeout Write timeout.
         * @return This for chaining.
         */
        public Builder<T> writeTimeout(long writeTimeout) {
            this.writeTimeout = writeTimeout;

            return this;
        }

        /**
         * @param daemon Daemon flag to create threads.
         * @return This for chaining.
         */
        public Builder<T> daemon(boolean daemon) {
            this.daemon = daemon;

            return this;
        }

        /**
         * @param formatter Message formatter.
         * @return This for chaining.
         */
        public Builder<T> messageFormatter(MessageFormatter formatter) {
            this.formatter = formatter;

            return this;
        }

        /**
         * @param skipRecoveryPred Skip recovery predicate.
         * @return This for chaining.
         */
        public Builder<T> skipRecoveryPredicate(IgnitePredicate<Message> skipRecoveryPred) {
            this.skipRecoveryPred = skipRecoveryPred;

            return this;
        }

        /**
         * @param msgQueueLsnr Message queue size listener.
         * @return Instance of this builder for chaining.
         */
        public Builder<T> messageQueueSizeListener(IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr) {
            this.msgQueueLsnr = msgQueueLsnr;

            return this;
        }
    }
}