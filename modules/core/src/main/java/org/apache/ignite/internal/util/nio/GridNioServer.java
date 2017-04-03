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
import java.lang.reflect.Field;
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
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
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
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
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
    /** */
    public static final String IGNITE_IO_BALANCE_RANDOM_BALANCE = "IGNITE_IO_BALANCE_RANDOM_BALANCER";

    /** Default session write timeout. */
    public static final int DFLT_SES_WRITE_TIMEOUT = 5000;

    /** Default send queue limit. */
    public static final int DFLT_SEND_QUEUE_LIMIT = 0;

    /** Time, which server will wait before retry operation. */
    private static final long ERR_WAIT_TIME = 2000;

    /** Buffer metadata key. */
    private static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL system data buffer metadata key. */
    private static final int BUF_SSL_SYSTEM_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL write buf limit. */
    private static final int WRITE_BUF_LIMIT = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private static final boolean DISABLE_KEYSET_OPTIMIZATION =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_NO_SELECTOR_OPTS);

    /**
     *
     */
    static {
        // This is a workaround for JDK bug (NPE in Selector.open()).
        // http://bugs.sun.com/view_bug.do?bug_id=6427854
        try {
            Selector.open().close();
        }
        catch (IOException ignored) {
            // No-op.
        }
    }

    /** Defines how many times selector should do {@code selectNow()} before doing {@code select(long)}. */
    private long selectorSpins;

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

    /** Index to select which thread will serve next incoming socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int readBalanceIdx;

    /** Index to select which thread will serve next out socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int writeBalanceIdx = 1;

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
    private GridNioMessageWriterFactory writerFactory;

    /** */
    @GridToStringExclude
    private IgnitePredicate<Message> skipRecoveryPred;

    /** Optional listener to monitor outbound message queue size. */
    private IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr;

    /** */
    private final AtomicLong readerMoveCnt = new AtomicLong();

    /** */
    private final AtomicLong writerMoveCnt = new AtomicLong();

    /** */
    private final IgniteRunnable balancer;

    /** */
    private final boolean readWriteSelectorsAssign;

    /**
     * @param addr Address.
     * @param port Port.
     * @param log Log.
     * @param selectorCnt Count of selectors and selecting threads.
     * @param gridName Grid name.
     * @param srvName Logical server name for threads identification.
     * @param selectorSpins Defines how many non-blocking {@code selector.selectNow()} should be made before
     *      falling into {@code selector.select(long)} in NIO server. Long value. Default is {@code 0}.
     *      Can be set to {@code Long.MAX_VALUE} so selector threads will never block.
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
     * @param writerFactory Writer factory.
     * @param skipRecoveryPred Skip recovery predicate.
     * @param msgQueueLsnr Message queue size listener.
     * @param readWriteSelectorsAssign If {@code true} then in/out connections are assigned to even/odd workers.
     * @param filters Filters for this server.
     * @throws IgniteCheckedException If failed.
     */
    private GridNioServer(
        InetAddress addr,
        int port,
        IgniteLogger log,
        int selectorCnt,
        @Nullable String gridName,
        @Nullable String srvName,
        long selectorSpins,
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
        GridNioMessageWriterFactory writerFactory,
        IgnitePredicate<Message> skipRecoveryPred,
        IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr,
        boolean readWriteSelectorsAssign,
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
        this.selectorSpins = selectorSpins;
        this.readWriteSelectorsAssign = readWriteSelectorsAssign;

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
            String threadName;

            if (srvName == null)
                threadName = "grid-nio-worker-" + i;
            else
                threadName = "grid-nio-worker-" + srvName + "-" + i;

            AbstractNioClientWorker worker = directMode ?
                new DirectNioClientWorker(i, gridName, threadName, log) :
                new ByteBufferNioClientWorker(i, gridName, threadName, log);

            clientWorkers.add(worker);

            clientThreads[i] = new IgniteThread(worker);

            clientThreads[i].setDaemon(daemon);
        }

        this.directMode = directMode;
        this.metricsLsnr = metricsLsnr;
        this.writerFactory = writerFactory;

        this.skipRecoveryPred = skipRecoveryPred != null ? skipRecoveryPred : F.<Message>alwaysFalse();

        long balancePeriod = IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, 5000);

        IgniteRunnable balancer0 = null;

        if (balancePeriod > 0) {
            boolean rndBalance = IgniteSystemProperties.getBoolean(IGNITE_IO_BALANCE_RANDOM_BALANCE, false);

            if (rndBalance)
                balancer0 = new RandomBalancer();
            else {
                balancer0 = readWriteSelectorsAssign ?
                    new ReadWriteSizeBasedBalancer(balancePeriod) :
                    new SizeBasedBalancer(balancePeriod);
            }
        }

        this.balancer = balancer0;
    }

    /**
     * @return Number of reader sessions move.
     */
    public long readerMoveCount() {
        return readerMoveCnt.get();
    }

    /**
     * @return Number of reader writer move.
     */
    public long writerMoveCount() {
        return writerMoveCnt.get();
    }

    /**
     * @return Configured port.
     */
    public int port() {
        return locAddr != null ? locAddr.getPort() : -1;
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
     * @return Selector spins.
     */
    public long selectorSpins() {
        return selectorSpins;
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

        impl.offerStateChange(fut);

        return fut;
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @param createFut {@code True} if future should be created.
     * @return Future for operation.
     */
    GridNioFuture<?> send(GridNioSession ses, ByteBuffer msg, boolean createFut) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl : ses;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (createFut) {
            NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, NioOperation.REQUIRE_WRITE, msg);

            send0(impl, fut, false);

            return fut;
        }
        else {
            SessionWriteRequest req = new WriteRequestImpl(ses, msg, true);

            send0(impl, req, false);

            return null;
        }
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @param createFut {@code True} if future should be created.
     * @return Future for operation.
     */
    GridNioFuture<?> send(GridNioSession ses, Message msg, boolean createFut) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (createFut) {
            NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, NioOperation.REQUIRE_WRITE, msg,
                skipRecoveryPred.apply(msg));

            send0(impl, fut, false);

            return fut;
        }
        else {
            SessionWriteRequest req = new WriteRequestImpl(ses, msg, skipRecoveryPred.apply(msg));

            send0(impl, req, false);

            return null;
        }
    }

    /**
     * @param ses Session.
     * @param req Request.
     * @param sys System message flag.
     * @throws IgniteCheckedException If session was closed.
     */
    private void send0(GridSelectorNioSessionImpl ses, SessionWriteRequest req, boolean sys) throws IgniteCheckedException {
        assert ses != null;
        assert req != null;

        int msgCnt = sys ? ses.offerSystemFuture(req) : ses.offerFuture(req);

        IgniteInClosure<IgniteException> ackC;

        if (!sys && (ackC = ses.removeMeta(ACK_CLOSURE.ordinal())) != null)
            req.ackClosure(ackC);

        if (ses.closed()) {
            if (ses.removeFuture(req)) {
                IOException err = new IOException("Failed to send message (connection was closed): " + ses);

                req.onError(err);

                if (!(req instanceof GridNioFuture))
                    throw new IgniteCheckedException(err);
            }
        }
        else if (!ses.procWrite.get() && ses.procWrite.compareAndSet(false, true)) {
            AbstractNioClientWorker worker = (AbstractNioClientWorker)ses.worker();

            if (worker != null)
                worker.offer((SessionChangeRequest)req);
        }

        if (msgQueueLsnr != null)
            msgQueueLsnr.apply(ses, msgCnt);
    }

    /**
     * Adds message at the front of the queue without acquiring back pressure semaphore.
     *
     * @param ses Session.
     * @param msg Message.
     * @throws IgniteCheckedException If session was closed.
     */
    public void sendSystem(GridNioSession ses, Message msg) throws IgniteCheckedException {
        sendSystem(ses, msg, null);
    }

    /**
     * Adds message at the front of the queue without acquiring back pressure semaphore.
     *
     * @param ses Session.
     * @param msg Message.
     * @param lsnr Future listener notified from the session thread.
     * @throws IgniteCheckedException If session was closed.
     */
    public void sendSystem(GridNioSession ses,
        Message msg,
        @Nullable IgniteInClosure<? super IgniteInternalFuture<?>> lsnr) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (lsnr != null) {
            NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, NioOperation.REQUIRE_WRITE, msg,
                skipRecoveryPred.apply(msg));

            fut.listen(lsnr);

            assert !fut.isDone();

            send0(impl, fut, true);
        }
        else {
            SessionWriteRequest req = new WriteRequestSystemImpl(ses, msg);

            send0(impl, req, true);
        }
    }

    /**
     * @param ses Session.
     */
    public void resend(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridNioRecoveryDescriptor recoveryDesc = ses.outRecoveryDescriptor();

        if (recoveryDesc != null && !recoveryDesc.messagesRequests().isEmpty()) {
            Deque<SessionWriteRequest> futs = recoveryDesc.messagesRequests();

            if (log.isDebugEnabled())
                log.debug("Resend messages [rmtNode=" + recoveryDesc.node().id() + ", msgCnt=" + futs.size() + ']');

            GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

            SessionWriteRequest fut0 = futs.iterator().next();

            for (SessionWriteRequest fut : futs) {
                fut.messageThread(true);

                fut.resetSession(ses0);
            }

            ses0.resend(futs);

            // Wake up worker.
            ses0.offerStateChange((GridNioServer.SessionChangeRequest)fut0);
        }
    }

    /**
     * @return Sessions.
     */
    public Collection<? extends GridNioSession> sessions() {
        return sessions;
    }

    /**
     * @return Workers.
     */
    public List<AbstractNioClientWorker> workers() {
        return clientWorkers;
    }

    /**
     * @param ses Session.
     * @param from Move from index.
     * @param to Move to index.
     */
    private void moveSession(GridNioSession ses, int from, int to) {
        assert from >= 0 && from < clientWorkers.size() : from;
        assert to >= 0 && to < clientWorkers.size() : to;
        assert from != to;

        GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

        SessionMoveFuture fut = new SessionMoveFuture(ses0, to);

        if (!ses0.offerMove(clientWorkers.get(from), fut))
            fut.onDone(false);
    }

    /**
     * @param ses Session.
     * @param op Operation.
     * @return Future for operation.
     */
    private GridNioFuture<?> pauseResumeReads(GridNioSession ses, NioOperation op) {
        assert ses instanceof GridSelectorNioSessionImpl;
        assert op == NioOperation.PAUSE_READ || op == NioOperation.RESUME_READ;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture(new IOException("Failed to pause/resume reads " +
                "(connection was closed): " + ses));

        NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, op);

        impl.offerStateChange(fut);

        return fut;
    }

    /**
     *
     */
    public void dumpStats() {
        U.warn(log, "NIO server statistics [readerSesBalanceCnt=" + readerMoveCnt.get() +
            ", writerSesBalanceCnt=" + writerMoveCnt.get() + ']');

        for (int i = 0; i < clientWorkers.size(); i++)
            clientWorkers.get(i).offer(new NioOperationFuture<Void>(null, NioOperation.DUMP_STATS));
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
        assert req.operation() == NioOperation.REGISTER : req;
        assert req.socketChannel() != null : req;

        int workers = clientWorkers.size();

        int balanceIdx;

        if (workers > 1) {
            if (readWriteSelectorsAssign) {
                if (req.accepted()) {
                    balanceIdx = readBalanceIdx;

                    readBalanceIdx += 2;

                    if (readBalanceIdx >= workers)
                        readBalanceIdx = 0;
                }
                else {
                    balanceIdx = writeBalanceIdx;

                    writeBalanceIdx += 2;

                    if (writeBalanceIdx >= workers)
                        writeBalanceIdx = 1;
                }
            }
            else {
                balanceIdx = readBalanceIdx;

                readBalanceIdx++;

                if (readBalanceIdx >= workers)
                    readBalanceIdx = 0;
            }
        }
        else
            balanceIdx = 0;

        clientWorkers.get(balanceIdx).offer(req);
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
                    LT.warn(log, "Read buffer contains data after filter chain processing (will discard " +
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
                SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

                // Check if there were any pending data from previous writes.
                if (buf == null) {
                    assert req == null;

                    req = ses.pollFuture();

                    if (req == null) {
                        if (ses.procWrite.get()) {
                            ses.procWrite.set(false);

                            if (ses.writeQueue().isEmpty()) {
                                if ((key.interestOps() & SelectionKey.OP_WRITE) != 0)
                                    key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
                            }
                            else
                                ses.procWrite.set(true);
                        }

                        break;
                    }

                    buf = (ByteBuffer)req.message();
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

                    req.onMessageWritten();
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ByteBufferNioClientWorker.class, this, super.toString());
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

            if (log.isTraceEnabled())
                log.trace("Bytes received [sockCh=" + sockCh + ", cnt=" + cnt + ']');

            if (cnt == 0)
                return;

            if (metricsLsnr != null)
                metricsLsnr.onBytesReceived(cnt);

            ses.bytesReceived(cnt);
            onRead(cnt);

            readBuf.flip();

            assert readBuf.hasRemaining();

            try {
                filterChain.onMessageReceived(ses, readBuf);

                if (readBuf.hasRemaining())
                    readBuf.compact();
                else
                    readBuf.clear();

                if (ses.hasSystemMessage() && !ses.procWrite.get()) {
                    ses.procWrite.set(true);

                    registerWrite(ses);
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

            if (writer == null) {
                try {
                    ses.addMeta(MSG_WRITER.ordinal(), writer = writerFactory.writer(ses));
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("Failed to create message writer.", e);
                }
            }

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

                SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

                while (true) {
                    if (req == null) {
                        req = systemMessage(ses);

                        if (req == null) {
                            req = ses.pollFuture();

                            if (req == null && buf.position() == 0) {
                                if (ses.procWrite.get()) {
                                    ses.procWrite.set(false);

                                    if (ses.writeQueue().isEmpty()) {
                                        if ((key.interestOps() & SelectionKey.OP_WRITE) != 0)
                                            key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
                                    }
                                    else
                                        ses.procWrite.set(true);
                                }

                                break;
                            }
                        }
                    }

                    Message msg;
                    boolean finished = false;

                    if (req != null) {
                        msg = (Message)req.message();

                        assert msg != null;

                        if (writer != null)
                            writer.setCurrentWriteClass(msg.getClass());

                        finished = msg.writeTo(buf, writer);

                        if (finished && writer != null)
                            writer.reset();
                    }

                    // Fill up as many messages as possible to write buffer.
                    while (finished) {
                        req.onMessageWritten();

                        req = systemMessage(ses);

                        if (req == null)
                            req = ses.pollFuture();

                        if (req == null)
                            break;

                        msg = (Message)req.message();

                        assert msg != null;

                        if (writer != null)
                            writer.setCurrentWriteClass(msg.getClass());

                        finished = msg.writeTo(buf, writer);

                        if (finished && writer != null)
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
            ConcurrentLinkedQueue<ByteBuffer> queue = ses.meta(BUF_SSL_SYSTEM_META_KEY);

            assert queue != null;

            ByteBuffer buf;

            while ((buf = queue.peek()) != null) {
                int cnt = sockCh.write(buf);

                if (metricsLsnr != null)
                    metricsLsnr.onBytesSent(cnt);

                ses.bytesSent(cnt);

                if (!buf.hasRemaining())
                    queue.poll();
                else
                    break;
            }
        }

        /**
         * @param ses Session.
         * @return System message request.
         */
        private SessionWriteRequest systemMessage(GridSelectorNioSessionImpl ses) {
            if (ses.hasSystemMessage()) {
                Object msg = ses.systemMessage();

                SessionWriteRequest req = new WriteRequestSystemImpl(ses, msg);

                assert !ses.hasSystemMessage();

                return req;
            }

            return null;
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
            SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

            MessageWriter writer = ses.meta(MSG_WRITER.ordinal());

            if (writer == null) {
                try {
                    ses.addMeta(MSG_WRITER.ordinal(), writer = writerFactory.writer(ses));
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("Failed to create message writer.", e);
                }
            }

            if (req == null) {
                req = systemMessage(ses);

                if (req == null) {
                    req = ses.pollFuture();

                    if (req == null && buf.position() == 0) {
                        if (ses.procWrite.get()) {
                            ses.procWrite.set(false);

                            if (ses.writeQueue().isEmpty()) {
                                if ((key.interestOps() & SelectionKey.OP_WRITE) != 0)
                                    key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
                            }
                            else
                                ses.procWrite.set(true);
                        }

                        return;
                    }
                }
            }

            Message msg;
            boolean finished = false;

            if (req != null) {
                msg = (Message)req.message();

                assert msg != null : req;

                if (writer != null)
                    writer.setCurrentWriteClass(msg.getClass());

                finished = msg.writeTo(buf, writer);

                if (finished && writer != null)
                    writer.reset();
            }

            // Fill up as many messages as possible to write buffer.
            while (finished) {
                req.onMessageWritten();

                req = systemMessage(ses);

                if (req == null)
                    req = ses.pollFuture();

                if (req == null)
                    break;

                msg = (Message)req.message();

                assert msg != null;

                if (writer != null)
                    writer.setCurrentWriteClass(msg.getClass());

                finished = msg.writeTo(buf, writer);

                if (finished && writer != null)
                    writer.reset();
            }

            buf.flip();

            assert buf.hasRemaining();

            if (!skipWrite) {
                int cnt = sockCh.write(buf);

                if (log.isTraceEnabled())
                    log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                if (metricsLsnr != null)
                    metricsLsnr.onBytesSent(cnt);

                ses.bytesSent(cnt);
                onWrite(cnt);
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

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DirectNioClientWorker.class, this, super.toString());
        }
    }

    /**
     * Thread performing only read operations from the channel.
     */
    private abstract class AbstractNioClientWorker extends GridWorker implements GridNioWorker {
        /** Queue of change requests on this selector. */
        private final ConcurrentLinkedQueue<SessionChangeRequest> changeReqs = new ConcurrentLinkedQueue<>();

        /** Selector to select read events. */
        private Selector selector;

        /** Selected keys. */
        private SelectedSelectionKeySet selectedKeys;

        /** Worker index. */
        private final int idx;

        /** */
        private long bytesRcvd;

        /** */
        private long bytesSent;

        /** */
        private volatile long bytesRcvd0;

        /** */
        private volatile long bytesSent0;

        /** Sessions assigned to this worker. */
        private final GridConcurrentHashSet<GridSelectorNioSessionImpl> workerSessions =
            new GridConcurrentHashSet<>();

        /** {@code True} if worker has called or is about to call {@code Selector.select()}. */
        private volatile boolean select;

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

            createSelector();

            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                boolean reset = false;

                while (!closed) {
                    try {
                        if (reset)
                            createSelector();

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
         * @throws IgniteCheckedException If failed.
         */
        private void createSelector() throws IgniteCheckedException {
            selectedKeys = null;

            selector = GridNioServer.this.createSelector(null);

            if (DISABLE_KEYSET_OPTIMIZATION)
                return;

            try {
                SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

                Class<?> selectorImplCls =
                    Class.forName("sun.nio.ch.SelectorImpl", false, U.gridClassLoader());

                // Ensure the current selector implementation is what we can instrument.
                if (!selectorImplCls.isAssignableFrom(selector.getClass()))
                    return;

                Field selectedKeysField = selectorImplCls.getDeclaredField("selectedKeys");
                Field publicSelectedKeysField = selectorImplCls.getDeclaredField("publicSelectedKeys");

                selectedKeysField.setAccessible(true);
                publicSelectedKeysField.setAccessible(true);

                selectedKeysField.set(selector, selectedKeySet);
                publicSelectedKeysField.set(selector, selectedKeySet);

                selectedKeys = selectedKeySet;

                if (log.isDebugEnabled())
                    log.debug("Instrumented an optimized java.util.Set into: " + selector);
            }
            catch (Exception e) {
                selectedKeys = null;

                if (log.isDebugEnabled())
                    log.debug("Failed to instrument an optimized java.util.Set into selector [selector=" + selector
                        + ", err=" + e + ']');
            }
        }

        /**
         * Adds socket channel to the registration queue and wakes up reading thread.
         *
         * @param req Change request.
         */
        public void offer(SessionChangeRequest req) {
            changeReqs.offer(req);

            if (select)
                selector.wakeup();
        }

        /** {@inheritDoc} */
        @Override public void offer(Collection<SessionChangeRequest> reqs) {
            for (SessionChangeRequest req : reqs)
                changeReqs.offer(req);

            selector.wakeup();
        }

        /** {@inheritDoc} */
        @Override public List<SessionChangeRequest> clearSessionRequests(GridNioSession ses) {
            List<SessionChangeRequest> sesReqs = null;

            for (SessionChangeRequest changeReq : changeReqs) {
                if (changeReq.session() == ses && !(changeReq instanceof SessionMoveFuture)) {
                    boolean rmv = changeReqs.remove(changeReq);

                    assert rmv : changeReq;

                    if (sesReqs == null)
                        sesReqs = new ArrayList<>();

                    sesReqs.add(changeReq);
                }
            }

            return sesReqs;
        }

        /**
         * Processes read and write events and registration requests.
         *
         * @throws IgniteCheckedException If IOException occurred or thread was unable to add worker to workers pool.
         */
        @SuppressWarnings("unchecked")
        private void bodyInternal() throws IgniteCheckedException, InterruptedException {
            try {
                long lastIdleCheck = U.currentTimeMillis();

                mainLoop:
                while (!closed && selector.isOpen()) {
                    SessionChangeRequest req0;

                    while ((req0 = changeReqs.poll()) != null) {
                        switch (req0.operation()) {
                            case REGISTER: {
                                register((NioOperationFuture)req0);

                                break;
                            }

                            case MOVE: {
                                SessionMoveFuture f = (SessionMoveFuture)req0;

                                GridSelectorNioSessionImpl ses = f.session();

                                if (idx == f.toIdx) {
                                    assert f.movedSocketChannel() != null : f;

                                    boolean add = workerSessions.add(ses);

                                    assert add;

                                    ses.finishMoveSession(this);

                                    if (idx % 2 == 0)
                                        readerMoveCnt.incrementAndGet();
                                    else
                                        writerMoveCnt.incrementAndGet();

                                    SelectionKey key = f.movedSocketChannel().register(selector,
                                        SelectionKey.OP_READ | SelectionKey.OP_WRITE,
                                        ses);

                                    ses.key(key);

                                    ses.procWrite.set(true);

                                    f.onDone(true);
                                }
                                else {
                                    assert f.movedSocketChannel() == null : f;

                                    if (workerSessions.remove(ses)) {
                                        ses.startMoveSession(this);

                                        SelectionKey key = ses.key();

                                        assert key.channel() != null : key;

                                        f.movedSocketChannel((SocketChannel)key.channel());

                                        key.cancel();

                                        clientWorkers.get(f.toIndex()).offer(f);
                                    }
                                    else
                                        f.onDone(false);
                                }

                                break;
                            }

                            case REQUIRE_WRITE: {
                                SessionWriteRequest req = (SessionWriteRequest)req0;

                                registerWrite((GridSelectorNioSessionImpl)req.session());

                                break;
                            }

                            case CLOSE: {
                                NioOperationFuture req = (NioOperationFuture)req0;

                                if (close(req.session(), null))
                                    req.onDone(true);
                                else
                                    req.onDone(false);

                                break;
                            }

                            case PAUSE_READ: {
                                NioOperationFuture req = (NioOperationFuture)req0;

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
                                NioOperationFuture req = (NioOperationFuture)req0;

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

                            case DUMP_STATS: {
                                NioOperationFuture req = (NioOperationFuture)req0;

                                try {
                                    dumpStats();
                                }
                                finally {
                                    // Complete the request just in case (none should wait on this future).
                                    req.onDone(true);
                                }
                            }
                        }
                    }

                    int res = 0;

                    for (long i = 0; i < selectorSpins && res == 0; i++) {
                        res = selector.selectNow();

                        if (res > 0) {
                            // Walk through the ready keys collection and process network events.
                            if (selectedKeys == null)
                                processSelectedKeys(selector.selectedKeys());
                            else
                                processSelectedKeysOptimized(selectedKeys.flip());
                        }

                        if (!changeReqs.isEmpty())
                            continue mainLoop;

                        // Just in case we do busy selects.
                        long now = U.currentTimeMillis();

                        if (now - lastIdleCheck > 2000) {
                            lastIdleCheck = now;

                            checkIdle(selector.keys());
                        }

                        if (isCancelled())
                            return;
                    }

                    // Falling to blocking select.
                    select = true;

                    try {
                        if (!changeReqs.isEmpty())
                            continue;

                        // Wake up every 2 seconds to check if closed.
                        if (selector.select(2000) > 0) {
                            // Walk through the ready keys collection and process network events.
                            if (selectedKeys == null)
                                processSelectedKeys(selector.selectedKeys());
                            else
                                processSelectedKeysOptimized(selectedKeys.flip());
                        }
                    }
                    finally {
                        select = false;
                    }

                    long now = U.currentTimeMillis();

                    if (now - lastIdleCheck > 2000) {
                        lastIdleCheck = now;

                        checkIdle(selector.keys());
                    }
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
         * @param ses Session.
         */
        public final void registerWrite(GridSelectorNioSessionImpl ses) {
            SelectionKey key = ses.key();

            if (key.isValid()) {
                if ((key.interestOps() & SelectionKey.OP_WRITE) == 0)
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

                // Update timestamp to protected against false write timeout.
                ses.bytesSent(0);
            }
        }

        /**
         *
         */
        private void dumpStats() {
            StringBuilder sb = new StringBuilder();

            Set<SelectionKey> keys = selector.keys();

            sb.append(U.nl())
                .append(">> Selector info [idx=").append(idx)
                .append(", keysCnt=").append(keys.size())
                .append(", bytesRcvd=").append(bytesRcvd)
                .append(", bytesRcvd0=").append(bytesRcvd0)
                .append(", bytesSent=").append(bytesSent)
                .append(", bytesSent0=").append(bytesSent0)
                .append("]").append(U.nl());

            for (SelectionKey key : keys) {
                GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

                MessageWriter writer = ses.meta(MSG_WRITER.ordinal());
                MessageReader reader = ses.meta(GridDirectParser.READER_META_KEY);

                sb.append("    Connection info [")
                    .append("in=").append(ses.accepted())
                    .append(", rmtAddr=").append(ses.remoteAddress())
                    .append(", locAddr=").append(ses.localAddress());

                GridNioRecoveryDescriptor outDesc = ses.outRecoveryDescriptor();

                if (outDesc != null) {
                    sb.append(", msgsSent=").append(outDesc.sent())
                        .append(", msgsAckedByRmt=").append(outDesc.acked())
                        .append(", descIdHash=").append(System.identityHashCode(outDesc));
                }
                else
                    sb.append(", outRecoveryDesc=null");

                GridNioRecoveryDescriptor inDesc = ses.inRecoveryDescriptor();

                if (inDesc != null) {
                    sb.append(", msgsRcvd=").append(inDesc.received())
                        .append(", lastAcked=").append(inDesc.lastAcknowledged())
                        .append(", descIdHash=").append(System.identityHashCode(inDesc));
                }
                else
                    sb.append(", inRecoveryDesc=null");

                sb.append(", bytesRcvd=").append(ses.bytesReceived())
                    .append(", bytesRcvd0=").append(ses.bytesReceived0())
                    .append(", bytesSent=").append(ses.bytesSent())
                    .append(", bytesSent0=").append(ses.bytesSent0())
                    .append(", opQueueSize=").append(ses.writeQueueSize())
                    .append(", msgWriter=").append(writer != null ? writer.toString() : "null")
                    .append(", msgReader=").append(reader != null ? reader.toString() : "null");

                int cnt = 0;

                for (SessionWriteRequest req : ses.writeQueue()) {
                    if (cnt == 0)
                        sb.append(",\n opQueue=[").append(req);
                    else
                        sb.append(',').append(req);

                    if (++cnt == 5) {
                        sb.append(']');

                        break;
                    }
                }

                sb.append("]").append(U.nl());
            }

            U.warn(log, sb.toString());
        }

        /**
         * Processes keys selected by a selector.
         *
         * @param keys Selected keys.
         * @throws ClosedByInterruptException If this thread was interrupted while reading data.
         */
        private void processSelectedKeysOptimized(SelectionKey[] keys) throws ClosedByInterruptException {
            for (int i = 0; ; i ++) {
                final SelectionKey key = keys[i];

                if (key == null)
                    break;

                // null out entry in the array to allow to have it GC'ed once the Channel close
                // See https://github.com/netty/netty/issues/2363
                keys[i] = null;

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
                catch (Exception e) {
                    U.warn(log, "Failed to process selector key (will close): " + ses, e);

                    close(ses, new GridNioException(e));
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

            if (keys.isEmpty())
                return;

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
                catch (Exception e) {
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

                    boolean opWrite = key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0;

                    // If we are writing and timeout passed.
                    if (opWrite && now - ses.lastSendTime() > writeTimeout0) {
                        filterChain.onSessionWriteTimeout(ses);

                        // Update timestamp to avoid multiple notifications within one timeout interval.
                        ses.bytesSent(0);

                        continue;
                    }

                    long idleTimeout0 = idleTimeout;

                    if (!opWrite &&
                        now - ses.lastReceiveTime() > idleTimeout0 &&
                        now - ses.lastSendScheduleTime() > idleTimeout0) {
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
                    this,
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
                workerSessions.add(ses);

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
         * Closes the session and all associated resources, then notifies the listener.
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
            workerSessions.remove(ses);

            SelectionKey key = ses.key();

            if (ses.setClosed()) {
                ses.onClosed();

                if (directBuf) {
                    if (ses.writeBuffer() != null)
                        ((DirectBuffer)ses.writeBuffer()).cleaner().clean();

                    if (ses.readBuffer() != null)
                        ((DirectBuffer)ses.readBuffer()).cleaner().clean();
                }

                // Shutdown input and output so that remote client will see correct socket close.
                Socket sock = ((SocketChannel)key.channel()).socket();

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

                ses.removeMeta(BUF_META_KEY);

                // Since ses is in closed state, no write requests will be added.
                SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

                GridNioRecoveryDescriptor outRecovery = ses.outRecoveryDescriptor();
                GridNioRecoveryDescriptor inRecovery = ses.inRecoveryDescriptor();

                IOException err = new IOException("Failed to send message (connection was closed): " + ses);

                if (outRecovery != null || inRecovery != null) {
                    try {
                        // Poll will update recovery data.
                        while ((req = ses.pollFuture()) != null) {
                            if (req.skipRecovery())
                                req.onError(err);
                        }
                    }
                    finally {
                        if (outRecovery != null)
                            outRecovery.release();

                        if (inRecovery != null && inRecovery != outRecovery)
                            inRecovery.release();
                    }
                }
                else {
                    if (req != null)
                        req.onError(err);

                    while ((req = ses.pollFuture()) != null)
                        req.onError(err);
                }

                try {
                    filterChain.onSessionClosed(ses);
                }
                catch (IgniteCheckedException e1) {
                    filterChain.onExceptionCaught(ses, e1);
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

        /**
         * @param cnt
         */
        final void onRead(int cnt) {
            bytesRcvd += cnt;
            bytesRcvd0 += cnt;
        }

        /**
         * @param cnt
         */
        final void onWrite(int cnt) {
            bytesSent += cnt;
            bytesSent0 += cnt;
        }

        /**
         *
         */
        final void reset0() {
            bytesSent0 = 0;
            bytesRcvd0 = 0;

            for (GridSelectorNioSessionImpl ses : workerSessions)
                ses.reset0();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AbstractNioClientWorker.class, this, super.toString());
        }
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

                    if (balancer != null)
                        balancer.run();
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
    enum NioOperation {
        /** Register read key selection. */
        REGISTER,

        /** Move session between workers. */
        MOVE,

        /** Register write key selection. */
        REQUIRE_WRITE,

        /** Close key. */
        CLOSE,

        /** Pause read. */
        PAUSE_READ,

        /** Resume read. */
        RESUME_READ,

        /** Dump statistics. */
        DUMP_STATS
    }

    /**
     *
     */
    private static final class WriteRequestSystemImpl implements SessionWriteRequest, SessionChangeRequest {
        /** */
        private final Object msg;

        /** */
        private final GridNioSession ses;

        /**
         * @param ses Session.
         * @param msg Message.
         */
        WriteRequestSystemImpl(GridNioSession ses, Object msg) {
            this.ses = ses;
            this.msg = msg;
        }

        /** {@inheritDoc} */
        @Override public void messageThread(boolean msgThread) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean messageThread() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean skipRecovery() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void ackClosure(IgniteInClosure<IgniteException> c) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public IgniteInClosure<IgniteException> ackClosure() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onError(Exception e) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object message() {
            return msg;
        }

        /** {@inheritDoc} */
        @Override public void onMessageWritten() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void resetSession(GridNioSession ses) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public GridNioSession session() {
            return ses;
        }

        /** {@inheritDoc} */
        @Override public NioOperation operation() {
            return NioOperation.REQUIRE_WRITE;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WriteRequestSystemImpl.class, this);
        }
    }

    /**
     *
     */
    private static final class WriteRequestImpl implements SessionWriteRequest, SessionChangeRequest {
        /** */
        private GridNioSession ses;

        /** */
        private final Object msg;

        /** */
        private boolean msgThread;

        /** */
        private final boolean skipRecovery;

        /** */
        private IgniteInClosure<IgniteException> ackC;

        /**
         * @param ses Session.
         * @param msg Message.
         * @param skipRecovery Skip recovery flag.
         */
        WriteRequestImpl(GridNioSession ses, Object msg, boolean skipRecovery) {
            this.ses = ses;
            this.msg = msg;
            this.skipRecovery = skipRecovery;
        }

        /** {@inheritDoc} */
        @Override public void messageThread(boolean msgThread) {
            this.msgThread = msgThread;
        }

        /** {@inheritDoc} */
        @Override public boolean messageThread() {
            return msgThread;
        }

        /** {@inheritDoc} */
        @Override public boolean skipRecovery() {
            return skipRecovery;
        }

        /** {@inheritDoc} */
        @Override public void ackClosure(IgniteInClosure<IgniteException> c) {
            ackC = c;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            assert msg instanceof Message;

            ((Message)msg).onAckReceived();
        }

        /** {@inheritDoc} */
        @Override public IgniteInClosure<IgniteException> ackClosure() {
            return ackC;
        }

        /** {@inheritDoc} */
        @Override public void onError(Exception e) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object message() {
            return msg;
        }

        /** {@inheritDoc} */
        @Override public void onMessageWritten() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void resetSession(GridNioSession ses) {
            this.ses = ses;
        }

        /** {@inheritDoc} */
        @Override public GridNioSession session() {
            return ses;
        }

        /** {@inheritDoc} */
        @Override public NioOperation operation() {
            return NioOperation.REQUIRE_WRITE;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WriteRequestImpl.class, this);
        }
    }

    /**
     * Class for requesting write and session close operations.
     */
    private static class NioOperationFuture<R> extends GridNioFutureImpl<R> implements SessionWriteRequest,
        SessionChangeRequest {
        /** */
        private static final long serialVersionUID = 0L;

        /** Socket channel in register request. */
        @GridToStringExclude
        private SocketChannel sockCh;

        /** Session to perform operation on. */
        @GridToStringExclude
        private GridSelectorNioSessionImpl ses;

        /** Is it a close request or a write request. */
        private NioOperation op;

        /** Message. */
        private Object msg;

        /** */
        @GridToStringExclude
        private boolean accepted;

        /** */
        @GridToStringExclude
        private Map<Integer, ?> meta;

        /** */
        @GridToStringExclude
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
            assert ses != null || op == NioOperation.DUMP_STATS : "Invalid params [ses=" + ses + ", op=" + op + ']';
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
        NioOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op, Object msg) {
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
            this.msg = commMsg;
            this.skipRecovery = skipRecovery;
        }

        /** {@inheritDoc} */
        public NioOperation operation() {
            return op;
        }

        /** {@inheritDoc} */
        public Object message() {
            return msg;
        }

        /** {@inheritDoc} */
        public void resetSession(GridNioSession ses) {
            assert msg instanceof Message : msg;

            this.ses = (GridSelectorNioSessionImpl)ses;
        }

        /**
         * @return Socket channel for register request.
         */
        SocketChannel socketChannel() {
            return sockCh;
        }

        /** {@inheritDoc} */
        public GridSelectorNioSessionImpl session() {
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

        /** {@inheritDoc} */
        @Override public void onError(Exception e) {
            onDone(e);
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            assert msg instanceof Message : msg;

            ((Message)msg).onAckReceived();
        }

        /** {@inheritDoc} */
        @Override public void onMessageWritten() {
            onDone();
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
     *
     */
    private static class SessionMoveFuture extends NioOperationFuture<Boolean> {
        /** */
        private final int toIdx;

        /** */
        @GridToStringExclude
        private SocketChannel movedSockCh;

        /**
         * @param ses Session.
         * @param toIdx Target worker index.
         */
        SessionMoveFuture(
            GridSelectorNioSessionImpl ses,
            int toIdx
        ) {
            super(ses, NioOperation.MOVE);

            this.toIdx = toIdx;
        }

        /**
         * @return Target worker index.
         */
        int toIndex() {
            return toIdx;
        }

        /**
         * @return Moved session socket channel.
         */
        SocketChannel movedSocketChannel() {
            return movedSockCh;
        }

        /**
         * @param movedSockCh Moved session socket channel.
         */
        void movedSocketChannel(SocketChannel movedSockCh) {
            assert movedSockCh != null;

            this.movedSockCh = movedSockCh;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SessionMoveFuture.class, this, super.toString());
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
                ses.addMeta(BUF_SSL_SYSTEM_META_KEY, new ConcurrentLinkedQueue<>());

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
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean fut) throws IgniteCheckedException {
            if (directMode) {
                boolean sslSys = sslFilter != null && msg instanceof ByteBuffer;

                if (sslSys) {
                    ConcurrentLinkedQueue<ByteBuffer> queue = ses.meta(BUF_SSL_SYSTEM_META_KEY);

                    assert queue != null;

                    queue.offer((ByteBuffer)msg);

                    GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

                    if (!ses0.procWrite.get() && ses0.procWrite.compareAndSet(false, true))
                        ses0.worker().registerWrite(ses0);

                    return null;
                }
                else
                    return send(ses, (Message)msg, fut);
            }
            else
                return send(ses, (ByteBuffer)msg, fut);
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

        /** Writer factory. */
        private GridNioMessageWriterFactory writerFactory;

        /** Skip recovery predicate. */
        private IgnitePredicate<Message> skipRecoveryPred;

        /** Message queue size listener. */
        private IgniteBiInClosure<GridNioSession, Integer> msgQueueLsnr;

        /** Name for threads identification. */
        private String srvName;

        /** */
        private long selectorSpins;

        /** */
        private boolean readWriteSelectorsAssign;

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
                srvName,
                selectorSpins,
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
                writerFactory,
                skipRecoveryPred,
                msgQueueLsnr,
                readWriteSelectorsAssign,
                filters != null ? Arrays.copyOf(filters, filters.length) : EMPTY_FILTERS
            );

            if (idleTimeout >= 0)
                ret.idleTimeout(idleTimeout);

            if (writeTimeout >= 0)
                ret.writeTimeout(writeTimeout);

            return ret;
        }

        /**
         * @param readWriteSelectorsAssign {@code True} to assign in/out connections even/odd workers.
         * @return This for chaining.
         */
        public Builder<T> readWriteSelectorsAssign(boolean readWriteSelectorsAssign) {
            this.readWriteSelectorsAssign = readWriteSelectorsAssign;

            return this;
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
         * @param srvName Logical server name for threads identification.
         * @return This for chaining.
         */
        public Builder<T> serverName(@Nullable String srvName) {
            this.srvName = srvName;

            return this;
        }

        /**
         * @param selectorSpins Defines how many non-blocking {@code selector.selectNow()} should be made before
         *      falling into {@code selector.select(long)} in NIO server. Long value. Default is {@code 0}.
         *      Can be set to {@code Long.MAX_VALUE} so selector threads will never block.
         * @return This for chaining.
         */
        public Builder<T> selectorSpins(long selectorSpins) {
            this.selectorSpins = selectorSpins;

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
         * @param writerFactory Writer factory.
         * @return This for chaining.
         */
        public Builder<T> writerFactory(GridNioMessageWriterFactory writerFactory) {
            this.writerFactory = writerFactory;

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

    /**
     *
     */
    private class ReadWriteSizeBasedBalancer implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private long lastBalance;

        /** */
        private final long balancePeriod;

        /**
         * @param balancePeriod Period.
         */
        ReadWriteSizeBasedBalancer(long balancePeriod) {
            this.balancePeriod = balancePeriod;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            long now = U.currentTimeMillis();

            if (lastBalance + balancePeriod < now) {
                lastBalance = now;

                long maxRcvd0 = -1, minRcvd0 = -1, maxSent0 = -1, minSent0 = -1;
                int maxRcvdIdx = -1, minRcvdIdx = -1, maxSentIdx = -1, minSentIdx = -1;

                for (int i = 0; i < clientWorkers.size(); i++) {
                    GridNioServer.AbstractNioClientWorker worker = clientWorkers.get(i);

                    int sesCnt = worker.workerSessions.size();

                    if (i % 2 == 0) {
                        // Reader.
                        long bytesRcvd0 = worker.bytesRcvd0;

                        if ((maxRcvd0 == -1 || bytesRcvd0 > maxRcvd0) && bytesRcvd0 > 0 && sesCnt > 1) {
                            maxRcvd0 = bytesRcvd0;
                            maxRcvdIdx = i;
                        }

                        if (minRcvd0 == -1 || bytesRcvd0 < minRcvd0) {
                            minRcvd0 = bytesRcvd0;
                            minRcvdIdx = i;
                        }
                    }
                    else {
                        // Writer.
                        long bytesSent0 = worker.bytesSent0;

                        if ((maxSent0 == -1 || bytesSent0 > maxSent0) && bytesSent0 > 0 && sesCnt > 1) {
                            maxSent0 = bytesSent0;
                            maxSentIdx = i;
                        }

                        if (minSent0 == -1 || bytesSent0 < minSent0) {
                            minSent0 = bytesSent0;
                            minSentIdx = i;
                        }
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Balancing data [minSent0=" + minSent0 + ", minSentIdx=" + minSentIdx +
                        ", maxSent0=" + maxSent0 + ", maxSentIdx=" + maxSentIdx +
                        ", minRcvd0=" + minRcvd0 + ", minRcvdIdx=" + minRcvdIdx +
                        ", maxRcvd0=" + maxRcvd0 + ", maxRcvdIdx=" + maxRcvdIdx + ']');

                if (maxSent0 != -1 && minSent0 != -1) {
                    GridSelectorNioSessionImpl ses = null;

                    long sentDiff = maxSent0 - minSent0;
                    long delta = sentDiff;
                    double threshold = sentDiff * 0.9;

                    GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions =
                        clientWorkers.get(maxSentIdx).workerSessions;

                    for (GridSelectorNioSessionImpl ses0 : sessions) {
                        long bytesSent0 = ses0.bytesSent0();

                        if (bytesSent0 < threshold &&
                            (ses == null || delta > U.safeAbs(bytesSent0 - sentDiff / 2))) {
                            ses = ses0;
                            delta = U.safeAbs(bytesSent0 - sentDiff / 2);
                        }
                    }

                    if (ses != null) {
                        if (log.isDebugEnabled())
                            log.debug("Will move session to less loaded writer [ses=" + ses +
                                ", from=" + maxSentIdx + ", to=" + minSentIdx + ']');

                        moveSession(ses, maxSentIdx, minSentIdx);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Unable to find session to move for writers.");
                    }
                }

                if (maxRcvd0 != -1 && minRcvd0 != -1) {
                    GridSelectorNioSessionImpl ses = null;

                    long rcvdDiff = maxRcvd0 - minRcvd0;
                    long delta = rcvdDiff;
                    double threshold = rcvdDiff * 0.9;

                    GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions =
                        clientWorkers.get(maxRcvdIdx).workerSessions;

                    for (GridSelectorNioSessionImpl ses0 : sessions) {
                        long bytesRcvd0 = ses0.bytesReceived0();

                        if (bytesRcvd0 < threshold &&
                            (ses == null || delta > U.safeAbs(bytesRcvd0 - rcvdDiff / 2))) {
                            ses = ses0;
                            delta = U.safeAbs(bytesRcvd0 - rcvdDiff / 2);
                        }
                    }

                    if (ses != null) {
                        if (log.isDebugEnabled())
                            log.debug("Will move session to less loaded reader [ses=" + ses +
                                ", from=" + maxRcvdIdx + ", to=" + minRcvdIdx + ']');

                        moveSession(ses, maxRcvdIdx, minRcvdIdx);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Unable to find session to move for readers.");
                    }
                }

                for (int i = 0; i < clientWorkers.size(); i++) {
                    GridNioServer.AbstractNioClientWorker worker = clientWorkers.get(i);

                    worker.reset0();
                }
            }
        }
    }

    /**
     *
     */
    private class SizeBasedBalancer implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private long lastBalance;

        /** */
        private final long balancePeriod;

        /**
         * @param balancePeriod Period.
         */
        SizeBasedBalancer(long balancePeriod) {
            this.balancePeriod = balancePeriod;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            long now = U.currentTimeMillis();

            if (lastBalance + balancePeriod < now) {
                lastBalance = now;

                long maxBytes0 = -1, minBytes0 = -1;
                int maxBytesIdx = -1, minBytesIdx = -1;

                for (int i = 0; i < clientWorkers.size(); i++) {
                    GridNioServer.AbstractNioClientWorker worker = clientWorkers.get(i);

                    int sesCnt = worker.workerSessions.size();

                    long bytes0 = worker.bytesRcvd0 + worker.bytesSent0;

                    if ((maxBytes0 == -1 || bytes0 > maxBytes0) && bytes0 > 0 && sesCnt > 1) {
                        maxBytes0 = bytes0;
                        maxBytesIdx = i;
                    }

                    if (minBytes0 == -1 || bytes0 < minBytes0) {
                        minBytes0 = bytes0;
                        minBytesIdx = i;
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Balancing data [min0=" + minBytes0 + ", minIdx=" + minBytesIdx +
                        ", max0=" + maxBytes0 + ", maxIdx=" + maxBytesIdx + ']');

                if (maxBytes0 != -1 && minBytes0 != -1) {
                    GridSelectorNioSessionImpl ses = null;

                    long bytesDiff = maxBytes0 - minBytes0;
                    long delta = bytesDiff;
                    double threshold = bytesDiff * 0.9;

                    GridConcurrentHashSet<GridSelectorNioSessionImpl> sessions =
                        clientWorkers.get(maxBytesIdx).workerSessions;

                    for (GridSelectorNioSessionImpl ses0 : sessions) {
                        long bytesSent0 = ses0.bytesSent0();

                        if (bytesSent0 < threshold &&
                            (ses == null || delta > U.safeAbs(bytesSent0 - bytesDiff / 2))) {
                            ses = ses0;
                            delta = U.safeAbs(bytesSent0 - bytesDiff / 2);
                        }
                    }

                    if (ses != null) {
                        if (log.isDebugEnabled())
                            log.debug("Will move session to less loaded worker [ses=" + ses +
                                ", from=" + maxBytesIdx + ", to=" + minBytesIdx + ']');

                        moveSession(ses, maxBytesIdx, minBytesIdx);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Unable to find session to move.");
                    }
                }

                for (int i = 0; i < clientWorkers.size(); i++) {
                    GridNioServer.AbstractNioClientWorker worker = clientWorkers.get(i);

                    worker.reset0();
                }
            }
        }
    }

    /**
     * For tests only.
     */
    @SuppressWarnings("unchecked")
    private class RandomBalancer implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void run() {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int w1 = rnd.nextInt(clientWorkers.size());

            if (clientWorkers.get(w1).workerSessions.isEmpty())
                return;

            int w2 = rnd.nextInt(clientWorkers.size());

            while (w2 == w1)
                w2 = rnd.nextInt(clientWorkers.size());

            GridNioSession ses = randomSession(clientWorkers.get(w1));

            if (ses != null) {
                log.info("Move session [from=" + w1 +
                    ", to=" + w2 +
                    ", ses=" + ses + ']');

                moveSession(ses, w1, w2);
            }
        }

        /**
         * @param worker Worker.
         * @return NIO session.
         */
        private GridNioSession randomSession(GridNioServer.AbstractNioClientWorker worker) {
            Collection<GridNioSession> sessions = worker.workerSessions;

            int size = sessions.size();

            if (size == 0)
                return null;

            int idx = ThreadLocalRandom.current().nextInt(size);

            Iterator<GridNioSession> it = sessions.iterator();

            int cnt = 0;

            while (it.hasNext()) {
                GridNioSession ses = it.next();

                if (cnt == idx)
                    return ses;
            }

            return null;
        }

    }

    /**
     *
     */
    interface SessionChangeRequest {
        /**
         * @return Session.
         */
        GridNioSession session();

        /**
         * @return Requested change operation.
         */
        NioOperation operation();
    }
}
