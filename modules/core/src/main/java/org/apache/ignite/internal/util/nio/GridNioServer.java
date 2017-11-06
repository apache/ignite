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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

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
@SuppressWarnings({"WeakerAccess", "TypeMayBeWeakened", "ForLoopReplaceableByForEach"})
public class GridNioServer<T> {
    /** */
    public static final String IGNITE_IO_BALANCE_RANDOM_BALANCE = "IGNITE_IO_BALANCE_RANDOM_BALANCER";

    /** Default session write timeout. */
    public static final int DFLT_SES_WRITE_TIMEOUT = 5000;

    /** Default send queue limit. */
    public static final int DFLT_SEND_QUEUE_LIMIT = 0;

    /** Time, which server will wait before retry operation. */
    static final long ERR_WAIT_TIME = 2000;

    /** Buffer metadata key. */
    static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL system data buffer metadata key. */
    static final int BUF_SSL_SYSTEM_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** SSL write buf limit. */
    static final int WRITE_BUF_LIMIT = GridNioSessionMetaKey.nextUniqueKey();

    /** Session future meta key. */
    public static final int RECOVERY_DESC_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Selection key meta key. */
    private static final int WORKER_IDX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    static final boolean DISABLE_KEYSET_OPTIMIZATION =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_NO_SELECTOR_OPTS);

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

    /** Accept worker thread. */
    @GridToStringExclude
    private final IgniteThread acceptThread;

    /** Read worker threads. */
    private final IgniteThread[] clientThreads;

    private final List<GridNioWorker> clientWorkers;

    /** Filter chain to use. */
    private final GridNioFilterChain<T> filterChain;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Closed flag. */
    private volatile boolean closed;

    /** Index to select which thread will serve next incoming socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int readBalanceIdx;

    /** Index to select which thread will serve next out socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int writeBalanceIdx = 1;

    /** Socket receive buffer. */
    private final int sockRcvBuf;

    /** Local address. */
    private final InetSocketAddress locAddr;

    /** Sessions. */
    private final Collection<GridSelectorNioSessionImpl> sessions = new GridConcurrentHashSet<>();

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
    private final boolean readWriteSelectorsAssign;

    /**
     * @param addr Address.
     * @param port Port.
     * @param log Log.
     * @param selectorCnt Count of selectors and selecting threads.
     * @param igniteInstanceName Ignite instance name.
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
        @Nullable String igniteInstanceName,
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
        long writeTimeout,
        long idleTimeout,
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
        this.sockRcvBuf = sockRcvBuf;
        this.msgQueueLsnr = msgQueueLsnr;
        this.readWriteSelectorsAssign = readWriteSelectorsAssign;

        if (port != -1) {
            // Once bind, we will not change the port in future.
            locAddr = new InetSocketAddress(addr, port);

            // fast fail in case the address is already in use.
            Selector selector = createSelector(locAddr);

            long balancePeriod = IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, 5000);

            IgniteRunnable balancer0 = null;

            if (balancePeriod > 0) {
                boolean rndBalance = IgniteSystemProperties.getBoolean(IGNITE_IO_BALANCE_RANDOM_BALANCE, false);

                if (rndBalance)
                    balancer0 = new RandomBalancer<>(this, readWriteSelectorsAssign, log);
                else {
                    balancer0 = readWriteSelectorsAssign ?
                        new ReadWriteSizeBasedBalancer<>(this, balancePeriod, log) :
                        new SizeBasedBalancer<>(this, balancePeriod, log);
                }
            }

            acceptThread = new IgniteThread(new GridNioAcceptWorker<>(this, igniteInstanceName,
                "nio-acceptor", locAddr, selector, balancer0, tcpNoDelay, sockSndBuf, sockRcvBuf, log));
        }
        else {
            locAddr = null;
            acceptThread = null;
        }

        GridNioSslFilter sslFilter = null;

        if (directMode) {
            for (GridNioFilter filter : filters) {
                if (filter instanceof GridNioSslFilter) {
                    sslFilter = (GridNioSslFilter)filter;

                    assert sslFilter.directMode();
                }
            }
        }

        filterChain = new GridNioFilterChain<>(log, lsnr, new HeadFilter<>(this, directMode, sslFilter != null), filters);

        clientWorkers = new ArrayList<>(selectorCnt);
        clientThreads = new IgniteThread[selectorCnt];

        for (int i = 0; i < selectorCnt; i++) {
            String threadName = "grid-nio-worker-" + srvName == null ? "" : (srvName + "-") + i;

            AbstractNioClientWorker worker;

            if (directMode) {
                workers().add(worker = DirectNioClientWorker.<T>builder()
                    .nio(this)
                    .idx(i)
                    .igniteInstanceName(igniteInstanceName)
                    .name(threadName)
                    .filterChain(filterChain)
                    .metricsLsnr(metricsLsnr)
                    .sslFilter(sslFilter)
                    .writerFactory(writerFactory)
                    .selectorSpins(selectorSpins)
                    .writeTimeout(writeTimeout)
                    .idleTimeout(idleTimeout)
                    .directBuf(directBuf)
                    .order(order)
                    .sndQueueLimit(sndQueueLimit)
                    .log(log)
                    .build());
            }
            else {
                workers().add(worker = ByteBufferNioClientWorker.<T>builder()
                    .nio(this)
                    .idx(i)
                    .igniteInstanceName(igniteInstanceName)
                    .name(threadName)
                    .filterChain(filterChain)
                    .metricsLsnr(metricsLsnr)
                    .selectorSpins(selectorSpins)
                    .writeTimeout(writeTimeout)
                    .idleTimeout(idleTimeout)
                    .directBuf(directBuf)
                    .order(order)
                    .sndQueueLimit(sndQueueLimit)
                    .log(log)
                    .build());
            }

            clientThreads[i] = new IgniteThread(worker);

            clientThreads[i].setDaemon(daemon);
        }

        this.skipRecoveryPred = skipRecoveryPred != null ? skipRecoveryPred : F.<Message>alwaysFalse();
    }

    /**
     * @return Number of reader sessions move.
     */
    public long readerMoveCount() {
        return readerMoveCnt.get();
    }

    /**
     * Increments reader sessions moving count.
     */
    void incrementReaderMovedCount() {
        readerMoveCnt.incrementAndGet();
    }

    /**
     * @return Number of reader writer move.
     */
    public long writerMoveCount() {
        return writerMoveCnt.get();
    }

    /**
     * Increments writer sessions moving count.
     */
    void incrementWriterMovedCount() {
        writerMoveCnt.incrementAndGet();
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

            List<AbstractNioClientWorker> workers = workers();

            U.cancel(workers);
            U.join(workers, log);

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
        assert ses instanceof GridSelectorNioSessionImpl : ses;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture<>(false);

        NioOperationFuture<Boolean> fut = new SessionCloseFuture(impl);

        if (log.isDebugEnabled())
            log.debug("Offered session change request [ses=" + impl + ", fut=" + fut + ']');

        impl.worker().offer(fut);

        return fut;
    }

    /**
     * For test purpose.
     * @param skipRead True if the worker should skip socket read operation.
     */
    public final void skipRead(boolean skipRead) {
        for (AbstractNioClientWorker worker : this.<AbstractNioClientWorker>workers())
            worker.skipRead(skipRead);
    }

    /**
     * For test purpose.
     * @param skipWrite True if the worker should skip socket write operation.
     */
    public final void skipWrite(boolean skipWrite) {
        for (AbstractNioClientWorker worker : this.<AbstractNioClientWorker>workers())
            worker.skipWrite(skipWrite);
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @param createFut {@code True} if future should be created.
     * @param ackC Closure invoked when message ACK is received.
     * @return Future for operation.
     */
    GridNioFuture<?> send(GridNioSession ses,
        ByteBuffer msg,
        boolean createFut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl : ses;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (createFut) {
            SessionWriteFuture fut = new SessionWriteFuture(impl, msg, ackC);

            send0(impl, fut);

            return fut;
        }
        else
            send0(impl, new SessionWriteRequestImpl(impl, msg, true, ackC));

        return null;
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @param createFut {@code True} if future should be created.
     * @param ackC Closure invoked when message ACK is received.
     * @return Future for operation.
     */
    GridNioFuture<?> send(GridNioSession ses,
        Message msg,
        boolean createFut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (createFut) {
            SessionWriteFuture fut = new SessionWriteFuture(impl, msg, skipRecoveryPred.apply(msg), ackC);

            send0(impl, fut);

            return fut;
        }
        else
            send0(impl, new SessionWriteRequestImpl(impl, msg, skipRecoveryPred.apply(msg), ackC));

        return null;
    }

    /**
     * @param ses Session.
     * @param req Request.
     * @throws IgniteCheckedException If session was closed.
     */
    private void send0(GridSelectorNioSessionImpl ses, SessionWriteRequest req) throws IgniteCheckedException {
        assert ses != null;
        assert req != null;

        if (ses.closed()) {
            IOException err = new IOException("Failed to send message (connection was closed): " + ses);

            req.onError(err);

            if (!(req instanceof GridNioFuture))
                throw new IgniteCheckedException(err);
        }
        else
            ses.add(req);

        if (msgQueueLsnr != null)
            msgQueueLsnr.apply(ses, ses.writeQueueSize());
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
            SessionWriteFuture fut = new SessionWriteFuture(impl, msg, skipRecoveryPred.apply(msg), true);

            fut.listen(lsnr);

            assert !fut.isDone();

            send0(impl, fut);
        }
        else
            send0(impl, new SessionWriteRequestImpl(impl, msg, true, true));
    }

    /**
     * @param ses Session.
     */
    public void resend(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridNioRecoveryDescriptor recoveryDesc = ses.outRecoveryDescriptor();

        if (recoveryDesc != null && !recoveryDesc.messagesRequests().isEmpty()) {
            Collection<SessionWriteRequest> futs = recoveryDesc.messagesRequests();

            if (log.isDebugEnabled())
                log.debug("Resend messages [rmtNode=" + recoveryDesc.node().id() + ", msgCnt=" + futs.size() + ']');

            GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

            for (SessionWriteRequest fut : futs) {
                fut.messageThread(true);

                fut.resetSession(ses0);
            }

            assert ses0.writeQueueSize() == 0 : ses0.writeQueueSize();

            ses0.add(futs);
        }
    }

    /**
     * @return Sessions.
     */
    public <R extends GridNioSession> Collection<R> sessions() {
        return (Collection<R>)sessions;
    }

    /**
     * @return Workers.
     */
    public <R extends GridNioWorker> List<R> workers() {
        return (List<R>)clientWorkers;
    }

    /**
     * @param ses Session.
     * @param from Move from index.
     * @param to Move to index.
     */
    void moveSession(GridNioSession ses, int from, int to) {
        assert from >= 0 && from < clientWorkers.size() : from;
        assert to >= 0 && to < clientWorkers.size() : to;
        assert from != to;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        GridNioWorker worker0 = impl.worker();

        if (clientWorkers.get(from) == worker0) {
            SessionChangeRequest fut = new SessionMoveFuture(impl, to);

            if (log.isDebugEnabled())
                log.debug("Offered session change request [ses=" + impl + ", fut=" + fut + ']');


            worker0.offer(fut);
        }
    }

    /**
     * @param ses Session.
     * @return Future for operation.
     */
    GridNioFuture<?> pauseReads(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture(new IOException("Failed to pause/resume reads " +
                "(connection was closed): " + ses));

        NioOperationFuture<?> fut = new PauseReadFuture(impl);

        if (log.isDebugEnabled())
            log.debug("Offered session change request [ses=" + impl + ", fut=" + fut + ']');

        impl.worker().offer(fut);

        return fut;
    }

    /**
     * @param ses Session.
     * @return Future for operation.
     */
    GridNioFuture<?> resumeReads(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        GridSelectorNioSessionImpl impl = (GridSelectorNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture(new IOException("Failed to pause/resume reads " +
                "(connection was closed): " + ses));

        NioOperationFuture<?> fut = new ResumeReadFuture(impl);

        if (log.isDebugEnabled())
            log.debug("Offered session change request [ses=" + impl + ", fut=" + fut + ']');

        impl.worker().offer(fut);

        return fut;
    }

    /**
     * @return Future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public IgniteInternalFuture<String> dumpStats() {
        String msg = "NIO server statistics [readerSesBalanceCnt=" + readerMoveCnt.get() +
            ", writerSesBalanceCnt=" + writerMoveCnt.get() + ']';

        return dumpStats(msg, null);
    }

    /**
     * @param msg Message to add.
     * @param p Session predicate.
     * @return Future.
     */
    public IgniteInternalFuture<String> dumpStats(final String msg, IgnitePredicate<GridNioSession> p) {
        GridCompoundFuture<String, String> fut = new GridCompoundFuture<>(new IgniteReducer<String, String>() {
            private final StringBuilder sb = new StringBuilder(msg);

            @Override public boolean collect(@Nullable String msg) {
                if (!F.isEmpty(msg)) {
                    synchronized (sb) {
                        if (sb.length() > 0)
                            sb.append(U.nl());

                        sb.append(msg);
                    }
                }

                return true;
            }

            @Override public String reduce() {
                synchronized (sb) {
                    return sb.toString();
                }
            }
        });

        for (int i = 0; i < clientWorkers.size(); i++) {
            NioOperationFuture<String> opFut = new DumpStatisticsFuture(p);

            clientWorkers.get(i).offer(opFut);

            fut.add(opFut);
        }

        fut.markInitialized();

        return fut;
    }

    /**
     * @param msg Message to add.
     * @param p Session predicate.
     * @return Future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public IgniteInternalFuture<String> dumpNodeStats(final String msg, IgnitePredicate<GridNioSession> p) {
        GridCompoundFuture<String, String> fut = new GridCompoundFuture<>(new IgniteReducer<String, String>() {
            private final StringBuilder sb = new StringBuilder(msg);

            @Override public boolean collect(@Nullable String msg) {
                if (!F.isEmpty(msg)) {
                    synchronized (sb) {
                        if (sb.length() > 0)
                            sb.append(U.nl());

                        sb.append(msg);
                    }
                }

                return true;
            }

            @Override public String reduce() {
                synchronized (sb) {
                    return sb.toString();
                }
            }
        });

        for (int i = 0; i < workers().size(); i++) {
            NioOperationFuture<String> opFut = new DumpStatisticsFuture(p);

            clientWorkers.get(i).offer(opFut);

            fut.add(opFut);
        }

        fut.markInitialized();

        return fut;
    }

    /**
     * Establishes a session.
     *
     * @param ch Channel to register within the server and create session for.
     * @param meta Optional meta for new session.
     * @param async Async connection.
     * @param lsnr Listener that should be invoked in NIO thread.
     * @return Future to get session.
     */
    public GridNioFuture<GridNioSession> createSession(
        final SocketChannel ch,
        @Nullable Map<Integer, Object> meta,
        boolean async,
        @Nullable IgniteInClosure<? super IgniteInternalFuture<GridNioSession>> lsnr
    ) {
        try {
            if (!closed) {
                ch.configureBlocking(false);

                ConnectionOperationFuture req;

                if (async) {
                    assert meta != null;

                    req = new ConnectionOperationFuture(ch, false, meta, NioOperation.CONNECT);
                }
                else
                    req = new ConnectionOperationFuture(ch, false, meta, NioOperation.REGISTER);

                if (lsnr != null)
                    req.listen(lsnr);

                offerBalanced(req, meta);

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
     * @param ch Channel.
     * @param meta Session meta.
     */
    public GridNioFuture<GridNioSession> cancelConnect(final SocketChannel ch, Map<Integer, ?> meta) {
        if (!closed) {
            NioOperationFuture<GridNioSession> req = new ConnectionOperationFuture(ch, false, meta, NioOperation.CANCEL_CONNECT);

            Integer idx = (Integer)meta.get(WORKER_IDX_META_KEY);

            assert idx != null : meta;

            clientWorkers.get(idx).offer(req);

            return req;
        }
        else
            return new GridNioFinishedFuture<>(
                new IgniteCheckedException("Failed to cancel connection, server is stopped."));
    }

    public boolean closed() {
        return closed;
    }

    /**
     * Creates selector and binds server socket to a given address and port. If address is null then will not bind any
     * address and just creates a selector.
     *
     * @param addr Local address to listen on.
     * @return Created selector.
     * @throws IgniteCheckedException If selector could not be created or port is already in use.
     */
    Selector createSelector(@Nullable SocketAddress addr) throws IgniteCheckedException {
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
     * @param meta Session metadata.
     */
    synchronized void offerBalanced(ConnectionOperationFuture req, @Nullable Map<Integer, Object> meta) {
        assert req.operation() == NioOperation.REGISTER || req.operation() == NioOperation.CONNECT : req;
        assert req.socketChannel() != null : req;

        int workers = workers().size();

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

        if (meta != null)
            meta.put(WORKER_IDX_META_KEY, balanceIdx);

        clientWorkers.get(balanceIdx).offer(req);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioServer.class, this);
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

        /** Ignite instance name. */
        private String igniteInstanceName;

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
        private long idleTimeout = ConnectorConfiguration.DFLT_IDLE_TIMEOUT;

        /** Write timeout. */
        private long writeTimeout = DFLT_SES_WRITE_TIMEOUT;

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
            return new GridNioServer<>(
                addr,
                port,
                log,
                selectorCnt,
                igniteInstanceName,
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
                writeTimeout,
                idleTimeout,
                metricsLsnr,
                writerFactory,
                skipRecoveryPred,
                msgQueueLsnr,
                readWriteSelectorsAssign,
                filters != null ? Arrays.copyOf(filters, filters.length) : EMPTY_FILTERS
            );
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
         * @param port Local port. If {@code -1} passed then server will not be accepting connections and only outgoing
         * connections will be possible.
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
         * @param igniteInstanceName Ignite instance name.
         * @return This for chaining.
         */
        public Builder<T> igniteInstanceName(@Nullable String igniteInstanceName) {
            this.igniteInstanceName = igniteInstanceName;

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
}
