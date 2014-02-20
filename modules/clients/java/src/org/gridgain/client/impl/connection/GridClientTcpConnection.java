// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl.connection;

import io.netty.bootstrap.*;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;
import io.netty.channel.socket.nio.*;
import io.netty.handler.codec.*;
import io.netty.handler.ssl.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import org.gridgain.client.*;
import org.gridgain.client.impl.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.gridgain.client.impl.connection.GridClientConnectionCloseReason.*;
import static org.gridgain.grid.kernal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.*;

/**
 * This class performs request to grid over tcp protocol. Serialization is performed with marshaller
 * provided.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridClientTcpConnection extends GridClientConnection {
    /** Logger */
    private static final Logger log = Logger.getLogger(GridClientTcpConnection.class.getName());

    /** Ping interval. */
    private final long pingInterval;

    /** Ping timeout. */
    private final long pingTimeout;

    /** Requests that are waiting for response. */
    private ConcurrentMap<Long, TcpClientFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Node by node id requests. Map for reducing server load. */
    private ConcurrentMap<UUID, TcpClientFuture> refreshNodeReqs = new ConcurrentHashMap<>();

    /** Latch indicating pending are empty and connection could be terminated. */
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    /** Reason why connection was closed. {@code null} means connection is still alive. */
    private volatile GridClientConnectionCloseReason closeReason;

    /** Request ID counter. */
    private AtomicLong reqIdCntr = new AtomicLong(1);

    /** Message marshaller */
    private GridClientMarshaller marsh;

    /** Communication channel. */
    private final Channel ch;

    /** Timestamp of last send message. */
    private volatile long lastMsgSndTime;

    /** Timestamp of last received message. */
    private volatile long lastMsgRcvTime;

    /** Connection create timestamp. */
    private long createTs;

    /** Session token. */
    private volatile byte[] sesTok;

    /** Timer to run ping checks. */
    private ScheduledFuture<?> pingTask;

    /**
     * Creates a client facade, tries to connect to remote server, in case of success starts reader thread.
     *
     * @param clientId Client identifier.
     * @param srvAddr Server to connect to.
     * @param sslCtx SSL context to use if SSL is enabled, {@code null} otherwise.
     * @param grp Event group.
     * @param connectTimeout Connect timeout.
     * @param pingInterval Ping interval.
     * @param pingTimeout Ping timeout.
     * @param tcpNoDelay TCP_NODELAY flag for outgoing socket connection.
     * @param marsh Marshaller to use in communication.
     * @param top Topology instance.
     * @param cred Client credentials.      @throws IOException If connection could not be established.
     * @param protoId Custom protocol ID, if marshaller is not defined.
     * @throws IOException If IO error occurs.
     * @throws InterruptedException If connection was interrupted.
     * @throws GridClientException If handshake error occurs.
     */
    GridClientTcpConnection(UUID clientId, InetSocketAddress srvAddr, final SSLContext sslCtx, EventLoopGroup grp,
        int connectTimeout, long pingInterval, long pingTimeout, boolean tcpNoDelay, final GridClientMarshaller marsh,
        GridClientTopology top, Object cred, Byte protoId)
        throws IOException, InterruptedException, GridClientException {
        super(clientId, srvAddr, sslCtx, top, cred);

        assert marsh != null || protoId != null;

        this.marsh = marsh;

        final HandshakeHandler handshakeHnd = new HandshakeHandler(marsh != null ? marsh.getProtocolId() : protoId);

        Bootstrap b = new Bootstrap().group(grp).
            channel(NioSocketChannel.class).
            option(ChannelOption.TCP_NODELAY, tcpNoDelay).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout).
            handler(new ChannelInitializer<SocketChannel>() {
                @Override protected void initChannel(SocketChannel ch) throws Exception {
                    if (sslCtx != null) {
                        SSLEngine sslEngine = sslCtx.createSSLEngine();
                        sslEngine.setUseClientMode(true);

                        ch.pipeline().addFirst("Ssl", new SslHandler(sslEngine));
                    }

                    // Configure pipeline to handle handshake,
                    // stable configuration will be set after initialization succeeded.
                    ch.pipeline().addLast(
                        new FixedLengthFrameDecoder(1),
                        new HandshakeDecoder(),
                        new HandshakeEncoder(),
                        handshakeHnd);
                }
            });

        ChannelFuture connectFut = b.connect(srvAddr);

        if (connectFut.await().isSuccess()) {
            ch = connectFut.channel();

            ChannelHandler first = ch.pipeline().first();

            if (first instanceof SslHandler) {// Ssl is used.
                Future handshake = ((SslHandler)first).handshakeFuture();

                if (!handshake.await().isSuccess()) { // Do not return until ssl session established.
                    close(FAILED, false);

                    throw new IOException("Failed to perform ssl handshake.", handshake.cause());
                }
            }

            ChannelFuture hsFut = handshakeHnd.handshakeFuture();

            if (connectTimeout > 0) {
                if (!hsFut.await(connectTimeout, TimeUnit.MILLISECONDS))
                    throw new GridClientException("Failed to await protocol handshake for timeout: " + connectTimeout);
                else if (!hsFut.isSuccess())
                    throw new GridClientException(hsFut.cause());
            }
            else if (!hsFut.await().isSuccess())
                throw new GridClientException(hsFut.cause());

            assert hsFut.isSuccess();

            ChannelPipeline pipe = ch.pipeline();

            // Rebuild pipeline to initialized configuration.
            pipe.remove(FixedLengthFrameDecoder.class);
            pipe.remove(HandshakeEncoder.class);
            pipe.remove(HandshakeDecoder.class);
            pipe.remove(handshakeHnd);

            pipe.addLast(
                new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, 0, 0),
                new GridHeaderPrepender(),
                new RestDecoder(),
                new RestEncoder(),
                new PingHandler(),
                new ConnectionHandler());
        }
        else {
            close(FAILED, false);

            if (connectFut.cause() instanceof IOException)
                throw ((IOException)connectFut.cause());
            else
                throw new IOException("Failed to establish connection.", connectFut.cause());
        }

        if (log.isLoggable(Level.INFO))
            log.info("Client TCP connection established: " + serverAddress());

        this.pingInterval = pingInterval;
        this.pingTimeout = pingTimeout;

        // Triggers ping checks in PingHandler.
        // Doing it frequently - extra messages will be dropped.
        pingTask = ch.eventLoop().scheduleAtFixedRate(
            new Runnable() {
                @Override public void run() {
                    ch.writeAndFlush(GridClientPingPacket.PING_MESSAGE);
                }
            },
            100, 100, TimeUnit.MILLISECONDS);

        createTs = System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override void close(GridClientConnectionCloseReason reason, boolean waitCompletion) {
        close(reason, waitCompletion, null);
    }

    /**
     * Closes connection facade.
     *
     * @param reason Why this connection should be closed.
     * @param waitCompletion If {@code true} this method will wait for all pending requests to be completed.
     * @param cause The cause of connection close, or {@code null} if it is an ordinal close.
     */
    private void close(GridClientConnectionCloseReason reason, boolean waitCompletion, @Nullable Throwable cause) {
        synchronized (this) {
            if (closeReason != null)
                return;

            closeReason = reason;
        }

        try {
            // Wait for all pending requests to be processed.
            if (waitCompletion && !pendingReqs.isEmpty() && ch.isOpen())
                closedLatch.await();
        }
        catch (InterruptedException ignored) {
            log.warning("Interrupted while waiting for all requests to be processed (all pending " +
                "requests will be failed): " + serverAddress());

            Thread.currentThread().interrupt();
        }

        if (pingTask != null)
            pingTask.cancel(false);

        if (ch != null)
            ch.close(); // Async close.

        for (Iterator<TcpClientFuture> it = pendingReqs.values().iterator(); it.hasNext(); ) {
            GridClientFutureAdapter fut = it.next();

            fut.onDone(getCloseReasonAsException(closeReason, cause));

            it.remove();
        }

        if (log.isLoggable(Level.INFO))
            log.info("Client TCP connection closed: " + serverAddress());
    }

    /**
     * Closes client only if there are no pending requests in map.
     *
     * @return {@code True} if client was closed.
     */
    @Override boolean closeIfIdle(long idleTimeout) {
        if (closeReason != null)
            return true;

        // Timestamp of the last sent or received message.
        long lastMsgTime = Math.max(Math.max(lastMsgSndTime, lastMsgRcvTime), createTs);

        if (lastMsgTime + idleTimeout < System.currentTimeMillis() && pendingReqs.isEmpty()) {
            // In case of new request came between empty check and setting closing flag
            // await for finishing all requests.
            close(CONN_IDLE, true);

            return true;
        }

        return false;
    }

    /**
     * Makes request to server via tcp protocol and returns a future that will be completed when
     * response is received.
     *
     * @param msg Message to request,
     * @param destId Destination node identifier.
     * @return Response object.
     * @throws GridClientConnectionResetException If request failed.
     * @throws GridClientClosedException If client was closed.
     */
    private <R> GridClientFutureAdapter<R> makeRequest(GridClientMessage msg, UUID destId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert msg != null;

        TcpClientFuture<R> res = new TcpClientFuture<>();

        msg.destinationId(destId);

        return makeRequest(msg, res);
    }

    /**
     * Makes request to server via tcp protocol and returns a future that will be completed when response is received.
     *
     * @param msg Message to request,
     * @param fut Future that will handle response.
     * @return Response object.
     * @throws GridClientConnectionResetException If request failed.
     * @throws GridClientClosedException If client was closed.
     */
    private <R> GridClientFutureAdapter<R> makeRequest(GridClientMessage msg, TcpClientFuture<R> fut)
        throws GridClientConnectionResetException, GridClientClosedException {
        try {
            return makeRequest(msg, fut, false);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return new GridClientFutureAdapter<>(
                new GridClientException("Interrupted when trying to perform request message.", e));
        }
    }

    /**
     * Makes request to server via tcp protocol and returns a future that will be completed when response is received.
     *
     * @param msg Message to request,
     * @param fut Future that will handle response.
     * @param routeMode If {@code true} then this method should overwrite session token by the cached one,
     *     otherwise keep original value.
     * @return Response object.
     * @throws GridClientConnectionResetException If request failed.
     * @throws GridClientClosedException If client was closed.
     * @throws InterruptedException Thread was interrupted while sending message or establishing a new connection.
     */
    private <R> GridClientFutureAdapter<R> makeRequest(GridClientMessage msg, final TcpClientFuture<R> fut,
        boolean routeMode) throws GridClientConnectionResetException, GridClientClosedException, InterruptedException {
        assert msg != null;

        long reqId = reqIdCntr.getAndIncrement();

        msg.requestId(reqId);

        if (!routeMode) {
            msg.clientId(clientId);
            msg.sessionToken(sesTok);
        }

        fut.pendingMessage(msg);

        checkClosed(closeReason);

        GridClientFutureAdapter old = pendingReqs.putIfAbsent(reqId, fut);

        assert old == null;

        ChannelFuture write = ch.write(msg);

        if (routeMode) {
            write.addListener(new ChannelFutureListener() {
                @Override public void operationComplete(ChannelFuture chFut) throws Exception {
                    if (!chFut.isSuccess()) {
                        close(FAILED, false, chFut.cause());

                        fut.onDone(getCloseReasonAsException(FAILED, chFut.cause()));
                    }
                }
            });
        }
        else if (!write.await().isSuccess())
            throw new GridClientConnectionResetException("Failed to send message over connection " +
                "(will try to reconnect): " + serverAddress(), write.cause());

        return fut;
    }

    /**
     * Handles incoming response message. If this connection is closed this method would signal empty event
     * if there is no more pending requests.
     *
     * @param msg Incoming response message.
     */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    private void handleResponse(GridClientMessage msg) {
        TcpClientFuture fut = pendingReqs.get(msg.requestId());

        if (fut == null) {
            log.warning("Response for an unknown request is received, ignoring. Message: " + msg);

            return;
        }

        if (msg instanceof GridRouterResponse) {
            removePending(msg.requestId());

            fut.onDone(msg);
        }
        else if (msg instanceof GridClientResponse)
            handleClientResponse(fut, (GridClientResponse)msg);
        else
            log.warning("Unsupported response type received: " + msg);
    }

    /**
     * Handles client handshake response.
     *
     * @param msg A handshake response.
     * @throws GridClientHandshakeException If handshake failed.
     */
    private void handleHandshakeResponse(GridClientHandshakeResponse msg) throws GridClientHandshakeException {
        byte rc = msg.resultCode();

        if (rc != GridClientHandshakeResponse.OK.resultCode()) {
            if (rc == GridClientHandshakeResponse.ERR_VERSION_CHECK_FAILED.resultCode())
                throw new GridClientHandshakeException(rc, "Illegal client version (see server log for more details).");
            else if (rc == GridClientHandshakeResponse.ERR_UNKNOWN_PROTO_ID.resultCode())
                throw new GridClientHandshakeException(rc, "Unknown/unsupported protocol ID.");
            else
                throw new GridClientHandshakeException(rc,
                    "Handshake failed due to internal error (see server log for more details).");
        }
    }

    /**
     * Handler responses addressed to this client.
     *
     * @param fut Response future.
     * @param resp Response.
     */
    @SuppressWarnings("unchecked")
    private void handleClientResponse(TcpClientFuture fut, GridClientResponse resp) {
        if (resp.sessionToken() != null)
            sesTok = resp.sessionToken();

        GridClientMessage src = fut.pendingMessage();

        switch (fut.retryState()) {
            case TcpClientFuture.STATE_INITIAL: {
                if (resp.successStatus() == GridClientResponse.STATUS_AUTH_FAILURE) {
                    fut.retryState(TcpClientFuture.STATE_AUTH_RETRY);

                    GridClientAuthenticationRequest req = buildAuthRequest();

                    req.requestId(resp.requestId());

                    ch.write(req);

                    return;
                }

                break;
            }

            case TcpClientFuture.STATE_AUTH_RETRY: {
                if (resp.successStatus() == GridClientResponse.STATUS_SUCCESS) {
                    fut.retryState(TcpClientFuture.STATE_REQUEST_RETRY);

                    src.sessionToken(sesTok);

                    ch.write(src);

                    return;
                }

                break;
            }
        }

        removePending(resp.requestId());

        if (resp.successStatus() == GridClientResponse.STATUS_AUTH_FAILURE)
            fut.onDone(new GridClientAuthenticationException("Client authentication failed [clientId=" + clientId +
                ", srvAddr=" + serverAddress() + ", errMsg=" + resp.errorMessage() +']'));
        else if (resp.errorMessage() != null)
            fut.onDone(new GridClientException(resp.errorMessage()));
        else
            fut.onDone(resp.result());
    }

    /**
     * Removes pending request and signals to {@link #closedLatch} if necessary.
     *
     * @param reqId Request Id.
     */
    private void removePending(long reqId) {
        pendingReqs.remove(reqId);

        if (pendingReqs.isEmpty() && closeReason != null)
            closedLatch.countDown();
    }

    /**
     * Builds authentication request message with credentials taken from credentials object.
     *
     * @return AuthenticationRequest message.
     */
    private GridClientAuthenticationRequest buildAuthRequest() {
        GridClientAuthenticationRequest req = new GridClientAuthenticationRequest();

        req.clientId(clientId);

        req.credentials(credentials());

        return req;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cachePutAll(String cacheName, Map<K, V> entries,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert entries != null;

        GridClientCacheRequest<K, V> req = new GridClientCacheRequest<>(PUT_ALL);

        req.cacheName(cacheName);
        req.values(entries);
        req.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(req, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Map<K, V>> cacheGetAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert keys != null;

        GridClientCacheRequest<K, V> req = new GridClientCacheRequest<>(GET_ALL);

        req.cacheName(cacheName);
        req.keys(new HashSet<>(keys));
        req.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(req, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFutureAdapter<Boolean> cacheRemove(String cacheName, K key, Set<GridClientCacheFlag> flags,
        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
        GridClientCacheRequest<K, Object> req = new GridClientCacheRequest<>(RMV);

        req.cacheName(cacheName);
        req.key(key);
        req.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(req, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFutureAdapter<Boolean> cacheRemoveAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert keys != null;

        GridClientCacheRequest<K, Object> req = new GridClientCacheRequest<>(RMV_ALL);

        req.cacheName(cacheName);
        req.keys(new HashSet<>(keys));
        req.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(req, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheReplace(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest<K, V> replace = new GridClientCacheRequest<>(REPLACE);

        replace.cacheName(cacheName);
        replace.key(key);
        replace.value(val);
        replace.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(replace, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheCompareAndSet(String cacheName, K key, V newVal, V oldVal,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;

        GridClientCacheRequest<K, V> msg = new GridClientCacheRequest<>(CAS);

        msg.cacheName(cacheName);
        msg.key(key);
        msg.value(newVal);
        msg.value2(oldVal);
        msg.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(msg, destNodeId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K> GridClientFutureAdapter<GridClientDataMetrics> cacheMetrics(String cacheName, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientCacheRequest<K, Object> metrics = new GridClientCacheRequest<>(METRICS);

        metrics.cacheName(cacheName);
        metrics.destinationId(destNodeId);

        TcpClientFuture fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                super.onDone(metricsMapToMetrics((Map<String, Number>)res));
            }
        };

        return makeRequest(metrics, fut);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheAppend(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest<K, Object> append = new GridClientCacheRequest<>(APPEND);

        append.cacheName(cacheName);
        append.key(key);
        append.value(val);
        append.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(append, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cachePrepend(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest<K, Object> prepend = new GridClientCacheRequest<>(PREPEND);

        prepend.cacheName(cacheName);
        prepend.key(key);
        prepend.value(val);
        prepend.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(prepend, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <R> GridClientFutureAdapter<R> execute(String taskName, Object arg, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientTaskRequest msg = new GridClientTaskRequest();

        msg.taskName(taskName);
        msg.argument(arg);

        return this.<GridClientTaskResultBean>makeRequest(msg, destNodeId).chain(
            new GridClientFutureCallback<GridClientTaskResultBean, R>() {
            @Override public R onComplete(GridClientFuture<GridClientTaskResultBean> fut) throws GridClientException {
                return fut.get().getResult();
            }
        });
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridClientFuture<GridClientNode> node(final UUID id, boolean inclAttrs, boolean inclMetrics,
        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
        assert id != null;

        TcpClientFuture fut = refreshNodeReqs.get(id);

        // Return request that is in progress.
        if (fut != null)
            return fut;

        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                //Clean up the node id requests map.
                refreshNodeReqs.remove(id);

                GridClientNodeImpl node = nodeBeanToNode((GridClientNodeBean)res);

                if (node != null)
                    top.updateNode(node);

                super.onDone(node);
            }
        };

        GridClientFutureAdapter old = refreshNodeReqs.putIfAbsent(id, fut);

        // If concurrent thread put request, do not send the message.
        if (old != null)
            return old;

        msg.nodeId(id);
        msg.includeAttributes(inclAttrs);
        msg.includeMetrics(inclMetrics);
        msg.destinationId(destNodeId);

        return makeRequest(msg, fut);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridClientFuture<GridClientNode> node(String ipAddr, boolean inclAttrs, boolean includeMetrics,
        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {

        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        TcpClientFuture fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                GridClientNodeImpl node = nodeBeanToNode((GridClientNodeBean)res);

                if (node != null)
                    super.onDone(top.updateNode(node));
                else
                    super.onDone(node);
            }
        };

        msg.nodeIp(ipAddr);
        msg.includeAttributes(inclAttrs);
        msg.includeMetrics(includeMetrics);
        msg.destinationId(destNodeId);

        return makeRequest(msg, fut);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridClientFuture<List<GridClientNode>> topology(boolean inclAttrs, boolean inclMetrics,
        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        TcpClientFuture fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                Collection<GridClientNodeBean> beans = (Collection<GridClientNodeBean>)res;

                Collection<GridClientNodeImpl> nodes = new ArrayList<>(beans.size());

                for (GridClientNodeBean bean : beans)
                    nodes.add(nodeBeanToNode(bean));

                super.onDone(top.updateTopology(nodes));
            }
        };

        msg.includeAttributes(inclAttrs);
        msg.includeMetrics(inclMetrics);
        msg.destinationId(destNodeId);

        return makeRequest(msg, fut);
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<String>> log(String path, int fromLine, int toLine, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientLogRequest msg = new GridClientLogRequest();

        msg.from(fromLine);
        msg.to(toLine);
        msg.path(path);

        return makeRequest(msg, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public GridClientFutureAdapter<GridRouterRequest> forwardMessage(Object msg)
        throws GridClientException {
        assert msg instanceof GridRouterRequest;

        TcpClientFuture<GridRouterRequest> res = new TcpClientFuture<>(true);

        try {
            makeRequest((GridClientMessage)msg, res, true);

            return res;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridClientException("Synchronous IO happened in routing mode and was interrupted.", e);
        }
    }

    /**
     * Creates client node instance from message.
     *
     * @param nodeBean Node bean message.
     * @return Created node.
     */
    @Nullable private GridClientNodeImpl nodeBeanToNode(@Nullable GridClientNodeBean nodeBean) {
        if (nodeBean == null)
            return null;

        GridClientNodeImpl.Builder nodeBuilder = GridClientNodeImpl.builder()
            .nodeId(nodeBean.getNodeId())
            .consistentId(nodeBean.getConsistentId())
            .tcpAddresses(nodeBean.getTcpAddresses())
            .jettyAddresses(nodeBean.getJettyAddresses())
            .tcpPort(nodeBean.getTcpPort())
            .httpPort(nodeBean.getJettyPort())
            .replicaCount(nodeBean.getReplicaCount());

        Map<String, GridClientCacheMode> caches = new HashMap<>();

        if (nodeBean.getCaches() != null) {
            for (Map.Entry<String, String> e : nodeBean.getCaches().entrySet()) {
                try {
                    caches.put(e.getKey(), GridClientCacheMode.valueOf(e.getValue()));
                }
                catch (IllegalArgumentException ignored) {
                    log.warning("Invalid cache mode received from remote node (will ignore) [srv=" + serverAddress() +
                        ", cacheName=" + e.getKey() + ", cacheMode=" + e.getValue() + ']');
                }
            }
        }

        if (nodeBean.getDefaultCacheMode() != null) {
            try {
                caches.put(null, GridClientCacheMode.valueOf(nodeBean.getDefaultCacheMode()));
            }
            catch (IllegalArgumentException ignored) {
                log.warning("Invalid cache mode received for default cache from remote node (will ignore) [srv="
                    + serverAddress() + ", cacheMode=" + nodeBean.getDefaultCacheMode() + ']');
            }
        }

        if (!caches.isEmpty())
            nodeBuilder.caches(caches);

        if (nodeBean.getAttributes() != null)
            nodeBuilder.attributes(nodeBean.getAttributes());

        GridClientNodeMetricsBean metricsBean = nodeBean.getMetrics();

        if (metricsBean != null) {
            GridClientNodeMetricsAdapter metrics = new GridClientNodeMetricsAdapter();

            metrics.setStartTime(metricsBean.getStartTime());
            metrics.setAverageActiveJobs(metricsBean.getAverageActiveJobs());
            metrics.setAverageCancelledJobs(metricsBean.getAverageCancelledJobs());
            metrics.setAverageCpuLoad(metricsBean.getAverageCpuLoad());
            metrics.setAverageJobExecuteTime(metricsBean.getAverageJobExecuteTime());
            metrics.setAverageJobWaitTime(metricsBean.getAverageJobWaitTime());
            metrics.setAverageRejectedJobs(metricsBean.getAverageRejectedJobs());
            metrics.setAverageWaitingJobs(metricsBean.getAverageWaitingJobs());
            metrics.setCurrentActiveJobs(metricsBean.getCurrentActiveJobs());
            metrics.setCurrentCancelledJobs(metricsBean.getCurrentCancelledJobs());
            metrics.setCurrentCpuLoad(metricsBean.getCurrentCpuLoad());
            metrics.setCurrentGcCpuLoad(metricsBean.getCurrentGcCpuLoad());
            metrics.setCurrentDaemonThreadCount(metricsBean.getCurrentDaemonThreadCount());
            metrics.setCurrentIdleTime(metricsBean.getCurrentIdleTime());
            metrics.setCurrentJobExecuteTime(metricsBean.getCurrentJobExecuteTime());
            metrics.setCurrentJobWaitTime(metricsBean.getCurrentJobWaitTime());
            metrics.setCurrentRejectedJobs(metricsBean.getCurrentRejectedJobs());
            metrics.setCurrentThreadCount(metricsBean.getCurrentThreadCount());
            metrics.setCurrentWaitingJobs(metricsBean.getCurrentWaitingJobs());
            metrics.setFileSystemFreeSpace(metricsBean.getFileSystemFreeSpace());
            metrics.setFileSystemTotalSpace(metricsBean.getFileSystemTotalSpace());
            metrics.setFileSystemUsableSpace(metricsBean.getFileSystemUsableSpace());
            metrics.setHeapMemoryCommitted(metricsBean.getHeapMemoryCommitted());
            metrics.setHeapMemoryInitialized(metricsBean.getHeapMemoryInitialized());
            metrics.setHeapMemoryMaximum(metricsBean.getHeapMemoryMaximum());
            metrics.setHeapMemoryUsed(metricsBean.getHeapMemoryUsed());
            metrics.setLastDataVersion(metricsBean.getLastDataVersion());
            metrics.setLastUpdateTime(metricsBean.getLastUpdateTime());
            metrics.setMaximumActiveJobs(metricsBean.getMaximumActiveJobs());
            metrics.setMaximumCancelledJobs(metricsBean.getMaximumCancelledJobs());
            metrics.setMaximumJobExecuteTime(metricsBean.getMaximumJobExecuteTime());
            metrics.setMaximumJobWaitTime(metricsBean.getMaximumJobWaitTime());
            metrics.setMaximumRejectedJobs(metricsBean.getMaximumRejectedJobs());
            metrics.setMaximumThreadCount(metricsBean.getMaximumThreadCount());
            metrics.setMaximumWaitingJobs(metricsBean.getMaximumWaitingJobs());
            metrics.setNodeStartTime(metricsBean.getNodeStartTime());
            metrics.setNonHeapMemoryCommitted(metricsBean.getNonHeapMemoryCommitted());
            metrics.setNonHeapMemoryInitialized(metricsBean.getNonHeapMemoryInitialized());
            metrics.setNonHeapMemoryMaximum(metricsBean.getNonHeapMemoryMaximum());
            metrics.setNonHeapMemoryUsed(metricsBean.getNonHeapMemoryUsed());
            metrics.setStartTime(metricsBean.getStartTime());
            metrics.setTotalCancelledJobs(metricsBean.getTotalCancelledJobs());
            metrics.setTotalCpus(metricsBean.getTotalCpus());
            metrics.setTotalExecutedJobs(metricsBean.getTotalExecutedJobs());
            metrics.setTotalIdleTime(metricsBean.getTotalIdleTime());
            metrics.setTotalRejectedJobs(metricsBean.getTotalRejectedJobs());
            metrics.setTotalStartedThreadCount(metricsBean.getTotalStartedThreadCount());
            metrics.setTotalExecutedTasks(metricsBean.getTotalExecutedTasks());
            metrics.setSentMessagesCount(metricsBean.getSentMessagesCount());
            metrics.setSentBytesCount(metricsBean.getSentBytesCount());
            metrics.setReceivedMessagesCount(metricsBean.getReceivedMessagesCount());
            metrics.setReceivedBytesCount(metricsBean.getReceivedBytesCount());
            metrics.setUpTime(metricsBean.getUpTime());

            nodeBuilder.metrics(metrics);
        }

        return nodeBuilder.build();
    }

    /**
     * Future extension that holds client tcp message and auth retry flag.
     */
    private static class TcpClientFuture<R> extends GridClientFutureAdapter<R> {
        /** Initial request. */
        private static final int STATE_INITIAL = 0;

        /** Authentication retry. */
        private static final int STATE_AUTH_RETRY = 1;

        /** Request retry after auth retry. */
        private static final int STATE_REQUEST_RETRY = 2;

        /** Flag indicating if connected message is a forwarded. */
        private final boolean forward;

        /** Pending message for this future. */
        private GridClientMessage pendingMsg;

        /** Flag indicating whether authentication retry was attempted for this request. */
        @SuppressWarnings("RedundantFieldInitialization")
        private int authRetry = STATE_INITIAL;

        /**
         * Creates new future with {@code forward} flag set to {@code false}.
         */
        private TcpClientFuture() {
            forward = false;
        }

        /**
         * Creates new future with the given {@code forward} flag value.
         *
         * @param forward Flag value.
         */
        private TcpClientFuture(boolean forward) {
            this.forward = forward;
        }

        /**
         * @return Originating request message.
         */
        public GridClientMessage pendingMessage() {
            return pendingMsg;
        }

        /**
         * @param pendingMsg Originating request message.
         */
        public void pendingMessage(GridClientMessage pendingMsg) {
            this.pendingMsg = pendingMsg;
        }

        /**
         * @return Whether or not authentication retry attempted.
         */
        public int retryState() {
            return authRetry;
        }

        /**
         * @param authRetry Whether or not authentication retry attempted.
         */
        public void retryState(int authRetry) {
            this.authRetry = authRetry;
        }

        /**
         * @return {@code true} if this future created for forwarded message, {@code false} otherwise.
         */
        public boolean forward() {
            return forward;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TcpClientFuture [state=" + authRetry + ", forward=" + forward + ", message=" + pendingMsg + "]";
        }
    }

    /**
     * Prepends gridgain header and length to downstream buffers.
     */
    private static class GridHeaderPrepender extends MessageToMessageEncoder<ByteBuf> {
        /** {@inheritDoc} */
        @Override protected void encode(ChannelHandlerContext ctx, ByteBuf body, List<Object> out) throws Exception {
            int len = body.readableBytes();

            ByteBuf hdr = ctx.alloc().buffer(5);

            hdr.writeByte((byte)0x90);
            hdr.writeInt(len);

            out.add(Unpooled.wrappedBuffer(hdr, body));
        }
    }

    /**
     * Tcp Rest protocol decoder.
     */
    private class RestDecoder extends MessageToMessageDecoder<ByteBuf> {
        /** {@inheritDoc} */
        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
            assert buf.readableBytes() >= 5;

            byte tag = buf.readByte();

            if (tag != (byte)0x90) {
                if (log.isLoggable(Level.FINE))
                    log.fine("Failed to parse incoming message (unexpected header received, will close) " +
                        "[srvAddr=" + serverAddress() + ", symbol=" + Integer.toHexString(tag & 0xFF));

                throw new IOException("Unexpected header value: " + Integer.toHexString(tag & 0xFF));
            }

            if (buf.readableBytes() == 4) // 0 length was decoded.
                out.add(GridClientPingPacket.PING_MESSAGE);
            else {
                buf.skipBytes(4); // Skip length.

                long reqId = buf.readLong();
                UUID clientId = readUuid(buf);
                UUID destId = readUuid(buf);

                TcpClientFuture fut = pendingReqs.get(reqId);

                if (fut != null && fut.forward()) {
                    byte[] body = new byte[buf.readableBytes()];

                    buf.readBytes(body);

                    out.add(new GridRouterResponse(body, reqId, clientId, destId));
                }
                else {
                    byte[] packet = new byte[buf.readableBytes()];

                    buf.readBytes(packet);

                    GridClientMessage clMsg = marsh.unmarshal(packet);

                    clMsg.requestId(reqId);
                    clMsg.clientId(clientId);
                    clMsg.destinationId(clientId);

                    out.add(clMsg);
                }
            }
        }

        /**
         * Reads an UUID from the current read position in the given buffer.
         * @param buf Buffer to read from.
         * @return UUID.
         * @throws IndexOutOfBoundsException If not enough bytes available.
         */
        private UUID readUuid(ByteBuf buf) {
            long most = buf.readLong();
            long least = buf.readLong();
            return new UUID(most, least);
        }
    }

    /**
     * Encodes messages using gridgain protobuf protocol.
     */
    private class RestEncoder extends MessageToMessageEncoder<GridClientMessage> {
        /**
         * Sets proper message type.
         */
        private RestEncoder() {
            super(GridClientMessage.class);
        }

        /** {@inheritDoc} */
        @Override protected void encode(ChannelHandlerContext ctx, GridClientMessage msg, List<Object> out)
            throws Exception {
            if (msg instanceof GridClientPingPacket)
                out.add(Unpooled.EMPTY_BUFFER);
            else {
                ByteBuf body = msg instanceof GridRouterRequest ?
                    Unpooled.wrappedBuffer(((GridRouterRequest)msg).body()) :
                    Unpooled.wrappedBuffer(marsh.marshal(msg));

                ByteBuf hdr = ctx.alloc().buffer(40);

                hdr.writeLong(msg.requestId());
                hdr.writeLong(msg.clientId().getMostSignificantBits());
                hdr.writeLong(msg.clientId().getLeastSignificantBits());

                if (msg.destinationId() != null) {
                    hdr.writeLong(msg.destinationId().getMostSignificantBits());
                    hdr.writeLong(msg.destinationId().getLeastSignificantBits());
                }
                else {
                    hdr.writeLong(0);
                    hdr.writeLong(0);
                }

                out.add(Unpooled.wrappedBuffer(hdr, body).retain());
            }
        }
    }

    /**
     * Intercepts ping messages, moving through pipeline and handles them.
     */
    private class PingHandler extends ChannelDuplexHandler {
        /**
         * Ping receive time.
         * {@code 0} until first ping send and {@link Long#MAX_VALUE} while response isn't received.
         */
        private long lastPingRcvTime;

        /** Ping send time. */
        private long lastPingSndTime;

        /** {@inheritDoc} */
        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof GridClientPingPacket)
                lastPingRcvTime = U.currentTimeMillis();
            else
                ctx.fireChannelRead(msg);
        }

        /** {@inheritDoc} */
        @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof GridClientPingPacket) {
                long now = U.currentTimeMillis();

                if (Math.min(now, lastPingRcvTime) - lastPingSndTime >= pingTimeout)
                    throw new IOException("Did not receive any packets within ping response interval (connection is " +
                        "considered to be half-opened) [lastPingReceiveTime=" + lastPingRcvTime +
                        ", lastPingSendTime=" + lastPingSndTime + ", now=" + now + ", timeout=" + pingTimeout +
                        ", addr=" + serverAddress() + ']');

                // Do not pass ping requests if ping interval didn't pass yet
                // or we've already waiting for ping response.
                if (now - lastPingSndTime > pingInterval && lastPingRcvTime != Long.MAX_VALUE) {
                    lastPingRcvTime = Long.MAX_VALUE;

                    ctx.writeAndFlush(msg, promise);

                    lastPingSndTime = now;
                }
            }
            else
                ctx.writeAndFlush(msg, promise);
        }
    }

    /**
     * Top level handler for all channel events. It's responsible for channel-connection interactions.
     */
    private class ConnectionHandler extends ChannelDuplexHandler {
        /** {@inheritDoc} */
        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            // Pings should be intercepted below, so count everything.
            lastMsgRcvTime = System.currentTimeMillis();

            handleResponse((GridClientMessage)msg);
        }

        /** {@inheritDoc} */
        @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            // Don't count pings.
            if (!(msg instanceof GridClientPingPacket))
                lastMsgSndTime = System.currentTimeMillis();

            ctx.writeAndFlush(msg, promise);
        }

        /** {@inheritDoc} */
        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            GridClientTcpConnection.this.close(FAILED, false,
                new GridClientConnectionResetException("Connection was unexpectedly closed: " + serverAddress()));
        }

        /** {@inheritDoc} */
        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (log.isLoggable(Level.FINE))
                log.log(Level.FINE, "Exception occurred in TCP channel: " + serverAddress(), cause);

            cause.printStackTrace();

            GridClientTcpConnection.this.close(FAILED, false, cause);
        }
    }

    /**
     * Client handshake encoder.
     */
    private static class HandshakeEncoder extends MessageToByteEncoder {
        /** {@inheritDoc} */
        @Override protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            assert msg instanceof GridClientHandshakeRequest;

            out.writeBytes(((GridClientHandshakeRequest)msg).rawBytes());
        }
    }

    /**
     * Client handshake decoder.
     */
    private static class HandshakeDecoder extends MessageToMessageDecoder<ByteBuf> {
        /** {@inheritDoc} */
        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
            assert buf.readableBytes() == 1;

            byte rc = buf.readByte();

            out.add(new GridClientHandshakeResponse(rc));
        }
    }

    /**
     * Client handshake handler.
     */
    private class HandshakeHandler extends ChannelInboundHandlerAdapter {
        /** */
        private final byte protoId;

        /** Handshake future. */
        private ChannelPromise handshake;

        /**
         * @param protoId Marshaller.
         */
        HandshakeHandler(byte protoId) {
            this.protoId = protoId;
        }

        /** {@inheritDoc} */
        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            assert msg instanceof GridClientHandshakeResponse;

            try {
                handleHandshakeResponse((GridClientHandshakeResponse)msg);

                // Wasn't set by other methods.
                if (!handshake.isDone())
                    handshake.setSuccess(null);

            }
            catch (GridClientHandshakeException e) {
                log.severe("Client handshake failed [code=" + e.getStatusCode() + ", msg=" + e.getMessage() + ']');

                // Wasn't set by other methods.
                if (!handshake.isDone())
                    handshake.setFailure(e);

                close(FAILED, false, e);
            }
        }

        /** {@inheritDoc} */
        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (log.isLoggable(Level.FINE)) {
                log.log(Level.FINE, "Exception occurred in TCP channel: " + serverAddress(), cause);
            }

            // Wasn't set by other methods.
            if (!handshake.isDone())
                handshake.setFailure(cause);

            close(FAILED, false, cause);

        }

        /** {@inheritDoc} */
        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            GridClientException ex = new GridClientConnectionResetException("Connection was unexpectedly closed.");

            // Wasn't set by other methods.
            if (!handshake.isDone())
                handshake.setFailure(ex);

            close(FAILED, false, ex);

            ctx.fireChannelInactive();
        }

        /** {@inheritDoc} */
        @Override public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            // Init future early - as it's not perfectly clear whether
            // some exception could happen before 'channel active' event.
            if (handshake == null)
                handshake = ctx.newPromise();
        }

        /** {@inheritDoc} */
        @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(new GridClientHandshakeRequest(protoId));

            ctx.fireChannelActive();
        }

        /**
         * @return Handshake future.
         */
        public ChannelFuture handshakeFuture() {
            return handshake;
        }
    }
}
