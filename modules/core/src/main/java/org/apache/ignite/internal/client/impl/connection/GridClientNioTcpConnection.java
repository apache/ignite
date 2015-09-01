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

package org.apache.ignite.internal.client.impl.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientCacheFlag;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientDataMetrics;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.impl.GridClientFutureAdapter;
import org.apache.ignite.internal.client.impl.GridClientFutureCallback;
import org.apache.ignite.internal.client.impl.GridClientNodeImpl;
import org.apache.ignite.internal.client.impl.GridClientNodeMetricsAdapter;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.client.marshaller.jdk.GridClientJdkMarshaller;
import org.apache.ignite.internal.client.marshaller.optimized.GridClientOptimizedMarshaller;
import org.apache.ignite.internal.processors.rest.client.message.GridClientAuthenticationRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientHandshakeRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeMetricsBean;
import org.apache.ignite.internal.processors.rest.client.message.GridClientPingPacket;
import org.apache.ignite.internal.processors.rest.client.message.GridClientResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskResultBean;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTopologyRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridRouterRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridRouterResponse;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioFutureImpl;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.client.GridClientCacheFlag.KEEP_PORTABLES;
import static org.apache.ignite.internal.client.impl.connection.GridClientConnectionCloseReason.CONN_IDLE;
import static org.apache.ignite.internal.client.impl.connection.GridClientConnectionCloseReason.FAILED;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.APPEND;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.CAS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.GET_ALL;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.METRICS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.PREPEND;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.PUT_ALL;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.REPLACE;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.RMV;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.RMV_ALL;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MARSHALLER;

/**
 * This class performs request to grid over tcp protocol. Serialization is performed with marshaller
 * provided.
 */
public class GridClientNioTcpConnection extends GridClientConnection {
    /** */
    static final int SES_META_HANDSHAKE = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    static final int SES_META_CONN = GridNioSessionMetaKey.nextUniqueKey();

    /** Logger */
    private static final Logger log = Logger.getLogger(GridClientNioTcpConnection.class.getName());

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

    /** Request ID counter. */
    private AtomicLong reqIdCntr = new AtomicLong(1);

    /** Timestamp of last sent message. */
    private volatile long lastMsgSndTime;

    /** Timestamp of last received message. */
    private volatile long lastMsgRcvTime;

    /**
     * Ping receive time.
     * {@code 0} until first ping send and {@link Long#MAX_VALUE} while response isn't received.
     */
    private volatile long lastPingRcvTime;

    /** Ping send time. */
    private volatile long lastPingSndTime;

    /** Connection create timestamp. */
    private long createTs;

    /** Session token. */
    private volatile byte[] sesTok;

    /** Timer to run ping checks. */
    private ScheduledFuture<?> pingTask;

    /** NIO session. */
    private GridNioSession ses;

    /** Marshaller. */
    private final GridClientMarshaller marsh;

    /** */
    private final ThreadLocal<Boolean> keepPortablesMode;

    /**
     * Creates a client facade, tries to connect to remote server, in case of success starts reader thread.
     *
     * @param srv NIO server.
     * @param clientId Client identifier.
     * @param srvAddr Server to connect to.
     * @param sslCtx SSL context to use if SSL is enabled, {@code null} otherwise.
     * @param pingExecutor Executor service for sending ping requests.
     * @param connectTimeout Connect timeout.
     * @param pingInterval Ping interval.
     * @param pingTimeout Ping timeout.
     * @param tcpNoDelay TCP_NODELAY flag for outgoing socket connection.
     * @param marsh Marshaller to use in communication.
     * @param top Topology instance.
     * @param cred Client credentials.      @throws IOException If connection could not be established.
     * @throws IOException If IO error occurs.
     * @throws GridClientException If handshake error occurs.
     */
    @SuppressWarnings("unchecked")
    GridClientNioTcpConnection(GridNioServer srv,
        UUID clientId,
        InetSocketAddress srvAddr,
        SSLContext sslCtx,
        ScheduledExecutorService pingExecutor,
        int connectTimeout,
        long pingInterval,
        long pingTimeout,
        boolean tcpNoDelay,
        GridClientMarshaller marsh,
        Byte marshId,
        GridClientTopology top,
        Object cred,
        ThreadLocal<Boolean> keepPortablesMode
    ) throws IOException, GridClientException {
        super(clientId, srvAddr, sslCtx, top, cred);

        assert marsh != null || marshId != null;

        this.marsh = marsh;
        this.pingInterval = pingInterval;
        this.pingTimeout = pingTimeout;
        this.keepPortablesMode = keepPortablesMode;

        SocketChannel ch = null;
        Socket sock = null;
        boolean cleanup = true;

        try {
            ch = SocketChannel.open();
            sock = ch.socket();

            sock.setTcpNoDelay(tcpNoDelay);
            sock.setKeepAlive(true);

            final long startConnTime = U.currentTimeMillis();

            sock.connect(srvAddr, connectTimeout);

            final long connTimeoutRest = connectTimeout - (U.currentTimeMillis() - startConnTime);

            GridClientFuture<?> handshakeFut = new GridClientFutureAdapter<>();

            Map<Integer, Object> meta = new HashMap<>();

            meta.put(SES_META_HANDSHAKE, handshakeFut);

            GridNioFuture<?> sslHandshakeFut = null;

            if (sslCtx != null) {
                sslHandshakeFut = new GridNioFutureImpl<>();

                meta.put(GridNioSslFilter.HANDSHAKE_FUT_META_KEY, sslHandshakeFut);
            }

            ses = (GridNioSession)srv.createSession(ch, meta).get();

            if (sslHandshakeFut != null)
                sslHandshakeFut.get();

            GridClientHandshakeRequest req = new GridClientHandshakeRequest();

            if (marshId != null)
                req.marshallerId(marshId);
            // marsh != null.
            else if (marsh instanceof GridClientOptimizedMarshaller)
                req.marshallerId(GridClientOptimizedMarshaller.ID);
            else if (marsh instanceof GridClientJdkMarshaller)
                req.marshallerId(GridClientJdkMarshaller.ID);

            ses.addMeta(MARSHALLER.ordinal(), marsh);

            ses.send(req);

            handshakeFut.get(connTimeoutRest, MILLISECONDS);

            ses.addMeta(SES_META_CONN, this);

            if (log.isLoggable(Level.INFO))
                log.info("Client TCP connection established: " + serverAddress());

            pingTask = pingExecutor.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    try {
                        makeRequest(GridClientPingPacket.PING_MESSAGE, (TcpClientFuture)null, false);
                    }
                    catch (Exception e) {
                        log.warning("Failed to send ping message: " + e);
                    }
                }
            }, 500, 500, MILLISECONDS);

            createTs = System.currentTimeMillis();

            cleanup = false;
        }
        catch (IgniteCheckedException e) {
            throw new GridClientException(e);
        }
        finally {
            if (cleanup) {
                if (ses != null)
                    srv.close(ses);

                if (sock!= null)
                    sock.close();

                if (ch != null)
                    ch.close();
            }
        }
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
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    private void close(GridClientConnectionCloseReason reason, boolean waitCompletion, @Nullable Throwable cause) {
        synchronized (this) {
            if (closeReason != null)
                return;

            closeReason = reason;
        }

        try {
            // Wait for all pending requests to be processed.
            if (waitCompletion && !pendingReqs.isEmpty() && ses.closeTime() == 0)
                closedLatch.await();
        }
        catch (InterruptedException ignored) {
            log.warning("Interrupted while waiting for all requests to be processed (all pending " +
                "requests will be failed): " + serverAddress());

            Thread.currentThread().interrupt();
        }

        if (pingTask != null)
            pingTask.cancel(false);

        if (ses != null)
            ses.close(); // Async close.

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
        return makeRequest(msg, destId, false);
    }

    /**
     * Makes request to server via tcp protocol and returns a future that will be completed when
     * response is received.
     *
     * @param msg Message to request,
     * @param destId Destination node identifier.
     * @param keepPortables Keep portables flag.
     * @return Response object.
     * @throws GridClientConnectionResetException If request failed.
     * @throws GridClientClosedException If client was closed.
     */
    private <R> GridClientFutureAdapter<R> makeRequest(GridClientMessage msg, UUID destId, boolean keepPortables)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert msg != null;

        TcpClientFuture<R> res = new TcpClientFuture<>(false, keepPortables);

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
        return makeRequest(msg, fut, false);
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
     * @throws GridClientClosedException If client closed.
     */
    private <R> GridClientFutureAdapter<R> makeRequest(GridClientMessage msg, final TcpClientFuture<R> fut,
        boolean routeMode) throws GridClientConnectionResetException, GridClientClosedException {
        assert msg != null;

        if (msg instanceof GridClientPingPacket) {
            long now = U.currentTimeMillis();

            if (Math.min(now, lastPingRcvTime) - lastPingSndTime >= pingTimeout)
                close(FAILED, false,
                    new IOException("Did not receive any packets within ping response interval (connection is " +
                        "considered to be half-opened) [lastPingReceiveTime=" + lastPingRcvTime +
                        ", lastPingSendTime=" + lastPingSndTime + ", now=" + now + ", timeout=" + pingTimeout +
                        ", addr=" + serverAddress() + ']')
                );
            // Do not pass ping requests if ping interval didn't pass yet
            // or we've already waiting for ping response.
            else if (now - lastPingSndTime > pingInterval && lastPingRcvTime != Long.MAX_VALUE) {
                lastPingRcvTime = Long.MAX_VALUE;

                ses.send(GridClientPingPacket.PING_MESSAGE);

                lastPingSndTime = now;
            }
        }
        else {
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

            GridNioFuture<?> sndFut = ses.send(msg);

            lastMsgSndTime = U.currentTimeMillis();

            if (routeMode) {
                sndFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> sndFut) {
                        try {
                            sndFut.get();
                        }
                        catch (Exception e) {
                            close(FAILED, false, e);

                            fut.onDone(getCloseReasonAsException(FAILED, e));
                        }
                    }
                });
            }
            else {
                try {
                    sndFut.get();
                }
                catch (Exception e) {
                    throw new GridClientConnectionResetException("Failed to send message over connection " +
                        "(will try to reconnect): " + serverAddress(), e);
                }
            }
        }

        return fut;
    }

    /**
     * Handles ping response.
     */
    void handlePingResponse() {
        lastPingRcvTime = U.currentTimeMillis();
    }

    /**
     * Handles incoming response message. If this connection is closed this method would signal empty event
     * if there is no more pending requests.
     *
     * @param res Incoming response data.
     */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    void handleResponse(GridClientMessage res) throws IOException {
        lastMsgRcvTime = U.currentTimeMillis();

        TcpClientFuture fut = pendingReqs.get(res.requestId());

        if (fut == null) {
            log.warning("Response for an unknown request is received, ignoring. " +
                "[res=" + res + ", ses=" + ses + ']');

            return;
        }

        if (fut.forward()) {
            removePending(res.requestId());

            fut.onDone(res);
        }
        else {
            GridClientMessage res0 = res;

            if (res instanceof GridRouterResponse) {
                res0 = marsh.unmarshal(((GridRouterResponse)res).body());

                res0.requestId(res.requestId());
                res0.clientId(res.clientId());
                res0.destinationId(res.destinationId());
            }

            if (res0 instanceof GridClientResponse)
                handleClientResponse(fut, (GridClientResponse)res0);
            else
                log.warning("Unsupported response type received: " + res0);
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
                    if (credentials() == null) {
                        fut.onDone(new GridClientAuthenticationException("Authentication failed on server " +
                            "(client has no credentials) [clientId=" + clientId +
                            ", srvAddr=" + serverAddress() + ", errMsg=" + resp.errorMessage() +']'));

                        return;
                    }

                    fut.retryState(TcpClientFuture.STATE_AUTH_RETRY);

                    GridClientAuthenticationRequest req = buildAuthRequest();

                    req.requestId(resp.requestId());

                    ses.send(req);

                    return;
                }

                break;
            }

            case TcpClientFuture.STATE_AUTH_RETRY: {
                if (resp.successStatus() == GridClientResponse.STATUS_SUCCESS) {
                    fut.retryState(TcpClientFuture.STATE_REQUEST_RETRY);

                    src.sessionToken(sesTok);

                    ses.send(src);

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

        GridClientCacheRequest req = new GridClientCacheRequest(PUT_ALL);

        req.cacheName(cacheName);
        req.values((Map<Object, Object>)entries);
        req.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(req, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Map<K, V>> cacheGetAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert keys != null;

        GridClientCacheRequest req = new GridClientCacheRequest(GET_ALL);

        req.cacheName(cacheName);
        req.keys((Iterable<Object>)keys);
        req.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(req, destNodeId, flags.contains(KEEP_PORTABLES));
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFutureAdapter<Boolean> cacheRemove(String cacheName, K key,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientCacheRequest req = new GridClientCacheRequest(RMV);

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

        GridClientCacheRequest req = new GridClientCacheRequest(RMV_ALL);

        req.cacheName(cacheName);
        req.keys((Iterable<Object>)keys);
        req.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(req, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheReplace(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest replace = new GridClientCacheRequest(REPLACE);

        replace.cacheName(cacheName);
        replace.key(key);
        replace.value(val);
        replace.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(replace, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheCompareAndSet(String cacheName, K key, V newVal,
        V oldVal, Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;

        GridClientCacheRequest msg = new GridClientCacheRequest(CAS);

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
        GridClientCacheRequest metrics = new GridClientCacheRequest(METRICS);

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

        GridClientCacheRequest append = new GridClientCacheRequest(APPEND);

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

        GridClientCacheRequest prepend = new GridClientCacheRequest(PREPEND);

        prepend.cacheName(cacheName);
        prepend.key(key);
        prepend.value(val);
        prepend.cacheFlagsOn(encodeCacheFlags(flags));

        return makeRequest(prepend, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <R> GridClientFutureAdapter<R> execute(String taskName, Object arg, UUID destNodeId,
        final boolean keepPortables) throws GridClientConnectionResetException, GridClientClosedException {
        GridClientTaskRequest msg = new GridClientTaskRequest();

        msg.taskName(taskName);
        msg.argument(arg);
        msg.keepPortables(keepPortables);

        return this.<GridClientTaskResultBean>makeRequest(msg, destNodeId).chain(
            new GridClientFutureCallback<GridClientTaskResultBean, R>() {
                @Override public R onComplete(GridClientFuture<GridClientTaskResultBean> fut)
                    throws GridClientException {
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
    @Override public GridClientFutureAdapter<GridRouterRequest> forwardMessage(Object msg)
        throws GridClientException {
        assert msg instanceof GridRouterRequest;

        TcpClientFuture<GridRouterRequest> res = new TcpClientFuture<>(true, false);

        makeRequest((GridClientMessage)msg, res, true);

        return res;
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
            .tcpPort(nodeBean.getTcpPort());

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
        /** */
        private static final long serialVersionUID = 0L;

        /** Initial request. */
        private static final int STATE_INITIAL = 0;

        /** Authentication retry. */
        private static final int STATE_AUTH_RETRY = 1;

        /** Request retry after auth retry. */
        private static final int STATE_REQUEST_RETRY = 2;

        /** Flag indicating if connected message is a forwarded. */
        private final boolean forward;

        /** Keep portables flag. */
        private final boolean keepPortables;

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
            keepPortables = false;
        }

        /**
         * Creates new future with the given {@code forward} flag value.
         *
         * @param forward Flag value.
         */
        private TcpClientFuture(boolean forward, boolean keepPortables) {
            this.forward = forward;
            this.keepPortables = keepPortables;
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

        /**
         * @return Keep portables flag.
         */
        public boolean keepPortables() {
            return keepPortables;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TcpClientFuture [state=" + authRetry + ", forward=" + forward + ", message=" + pendingMsg + "]";
        }
    }
}