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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.ClientReconnectedException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryByteBufferInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.client.thin.io.ClientConnection;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.thin.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.thin.io.ClientMessageHandler;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.platform.client.ClientFlag;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature.HEARTBEAT;
import static org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature.USER_ATTRIBUTES;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.LATEST_VER;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_0_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_1_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_2_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_3_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_4_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_5_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_6_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_7_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersionFeature.AUTHORIZATION;
import static org.apache.ignite.internal.client.thin.ProtocolVersionFeature.BITMAP_FEATURES;
import static org.apache.ignite.internal.client.thin.ProtocolVersionFeature.PARTITION_AWARENESS;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel, ClientMessageHandler, ClientConnectionStateHandler {
    /** Protocol version used by default on first connection attempt. */
    private static final ProtocolVersion DEFAULT_VERSION = LATEST_VER;

    /** Supported protocol versions. */
    private static final Collection<ProtocolVersion> supportedVers = Arrays.asList(
        V1_7_0,
        V1_6_0,
        V1_5_0,
        V1_4_0,
        V1_3_0,
        V1_2_0,
        V1_1_0,
        V1_0_0
    );

    /** GridNioServer has minimum idle check interval of 2 seconds, even if idleTimeout is lower. */
    private static final long MIN_RECOMMENDED_HEARTBEAT_INTERVAL = 500;

    /** Preallocated empty bytes. */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /** Protocol context. */
    private volatile ProtocolContext protocolCtx;

    /** Server node ID. */
    private volatile UUID srvNodeId;

    /** Server topology version. */
    private volatile AffinityTopologyVersion srvTopVer;

    /** Channel. */
    private final ClientConnection sock;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Topology change listeners. */
    private final Collection<Consumer<ClientChannel>> topChangeLsnrs = new CopyOnWriteArrayList<>();

    /** Notification listeners. */
    @SuppressWarnings("unchecked")
    private final Map<Long, NotificationListener>[] notificationLsnrs = new Map[ClientNotificationType.values().length];

    /** Pending notification. */
    @SuppressWarnings("unchecked")
    private final Map<Long, Queue<T2<ByteBuffer, Exception>>>[] pendingNotifications =
        new Map[ClientNotificationType.values().length];

    /** Guard for notification listeners. */
    private final ReadWriteLock notificationLsnrsGuard = new ReentrantReadWriteLock();

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Executor for async operation listeners. */
    private final Executor asyncContinuationExecutor;

    /** Send/receive timeout in milliseconds. */
    private final int timeout;

    /** Heartbeat timer. */
    private final Timer heartbeatTimer;

    /** Last send operation timestamp. */
    private volatile long lastSendMillis;

    /** Constructor. */
    TcpClientChannel(ClientChannelConfiguration cfg, ClientConnectionMultiplexer connMgr)
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        validateConfiguration(cfg);

        for (ClientNotificationType type : ClientNotificationType.values()) {
            if (type.keepNotificationsWithoutListener())
                pendingNotifications[type.ordinal()] = new ConcurrentHashMap<>();
        }

        Executor cfgExec = cfg.getAsyncContinuationExecutor();
        asyncContinuationExecutor = cfgExec != null ? cfgExec : ForkJoinPool.commonPool();

        timeout = cfg.getTimeout();

        sock = connMgr.open(cfg.getAddress(), this, this);

        handshake(DEFAULT_VERSION, cfg.getUserName(), cfg.getUserPassword(), cfg.getUserAttributes());

        heartbeatTimer = protocolCtx.isFeatureSupported(HEARTBEAT) && cfg.getHeartbeatEnabled()
                ? initHeartbeat(cfg.getHeartbeatInterval())
                : null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        close(null);
    }

    /** {@inheritDoc} */
    @Override public void onMessage(ByteBuffer buf) {
        processNextMessage(buf);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(@Nullable Exception e) {
        close(e);
    }

    /**
     * Close the channel with cause.
     */
    private void close(Exception cause) {
        if (closed.compareAndSet(false, true)) {
            if (heartbeatTimer != null)
                heartbeatTimer.cancel();

            U.closeQuiet(sock);

            for (ClientRequestFuture pendingReq : pendingReqs.values())
                pendingReq.onDone(new ClientConnectionException("Channel is closed", cause));

            notificationLsnrsGuard.readLock().lock();

            try {
                for (Map<Long, NotificationListener> lsnrs : notificationLsnrs) {
                    if (lsnrs != null)
                        lsnrs.values().forEach(lsnr -> lsnr.onChannelClosed(cause));
                }
            }
            finally {
                notificationLsnrsGuard.readLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException {
        ClientRequestFuture fut = send(op, payloadWriter);

        return receive(fut, payloadReader);
    }

    /** {@inheritDoc} */
    @Override public <T> CompletableFuture<T> serviceAsync(
            ClientOperation op,
            Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader
    ) {
        try {
            ClientRequestFuture fut = send(op, payloadWriter);

            return receiveAsync(fut, payloadReader);
        }
        catch (Throwable t) {
            CompletableFuture<T> fut = new CompletableFuture<>();
            fut.completeExceptionally(t);

            return fut;
        }
    }

    /**
     * @param op Operation.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @return Request future.
     */
    private ClientRequestFuture send(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter)
        throws ClientException {
        long id = reqId.getAndIncrement();

        PayloadOutputChannel payloadCh = new PayloadOutputChannel(this);

        try {
            if (closed())
                throw new ClientConnectionException("Channel is closed");

            ClientRequestFuture fut = new ClientRequestFuture();

            pendingReqs.put(id, fut);

            BinaryOutputStream req = payloadCh.out();

            req.writeInt(0); // Reserve an integer for the request size.
            req.writeShort(op.code());
            req.writeLong(id);

            if (payloadWriter != null)
                payloadWriter.accept(payloadCh);

            req.writeInt(0, req.position() - 4); // Actual size.

            write(req.array(), req.position(), payloadCh::close);

            return fut;
        }
        catch (Throwable t) {
            pendingReqs.remove(id);

            // Potential double-close is handled in PayloadOutputChannel.
            payloadCh.close();

            throw t;
        }
    }

    /**
     * @param pendingReq Request future.
     * @param payloadReader Payload reader from stream.
     * @return Received operation payload or {@code null} if response has no payload.
     */
    private <T> T receive(ClientRequestFuture pendingReq, Function<PayloadInputChannel, T> payloadReader)
        throws ClientException {
        try {
            ByteBuffer payload = timeout > 0 ? pendingReq.get(timeout) : pendingReq.get();

            if (payload == null || payloadReader == null)
                return null;

            return payloadReader.apply(new PayloadInputChannel(this, payload));
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /**
     * Receives the response asynchronously.
     *
     * @param pendingReq Request future.
     * @param payloadReader Payload reader from stream.
     * @return Future for the operation.
     */
    private <T> CompletableFuture<T> receiveAsync(ClientRequestFuture pendingReq, Function<PayloadInputChannel, T> payloadReader) {
        CompletableFuture<T> fut = new CompletableFuture<>();

        pendingReq.listen(payloadFut -> asyncContinuationExecutor.execute(() -> {
            try {
                ByteBuffer payload = payloadFut.get();

                if (payload == null || payloadReader == null)
                    fut.complete(null);
                else {
                    T res = payloadReader.apply(new PayloadInputChannel(this, payload));
                    fut.complete(res);
                }
            }
            catch (Throwable t) {
                fut.completeExceptionally(convertException(t));
            }
        }));

        return fut;
    }

    /**
     * Converts exception to {@link org.apache.ignite.internal.processors.platform.client.IgniteClientException}.
     * @param e Exception to convert.
     * @return Resulting exception.
     */
    private RuntimeException convertException(Throwable e) {
        if (e.getCause() instanceof ClientError)
            return new ClientException(e.getMessage(), e.getCause());

        // For every known class derived from ClientException, wrap cause in a new instance.
        // We could rethrow e.getCause() when instanceof ClientException,
        // but this results in an incomplete stack trace from the receiver thread.
        // This is similar to IgniteUtils.exceptionConverters.
        if (e.getCause() instanceof ClientConnectionException)
            return new ClientConnectionException(e.getMessage(), e.getCause());

        if (e.getCause() instanceof ClientReconnectedException)
            return new ClientReconnectedException(e.getMessage(), e.getCause());

        if (e.getCause() instanceof ClientAuthenticationException)
            return new ClientAuthenticationException(e.getMessage(), e.getCause());

        if (e.getCause() instanceof ClientAuthorizationException)
            return new ClientAuthorizationException(e.getMessage(), e.getCause());

        if (e.getCause() instanceof ClientFeatureNotSupportedByServerException)
            return new ClientFeatureNotSupportedByServerException(e.getMessage(), e.getCause());

        if (e.getCause() instanceof ClientException)
            return new ClientException(e.getMessage(), e.getCause());

        return new ClientException(e.getMessage(), e);
    }

    /**
     * Process next message from the input stream and complete corresponding future.
     */
    private void processNextMessage(ByteBuffer buf) throws ClientProtocolError, ClientConnectionException {
        BinaryInputStream dataInput = BinaryByteBufferInputStream.create(buf);

        if (protocolCtx == null) {
            // Process handshake.
            pendingReqs.remove(-1L).onDone(buf);
            return;
        }

        Long resId = dataInput.readLong();

        int status = 0;

        ClientOperation notificationOp = null;

        if (protocolCtx.isFeatureSupported(PARTITION_AWARENESS)) {
            short flags = dataInput.readShort();

            if ((flags & ClientFlag.AFFINITY_TOPOLOGY_CHANGED) != 0) {
                long topVer = dataInput.readLong();
                int minorTopVer = dataInput.readInt();

                srvTopVer = new AffinityTopologyVersion(topVer, minorTopVer);

                for (Consumer<ClientChannel> lsnr : topChangeLsnrs)
                    lsnr.accept(this);
            }

            if ((flags & ClientFlag.NOTIFICATION) != 0) {
                short notificationCode = dataInput.readShort();

                notificationOp = ClientOperation.fromCode(notificationCode);

                if (notificationOp == null || notificationOp.notificationType() == null)
                    throw new ClientProtocolError(String.format("Unexpected notification code [%d]", notificationCode));
            }

            if ((flags & ClientFlag.ERROR) != 0)
                status = dataInput.readInt();
        }
        else
            status = dataInput.readInt();

        int hdrSize = dataInput.position();
        int msgSize = buf.limit();

        ByteBuffer res;
        Exception err;

        if (status == 0) {
            err = null;
            res = msgSize > hdrSize ? buf : null;
        }
        else if (status == ClientStatus.SECURITY_VIOLATION) {
            err = new ClientAuthorizationException();
            res = null;
        }
        else {
            String errMsg = ClientUtils.createBinaryReader(null, dataInput).readString();

            err = new ClientServerError(errMsg, status, resId);
            res = null;
        }

        if (notificationOp == null) { // Respone received.
            ClientRequestFuture pendingReq = pendingReqs.remove(resId);

            if (pendingReq == null)
                throw new ClientProtocolError(String.format("Unexpected response ID [%s]", resId));

            pendingReq.onDone(res, err);
        }
        else { // Notification received.
            ClientNotificationType notificationType = notificationOp.notificationType();

            asyncContinuationExecutor.execute(() -> {
                NotificationListener lsnr = null;

                notificationLsnrsGuard.readLock().lock();

                try {
                    Map<Long, NotificationListener> lsrns = notificationLsnrs[notificationType.ordinal()];

                    if (lsrns != null)
                        lsnr = lsrns.get(resId);

                    if (notificationType.keepNotificationsWithoutListener() && lsnr == null) {
                        pendingNotifications[notificationType.ordinal()].computeIfAbsent(resId,
                            k -> new ConcurrentLinkedQueue<>()).add(new T2<>(res, err));
                    }
                }
                finally {
                    notificationLsnrsGuard.readLock().unlock();
                }

                if (lsnr != null)
                    lsnr.acceptNotification(res, err);
            });
        }
    }

    /** {@inheritDoc} */
    @Override public ProtocolContext protocolCtx() {
        return protocolCtx;
    }

    /** {@inheritDoc} */
    @Override public UUID serverNodeId() {
        return srvNodeId;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion serverTopologyVersion() {
        return srvTopVer;
    }

    /** {@inheritDoc} */
    @Override public void addTopologyChangeListener(Consumer<ClientChannel> lsnr) {
        topChangeLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void addNotificationListener(ClientNotificationType type, Long rsrcId, NotificationListener lsnr) {
        Queue<T2<ByteBuffer, Exception>> pendingQueue = null;

        notificationLsnrsGuard.writeLock().lock();

        try {
            if (closed())
                throw new ClientConnectionException("Channel is closed");

            Map<Long, NotificationListener> lsnrs = notificationLsnrs[type.ordinal()];

            if (lsnrs == null)
                notificationLsnrs[type.ordinal()] = lsnrs = new ConcurrentHashMap<>();

            lsnrs.put(rsrcId, lsnr);

            if (type.keepNotificationsWithoutListener())
                pendingQueue = pendingNotifications[type.ordinal()].remove(rsrcId);
        }
        finally {
            notificationLsnrsGuard.writeLock().unlock();
        }

        // Drain pending notifications queue.
        if (pendingQueue != null)
            pendingQueue.forEach(n -> lsnr.acceptNotification(n.get1(), n.get2()));
    }

    /** {@inheritDoc} */
    @Override public void removeNotificationListener(ClientNotificationType type, Long rsrcId) {
        notificationLsnrsGuard.writeLock().lock();

        try {
            Map<Long, NotificationListener> lsnrs = notificationLsnrs[type.ordinal()];

            if (lsnrs == null)
                return;

            lsnrs.remove(rsrcId);

            if (type.keepNotificationsWithoutListener())
                pendingNotifications[type.ordinal()].remove(rsrcId);

        }
        finally {
            notificationLsnrsGuard.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean closed() {
        return closed.get();
    }

    /** Validate {@link ClientConfiguration}. */
    private static void validateConfiguration(ClientChannelConfiguration cfg) {
        String error = null;

        InetSocketAddress addr = cfg.getAddress();

        if (addr == null)
            error = "At least one Ignite server node must be specified in the Ignite client configuration";
        else if (addr.getPort() < 1024 || addr.getPort() > 49151)
            error = String.format("Ignite client port %s is out of valid ports range 1024...49151", addr.getPort());
        else if (cfg.getHeartbeatInterval() <= 0)
            error = "heartbeatInterval cannot be zero or less.";

        if (error != null)
            throw new IllegalArgumentException(error);
    }

    /** Client handshake. */
    private void handshake(ProtocolVersion ver, String user, String pwd, Map<String, String> userAttrs)
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        ClientRequestFuture fut = new ClientRequestFuture();
        pendingReqs.put(-1L, fut);

        handshakeReq(ver, user, pwd, userAttrs);

        try {
            ByteBuffer res = timeout > 0 ? fut.get(timeout) : fut.get();
            handshakeRes(res, ver, user, pwd, userAttrs);
        }
        catch (IgniteCheckedException e) {
            throw new ClientConnectionException(e.getMessage(), e);
        }
    }

    /** Send handshake request. */
    private void handshakeReq(ProtocolVersion proposedVer, String user, String pwd,
        Map<String, String> userAttrs) throws ClientConnectionException {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);

        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, new BinaryHeapOutputStream(32), null, null)) {
            ProtocolContext protocolCtx = protocolContextFromVersion(proposedVer);

            writer.writeInt(0); // reserve an integer for the request size
            writer.writeByte((byte)ClientListenerRequest.HANDSHAKE);

            writer.writeShort(proposedVer.major());
            writer.writeShort(proposedVer.minor());
            writer.writeShort(proposedVer.patch());

            writer.writeByte(ClientListenerNioListener.THIN_CLIENT);

            if (protocolCtx.isFeatureSupported(BITMAP_FEATURES)) {
                byte[] features = ProtocolBitmaskFeature.featuresAsBytes(protocolCtx.features());
                writer.writeByteArray(features);
            }

            if (protocolCtx.isFeatureSupported(USER_ATTRIBUTES))
                writer.writeMap(userAttrs);

            boolean authSupported = protocolCtx.isFeatureSupported(AUTHORIZATION);

            if (authSupported && user != null && !user.isEmpty()) {
                writer.writeString(user);
                writer.writeString(pwd);
            }

            writer.out().writeInt(0, writer.out().position() - 4); // actual size

            write(writer.out().arrayCopy(), writer.out().position(), null);
        }
    }

    /**
     * @param ver Protocol version.
     * @return Protocol context for a version.
     */
    private ProtocolContext protocolContextFromVersion(ProtocolVersion ver) {
        EnumSet<ProtocolBitmaskFeature> features = null;
        if (ProtocolContext.isFeatureSupported(ver, BITMAP_FEATURES))
            features = ProtocolBitmaskFeature.allFeaturesAsEnumSet();

        return new ProtocolContext(ver, features);
    }

    /** Receive and handle handshake response. */
    private void handshakeRes(ByteBuffer buf, ProtocolVersion proposedVer, String user, String pwd, Map<String, String> userAttrs)
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        BinaryInputStream res = BinaryByteBufferInputStream.create(buf);

        try (BinaryReaderExImpl reader = ClientUtils.createBinaryReader(null, res)) {
            boolean success = res.readBoolean();

            if (success) {
                byte[] features = EMPTY_BYTES;

                if (ProtocolContext.isFeatureSupported(proposedVer, BITMAP_FEATURES))
                    features = reader.readByteArray();

                protocolCtx = new ProtocolContext(proposedVer, ProtocolBitmaskFeature.enumSet(features));

                if (protocolCtx.isFeatureSupported(PARTITION_AWARENESS)) {
                    // Reading server UUID
                    srvNodeId = reader.readUuid();
                }
            }
            else {
                ProtocolVersion srvVer = new ProtocolVersion(res.readShort(), res.readShort(), res.readShort());

                String err = reader.readString();
                int errCode = ClientStatus.FAILED;

                if (res.remaining() > 0)
                    errCode = reader.readInt();

                if (errCode == ClientStatus.AUTH_FAILED)
                    throw new ClientAuthenticationException(err);
                else if (proposedVer.equals(srvVer))
                    throw new ClientProtocolError(err);
                else if (!supportedVers.contains(srvVer) ||
                    (!ProtocolContext.isFeatureSupported(srvVer, AUTHORIZATION) && !F.isEmpty(user)))
                    // Server version is not supported by this client OR server version is less than 1.1.0 supporting
                    // authentication and authentication is required.
                    throw new ClientProtocolError(String.format(
                        "Protocol version mismatch: client %s / server %s. Server details: %s",
                        proposedVer,
                        srvVer,
                        err
                    ));
                else { // Retry with server version.
                    handshake(srvVer, user, pwd, userAttrs);
                }
            }
        }
        catch (IOException e) {
            throw handleIOError(e);
        }
    }

    /** Write bytes to the output stream. */
    private void write(byte[] bytes, int len, @Nullable Runnable onDone) throws ClientConnectionException {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, len);

        try {
            sock.send(buf, onDone);
            lastSendMillis = System.currentTimeMillis();
        }
        catch (IgniteCheckedException e) {
            throw new ClientConnectionException(e.getMessage(), e);
        }
    }

    /**
     * @param ex IO exception (cause).
     */
    private ClientException handleIOError(@Nullable IOException ex) {
        return handleIOError("sock=" + sock, ex);
    }

    /**
     * @param chInfo Additional channel info
     * @param ex IO exception (cause).
     */
    private ClientException handleIOError(String chInfo, @Nullable IOException ex) {
        return new ClientConnectionException("Ignite cluster is unavailable [" + chInfo + ']', ex);
    }

    /**
     * Initializes heartbeats.
     *
     * @param configuredInterval Configured heartbeat interval, in milliseconds.
     * @return Heartbeat timer.
     */
    private Timer initHeartbeat(long configuredInterval) {
        long heartbeatInterval = getHeartbeatInterval(configuredInterval);

        Timer timer = new Timer("tcp-client-channel-heartbeats-" + hashCode());

        timer.schedule(new HeartbeatTask(heartbeatInterval), heartbeatInterval, heartbeatInterval);

        return timer;
    }

    /**
     * Gets the heartbeat interval based on the configured value and served-side idle timeout.
     *
     * @param configuredInterval Configured interval.
     * @return Resolved interval.
     */
    private long getHeartbeatInterval(long configuredInterval) {
        long serverIdleTimeoutMs = service(ClientOperation.GET_IDLE_TIMEOUT, null, in -> in.in().readLong());

        if (serverIdleTimeoutMs <= 0)
            return configuredInterval;

        long recommendedHeartbeatInterval = serverIdleTimeoutMs / 3;

        if (recommendedHeartbeatInterval < MIN_RECOMMENDED_HEARTBEAT_INTERVAL)
            recommendedHeartbeatInterval = MIN_RECOMMENDED_HEARTBEAT_INTERVAL;

        return Math.min(configuredInterval, recommendedHeartbeatInterval);
    }

    /**
     *
     */
    private static class ClientRequestFuture extends GridFutureAdapter<ByteBuffer> {
    }

    /**
     * Sends heartbeat messages.
     */
    private class HeartbeatTask extends TimerTask {
        /** Heartbeat interval. */
        private final long interval;

        /** Constructor. */
        public HeartbeatTask(long interval) {
            this.interval = interval;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                if (System.currentTimeMillis() - lastSendMillis > interval)
                    service(ClientOperation.HEARTBEAT, null, null);
            }
            catch (Throwable ignored) {
                // Ignore failed heartbeats.
            }
        }
    }
}
