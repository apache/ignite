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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.ClientReconnectedException;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryPrimitives;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.platform.client.ClientFlag;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

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
class TcpClientChannel implements ClientChannel {
    /** Protocol version used by default on first connection attempt. */
    private static final ProtocolVersion DEFAULT_VERSION = LATEST_VER;

    /** Receiver thread prefix. */
    static final String RECEIVER_THREAD_PREFIX = "thin-client-channel#";

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

    /** Protocol context. */
    private ProtocolContext protocolCtx;

    /** Server node ID. */
    private UUID srvNodeId;

    /** Server topology version. */
    private AffinityTopologyVersion srvTopVer;

    /** Channel. */
    private final Socket sock;

    /** Output stream. */
    private final OutputStream out;

    /** Data input. */
    private final ByteCountingDataInput dataInput;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Send lock. */
    private final Lock sndLock = new ReentrantLock();

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Topology change listeners. */
    private final Collection<Consumer<ClientChannel>> topChangeLsnrs = new CopyOnWriteArrayList<>();

    /** Notification listeners. */
    private final Collection<NotificationListener> notificationLsnrs = new CopyOnWriteArrayList<>();

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Executor for async operation listeners. */
    private final Executor asyncContinuationExecutor;

    /** Receiver thread (processes incoming messages). */
    private Thread receiverThread;

    /** Constructor. */
    TcpClientChannel(ClientChannelConfiguration cfg)
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        validateConfiguration(cfg);

        Executor cfgExec = cfg.getAsyncContinuationExecutor();
        asyncContinuationExecutor = cfgExec != null ? cfgExec : ForkJoinPool.commonPool();

        try {
            sock = createSocket(cfg);

            out = sock.getOutputStream();
            dataInput = new ByteCountingDataInput(sock.getInputStream());
        }
        catch (IOException e) {
            throw handleIOError("addr=" + cfg.getAddress(), e);
        }

        handshake(DEFAULT_VERSION, cfg.getUserName(), cfg.getUserPassword(), cfg.getUserAttributes());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        close(null);
    }

    /**
     * Close the channel with cause.
     */
    private void close(Throwable cause) {
        if (closed.compareAndSet(false, true)) {
            U.closeQuiet(dataInput);
            U.closeQuiet(out);
            U.closeQuiet(sock);

            sndLock.lock(); // Lock here to prevent creation of new pending requests.

            try {
                for (ClientRequestFuture pendingReq : pendingReqs.values())
                    pendingReq.onDone(new ClientConnectionException("Channel is closed", cause));

                if (receiverThread != null)
                    receiverThread.interrupt();
            }
            finally {
                sndLock.unlock();
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
        } catch (Throwable t) {
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

        // Only one thread at a time can have access to write to the channel.
        sndLock.lock();

        try (PayloadOutputChannel payloadCh = new PayloadOutputChannel(this)) {
            if (closed())
                throw new ClientConnectionException("Channel is closed");

            initReceiverThread(); // Start the receiver thread with the first request.

            ClientRequestFuture fut = new ClientRequestFuture();

            pendingReqs.put(id, fut);

            BinaryOutputStream req = payloadCh.out();

            req.writeInt(0); // Reserve an integer for the request size.
            req.writeShort(op.code());
            req.writeLong(id);

            if (payloadWriter != null)
                payloadWriter.accept(payloadCh);

            req.writeInt(0, req.position() - 4); // Actual size.

            write(req.array(), req.position());

            return fut;
        }
        catch (Throwable t) {
            pendingReqs.remove(id);

            throw t;
        }
        finally {
            sndLock.unlock();
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
            byte[] payload = pendingReq.get();

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
                byte[] payload = payloadFut.get();

                if (payload == null || payloadReader == null)
                    fut.complete(null);
                else {
                    T res = payloadReader.apply(new PayloadInputChannel(this, payload));
                    fut.complete(res);
                }
            } catch (Throwable t) {
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
     * Init and start receiver thread if it wasn't started before.
     *
     * Note: Method should be called only under external synchronization.
     */
    private void initReceiverThread() {
        if (receiverThread == null) {
            Socket sock = this.sock;

            String sockInfo = sock == null ? null : sock.getInetAddress().getHostName() + ":" + sock.getPort();

            receiverThread = new Thread(() -> {
                try {
                    while (!closed())
                        processNextMessage();
                }
                catch (Throwable e) {
                    close(e);
                }
            }, RECEIVER_THREAD_PREFIX + sockInfo);

            receiverThread.setDaemon(true);

            receiverThread.start();
        }
    }

    /**
     * Process next message from the input stream and complete corresponding future.
     */
    private void processNextMessage() throws ClientProtocolError, ClientConnectionException {
        // blocking read a message header not to fall into a busy loop
        int msgSize = dataInput.readInt(2048);

        if (msgSize <= 0)
            throw new ClientProtocolError(String.format("Invalid message size: %s", msgSize));

        long bytesReadOnStartMsg = dataInput.totalBytesRead();

        long resId = dataInput.spinReadLong();

        int status = 0;

        ClientOperation notificationOp = null;

        BinaryInputStream resIn;

        if (protocolCtx.isFeatureSupported(PARTITION_AWARENESS)) {
            short flags = dataInput.spinReadShort();

            if ((flags & ClientFlag.AFFINITY_TOPOLOGY_CHANGED) != 0) {
                long topVer = dataInput.spinReadLong();
                int minorTopVer = dataInput.spinReadInt();

                srvTopVer = new AffinityTopologyVersion(topVer, minorTopVer);

                for (Consumer<ClientChannel> lsnr : topChangeLsnrs)
                    lsnr.accept(this);
            }

            if ((flags & ClientFlag.NOTIFICATION) != 0) {
                short notificationCode = dataInput.spinReadShort();

                notificationOp = ClientOperation.fromCode(notificationCode);

                if (notificationOp == null || !notificationOp.isNotification())
                    throw new ClientProtocolError(String.format("Unexpected notification code [%d]", notificationCode));
            }

            if ((flags & ClientFlag.ERROR) != 0)
                status = dataInput.spinReadInt();
        }
        else
            status = dataInput.spinReadInt();

        int hdrSize = (int)(dataInput.totalBytesRead() - bytesReadOnStartMsg);

        byte[] res = null;
        Exception err = null;

        if (status == 0) {
            if (msgSize > hdrSize)
                res = dataInput.spinRead(msgSize - hdrSize);
        }
        else if (status == ClientStatus.SECURITY_VIOLATION)
            err = new ClientAuthorizationException();
        else {
            resIn = new BinaryHeapInputStream(dataInput.spinRead(msgSize - hdrSize));

            String errMsg = ClientUtils.createBinaryReader(null, resIn).readString();

            err = new ClientServerError(errMsg, status, resId);
        }

        if (notificationOp == null) { // Respone received.
            ClientRequestFuture pendingReq = pendingReqs.remove(resId);

            if (pendingReq == null)
                throw new ClientProtocolError(String.format("Unexpected response ID [%s]", resId));

            pendingReq.onDone(res, err);
        }
        else { // Notification received.
            for (NotificationListener lsnr : notificationLsnrs)
                lsnr.acceptNotification(this, notificationOp, resId, res, err);
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
    @Override public void addNotificationListener(NotificationListener lsnr) {
        notificationLsnrs.add(lsnr);
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

        if (error != null)
            throw new IllegalArgumentException(error);
    }

    /** Create socket. */
    private static Socket createSocket(ClientChannelConfiguration cfg) throws IOException {
        Socket sock = cfg.getSslMode() == SslMode.REQUIRED ?
            new ClientSslSocketFactory(cfg).create() :
            new Socket(cfg.getAddress().getHostName(), cfg.getAddress().getPort());

        sock.setTcpNoDelay(cfg.isTcpNoDelay());

        if (cfg.getTimeout() > 0)
            sock.setSoTimeout(cfg.getTimeout());

        if (cfg.getSendBufferSize() > 0)
            sock.setSendBufferSize(cfg.getSendBufferSize());

        if (cfg.getReceiveBufferSize() > 0)
            sock.setReceiveBufferSize(cfg.getReceiveBufferSize());

        return sock;
    }

    /** Client handshake. */
    private void handshake(ProtocolVersion ver, String user, String pwd, Map<String, String> userAttrs)
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        handshakeReq(ver, user, pwd, userAttrs);
        handshakeRes(ver, user, pwd, userAttrs);
    }

    /** Send handshake request. */
    private void handshakeReq(ProtocolVersion proposedVer, String user, String pwd,
        Map<String, String> userAttrs) throws ClientConnectionException {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);

        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, new BinaryHeapOutputStream(32), null, null)) {
            ProtocolContext protocolCtx = protocolContextFromVersion(proposedVer);

            writer.writeInt(0); // reserve an integer for the request size
            writer.writeByte((byte) ClientListenerRequest.HANDSHAKE);

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

            writer.out().writeInt(0, writer.out().position() - 4);// actual size

            write(writer.array(), writer.out().position());
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
    private void handshakeRes(ProtocolVersion proposedVer, String user, String pwd, Map<String, String> userAttrs)
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        int resSize = dataInput.readInt();

        if (resSize <= 0)
            throw new ClientProtocolError(String.format("Invalid handshake response size: %s", resSize));

        BinaryInputStream res = new BinaryHeapInputStream(dataInput.read(resSize));

        try (BinaryReaderExImpl reader = ClientUtils.createBinaryReader(null, res)) {
            boolean success = res.readBoolean();

            if (success) {
                byte[] features = new byte[0];

                if (ProtocolContext.isFeatureSupported(proposedVer, BITMAP_FEATURES))
                    features = reader.readByteArray();

                protocolCtx = new ProtocolContext(proposedVer, ProtocolBitmaskFeature.enumSet(features));

                if (protocolCtx.isFeatureSupported(PARTITION_AWARENESS)) {
                    // Reading server UUID
                    srvNodeId = reader.readUuid();
                }
            } else {
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
    private void write(byte[] bytes, int len) throws ClientConnectionException {
        try {
            out.write(bytes, 0, len);
            out.flush();
        }
        catch (IOException e) {
            throw handleIOError(e);
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
     * Auxiliary class to read byte buffers and numeric values, counting total bytes read.
     * Numeric values are read in the little-endian byte order.
     */
    private class ByteCountingDataInput implements AutoCloseable {
        /** Input stream. */
        private final InputStream in;

        /** Total bytes read from the input stream. */
        private long totalBytesRead;

        /** Temporary buffer to read long, int and short values. */
        private final byte[] tmpBuf = new byte[Long.BYTES];

        /**
         * @param in Input stream.
         */
        public ByteCountingDataInput(InputStream in) {
            this.in = in;
        }

        /** Read bytes from the input stream. */
        public byte[] read(int len) throws ClientConnectionException {
            byte[] bytes = new byte[len];

            read(bytes, len, 0);

            return bytes;
        }

        /** Read bytes from the input stream. */
        public byte[] spinRead(int len) {
            byte[] bytes = new byte[len];

            read(bytes, len, Integer.MAX_VALUE);

            return bytes;
        }

        /**
         * Read bytes from the input stream to the buffer.
         *
         * @param bytes Bytes buffer.
         * @param len Length.
         * @param tryReadCnt Number of reads before falling into blocking read.
         */
        public void read(byte[] bytes, int len, int tryReadCnt) throws ClientConnectionException {
            int offset = 0;

            try {
                while (offset < len) {
                    int toRead;

                    if (tryReadCnt == 0)
                        toRead = len - offset;
                    else if ((toRead = Math.min(in.available(), len - offset)) == 0) {
                        tryReadCnt--;

                        continue;
                    }

                    int read = in.read(bytes, offset, toRead);

                    if (read < 0)
                        throw handleIOError(null);

                    offset += read;
                    totalBytesRead += read;
                }
            }
            catch (IOException e) {
                throw handleIOError(e);
            }
        }

        /**
         * Read long value from the input stream.
         */
        public long readLong() throws ClientConnectionException {
            return readLong(0);
        }

        /**
         * Read long value from the input stream.
         */
        public long spinReadLong() throws ClientConnectionException {
            return readLong(Integer.MAX_VALUE);
        }

        /**
         * Read long value from the input stream.
         *
         * @param tryReadCnt Number of reads before falling into blocking read.
         */
        private long readLong(int tryReadCnt) throws ClientConnectionException {
            read(tmpBuf, Long.BYTES, tryReadCnt);

            return BinaryPrimitives.readLong(tmpBuf, 0);
        }

        /**
         * Read int value from the input stream.
         */
        public int readInt() throws ClientConnectionException {
            return readInt(0);
        }

        /**
         * Read int value from the input stream.
         */
        public int spinReadInt() throws ClientConnectionException {
            return readInt(Integer.MAX_VALUE);
        }

        /**
         * Read int value from the input stream.
         *
         * @param tryReadCnt Number of reads before falling into blocking read.
         */
        private int readInt(int tryReadCnt) throws ClientConnectionException {
            read(tmpBuf, Integer.BYTES, tryReadCnt);

            return BinaryPrimitives.readInt(tmpBuf, 0);
        }

        /**
         * Read short value from the input stream.
         */
        public short readShort() throws ClientConnectionException {
            return readShort(0);
        }

        /**
         * Read short value from the input stream.
         */
        public short spinReadShort() throws ClientConnectionException {
            return readShort(Integer.MAX_VALUE);
        }

        /**
         * Read short value from the input stream.
         *
         * @param tryReadCnt Number of reads before falling into blocking read.
         */
        public short readShort(int tryReadCnt) throws ClientConnectionException {
            read(tmpBuf, Short.BYTES, tryReadCnt);

            return BinaryPrimitives.readShort(tmpBuf, 0);
        }

        /**
         * Gets total bytes read from the input stream.
         */
        public long totalBytesRead() {
            return totalBytesRead;
        }

        /**
         * Close input stream.
         */
        @Override public void close() throws IOException {
            in.close();
        }
    }

    /**
     *
     */
    private static class ClientRequestFuture extends GridFutureAdapter<byte[]> {
    }

    /** SSL Socket Factory. */
    private static class ClientSslSocketFactory {
        /** Trust manager ignoring all certificate checks. */
        private static final TrustManager ignoreErrorsTrustMgr = new X509TrustManager() {
            @Override public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
            }

            @Override public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
            }
        };

        /** Config. */
        private final ClientChannelConfiguration cfg;

        /** Constructor. */
        ClientSslSocketFactory(ClientChannelConfiguration cfg) {
            this.cfg = cfg;
        }

        /** Create SSL socket. */
        SSLSocket create() throws IOException {
            InetSocketAddress addr = cfg.getAddress();

            SSLSocket sock = (SSLSocket)getSslSocketFactory(cfg).createSocket(addr.getHostName(), addr.getPort());

            sock.setUseClientMode(true);

            sock.startHandshake();

            return sock;
        }

        /** Create SSL socket factory. */
        private static SSLSocketFactory getSslSocketFactory(ClientChannelConfiguration cfg) {
            Factory<SSLContext> sslCtxFactory = cfg.getSslContextFactory();

            if (sslCtxFactory != null) {
                try {
                    return sslCtxFactory.create().getSocketFactory();
                }
                catch (Exception e) {
                    throw new ClientError("SSL Context Factory failed", e);
                }
            }

            BiFunction<String, String, String> or = (val, dflt) -> val == null || val.isEmpty() ? dflt : val;

            String keyStore = or.apply(
                cfg.getSslClientCertificateKeyStorePath(),
                System.getProperty("javax.net.ssl.keyStore")
            );

            String keyStoreType = or.apply(
                cfg.getSslClientCertificateKeyStoreType(),
                or.apply(System.getProperty("javax.net.ssl.keyStoreType"), "JKS")
            );

            String keyStorePwd = or.apply(
                cfg.getSslClientCertificateKeyStorePassword(),
                System.getProperty("javax.net.ssl.keyStorePassword")
            );

            String trustStore = or.apply(
                cfg.getSslTrustCertificateKeyStorePath(),
                System.getProperty("javax.net.ssl.trustStore")
            );

            String trustStoreType = or.apply(
                cfg.getSslTrustCertificateKeyStoreType(),
                or.apply(System.getProperty("javax.net.ssl.trustStoreType"), "JKS")
            );

            String trustStorePwd = or.apply(
                cfg.getSslTrustCertificateKeyStorePassword(),
                System.getProperty("javax.net.ssl.trustStorePassword")
            );

            String algorithm = or.apply(cfg.getSslKeyAlgorithm(), "SunX509");

            String proto = toString(cfg.getSslProtocol());

            if (Stream.of(keyStore, keyStorePwd, keyStoreType, trustStore, trustStorePwd, trustStoreType)
                .allMatch(s -> s == null || s.isEmpty())
                ) {
                try {
                    return SSLContext.getDefault().getSocketFactory();
                }
                catch (NoSuchAlgorithmException e) {
                    throw new ClientError("Default SSL context cryptographic algorithm is not available", e);
                }
            }

            KeyManager[] keyManagers = getKeyManagers(algorithm, keyStore, keyStoreType, keyStorePwd);

            TrustManager[] trustManagers = cfg.isSslTrustAll() ?
                new TrustManager[] {ignoreErrorsTrustMgr} :
                getTrustManagers(algorithm, trustStore, trustStoreType, trustStorePwd);

            try {
                SSLContext sslCtx = SSLContext.getInstance(proto);

                sslCtx.init(keyManagers, trustManagers, null);

                return sslCtx.getSocketFactory();
            }
            catch (NoSuchAlgorithmException e) {
                throw new ClientError("SSL context cryptographic algorithm is not available", e);
            }
            catch (KeyManagementException e) {
                throw new ClientError("Failed to create SSL Context", e);
            }
        }

        /**
         * @return String representation of {@link SslProtocol} as required by {@link SSLContext}.
         */
        private static String toString(SslProtocol proto) {
            switch (proto) {
                case TLSv1_1:
                    return "TLSv1.1";

                case TLSv1_2:
                    return "TLSv1.2";

                default:
                    return proto.toString();
            }
        }

        /** */
        private static KeyManager[] getKeyManagers(
            String algorithm,
            String keyStore,
            String keyStoreType,
            String keyStorePwd
        ) {
            KeyManagerFactory keyMgrFactory;

            try {
                keyMgrFactory = KeyManagerFactory.getInstance(algorithm);
            }
            catch (NoSuchAlgorithmException e) {
                throw new ClientError("Key manager cryptographic algorithm is not available", e);
            }

            Predicate<String> empty = s -> s == null || s.isEmpty();

            if (!empty.test(keyStore) && !empty.test(keyStoreType)) {
                char[] pwd = (keyStorePwd == null) ? new char[0] : keyStorePwd.toCharArray();

                KeyStore store = loadKeyStore("Client", keyStore, keyStoreType, pwd);

                try {
                    keyMgrFactory.init(store, pwd);
                }
                catch (UnrecoverableKeyException e) {
                    throw new ClientError("Could not recover key store key", e);
                }
                catch (KeyStoreException e) {
                    throw new ClientError(
                        String.format("Client key store provider of type [%s] is not available", keyStoreType),
                        e
                    );
                }
                catch (NoSuchAlgorithmException e) {
                    throw new ClientError("Client key store integrity check algorithm is not available", e);
                }
            }

            return keyMgrFactory.getKeyManagers();
        }

        /** */
        private static TrustManager[] getTrustManagers(
            String algorithm,
            String trustStore,
            String trustStoreType,
            String trustStorePwd
        ) {
            TrustManagerFactory trustMgrFactory;

            try {
                trustMgrFactory = TrustManagerFactory.getInstance(algorithm);
            }
            catch (NoSuchAlgorithmException e) {
                throw new ClientError("Trust manager cryptographic algorithm is not available", e);
            }

            Predicate<String> empty = s -> s == null || s.isEmpty();

            if (!empty.test(trustStore) && !empty.test(trustStoreType)) {
                char[] pwd = (trustStorePwd == null) ? new char[0] : trustStorePwd.toCharArray();

                KeyStore store = loadKeyStore("Trust", trustStore, trustStoreType, pwd);

                try {
                    trustMgrFactory.init(store);
                }
                catch (KeyStoreException e) {
                    throw new ClientError(
                        String.format("Trust key store provider of type [%s] is not available", trustStoreType),
                        e
                    );
                }
            }

            return trustMgrFactory.getTrustManagers();
        }

        /** */
        private static KeyStore loadKeyStore(String lb, String path, String type, char[] pwd) {
            KeyStore store;

            try {
                store = KeyStore.getInstance(type);
            }
            catch (KeyStoreException e) {
                throw new ClientError(
                    String.format("%s key store provider of type [%s] is not available", lb, type),
                    e
                );
            }

            try (InputStream in = new FileInputStream(new File(path))) {

                store.load(in, pwd);

                return store;
            }
            catch (FileNotFoundException e) {
                throw new ClientError(String.format("%s key store file [%s] does not exist", lb, path), e);
            }
            catch (NoSuchAlgorithmException e) {
                throw new ClientError(
                    String.format("%s key store integrity check algorithm is not available", lb),
                    e
                );
            }
            catch (CertificateException e) {
                throw new ClientError(String.format("Could not load certificate from %s key store", lb), e);
            }
            catch (IOException e) {
                throw new ClientError(String.format("Could not read %s key store", lb), e);
            }
        }
    }
}
