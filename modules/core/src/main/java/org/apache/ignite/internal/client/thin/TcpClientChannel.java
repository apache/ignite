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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.ThinProtocolFeature;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.client.thin.ProtocolVersion.LATEST_VER;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_0_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_1_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_2_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_3_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_4_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_5_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_6_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_7_0;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel {
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

    /** Timeout before next attempt to lock channel and process next response by current thread. */
    private static final long PAYLOAD_WAIT_TIMEOUT = 10L;

    /** Protocol context. */
    private ProtocolContext protocolContext;

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

    /** Receive lock. */
    private final Lock rcvLock = new ReentrantLock();

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Topology change listeners. */
    private final Collection<Consumer<ClientChannel>> topChangeLsnrs = new CopyOnWriteArrayList<>();

    /** Constructor. */
    TcpClientChannel(ClientChannelConfiguration cfg) throws ClientConnectionException, ClientAuthenticationException {
        validateConfiguration(cfg);

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
    @Override public void close() throws Exception {
        dataInput.close();
        out.close();
        sock.close();

        for (ClientRequestFuture pendingReq : pendingReqs.values())
            pendingReq.onDone(new ClientConnectionException("Channel is closed"));
    }

    /** {@inheritDoc} */
    @Override public <T> T service(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader) throws ClientConnectionException, ClientAuthorizationException {
        long id = send(op, payloadWriter);

        return receive(id, payloadReader);
    }

    /**
     * @param op Operation.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @return Request ID.
     */
    private long send(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter)
        throws ClientConnectionException {
        long id = reqId.getAndIncrement();

        // Only one thread at a time can have access to write to the channel.
        sndLock.lock();

        try (PayloadOutputChannel payloadCh = new PayloadOutputChannel(this)) {
            pendingReqs.put(id, new ClientRequestFuture());

            BinaryOutputStream req = payloadCh.out();

            req.writeInt(0); // Reserve an integer for the request size.
            req.writeShort(op.code());
            req.writeLong(id);

            if (payloadWriter != null)
                payloadWriter.accept(payloadCh);

            req.writeInt(0, req.position() - 4); // Actual size.

            write(req.array(), req.position());
        }
        catch (Throwable t) {
            pendingReqs.remove(id);

            throw t;
        }
        finally {
            sndLock.unlock();
        }

        return id;
    }

    /**
     * @param reqId ID of the request to receive the response for.
     * @param payloadReader Payload reader from stream.
     * @return Received operation payload or {@code null} if response has no payload.
     */
    private <T> T receive(long reqId, Function<PayloadInputChannel, T> payloadReader)
        throws ClientConnectionException, ClientAuthorizationException {
        ClientRequestFuture pendingReq = pendingReqs.get(reqId);

        assert pendingReq != null : "Pending request future not found for request " + reqId;

        // Each thread creates a future on request sent and returns a response when this future is completed.
        // Only one thread at a time can have access to read from the channel. This thread reads the next available
        // response and complete corresponding future. All other concurrent threads wait for their own futures with
        // a timeout and periodically try to lock the channel to process the next response.
        try {
            while (true) {
                if (rcvLock.tryLock()) {
                    try {
                        if (!pendingReq.isDone())
                            processNextResponse();
                    }
                    finally {
                        rcvLock.unlock();
                    }
                }

                try {
                    byte[] payload = pendingReq.get(PAYLOAD_WAIT_TIMEOUT);

                    if (payload == null || payloadReader == null)
                        return null;

                    return payloadReader.apply(new PayloadInputChannel(this, payload));
                }
                catch (IgniteFutureTimeoutCheckedException ignore) {
                    // Next cycle if timed out.
                }
            }
        }
        catch (IgniteCheckedException e) {
            if (e.getCause() instanceof ClientError)
                throw (ClientError)e.getCause();

            if (e.getCause() instanceof ClientException)
                throw (ClientException)e.getCause();

            throw new ClientException(e.getMessage(), e);
        }
        finally {
            pendingReqs.remove(reqId);
        }
    }

    /**
     * Process next response from the input stream and complete corresponding future.
     */
    private void processNextResponse() throws ClientProtocolError, ClientConnectionException {
        int resSize = dataInput.readInt();

        if (resSize <= 0)
            throw new ClientProtocolError(String.format("Invalid response size: %s", resSize));

        long bytesReadOnStartReq = dataInput.totalBytesRead();

        long resId = dataInput.readLong();

        ClientRequestFuture pendingReq = pendingReqs.get(resId);

        if (pendingReq == null)
            throw new ClientProtocolError(String.format("Unexpected response ID [%s]", resId));

        int status = 0;

        BinaryInputStream resIn;

        if (protocolContext.isPartitionAwarenessSupported()) {
            short flags = dataInput.readShort();

            if ((flags & ClientFlag.AFFINITY_TOPOLOGY_CHANGED) != 0) {
                long topVer = dataInput.readLong();
                int minorTopVer = dataInput.readInt();

                srvTopVer = new AffinityTopologyVersion(topVer, minorTopVer);

                for (Consumer<ClientChannel> lsnr : topChangeLsnrs)
                    lsnr.accept(this);
            }

            if ((flags & ClientFlag.ERROR) != 0)
                status = dataInput.readInt();
        }
        else
            status = dataInput.readInt();

        int hdrSize = (int)(dataInput.totalBytesRead() - bytesReadOnStartReq);

        if (status == 0) {
            if (resSize <= hdrSize)
                pendingReq.onDone();
            else
                pendingReq.onDone(dataInput.read(resSize - hdrSize));
        }
        else {
            resIn = new BinaryHeapInputStream(dataInput.read(resSize - hdrSize));

            String err = new BinaryReaderExImpl(null, resIn, null, true).readString();

            switch (status) {
                case ClientStatus.SECURITY_VIOLATION:
                    pendingReq.onDone(new ClientAuthorizationException());
                default:
                    pendingReq.onDone(new ClientServerError(err, status, resId));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public ProtocolContext protocolContext() {
        return protocolContext;
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
        throws ClientConnectionException, ClientAuthenticationException {
        handshakeReq(ver, user, pwd, userAttrs);
        handshakeRes(ver, user, pwd, userAttrs);
    }

    /** Send handshake request. */
    private void handshakeReq(ProtocolVersion proposedVer, String user, String pwd,
        Map<String, String> userAttrs) throws ClientConnectionException {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);
        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, new BinaryHeapOutputStream(32), null, null)) {
            ProtocolContext protocolContext = protocolContextFromVersion(proposedVer);

            writer.writeInt(0); // reserve an integer for the request size
            writer.writeByte((byte) ClientListenerRequest.HANDSHAKE);

            writer.writeShort(proposedVer.major());
            writer.writeShort(proposedVer.minor());
            writer.writeShort(proposedVer.patch());

            writer.writeByte(ClientListenerNioListener.THIN_CLIENT);

            if (protocolContext.isFeaturesSupported()) {
                byte[] features = ThinProtocolFeature.featuresAsBytes(protocolContext.features());
                writer.writeByteArray(features);
            }

            if (protocolContext.isUserAttributesSupported())
                writer.writeMap(userAttrs);

            if (protocolContext.isAuthorizationSupported() && user != null && !user.isEmpty()) {
                writer.writeString(user);
                writer.writeString(pwd);
            }

            writer.out().writeInt(0, writer.out().position() - 4);// actual size

            write(writer.array(), writer.out().position());
        }
    }

    /**
     * @param version Protocol version.
     * @return Protocol context for a version.
     */
    private ProtocolContext protocolContextFromVersion(ProtocolVersion version) {
        EnumSet<ProtocolFeature> features = null;
        if (ProtocolContext.isFeaturesSupported(version))
            features = ProtocolFeature.allFeaturesAsEnumSet();

        return new ProtocolContext(version, features);
    }

    /** Receive and handle handshake response. */
    private void handshakeRes(ProtocolVersion proposedVer, String user, String pwd, Map<String, String> userAttrs)
        throws ClientConnectionException, ClientAuthenticationException {
        int resSize = dataInput.readInt();

        if (resSize <= 0)
            throw new ClientProtocolError(String.format("Invalid handshake response size: %s", resSize));

        BinaryInputStream res = new BinaryHeapInputStream(dataInput.read(resSize));

        try (BinaryReaderExImpl reader = new BinaryReaderExImpl(null, res, null, true)) {

            boolean success = res.readBoolean();
            if (success) {
                byte[] features = new byte[0];
                if (ProtocolContext.isFeaturesSupported(proposedVer)) {
                    features = reader.readByteArray();
                }

                protocolContext = new ProtocolContext(proposedVer, ProtocolFeature.enumSet(features));

                if (protocolContext.isPartitionAwarenessSupported()) {
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
                    (srvVer.compareTo(V1_1_0) < 0 && !F.isEmpty(user)))
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
    private class ByteCountingDataInput {
        /** Input stream. */
        private final InputStream in;

        /** Total bytes read from the input stream. */
        private long totalBytesRead;

        /** Temporary buffer to read long, int and short values. */
        private byte[] tmpBuf = new byte[Long.BYTES];

        /**
         * @param in Input stream.
         */
        public ByteCountingDataInput(InputStream in) {
            this.in = in;
        }

        /**
         * Read bytes from the input stream to the buffer.
         *
         * @param bytes Bytes buffer.
         * @param len Length.
         */
        private void read(byte[] bytes, int len) throws ClientConnectionException {
            int bytesNum;
            int readBytesNum = 0;

            while (readBytesNum < len) {
                try {
                    bytesNum = in.read(bytes, readBytesNum, len - readBytesNum);
                }
                catch (IOException e) {
                    throw handleIOError(e);
                }

                if (bytesNum < 0)
                    throw handleIOError(null);

                readBytesNum += bytesNum;
            }

            totalBytesRead += readBytesNum;
        }

        /** Read bytes from the input stream. */
        public byte[] read(int len) throws ClientConnectionException {
            byte[] bytes = new byte[len];

            read(bytes, len);

            return bytes;
        }

        /**
         * Read long value from the input stream.
         */
        public long readLong() throws ClientConnectionException {
            read(tmpBuf, Long.BYTES);

            return BinaryPrimitives.readLong(tmpBuf, 0);
        }

        /**
         * Read int value from the input stream.
         */
        public int readInt() throws ClientConnectionException {
            read(tmpBuf, Integer.BYTES);

            return BinaryPrimitives.readInt(tmpBuf, 0);
        }

        /**
         * Read short value from the input stream.
         */
        public short readShort() throws ClientConnectionException {
            read(tmpBuf, Short.BYTES);

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
        public void close() throws IOException {
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
        private static TrustManager ignoreErrorsTrustMgr = new X509TrustManager() {
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
