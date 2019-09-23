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

import java.io.DataInput;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_0_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_1_0;
import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_2_0;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel {
    /** Supported protocol versions. */
    private static final Collection<ProtocolVersion> supportedVers = Arrays.asList(
        V1_2_0,
        V1_1_0, 
        V1_0_0
    );

    /** Timeout before next attempt to lock channel and process next response by current thread. */
    private static final long PAYLOAD_WAIT_TIMEOUT = 10L;

    /** Protocol version agreed with the server. */
    private ProtocolVersion ver = V1_2_0;

    /** Channel. */
    private final Socket sock;

    /** Output stream. */
    private final OutputStream out;

    /** Input stream. */
    private final InputStream in;

    /** Data input. */
    private final DataInput dataInput;

    /** Total bytes read by channel. */
    private long totalBytesRead;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Send lock. */
    private final Lock sndLock = new ReentrantLock();

    /** Receive lock. */
    private final Lock rcvLock = new ReentrantLock();

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Constructor. */
    TcpClientChannel(ClientChannelConfiguration cfg) throws ClientConnectionException, ClientAuthenticationException {
        validateConfiguration(cfg);

        try {
            sock = createSocket(cfg);

            out = sock.getOutputStream();
            in = sock.getInputStream();

            GridUnsafeDataInput dis = new GridUnsafeDataInput();
            dis.inputStream(in);
            dataInput = dis;
        }
        catch (IOException e) {
            throw handleIOError("addr=" + cfg.getAddress(), e);
        }

        handshake(cfg.getUserName(), cfg.getUserPassword());
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        in.close();
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
        int resSize = readInt();

        if (resSize <= 0)
            throw new ClientProtocolError(String.format("Invalid response size: %s", resSize));

        long bytesReadOnStartReq = totalBytesRead;

        long resId = readLong();

        ClientRequestFuture pendingReq = pendingReqs.get(resId);

        if (pendingReq == null)
            throw new ClientProtocolError(String.format("Unexpected response ID [%s]", resId));

        int status;

        BinaryInputStream resIn;

        status = readInt();

        int hdrSize = (int)(totalBytesRead - bytesReadOnStartReq);

        if (status == 0) {
            if (resSize <= hdrSize)
                pendingReq.onDone();
            else
                pendingReq.onDone(read(resSize - hdrSize));
        }
        else {
            resIn = new BinaryHeapInputStream(read(resSize - hdrSize));

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
    @Override public ProtocolVersion serverVersion() {
        return ver;
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

    /** Serialize String for thin client protocol. */
    private static byte[] marshalString(String s) {
        try (BinaryOutputStream out = new BinaryHeapOutputStream(s == null ? 1 : s.length() + 20);
             BinaryRawWriterEx writer = new BinaryWriterExImpl(null, out, null, null)
        ) {
            writer.writeString(s);

            return out.arrayCopy();
        }
    }

    /** Client handshake. */
    private void handshake(String user, String pwd)
        throws ClientConnectionException, ClientAuthenticationException {
        handshakeReq(user, pwd);
        handshakeRes(user, pwd);
    }

    /** Send handshake request. */
    private void handshakeReq(String user, String pwd) throws ClientConnectionException {
        try (BinaryOutputStream req = new BinaryHeapOutputStream(32)) {
            req.writeInt(0); // reserve an integer for the request size
            req.writeByte((byte)1); // handshake code, always 1
            req.writeShort(ver.major());
            req.writeShort(ver.minor());
            req.writeShort(ver.patch());
            req.writeByte((byte)2); // client code, always 2

            if (ver.compareTo(V1_1_0) >= 0 && user != null && !user.isEmpty()) {
                req.writeByteArray(marshalString(user));
                req.writeByteArray(marshalString(pwd));
            }

            req.writeInt(0, req.position() - 4); // actual size

            write(req.array(), req.position());
        }
    }

    /** Receive and handle handshake response. */
    private void handshakeRes(String user, String pwd)
        throws ClientConnectionException, ClientAuthenticationException {
        int resSize = readInt();

        if (resSize <= 0)
            throw new ClientProtocolError(String.format("Invalid handshake response size: %s", resSize));

        BinaryInputStream res = new BinaryHeapInputStream(read(resSize));

        if (!res.readBoolean()) { // success flag
            ProtocolVersion srvVer = new ProtocolVersion(res.readShort(), res.readShort(), res.readShort());

            try (BinaryReaderExImpl r = new BinaryReaderExImpl(null, res, null, true)) {
                String err = r.readString();

                int errCode = ClientStatus.FAILED;

                if (res.remaining() > 0)
                    errCode = r.readInt();

                if (errCode == ClientStatus.AUTH_FAILED)
                    throw new ClientAuthenticationException(err);
                else if (ver.equals(srvVer))
                    throw new ClientProtocolError(err);
                else if (!supportedVers.contains(srvVer) ||
                    (srvVer.compareTo(V1_1_0) < 0 && user != null && !user.isEmpty()))
                    // Server version is not supported by this client OR server version is less than 1.1.0 supporting
                    // authentication and authentication is required.
                    throw new ClientProtocolError(String.format(
                        "Protocol version mismatch: client %s / server %s. Server details: %s",
                        ver,
                        srvVer,
                        err
                    ));
                else { // retry with server version
                    ver = srvVer;

                    handshake(user, pwd);
                }
            }
            catch (IOException e) {
                throw handleIOError(e);
            }
        }
    }

    /** Read bytes from the input stream. */
    private byte[] read(int len) throws ClientConnectionException {
        byte[] bytes = new byte[len];
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

        return bytes;
    }

    /**
     * Read long value from input stream.
     */
    private long readLong() {
        try {
            long val = dataInput.readLong();

            totalBytesRead += Long.BYTES;

            return val;
        }
        catch (IOException e) {
            throw handleIOError(e);
        }
    }

    /**
     * Read int value from input stream.
     */
    private int readInt() {
        try {
            int val = dataInput.readInt();

            totalBytesRead += Integer.BYTES;

            return val;
        }
        catch (IOException e) {
            throw handleIOError(e);
        }
    }

    /**
     * Read short value from input stream.
     */
    private short readShort() {
        try {
            short val = dataInput.readShort();

            totalBytesRead += Short.BYTES;

            return val;
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
