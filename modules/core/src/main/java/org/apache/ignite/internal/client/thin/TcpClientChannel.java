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
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOffheapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel {
    /** Protocol version: 1.2.0. */
    private static final ProtocolVersion V1_2_0 = new ProtocolVersion((short)1, (short)2, (short)0);
    
    /** Protocol version: 1.1.0. */
    private static final ProtocolVersion V1_1_0 = new ProtocolVersion((short)1, (short)1, (short)0);

    /** Protocol version 1 0 0. */
    private static final ProtocolVersion V1_0_0 = new ProtocolVersion((short)1, (short)0, (short)0);

    /** Supported protocol versions. */
    private static final Collection<ProtocolVersion> supportedVers = Arrays.asList(
        V1_2_0, 
        V1_1_0, 
        V1_0_0
    );

    /** Protocol version agreed with the server. */
    private ProtocolVersion ver = V1_2_0;

    /** Channel. */
    private final Socket sock;

    /** Output stream. */
    private final OutputStream out;

    /** Input stream. */
    private final InputStream in;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Constructor. */
    TcpClientChannel(ClientChannelConfiguration cfg) throws ClientConnectionException, ClientAuthenticationException {
        validateConfiguration(cfg);

        try {
            sock = createSocket(cfg);

            out = sock.getOutputStream();
            in = sock.getInputStream();
        }
        catch (IOException e) {
            throw new ClientConnectionException(e);
        }

        handshake(cfg.getUserName(), cfg.getUserPassword());
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        in.close();
        out.close();
        sock.close();
    }

    /** {@inheritDoc} */
    @Override public long send(ClientOperation op, Consumer<BinaryOutputStream> payloadWriter)
        throws ClientConnectionException {
        long id = reqId.getAndIncrement();

        try (BinaryOutputStream req = new BinaryHeapOutputStream(1024)) {
            req.writeInt(0); // reserve an integer for the request size
            req.writeShort(op.code());
            req.writeLong(id);

            if (payloadWriter != null)
                payloadWriter.accept(req);

            req.writeInt(0, req.position() - 4); // actual size

            write(req.array(), req.position());
        }

        return id;
    }

    /** {@inheritDoc} */
    public <T> T receive(ClientOperation op, long reqId, Function<BinaryInputStream, T> payloadReader)
        throws ClientConnectionException, ClientAuthorizationException {

        final int MIN_RES_SIZE = 8 + 4; // minimal response size: long (8 bytes) ID + int (4 bytes) status

        int resSize = new BinaryHeapInputStream(read(4)).readInt();

        if (resSize < 0)
            throw new ClientProtocolError(String.format("Invalid response size: %s", resSize));

        if (resSize == 0)
            return null;

        BinaryInputStream resIn = new BinaryHeapInputStream(read(MIN_RES_SIZE));

        long resId = resIn.readLong();

        if (resId != reqId)
            throw new ClientProtocolError(String.format("Unexpected response ID [%s], [%s] was expected", resId, reqId));

        int status = resIn.readInt();

        if (status != 0) {
            resIn = new BinaryHeapInputStream(read(resSize - MIN_RES_SIZE));

            String err = new BinaryReaderExImpl(null, resIn, null, true).readString();

            switch (status) {
                case ClientStatus.SECURITY_VIOLATION:
                    throw new ClientAuthorizationException();
                default:
                    throw new ClientServerError(err, status, reqId);
            }
        }

        if (resSize <= MIN_RES_SIZE || payloadReader == null)
            return null;

        BinaryInputStream payload = new BinaryHeapInputStream(read(resSize - MIN_RES_SIZE));

        return payloadReader.apply(payload);
    }

    /** {@inheritDoc} */
    public ProtocolVersion serverVersion() {
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
        try (BinaryOutputStream req = new BinaryOffheapOutputStream(32)) {
            req.writeInt(0); // reserve an integer for the request size
            req.writeByte((byte)1); // handshake code, always 1
            req.writeShort(ver.major());
            req.writeShort(ver.minor());
            req.writeShort(ver.patch());
            req.writeByte((byte)2); // client code, always 2

            if (ver.compareTo(V1_1_0) >= 0 && user != null && user.length() > 0) {
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
        int resSize = new BinaryHeapInputStream(read(4)).readInt();

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
                    (srvVer.compareTo(V1_1_0) < 0 && user != null && user.length() > 0))
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
                throw new ClientConnectionException(e);
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
                throw new ClientConnectionException(e);
            }

            if (bytesNum < 0)
                throw new ClientConnectionException();

            readBytesNum += bytesNum;
        }

        return bytes;
    }

    /** Write bytes to the output stream. */
    private void write(byte[] bytes, int len) throws ClientConnectionException {
        try {
            out.write(bytes, 0, len);
            out.flush();
        }
        catch (IOException e) {
            throw new ClientConnectionException(e);
        }
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

            BiFunction<String, String, String> or = (val, dflt) -> val == null || val.length() == 0 ? dflt : val;

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
                .allMatch(s -> s == null || s.length() == 0)
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

            Predicate<String> empty = s -> s == null || s.length() == 0;

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

            Predicate<String> empty = s -> s == null || s.length() == 0;

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
