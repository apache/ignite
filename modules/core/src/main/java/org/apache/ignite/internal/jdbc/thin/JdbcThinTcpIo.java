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

package org.apache.ignite.internal.jdbc.thin;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.ThinProtocolFeature;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryThreadLocalContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcProtocolContext;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQuery;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryCancelRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryFetchRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResponse;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcThinFeature;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcUtils;
import org.apache.ignite.internal.util.ipc.loopback.IpcClientTcpEndpoint;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static java.lang.Math.abs;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.nullableBooleanToByte;

/**
 * JDBC IO layer implementation based on blocking IPC streams.
 */
public class JdbcThinTcpIo {
    /** Version 2.1.0. */
    private static final ClientListenerProtocolVersion VER_2_1_0 = ClientListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    private static final ClientListenerProtocolVersion VER_2_1_5 = ClientListenerProtocolVersion.create(2, 1, 5);

    /** Version 2.3.1. */
    private static final ClientListenerProtocolVersion VER_2_3_0 = ClientListenerProtocolVersion.create(2, 3, 0);

    /** Version 2.4.0. */
    private static final ClientListenerProtocolVersion VER_2_4_0 = ClientListenerProtocolVersion.create(2, 4, 0);

    /** Version 2.5.0. */
    private static final ClientListenerProtocolVersion VER_2_5_0 = ClientListenerProtocolVersion.create(2, 5, 0);

    /** Version 2.7.0. */
    private static final ClientListenerProtocolVersion VER_2_7_0 = ClientListenerProtocolVersion.create(2, 7, 0);

    /** Version 2.8.0. */
    private static final ClientListenerProtocolVersion VER_2_8_0 = ClientListenerProtocolVersion.create(2, 8, 0);

    /** Version 2.9.0. Adds user attributes support. Adds features flags support. */
    private static final ClientListenerProtocolVersion VER_2_9_0 = ClientListenerProtocolVersion.create(2, 9, 0);

    /** Current version. */
    private static final ClientListenerProtocolVersion CURRENT_VER = VER_2_9_0;

    /** Initial output stream capacity for handshake. */
    private static final int HANDSHAKE_MSG_SIZE = 13;

    /** Initial output for query message. */
    private static final int DYNAMIC_SIZE_MSG_CAP = 256;

    /** Maximum batch query count. */
    private static final int MAX_BATCH_QRY_CNT = 32;

    /** Initial output for query fetch message. */
    private static final int QUERY_FETCH_MSG_SIZE = 13;

    /** Initial output for query fetch message. */
    private static final int QUERY_META_MSG_SIZE = 9;

    /** Initial output for query close message. */
    private static final int QUERY_CLOSE_MSG_SIZE = 9;

    /** Random. */
    private static final AtomicLong IDX_GEN = new AtomicLong(new Random(U.currentTimeMillis()).nextLong());

    /** Connection properties. */
    private final ConnectionProperties connProps;

    /** Socket address. */
    private final InetSocketAddress sockAddr;

    /** Endpoint. */
    private final IpcClientTcpEndpoint endpoint;

    /** Output stream. */
    private final BufferedOutputStream out;

    /** Input stream. */
    private final BufferedInputStream in;

    /** Connected flag. */
    private volatile boolean connected;

    /** Ignite server version. */
    private final IgniteProductVersion igniteVer;

    /** Node Id. */
    private final UUID nodeId;

    /** Connection mutex. */
    private final Object connMux = new Object();

    /** Current protocol version used to connection to Ignite. */
    private final ClientListenerProtocolVersion srvProtoVer;

    /** Protocol context (version, supported features, etc). */
    private JdbcProtocolContext protoCtx;

    /** Binary context for serialization/deserialization of binary objects. */
    private final BinaryContext ctx;

    /**
     * Start connection and perform handshake.
     *
     * @param connProps Connection properties.
     * @param sockAddr Socket address.
     * @param ctx Binary context for proper serialization/deserialization of binary objects.
     * @param timeout Socket connection timeout in ms.
     *
     * @throws SQLException On connection error or reject.
     * @throws IOException On IO error in handshake.
     */
    public JdbcThinTcpIo(ConnectionProperties connProps, InetSocketAddress sockAddr, BinaryContext ctx, int timeout)
        throws SQLException, IOException {
        this.connProps = connProps;
        this.sockAddr = sockAddr;
        this.ctx = ctx;

        Socket sock = null;

        try {
            if (ConnectionProperties.SSL_MODE_REQUIRE.equalsIgnoreCase(connProps.getSslMode()))
                sock = JdbcThinSSLUtil.createSSLSocket(sockAddr, connProps);
            else if (ConnectionProperties.SSL_MODE_DISABLE.equalsIgnoreCase(connProps.getSslMode())) {
                sock = new Socket();

                try {
                    sock.connect(sockAddr, timeout);
                }
                catch (IOException e) {
                    throw new SQLException("Failed to connect to server [host=" + sockAddr.getHostName() +
                        ", port=" + sockAddr.getPort() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
                }
            }
            else {
                throw new SQLException("Unknown sslMode. [sslMode=" + connProps.getSslMode() + ']',
                    SqlStateCode.CLIENT_CONNECTION_FAILED);
            }

            if (connProps.getSocketSendBuffer() != 0)
                sock.setSendBufferSize(connProps.getSocketSendBuffer());

            if (connProps.getSocketReceiveBuffer() != 0)
                sock.setReceiveBufferSize(connProps.getSocketReceiveBuffer());

            sock.setTcpNoDelay(connProps.isTcpNoDelay());

            BufferedOutputStream out = null;
            BufferedInputStream in = null;

            try {
                endpoint = new IpcClientTcpEndpoint(sock);

                out = new BufferedOutputStream(endpoint.outputStream());
                in = new BufferedInputStream(endpoint.inputStream());

                connected = true;

                this.in = in;
                this.out = out;
            }
            catch (IgniteCheckedException e) {
                U.closeQuiet(in);
                U.closeQuiet(out);

                throw new SQLException("Failed to connect to server [url=" + connProps.getUrl() +
                    " address=" + sockAddr + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }
        catch (Exception e) {
            if (sock != null && !sock.isClosed())
                U.closeQuiet(sock);

            throw e;
        }

        HandshakeResult handshakeRes = handshake(CURRENT_VER);

        igniteVer = handshakeRes.igniteVersion();

        nodeId = handshakeRes.nodeId();

        srvProtoVer = handshakeRes.serverProtocolVersion();

        protoCtx = new JdbcProtocolContext(srvProtoVer, handshakeRes.features(), connProps.isKeepBinary());
    }

    /**
     * Used for versions: 2.1.5 and 2.3.0. The protocol version is changed but handshake format isn't changed.
     *
     * @param ver JDBC client version.
     * @throws IOException On IO error.
     * @throws SQLException On connection reject.
     */
    private HandshakeResult handshake(ClientListenerProtocolVersion ver) throws IOException, SQLException {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextImpl(null, null));

        ctx.configure(marsh);

        BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte)ClientListenerRequest.HANDSHAKE);

        writer.writeShort(ver.major());
        writer.writeShort(ver.minor());
        writer.writeShort(ver.maintenance());

        writer.writeByte(ClientListenerNioListener.JDBC_CLIENT);

        writer.writeBoolean(connProps.isDistributedJoins());
        writer.writeBoolean(connProps.isEnforceJoinOrder());
        writer.writeBoolean(connProps.isCollocated());
        writer.writeBoolean(connProps.isReplicatedOnly());
        writer.writeBoolean(connProps.isAutoCloseServerCursor());
        writer.writeBoolean(connProps.isLazy());
        writer.writeBoolean(connProps.isSkipReducerOnUpdate());

        if (ver.compareTo(VER_2_7_0) >= 0)
            writer.writeString(connProps.nestedTxMode());

        if (ver.compareTo(VER_2_8_0) >= 0) {
            writer.writeByte(nullableBooleanToByte(connProps.isDataPageScanEnabled()));

            JdbcUtils.writeNullableInteger(writer, connProps.getUpdateBatchSize());
        }

        if (ver.compareTo(VER_2_9_0) >= 0) {
            String userAttrs = connProps.getUserAttributesFactory();

            if (F.isEmpty(userAttrs))
                writer.writeMap(null);
            else {
                try {
                    Class<Factory<Map<String, String>>> cls = (Class<Factory<Map<String, String>>>)
                        JdbcThinSSLUtil.class.getClassLoader().loadClass(userAttrs);

                    Map<String, String> attrs = cls.newInstance().create();

                    writer.writeMap(attrs);
                }
                catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                    throw new SQLException("Could not found user attributes factory class: " + userAttrs,
                        SqlStateCode.CLIENT_CONNECTION_FAILED, e);
                }
            }

            writer.writeByteArray(ThinProtocolFeature.featuresAsBytes(enabledFeatures()));
        }

        if (!F.isEmpty(connProps.getUsername())) {
            assert ver.compareTo(VER_2_5_0) >= 0 : "Authentication is supported since 2.5";

            writer.writeString(connProps.getUsername());
            writer.writeString(connProps.getPassword());
        }

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx, new BinaryHeapInputStream(read()),
            null, null, false);

        boolean accepted = reader.readBoolean();

        if (accepted) {
            HandshakeResult handshakeRes = new HandshakeResult();

            if (reader.available() > 0) {
                byte maj = reader.readByte();
                byte min = reader.readByte();
                byte maintenance = reader.readByte();

                String stage = reader.readString();

                long ts = reader.readLong();
                byte[] hash = reader.readByteArray();

                if (ver.compareTo(VER_2_8_0) >= 0)
                    handshakeRes.nodeId(reader.readUuid());

                handshakeRes.igniteVersion(new IgniteProductVersion(maj, min, maintenance, stage, ts, hash));

                if (ver.compareTo(VER_2_9_0) >= 0) {
                    byte[] srvFeatures = reader.readByteArray();

                    EnumSet<JdbcThinFeature> features = JdbcThinFeature.enumSet(srvFeatures);

                    handshakeRes.features(features);
                }
            }
            else {
                handshakeRes.igniteVersion(
                    new IgniteProductVersion((byte)2, (byte)0, (byte)0, "Unknown", 0L, null));
            }

            handshakeRes.serverProtocolVersion(ver);

            return handshakeRes;
        }
        else {
            short maj = reader.readShort();
            short min = reader.readShort();
            short maintenance = reader.readShort();

            String err = reader.readString();

            ClientListenerProtocolVersion srvProtoVer0 = ClientListenerProtocolVersion.create(maj, min, maintenance);

            if (srvProtoVer0.compareTo(VER_2_5_0) < 0 && !F.isEmpty(connProps.getUsername())) {
                throw new SQLException("Authentication doesn't support by remote server[driverProtocolVer="
                    + CURRENT_VER + ", remoteNodeProtocolVer=" + srvProtoVer0 + ", err=" + err
                    + ", url=" + connProps.getUrl() + " address=" + sockAddr + ']', SqlStateCode.CONNECTION_REJECTED);
            }

            if (VER_2_8_0.equals(srvProtoVer0)
                || VER_2_7_0.equals(srvProtoVer0)
                || VER_2_5_0.equals(srvProtoVer0)
                || VER_2_4_0.equals(srvProtoVer0)
                || VER_2_3_0.equals(srvProtoVer0)
                || VER_2_1_5.equals(srvProtoVer0))
                return handshake(srvProtoVer0);
            else if (VER_2_1_0.equals(srvProtoVer0))
                return handshake_2_1_0();
            else {
                throw new SQLException("Handshake failed [driverProtocolVer=" + CURRENT_VER +
                    ", remoteNodeProtocolVer=" + srvProtoVer0 + ", err=" + err + ']',
                    SqlStateCode.CONNECTION_REJECTED);
            }
        }
    }

    /**
     * Compatibility handshake for server version 2.1.0
     *
     * @throws IOException On IO error.
     * @throws SQLException On connection reject.
     */
    private HandshakeResult handshake_2_1_0() throws IOException, SQLException {
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte)ClientListenerRequest.HANDSHAKE);

        writer.writeShort(VER_2_1_0.major());
        writer.writeShort(VER_2_1_0.minor());
        writer.writeShort(VER_2_1_0.maintenance());

        writer.writeByte(ClientListenerNioListener.JDBC_CLIENT);

        writer.writeBoolean(connProps.isDistributedJoins());
        writer.writeBoolean(connProps.isEnforceJoinOrder());
        writer.writeBoolean(connProps.isCollocated());
        writer.writeBoolean(connProps.isReplicatedOnly());
        writer.writeBoolean(connProps.isAutoCloseServerCursor());

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()),
            null, null, false);

        boolean accepted = reader.readBoolean();

        if (accepted) {
            HandshakeResult handshakeRes = new HandshakeResult();

            handshakeRes.igniteVersion(
                new IgniteProductVersion((byte)2, (byte)1, (byte)0, "Unknown", 0L, null));

            handshakeRes.serverProtocolVersion(VER_2_1_0);

            return handshakeRes;
        }
        else {
            short maj = reader.readShort();
            short min = reader.readShort();
            short maintenance = reader.readShort();

            String err = reader.readString();

            ClientListenerProtocolVersion ver = ClientListenerProtocolVersion.create(maj, min, maintenance);

            throw new SQLException("Handshake failed [driverProtocolVer=" + CURRENT_VER +
                ", remoteNodeProtocolVer=" + ver + ", err=" + err + ']', SqlStateCode.CONNECTION_REJECTED);
        }
    }

    /**
     * @param req Request.
     * @throws IOException In case of IO error.
     * @throws SQLException On error.
     */
    void sendRequestNoWaitResponse(JdbcRequest req) throws IOException, SQLException {
        if (!isUnorderedStreamSupported()) {
            throw new SQLException("Streaming without response doesn't supported by server [driverProtocolVer="
                + CURRENT_VER + ", remoteNodeVer=" + igniteVer + ']', SqlStateCode.INTERNAL_ERROR);
        }

        sendRequestRaw(req);
    }

    /**
     * @param req Request.
     * @param stmt Statement.
     * @return Server response.
     * @throws IOException In case of IO error.
     */
    JdbcResponse sendRequest(JdbcRequest req, JdbcThinStatement stmt) throws IOException {
        if (stmt != null) {
            synchronized (stmt.cancellationMutex()) {
                if (stmt.isCancelled()) {
                    if (req instanceof JdbcQueryCloseRequest)
                        return new JdbcResponse(null);

                    return new JdbcResponse(IgniteQueryErrorCode.QUERY_CANCELED, QueryCancelledException.ERR_MSG);
                }

                sendRequestRaw(req);

                if (req instanceof JdbcQueryExecuteRequest || req instanceof JdbcBatchExecuteRequest)
                    stmt.currentRequestMeta(req.requestId(), this);
            }
        }
        else
            sendRequestRaw(req);

        JdbcResponse resp = readResponse();

        return stmt != null && stmt.isCancelled() ?
            new JdbcResponse(IgniteQueryErrorCode.QUERY_CANCELED, QueryCancelledException.ERR_MSG) :
            resp;
    }

    /**
     * Sends cancel request.
     *
     * @param cancellationReq contains request id to be cancelled
     * @throws IOException In case of IO error.
     */
    void sendCancelRequest(JdbcQueryCancelRequest cancellationReq) throws IOException {
        sendRequestRaw(cancellationReq);
    }

    /**
     * @return Server response.
     * @throws IOException In case of IO error.
     */
    JdbcResponse readResponse() throws IOException {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx, new BinaryHeapInputStream(read()), null, true);

        JdbcResponse res = new JdbcResponse();

        res.readBinary(reader, protoCtx);

        return res;
    }

    /**
     * Try to guess request capacity.
     *
     * @param req Request.
     * @return Expected capacity.
     */
    private static int guessCapacity(JdbcRequest req) {
        int cap;

        if (req instanceof JdbcBatchExecuteRequest) {
            List<JdbcQuery> qrys = ((JdbcBatchExecuteRequest)req).queries();

            int cnt = !F.isEmpty(qrys) ? Math.min(MAX_BATCH_QRY_CNT, qrys.size()) : 0;

            // One additional byte for autocommit and last batch flags.
            cap = cnt * DYNAMIC_SIZE_MSG_CAP + 2;
        }
        else if (req instanceof JdbcQueryCloseRequest)
            cap = QUERY_CLOSE_MSG_SIZE;
        else if (req instanceof JdbcQueryMetadataRequest)
            cap = QUERY_META_MSG_SIZE;
        else if (req instanceof JdbcQueryFetchRequest)
            cap = QUERY_FETCH_MSG_SIZE;
        else
            cap = DYNAMIC_SIZE_MSG_CAP;

        return cap;
    }

    /**
     * @param req Request.
     * @throws IOException In case of IO error.
     */
    private void sendRequestRaw(JdbcRequest req) throws IOException {
        int cap = guessCapacity(req);

        BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, new BinaryHeapOutputStream(cap),
            BinaryThreadLocalContext.get().schemaHolder(), null);

        req.writeBinary(writer, protoCtx);

        synchronized (connMux) {
            send(writer.array());
        }
    }

    /**
     * @param req JDBC request bytes.
     * @throws IOException On error.
     */
    private void send(byte[] req) throws IOException {
        int size = req.length;

        out.write(size & 0xFF);
        out.write((size >> 8) & 0xFF);
        out.write((size >> 16) & 0xFF);
        out.write((size >> 24) & 0xFF);

        out.write(req);

        out.flush();
    }

    /**
     * @return Bytes of a response from server.
     * @throws IOException On error.
     */
    private byte[] read() throws IOException {
        byte[] sizeBytes = read(4);

        int msgSize = (((0xFF & sizeBytes[3]) << 24) | ((0xFF & sizeBytes[2]) << 16)
            | ((0xFF & sizeBytes[1]) << 8) + (0xFF & sizeBytes[0]));

        return read(msgSize);
    }

    /**
     * @param size Count of bytes to read from stream.
     * @return Read bytes.
     * @throws IOException On error.
     */
    private byte[] read(int size) throws IOException {
        int off = 0;

        byte[] data = new byte[size];

        while (off != size) {
            int res = in.read(data, off, size - off);

            if (res == -1)
                throw new IOException("Failed to read incoming message (not enough data).");

            off += res;
        }

        return data;
    }

    /**
     * Close the client IO.
     */
    public void close() {
        if (!connected)
            return;

        // Clean up resources.
        U.closeQuiet(out);
        U.closeQuiet(in);

        if (endpoint != null)
            endpoint.close();

        connected = false;
    }

    /**
     * @return Connection properties.
     */
    public ConnectionProperties connectionProperties() {
        return connProps;
    }

    /**
     * @return Ignite server version.
     */
    IgniteProductVersion igniteVersion() {
        return igniteVer;
    }

    /**
     * @return {@code true} If the unordered streaming supported.
     */
    boolean isUnorderedStreamSupported() {
        assert srvProtoVer != null;

        return srvProtoVer.compareTo(VER_2_5_0) >= 0;
    }

    /**
     * @return True if query cancellation supported, false otherwise.
     */
    boolean isQueryCancellationSupported() {
        assert srvProtoVer != null;

        return srvProtoVer.compareTo(VER_2_8_0) >= 0;
    }

    /**
     * @return True if partition awareness supported, false otherwise.
     */
    boolean isPartitionAwarenessSupported() {
        assert srvProtoVer != null;

        return srvProtoVer.compareTo(VER_2_8_0) >= 0;
    }

    /**
     * Whether custom objects are supported by the server or not.
     *
     * @return {@code true} if custom objects are supported, {@code false} otherwise.
     */
    boolean isCustomObjectSupported() {
        return protoCtx.isFeatureSupported(JdbcThinFeature.CUSTOM_OBJECT);
    }

    /**
     * Get next server index.
     *
     * @param len Number of servers.
     * @return Index of the next server to connect to.
     */
    private static int nextServerIndex(int len) {
        if (len == 1)
            return 0;
        else {
            long nextIdx = IDX_GEN.getAndIncrement();

            return (int)(abs(nextIdx) % len);
        }
    }

    /**
     * Enable/disable socket timeout with specified timeout.
     *
     * @param ms the specified timeout, in milliseconds.
     * @throws SQLException if there is an error in the underlying protocol.
     */
    public void timeout(int ms) throws SQLException {
        endpoint.timeout(ms);
    }

    /**
     * Returns socket timeout.
     *
     * @throws SQLException if there is an error in the underlying protocol.
     */
    public int timeout() throws SQLException {
        return endpoint.timeout();
    }

    /**
     * @return Node Id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Socket address.
     */
    public InetSocketAddress socketAddress() {
        return sockAddr;
    }

    /**
     * @return Connected flag.
     */
    public boolean connected() {
        return connected;
    }

    /**
     * Set of features enabled on client side. To get features supported by both sides use {@link #protoCtx}.
     * Since {@link #protoCtx} only available after handshake, any handshake protocol change should not use features,
     * but increment protocol version.
     */
    private EnumSet<JdbcThinFeature> enabledFeatures() {
        EnumSet<JdbcThinFeature> features = JdbcThinFeature.allFeaturesAsEnumSet();

        String disabledFeaturesStr = connProps.disabledFeatures();

        if (Objects.isNull(disabledFeaturesStr))
            return features;

        for (String f : disabledFeaturesStr.split("\\W+"))
            features.remove(JdbcThinFeature.valueOf(f.toUpperCase()));

        return features;
    }
}
