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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryFetchRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResponse;
import org.apache.ignite.internal.util.ipc.loopback.IpcClientTcpEndpoint;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * JDBC IO layer implementation based on blocking IPC streams.
 */
public class JdbcThinTcpIo {
    /** Version 2.1.0. */
    private static final ClientListenerProtocolVersion VER_2_1_0 = ClientListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.3.1. */
    private static final ClientListenerProtocolVersion VER_2_3_1 = ClientListenerProtocolVersion.create(2, 3, 1);

    /** Current version. */
    private static final ClientListenerProtocolVersion CURRENT_VER = VER_2_3_1;

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

    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /** Distributed joins. */
    private final boolean distributedJoins;

    /** Enforce join order. */
    private final boolean enforceJoinOrder;

    /** Collocated flag. */
    private final boolean collocated;

    /** Replicated only flag. */
    private final boolean replicatedOnly;

    /** Lazy execution query flag. */
    private final boolean lazy;

    /** Flag to automatically close server cursor. */
    private final boolean autoCloseServerCursor;

    /** Socket send buffer. */
    private final int sockSndBuf;

    /** Socket receive buffer. */
    private final int sockRcvBuf;

    /** TCP no delay flag. */
    private final boolean tcpNoDelay;

    /** Endpoint. */
    private IpcClientTcpEndpoint endpoint;

    /** Output stream. */
    private BufferedOutputStream out;

    /** Input stream. */
    private BufferedInputStream in;

    /** Closed flag. */
    private boolean closed;

    /** Ignite server version. */
    private IgniteProductVersion igniteVer;

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param collocated Collocated flag.
     * @param replicatedOnly Replicated only flag.
     * @param autoCloseServerCursor Flag to automatically close server cursors.
     * @param lazy Lazy execution query flag.
     * @param sockSndBuf Socket send buffer.
     * @param sockRcvBuf Socket receive buffer.
     * @param tcpNoDelay TCP no delay flag.
     */
    JdbcThinTcpIo(String host, int port, boolean distributedJoins, boolean enforceJoinOrder, boolean collocated,
        boolean replicatedOnly, boolean autoCloseServerCursor, boolean lazy, int sockSndBuf, int sockRcvBuf,
        boolean tcpNoDelay) {
        this.host = host;
        this.port = port;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.collocated = collocated;
        this.replicatedOnly = replicatedOnly;
        this.autoCloseServerCursor = autoCloseServerCursor;
        this.lazy = lazy;
        this.sockSndBuf = sockSndBuf;
        this.sockRcvBuf = sockRcvBuf;
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * @throws SQLException On connection error or reject.
     * @throws IOException On IO error in handshake.
     */
    public void start() throws SQLException, IOException {
        Socket sock = new Socket();

        if (sockSndBuf != 0)
            sock.setSendBufferSize(sockSndBuf);

        if (sockRcvBuf != 0)
            sock.setReceiveBufferSize(sockRcvBuf);

        sock.setTcpNoDelay(tcpNoDelay);

        try {
            sock.connect(new InetSocketAddress(host, port));

            endpoint = new IpcClientTcpEndpoint(sock);

            out = new BufferedOutputStream(endpoint.outputStream());
            in = new BufferedInputStream(endpoint.inputStream());
        }
        catch (IOException | IgniteCheckedException e) {
            throw new SQLException("Failed to connect to server [host=" + host + ", port=" + port + ']',
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }

        handshake();
    }

    /**
     * @throws IOException On IO error.
     * @throws SQLException On connection reject.
     */
    public void handshake() throws IOException, SQLException {
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte) ClientListenerRequest.HANDSHAKE);

        writer.writeShort(CURRENT_VER.major());
        writer.writeShort(CURRENT_VER.minor());
        writer.writeShort(CURRENT_VER.maintenance());

        writer.writeByte(ClientListenerNioListener.JDBC_CLIENT);

        writer.writeBoolean(distributedJoins);
        writer.writeBoolean(enforceJoinOrder);
        writer.writeBoolean(collocated);
        writer.writeBoolean(replicatedOnly);
        writer.writeBoolean(autoCloseServerCursor);
        writer.writeBoolean(lazy);

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()),
            null, null, false);

        boolean accepted = reader.readBoolean();

        if (accepted) {
            if (reader.available() > 0) {
                byte maj = reader.readByte();
                byte min = reader.readByte();
                byte maintenance = reader.readByte();

                String stage = reader.readString();

                long ts = reader.readLong();
                byte[] hash = reader.readByteArray();

                igniteVer = new IgniteProductVersion(maj, min, maintenance, stage, ts, hash);
            }
            else
                igniteVer = new IgniteProductVersion((byte)2, (byte)0, (byte)0, "Unknown", 0L, null);
        }
        else {
            short maj = reader.readShort();
            short min = reader.readShort();
            short maintenance = reader.readShort();

            String err = reader.readString();

            ClientListenerProtocolVersion srvProtocolVer = ClientListenerProtocolVersion.create(maj, min, maintenance);

            if (VER_2_1_0.equals(srvProtocolVer))
                handshake_2_1_0();
            else {
                throw new SQLException("Handshake failed [driverProtocolVer=" + CURRENT_VER +
                    ", remoteNodeProtocolVer=" + srvProtocolVer + ", err=" + err + ']',
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
    private void handshake_2_1_0() throws IOException, SQLException {
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte) ClientListenerRequest.HANDSHAKE);

        writer.writeShort(VER_2_1_0.major());
        writer.writeShort(VER_2_1_0.minor());
        writer.writeShort(VER_2_1_0.maintenance());

        writer.writeByte(ClientListenerNioListener.JDBC_CLIENT);

        writer.writeBoolean(distributedJoins);
        writer.writeBoolean(enforceJoinOrder);
        writer.writeBoolean(collocated);
        writer.writeBoolean(replicatedOnly);
        writer.writeBoolean(autoCloseServerCursor);

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()),
            null, null, false);

        boolean accepted = reader.readBoolean();

        if (accepted)
            igniteVer = new IgniteProductVersion((byte)2, (byte)1, (byte)0, "Unknown", 0L, null);
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
     * @return Server response.
     * @throws IOException In case of IO error.
     */
    @SuppressWarnings("unchecked")
    JdbcResponse sendRequest(JdbcRequest req) throws IOException {
        int cap = guessCapacity(req);

        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(cap), null, null);

        req.writeBinary(writer);

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()), null, null, false);

        JdbcResponse res = new JdbcResponse();

        res.readBinary(reader);

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
            int cnt = Math.min(MAX_BATCH_QRY_CNT, ((JdbcBatchExecuteRequest)req).queries().size());

            cap = cnt * DYNAMIC_SIZE_MSG_CAP;
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

        int msgSize  = (((0xFF & sizeBytes[3]) << 24) | ((0xFF & sizeBytes[2]) << 16)
            | ((0xFF & sizeBytes[1]) << 8) + (0xFF & sizeBytes[0]));

        return read(msgSize);
    }

    /**
     * @param size Count of bytes to read from stream.
     * @return Read bytes.
     * @throws IOException On error.
     */
    private byte [] read(int size) throws IOException {
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
        if (closed)
            return;

        // Clean up resources.
        U.closeQuiet(out);
        U.closeQuiet(in);

        if (endpoint != null)
            endpoint.close();

        closed = true;
    }

    /**
     * @return Distributed joins flag.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return Collocated flag.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return Replicated only flag.
     */
    public boolean replicatedOnly() {
        return replicatedOnly;
    }

    /**
     * @return Auto close server cursors flag.
     */
    public boolean autoCloseServerCursor() {
        return autoCloseServerCursor;
    }

    /**
     * @return Socket send buffer size.
     */
    public int socketSendBuffer() {
        return sockSndBuf;
    }

    /**
     * @return Socket receive buffer size.
     */
    public int socketReceiveBuffer() {
        return sockRcvBuf;
    }

    /**
     * @return TCP no delay flag.
     */
    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @return Ignite server version.
     */
    IgniteProductVersion igniteVersion() {
        return igniteVer;
    }

    /**
     * @return Lazy query execution flag.
     */
    public boolean lazy() {
        return lazy;
    }
}