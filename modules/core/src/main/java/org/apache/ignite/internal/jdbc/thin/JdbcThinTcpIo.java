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
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.odbc.SqlListenerNioListener;
import org.apache.ignite.internal.processors.odbc.SqlListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryFetchRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryFetchResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryMetadataResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResponse;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResult;
import org.apache.ignite.internal.util.ipc.loopback.IpcClientTcpEndpoint;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * JDBC IO layer implementation based on blocking IPC streams.
 */
public class JdbcThinTcpIo {
    /** Current version. */
    private static final SqlListenerProtocolVersion CURRENT_VER = SqlListenerProtocolVersion.create(2, 1, 0);

    /** Initial output stream capacity for handshake. */
    private static final int HANDSHAKE_MSG_SIZE = 12;

    /** Initial output for query message. */
    private static final int QUERY_EXEC_MSG_INIT_CAP = 256;

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

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param collocated Collocated flag.
     * @param replicatedOnly Replicated only flag.
     * @param sockSndBuf Socket send buffer.
     * @param sockRcvBuf Socket receive buffer.
     * @param tcpNoDelay TCP no delay flag.
     */
    JdbcThinTcpIo(String host, int port, boolean distributedJoins, boolean enforceJoinOrder, boolean collocated,
        boolean replicatedOnly, int sockSndBuf, int sockRcvBuf, boolean tcpNoDelay) {
        this.host = host;
        this.port = port;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.collocated = collocated;
        this.replicatedOnly = replicatedOnly;
        this.sockSndBuf = sockSndBuf;
        this.sockRcvBuf = sockRcvBuf;
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * @throws IgniteCheckedException On error.
     * @throws IOException On IO error in handshake.
     */
    public void start() throws IgniteCheckedException, IOException {
        Socket sock = new Socket();

        if (sockSndBuf != 0)
            sock.setSendBufferSize(sockSndBuf);

        if (sockRcvBuf != 0)
            sock.setReceiveBufferSize(sockRcvBuf);

        sock.setTcpNoDelay(tcpNoDelay);

        try {
            sock.connect(new InetSocketAddress(host, port));
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to connect to server [host=" + host + ", port=" + port + ']', e);
        }

        endpoint = new IpcClientTcpEndpoint(sock);

        out = new BufferedOutputStream(endpoint.outputStream());
        in = new BufferedInputStream(endpoint.inputStream());

        handshake();
    }

    /**
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public void handshake() throws IOException, IgniteCheckedException {
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte)SqlListenerRequest.HANDSHAKE);

        writer.writeShort(CURRENT_VER.major());
        writer.writeShort(CURRENT_VER.minor());
        writer.writeShort(CURRENT_VER.maintenance());

        writer.writeByte(SqlListenerNioListener.JDBC_CLIENT);

        writer.writeBoolean(distributedJoins);
        writer.writeBoolean(enforceJoinOrder);
        writer.writeBoolean(collocated);
        writer.writeBoolean(replicatedOnly);

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()),
            null, null, false);

        boolean accepted = reader.readBoolean();

        if (accepted)
            return;

        short maj = reader.readShort();
        short min = reader.readShort();
        short maintenance = reader.readShort();

        String err = reader.readString();

        SqlListenerProtocolVersion ver = SqlListenerProtocolVersion.create(maj, min, maintenance);

        throw new IgniteCheckedException("Handshake failed [driverProtocolVer=" + CURRENT_VER +
            ", remoteNodeProtocolVer=" + ver + ", err=" + err + ']');
    }

    /**
     * @param cache Cache name.
     * @param fetchSize Fetch size.
     * @param maxRows Max rows.
     * @param sql SQL statement.
     * @param args Query parameters.
     * @return Execute query results.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public JdbcQueryExecuteResult queryExecute(String cache, int fetchSize, int maxRows,
        String sql, List<Object> args)
        throws IOException, IgniteCheckedException {
        return sendRequest(new JdbcQueryExecuteRequest(cache, fetchSize, maxRows, sql,
            args == null ? null : args.toArray(new Object[args.size()])), QUERY_EXEC_MSG_INIT_CAP);
    }

    /**
     * @param req Request.
     * @param cap Initial ouput stream capacity.
     * @return Server response.
     * @throws IOException On IO error.
     * @throws IgniteCheckedException On error.
     */
    @SuppressWarnings("unchecked")
    public <R extends JdbcResult> R sendRequest(JdbcRequest req, int cap) throws IOException, IgniteCheckedException {
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(cap), null, null);

        req.writeBinary(writer);

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()), null, null, false);

        JdbcResponse res = new JdbcResponse();

        res.readBinary(reader);

        if (res.status() != SqlListenerResponse.STATUS_SUCCESS)
            throw new IgniteCheckedException("Error server response: [req=" + req + ", resp=" + res + ']');

        return (R)res.response();
    }

    /**
     * @param qryId Query ID.
     * @param pageSize pageSize.
     * @return Fetch results.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public JdbcQueryFetchResult queryFetch(Long qryId, int pageSize)
        throws IOException, IgniteCheckedException {
        return sendRequest(new JdbcQueryFetchRequest(qryId, pageSize), QUERY_FETCH_MSG_SIZE);
    }


    /**
     * @param qryId Query ID.
     * @return Fetch results.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public JdbcQueryMetadataResult queryMeta(Long qryId)
        throws IOException, IgniteCheckedException {
        return sendRequest(new JdbcQueryMetadataRequest(qryId), QUERY_META_MSG_SIZE);
    }

    /**
     * @param qryId Query ID.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public void queryClose(long qryId) throws IOException, IgniteCheckedException {
        sendRequest(new JdbcQueryCloseRequest(qryId), QUERY_CLOSE_MSG_SIZE);
    }

    /**
     * @param req ODBC request.
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
     * @throws IgniteCheckedException On error.
     */
    private byte[] read() throws IOException, IgniteCheckedException {
        byte[] sizeBytes = read(4);

        int msgSize  = (((0xFF & sizeBytes[3]) << 24) | ((0xFF & sizeBytes[2]) << 16)
            | ((0xFF & sizeBytes[1]) << 8) + (0xFF & sizeBytes[0]));

        return read(msgSize);
    }

    /**
     * @param size Count of bytes to read from stream.
     * @return Read bytes.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    private byte [] read(int size) throws IOException, IgniteCheckedException {
        int off = 0;

        byte[] data = new byte[size];

        while (off != size) {
            int res = in.read(data, off, size - off);

            if (res == -1)
                throw new IgniteCheckedException("Failed to read incoming message (not enough data).");

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
}