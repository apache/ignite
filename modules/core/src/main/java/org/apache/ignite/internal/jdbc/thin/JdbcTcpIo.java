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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.odbc.SqlListenerColumnMeta;
import org.apache.ignite.internal.processors.odbc.SqlListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryExecuteResult;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryFetchResult;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.apache.ignite.internal.processors.odbc.SqlNioListener;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBinaryReader;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBinaryWriter;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * JDBC IO layer implementation based on blocking IPC streams.
 */
public class JdbcTcpIo {
    /** Current version. */
    private static final SqlListenerProtocolVersion CURRENT_VER = SqlListenerProtocolVersion.create(2, 1, 0);

    /** Initial output stream capacity for handshake. */
    private static final int HANDSHAKE_MSG_SIZE = 10;

    /** Initial output for query message. */
    private static final int QUERY_EXEC_MSG_INIT_CAP = 1024;

    /** Initial output for query fetch message. */
    private static final int QUERY_FETCH_MSG_SIZE = 13;

    /** Initial output for query close message. */
    private static final int QUERY_CLOSE_MSG_SIZE = 9;

    /** Logger. */
    private final IgniteLogger log;

    /** Server endpoint address. */
    private final String endpointAddr;

    /** Endpoint. */
    private IpcEndpoint endpoint;

    /** Output stream. */
    private BufferedOutputStream out;

    /** Input stream. */
    private BufferedInputStream in;

    /** Distributed joins. */
    private boolean distributedJoins;

    /** Enforce join order. */
    private boolean enforceJoinOrder;

    /**
     * @param endpointAddr Endpoint.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param log Logger to use.
     */
    JdbcTcpIo(String endpointAddr, boolean distributedJoins, boolean enforceJoinOrder, IgniteLogger log) {
        assert endpointAddr != null;

        this.endpointAddr = endpointAddr;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder= enforceJoinOrder;
        this.log = log;
    }

    /**
     * @throws IgniteCheckedException On error.
     * @throws IOException On IO error in handshake.
     */
    public void start() throws IgniteCheckedException, IOException {
        endpoint = IpcEndpointFactory.connectEndpoint(endpointAddr, log);

        out = new BufferedOutputStream(endpoint.outputStream());
        in = new BufferedInputStream(endpoint.inputStream());

        handshake();
    }

    /**
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public void handshake() throws IOException, IgniteCheckedException {
        JdbcBinaryWriter writer = new JdbcBinaryWriter(new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE));

        writer.writeByte((byte)SqlListenerRequest.HANDSHAKE);

        writer.writeShort(CURRENT_VER.major());
        writer.writeShort(CURRENT_VER.minor());
        writer.writeShort(CURRENT_VER.maintenance());

        writer.writeByte(SqlNioListener.JDBC_CLIENT);

        writer.writeBoolean(distributedJoins);
        writer.writeBoolean(enforceJoinOrder);

        send(writer.array());

        JdbcBinaryReader reader = new JdbcBinaryReader(new BinaryHeapInputStream(read()));

        boolean accepted = reader.readBoolean();

        if (accepted)
            return;

        short maj = reader.readShort();
        short min = reader.readShort();
        short maintenance = reader.readShort();

        String err = reader.readString();

        SqlListenerProtocolVersion ver = SqlListenerProtocolVersion.create(maj, min, maintenance);

        throw new IgniteCheckedException("Handshake error: the protocol version is not supported by Ignite version:"
            + ver + (F.isEmpty(err) ? "" : ". Error message: " + err));
    }

    /**
     * @param cache Cache name.
     * @param sql SQL statement.
     * @param args Query parameters.
     * @return Execute query results.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public SqlListenerQueryExecuteResult queryExecute(String cache, String sql, Object[] args)
        throws IOException, IgniteCheckedException {
        JdbcBinaryWriter writer = new JdbcBinaryWriter(new BinaryHeapOutputStream(QUERY_EXEC_MSG_INIT_CAP));

        writer.writeByte((byte)SqlListenerRequest.QRY_EXEC);

        writer.writeString(cache);
        writer.writeString(sql);
        writer.writeInt(args == null ? 0 : args.length);

        if (args != null) {
            for (Object arg : args)
                writer.writeObjectDetached(arg);
        }

        send(writer.array());

        JdbcBinaryReader reader = new JdbcBinaryReader(new BinaryHeapInputStream(read()));

        byte status = reader.readByte();

        if (status != SqlListenerResponse.STATUS_SUCCESS) {
            String err = reader.readString();

            throw new IgniteCheckedException("Query execute error: " + err);
        }

        long qryId = reader.readLong();
        int metaSize = reader.readInt();

        List<SqlListenerColumnMeta> meta = null;

        if (metaSize > 0) {
            meta = new ArrayList<>(metaSize);

            for (int i = 0; i < metaSize; ++i) {
                SqlListenerColumnMeta m = new SqlListenerColumnMeta();

                m.read(reader);

                meta.add(m);
            }
        }

        return new SqlListenerQueryExecuteResult(qryId, meta);
    }

    /**
     * @param qryId Query ID.
     * @param fetchSize Fetch page size.
     * @return Fetch results.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public SqlListenerQueryFetchResult queryFetch(Long qryId, int fetchSize)
        throws IOException, IgniteCheckedException {
        JdbcBinaryWriter writer = new JdbcBinaryWriter(new BinaryHeapOutputStream(QUERY_FETCH_MSG_SIZE));

        writer.writeByte((byte)SqlListenerRequest.QRY_FETCH);

        writer.writeLong(qryId);
        writer.writeInt(fetchSize);

        send(writer.array());

        JdbcBinaryReader reader = new JdbcBinaryReader(new BinaryHeapInputStream(read()));

        byte status = reader.readByte();

        if (status != SqlListenerResponse.STATUS_SUCCESS) {
            String err = reader.readString();

            throw new IgniteCheckedException("Query execute error: " + err);
        }

        long respQryId = reader.readLong();

        assert respQryId == qryId : "Invalid query ID in the response: [reqQueryId=" + qryId + ", respQueryId="
            + respQryId + ']';

        boolean last = reader.readBoolean();

        int rowsSize = reader.readInt();

        List<List<Object>> rows = null;

        if (rowsSize > 0) {
            rows = new ArrayList<>(rowsSize);

            for (int i = 0; i < rowsSize; ++i) {

                int colsSize = reader.readInt();

                List<Object> col = new ArrayList<>(colsSize);

                for (int colCnt = 0; colCnt < colsSize; ++colCnt)
                    col.add(reader.readObjectDetached());

                rows.add(col);
            }
        }

        return new SqlListenerQueryFetchResult(qryId, rows, last);
    }

    /**
     * @param qryId Query ID.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public void queryClose(long qryId) throws IOException, IgniteCheckedException {
        JdbcBinaryWriter writer = new JdbcBinaryWriter(new BinaryHeapOutputStream(QUERY_CLOSE_MSG_SIZE));

        writer.writeByte((byte)SqlListenerRequest.QRY_CLOSE);
        writer.writeLong(qryId);

        send(writer.array());

        JdbcBinaryReader reader = new JdbcBinaryReader(new BinaryHeapInputStream(read()));

        byte status = reader.readByte();

        if (status != SqlListenerResponse.STATUS_SUCCESS) {
            String err = reader.readString();

            throw new IgniteCheckedException("Query execute error: " + err);
        }

        long respQryId = reader.readLong();

        assert respQryId == qryId : "Invalid query ID in the response: [reqQueryId=" + qryId + ", respQueryId="
            + respQryId + ']';
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
    private  byte[] read() throws IOException, IgniteCheckedException {
        byte[] sizeBytes = new byte[4];

        int readLen = in.read(sizeBytes);

        if (readLen != 4) {
            close();

            throw new IgniteCheckedException("IO error. Cannot receive message len. lenSize = " + readLen);
        }

        int size  = (((0xFF & sizeBytes[3]) << 24) | ((0xFF & sizeBytes[2]) << 16)
            | ((0xFF & sizeBytes[1]) << 8) + (0xFF & sizeBytes[0]));

        byte[] msgData = new byte[size];

        readLen = in.read(msgData);

        if (readLen != size) {
            close();

            throw new IgniteCheckedException("IO error. Cannot receive massage. [received=" + readLen + ", size="
                + size + ']');
        }

        return msgData;
    }

    /**
     * Close the client IO.
     */
    public void close() {
        // Clean up resources.
        U.closeQuiet(out);
        U.closeQuiet(in);

        if (endpoint != null)
            endpoint.close();
    }
}