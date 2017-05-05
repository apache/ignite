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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.odbc.SqlListenerHandshakeRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerHandshakeResult;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.odbc.SqlListenerProtocolVersion.VERSION_2_1_0;

/**
 * JDBC IO layer implementation based on blocking IPC streams.
 */
public class JdbcTcpIo {
    /** Initial output stream capacity. */
    private static final int INIT_CAP = 1024;

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

    /** Stopping flag. */
    private volatile boolean stopping;

    /**
     * @param endpointAddr Endpoint.
     * @param log Logger to use.
     */
    public JdbcTcpIo(String endpointAddr, IgniteLogger log) {
        assert endpointAddr != null;

        this.endpointAddr = endpointAddr;
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
        sendRequest(new SqlListenerHandshakeRequest(VERSION_2_1_0.longValue()));

        SqlListenerResponse res = readResponse(ResponseType.HANDSHAKE);

        if (res.status() != SqlListenerResponse.STATUS_SUCCESS)
            throw new IgniteCheckedException("Handshake error: " + res.error());

        if (res.response() instanceof SqlListenerHandshakeResult) {
            SqlListenerHandshakeResult hsRes = (SqlListenerHandshakeResult)res.response();

            if (!hsRes.accepted()) {
                throw new IgniteCheckedException("Handshake error: the protocol version is supported by Ignite since "
                    + hsRes.protocolVersionSince() + " version.");
            }
        }
    }

    /**
     * @param req ODBC request.
     * @throws IOException On error.
     */
    public void sendRequest(SqlListenerRequest req) throws IOException {
        BinaryHeapOutputStream os = new BinaryHeapOutputStream(INIT_CAP);

        // Set offset to data array
        os.position(4);

        if (req instanceof SqlListenerHandshakeRequest) {
            SqlListenerHandshakeRequest handshakeReq = (SqlListenerHandshakeRequest)req;

            os.writeByte((byte)handshakeReq.command());
            os.writeLong(handshakeReq.version().longValue());
            os.writeBoolean(handshakeReq.distributedJoins());
            os.writeBoolean(handshakeReq.enforceJoinOrder());
        }

        int size = os.position() - 4;

        // Fill data packet size.
        os.position(0);
        os.writeInt(size);

        out.write(os.array(), 0, size + 4);
        out.flush();
    }

    /**
     * @param bin Input stream.
     * @param type Expected response type.
     * @return Response object.
     */
    private SqlListenerResponse parseResponse(BinaryHeapInputStream bin, ResponseType type) {
        switch (type) {
            case HANDSHAKE: {
                String protoVerSince = null;
                String curVer = null;

                boolean accepted = bin.readBoolean();

                if (!accepted) {
                    protoVerSince = BinaryUtils.doReadString(bin);

                    curVer = BinaryUtils.doReadString(bin);
                }

                return new SqlListenerResponse(new SqlListenerHandshakeResult(accepted, protoVerSince, curVer));
            }
            case QUERY_CLOSE:
                break;
            case QUERY_FETCH:
                break;
            case QUERY_EXECUTE:
                break;
            case QUERY_GET_PARAMS_META:
                break;
            case GET_TABLES_META:
                break;
        }

        return null;
    }

    /**
     * @param respType Expected response type.
     * @return ODBC response.
     * @throws IOException On error.
     * @throws IgniteCheckedException On error.
     */
    public SqlListenerResponse readResponse(ResponseType respType) throws IOException, IgniteCheckedException {
        byte[] sizeBytes = new byte[4];

        in.read(sizeBytes);

        int size = U.bytesToInt(sizeBytes, 0);

        byte[] msgData = new byte[size];

        in.read(msgData);

        BinaryHeapInputStream bin = new BinaryHeapInputStream(msgData);

        int status = (int)bin.readByte();

        if (status != SqlListenerResponse.STATUS_SUCCESS) {
            String err = BinaryUtils.doReadString(bin);

            return new SqlListenerResponse(status, err);
        }

        return parseResponse(bin, respType);
    }

    /**
     *
     */
    public void close() {
        close0();
    }

    /**
     * Closes client but does not wait.
     */
    private void close0() {
        if (stopping)
            return;

        stopping = true;

        // Clean up resources.
        U.closeQuiet(out);
        U.closeQuiet(in);

        if (endpoint != null)
            endpoint.close();
    }

    /**
     * The response type is used to define expected response type.
     */
    public enum ResponseType {
        /** Handshake. */
        HANDSHAKE,

        /** Query close. */
        QUERY_CLOSE,

        /** Query fetch. */
        QUERY_FETCH,

        /** Query execute. */
        QUERY_EXECUTE,

        /** Query get params meta. */
        QUERY_GET_PARAMS_META,

        /** Get tables meta. */
        GET_TABLES_META,
    }
}