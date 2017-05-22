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
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.odbc.SqlListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlNioListener;
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

    /** Initial output stream capacity. */
    private static final int HANDSHAKE_MSG_SIZE = 10;

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

    private boolean closed;

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
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte)SqlListenerRequest.HANDSHAKE);

        writer.writeShort(CURRENT_VER.major());
        writer.writeShort(CURRENT_VER.minor());
        writer.writeShort(CURRENT_VER.maintenance());

        writer.writeByte(SqlNioListener.JDBC_CLIENT);

        writer.writeBoolean(distributedJoins);
        writer.writeBoolean(enforceJoinOrder);

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

        throw new IgniteCheckedException("Ignite node reject handshake message: " +
            "the protocol version is not supported by Ignite version: " + ver + (F.isEmpty(err) ? "" : ". " +
            "The driver protocol version introduced in Ignite version: " + CURRENT_VER.toString() + ". " +
            "Error message: " + err));
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

            throw new IgniteCheckedException("IO error. Cannot receive message length (4 bytes expected). " +
                "[received = " + readLen + ']');
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
        if (closed)
            return;

        // Clean up resources.
        U.closeQuiet(out);
        U.closeQuiet(in);

        if (endpoint != null)
            endpoint.close();

        closed = true;
    }
}