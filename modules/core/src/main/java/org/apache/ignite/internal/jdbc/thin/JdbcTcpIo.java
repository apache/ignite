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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.internal.igfs.common.IgfsControlResponse;
import org.apache.ignite.internal.igfs.common.IgfsDataInputStream;
import org.apache.ignite.internal.igfs.common.IgfsIpcCommand;
import org.apache.ignite.internal.igfs.common.IgfsMarshaller;
import org.apache.ignite.internal.processors.odbc.OdbcHandshakeRequest;
import org.apache.ignite.internal.processors.odbc.OdbcRequest;
import org.apache.ignite.internal.processors.odbc.OdbcResponse;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.OdbcProtocolVersion.VERSION_2_1_0;

/**
 * JDBC IO layer implementation based on blocking IPC streams.
 */
public class JdbcTcpIo {
    /** Logger. */
    private final IgniteLogger log;

    /** Endpoint. */
    private IpcEndpoint endpoint;

    /** Server endpoint address. */
    private final String endpointAddr;

    /** Output stream. */
    private DataOutputStream out;

    /** Client reader thread. */
    private Thread reader;

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
     */
    public void start() throws IgniteCheckedException {
        endpoint = IpcEndpointFactory.connectEndpoint(endpointAddr, log);

        out = new DataOutputStream(new BufferedOutputStream(endpoint.outputStream()));

        reader = new ReaderThread();
    }

    /**
     * @throws IOException On error.
     */
    public void handshake() throws IOException {
        OdbcHandshakeRequest handshakeReq = new OdbcHandshakeRequest(VERSION_2_1_0.longValue());

        out.writeByte((byte)handshakeReq.command());
        out.writeLong(handshakeReq.version().longValue());
        out.writeBoolean(handshakeReq.distributedJoins());
        out.writeBoolean(handshakeReq.enforceJoinOrder());

        out.flush();
    }

    /**
     * @param req ODBC request.
     * @return ODBC responce.
     */
    public OdbcResponse sendRequest(OdbcRequest req) {

        return null;
    }

    /**
     *
     */
    public void close() {
        close0(null);

    }

    /**
     *
     */
    private void close0(@Nullable Throwable err) {
        if (stopping)
            return;

        stopping = true;

        if (err == null)
            err = new IgniteCheckedException("Failed to perform request (connection was concurrently closed before response " +
                "is received).");

        // Clean up resources.
        U.closeQuiet(out);

        if (endpoint != null)
            endpoint.close();
    }

    /**
     * Do not extend {@code GridThread} to minimize class dependencies.
     */
    private class ReaderThread extends Thread {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void run() {
            // Error to fail pending futures.
            Throwable err = null;

            try {
                InputStream in = endpoint.inputStream();

                DataInputStream dis = new DataInputStream(in);

                byte[] hdr = new byte[IgfsMarshaller.HEADER_SIZE];
                byte[] msgHdr = new byte[IgfsControlResponse.RES_HEADER_SIZE];

                while (!Thread.currentThread().isInterrupted()) {
                    dis.readFully(hdr);

                    long reqId = U.bytesToLong(hdr, 0);

                    // We don't wait for write responses, therefore reqId is -1.
                    if (reqId == -1) {
                        // We received a response which normally should not be sent. It must contain an error.
                        dis.readFully(msgHdr);

                        assert msgHdr[4] != 0;

                        String errMsg = dis.readUTF();

                        // Error code.
                        dis.readInt();

                        long streamId = dis.readLong();

                        for (HadoopIgfsIpcIoListener lsnr : lsnrs)
                            lsnr.onError(streamId, errMsg);
                    }
                    else {
                        HadoopIgfsFuture<Object> fut = reqMap.remove(reqId);

                        if (fut == null) {
                            String msg = "Failed to read response from server: response closure is unavailable for " +
                                "requestId (will close connection):" + reqId;

                            log.warn(msg);

                            err = new IgniteCheckedException(msg);

                            break;
                        }
                        else {
                            try {
                                IgfsIpcCommand cmd = IgfsIpcCommand.valueOf(U.bytesToInt(hdr, 8));

                                if (log.isDebugEnabled())
                                    log.debug("Received IGFS response [reqId=" + reqId + ", cmd=" + cmd + ']');

                                Object res = null;

                                if (fut.read()) {
                                    dis.readFully(msgHdr);

                                    boolean hasErr = msgHdr[4] != 0;

                                    if (hasErr) {
                                        String errMsg = dis.readUTF();

                                        // Error code.
                                        Integer errCode = dis.readInt();

                                        IgfsControlResponse.throwError(errCode, errMsg);
                                    }

                                    int blockLen = U.bytesToInt(msgHdr, 5);

                                    int readLen = Math.min(blockLen, fut.outputLength());

                                    if (readLen > 0) {
                                        assert fut.outputBuffer() != null;

                                        dis.readFully(fut.outputBuffer(), fut.outputOffset(), readLen);
                                    }

                                    if (readLen != blockLen) {
                                        byte[] buf = new byte[blockLen - readLen];

                                        dis.readFully(buf);

                                        res = buf;
                                    }
                                }
                                else
                                    res = marsh.unmarshall(cmd, hdr, dis);

                                fut.onDone(res);
                            }
                            catch (IgfsException | IgniteCheckedException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to apply response closure (will fail request future): " +
                                        e.getMessage());

                                fut.onDone(e);

                                err = e;
                            }
                            catch (Throwable t) {
                                fut.onDone(t);

                                throw t;
                            }
                        }
                    }
                }
            }
            catch (EOFException ignored) {
                err = new IgniteCheckedException("Failed to read response from server (connection was closed by remote peer).");
            }
            catch (IOException e) {
                if (!stopping)
                    log.error("Failed to read data (connection will be closed)", e);

                err = new HadoopIgfsCommunicationException(e);
            }
            catch (Throwable e) {
                if (!stopping)
                    log.error("Failed to obtain endpoint input stream (connection will be closed)", e);

                err = e;

                if (e instanceof Error)
                    throw (Error)e;
            }
            finally {
                close();
            }
        }
    }
}