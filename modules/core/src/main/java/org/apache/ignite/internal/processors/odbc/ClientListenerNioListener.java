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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.jetbrains.annotations.Nullable;

/**
 * Client message listener.
 */
public class ClientListenerNioListener extends GridNioServerListenerAdapter<byte[]> {
    /** ODBC driver handshake code. */
    public static final byte ODBC_CLIENT = 0;

    /** JDBC driver handshake code. */
    public static final byte JDBC_CLIENT = 1;

    /** Thin client handshake code. */
    public static final byte THIN_CLIENT = 2;

    /** Connection-related metadata key. */
    private static final int CONN_CTX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Logger. */
    private final IgniteLogger log;

    /** Client connection config. */
    private ClientConnectorConfiguration cliConnCfg;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param busyLock Shutdown busy lock.
     * @param cliConnCfg Client connector configuration.
     */
    public ClientListenerNioListener(GridKernalContext ctx, GridSpinBusyLock busyLock,
        ClientConnectorConfiguration cliConnCfg) {
        assert cliConnCfg != null;

        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = cliConnCfg.getMaxOpenCursorsPerConnection();
        this.cliConnCfg = cliConnCfg;
        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (log.isDebugEnabled())
            log.debug("Client connected: " + ses.remoteAddress());
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

        if (connCtx != null)
            connCtx.onDisconnected();

        if (log.isDebugEnabled()) {
            if (e == null)
                log.debug("Client disconnected: " + ses.remoteAddress());
            else
                log.debug("Client disconnected due to an error [addr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, byte[] msg) {
        assert msg != null;

        ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

        if (connCtx == null) {
            onHandshake(ses, msg);

            return;
        }

        ClientListenerMessageParser parser = connCtx.parser();
        ClientListenerRequestHandler handler = connCtx.handler();

        ClientListenerRequest req;

        try {
            req = parser.decode(msg);
        }
        catch (Exception e) {
            log.error("Failed to parse client request.", e);

            ses.close();

            return;
        }

        assert req != null;

        try {
            long startTime = 0;

            if (log.isDebugEnabled()) {
                startTime = System.nanoTime();

                log.debug("Client request received [reqId=" + req.requestId() + ", addr=" +
                    ses.remoteAddress() + ", req=" + req + ']');
            }

            ClientListenerResponse resp = handler.handle(req);

            if (log.isDebugEnabled()) {
                long dur = (System.nanoTime() - startTime) / 1000;

                log.debug("Client request processed [reqId=" + req.requestId() + ", dur(mcs)=" + dur  +
                    ", resp=" + resp.status() + ']');
            }

            byte[] outMsg = parser.encode(resp);

            ses.send(outMsg);
        }
        catch (Exception e) {
            log.error("Failed to process client request [req=" + req + ']', e);

            ses.send(parser.encode(handler.handleException(e, req)));
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        ses.close();
    }

    /**
     * Perform handshake.
     *
     * @param ses Session.
     * @param msg Message bytes.
     */
    private void onHandshake(GridNioSession ses, byte[] msg) {
        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, stream, null, true);

        byte cmd = reader.readByte();

        if (cmd != ClientListenerRequest.HANDSHAKE) {
            log.error("Unexpected client request (will close session): " + ses.remoteAddress());

            ses.close();

            return;
        }

        short verMajor = reader.readShort();
        short verMinor = reader.readShort();
        short verMaintenance = reader.readShort();

        ClientListenerProtocolVersion ver = ClientListenerProtocolVersion.create(verMajor, verMinor, verMaintenance);

        ClientListenerConnectionContext connCtx;

        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(8), null, null);

        try {
            byte clientType = reader.readByte();

            connCtx = prepareContext(clientType);
        }
        catch (IgniteException e) {
            writer.writeBoolean(false);
            writer.writeShort((short)-1);
            writer.writeShort((short)-1);
            writer.writeShort((short)-1);

            writer.doWriteString(e.getMessage());

            ses.send(writer.array());

            return;
        }

        assert connCtx != null;

        String errMsg = null;

        if (connCtx.isVersionSupported(ver)) {
            connCtx.initializeFromHandshake(ver, reader);

            ses.addMeta(CONN_CTX_META_KEY, connCtx);
        }
        else {
            log.warning("Unsupported version: " + ver.toString());

            errMsg = "Unsupported version.";
        }

        if (errMsg == null)
            connCtx.handler().writeHandshake(writer);
        else {
            ClientListenerProtocolVersion currentVer = connCtx.currentVersion();

            // Failed handshake response
            writer.writeBoolean(false);
            writer.writeShort(currentVer.major());
            writer.writeShort(currentVer.minor());
            writer.writeShort(currentVer.maintenance());
            writer.doWriteString(errMsg);
        }

        ses.send(writer.array());
    }

    /**
     * Prepare context.
     *
     * @param clientType Client type.
     * @return Context.
     */
    private ClientListenerConnectionContext prepareContext(byte clientType) {
        switch (clientType) {
            case ODBC_CLIENT: {
                if (!cliConnCfg.isOdbcEnabled())
                    throw new IgniteException("ODBC connection is not allowed, " +
                        "see ClientConnectorConfiguration.odbcEnabled.");

                return new OdbcConnectionContext(ctx, busyLock, maxCursors);
            }

            case JDBC_CLIENT: {
                if (!cliConnCfg.isJdbcEnabled())
                    throw new IgniteException("JDBC connection is not allowed, " +
                        "see ClientConnectorConfiguration.jdbcEnabled.");

                return new JdbcConnectionContext(ctx, busyLock, maxCursors);
            }

            case THIN_CLIENT: {
                if (!cliConnCfg.isThinClientEnabled())
                    throw new IgniteException("Thin client connection is not allowed, " +
                        "see ClientConnectorConfiguration.thinClientEnabled.");

                return new ClientConnectionContext(ctx, maxCursors);
            }

            default:
                throw new IgniteException("Unknown client type: " + clientType);
        }
    }
}
