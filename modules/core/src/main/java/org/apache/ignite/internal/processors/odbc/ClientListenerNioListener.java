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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /** Connection handshake timeout task. */
    public static final int CONN_CTX_HANDSHAKE_TIMEOUT_TASK = GridNioSessionMetaKey.nextUniqueKey();

    /** Connection-related metadata key. */
    public static final int CONN_CTX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Next connection id. */
    private static AtomicInteger nextConnId = new AtomicInteger(1);

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

    /** Thin client configuration. */
    private final ThinClientConfiguration thinCfg;

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
        this.cliConnCfg = cliConnCfg;

        maxCursors = cliConnCfg.getMaxOpenCursorsPerConnection();
        log = ctx.log(getClass());

        thinCfg = cliConnCfg.getThinClientConfiguration() == null ? new ThinClientConfiguration()
            : new ThinClientConfiguration(cliConnCfg.getThinClientConfiguration());
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (log.isDebugEnabled())
            log.debug("Client connected: " + ses.remoteAddress());

        long handshakeTimeout = cliConnCfg.getHandshakeTimeout();

        if (handshakeTimeout > 0)
            scheduleHandshakeTimeout(ses, handshakeTimeout);
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
            try {
                onHandshake(ses, msg);
            }
            catch (Exception e) {
                U.error(log, "Failed to handle handshake request " +
                    "(probably, connection has already been closed).", e);
            }

            return;
        }

        ClientListenerMessageParser parser = connCtx.parser();
        ClientListenerRequestHandler handler = connCtx.handler();

        ClientListenerRequest req;

        try {
            req = parser.decode(msg);
        }
        catch (Exception e) {
            try {
                handler.unregisterRequest(parser.decodeRequestId(msg));
            }
            catch (Exception e1) {
                U.error(log, "Failed to unregister request.", e1);
            }

            U.error(log, "Failed to parse client request.", e);

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

            ClientListenerResponse resp;

            AuthorizationContext authCtx = connCtx.authorizationContext();

            if (authCtx != null)
                AuthorizationContext.context(authCtx);

            try(OperationSecurityContext s = ctx.security().withContext(connCtx.securityContext())) {
                resp = handler.handle(req);
            }
            finally {
                if (authCtx != null)
                    AuthorizationContext.clear();
            }

            if (resp != null) {
                if (log.isDebugEnabled()) {
                    long dur = (System.nanoTime() - startTime) / 1000;

                    log.debug("Client request processed [reqId=" + req.requestId() + ", dur(mcs)=" + dur +
                        ", resp=" + resp.status() + ']');
                }

                byte[] outMsg = parser.encode(resp);

                ses.send(outMsg);
            }
        }
        catch (Exception e) {
            handler.unregisterRequest(req.requestId());

            U.error(log, "Failed to process client request [req=" + req + ']', e);

            ses.send(parser.encode(handler.handleException(e, req)));
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        ses.close();
    }

    /**
     * Schedule handshake timeout.
     * @param ses Connection session.
     * @param handshakeTimeout Handshake timeout.
     */
    private void scheduleHandshakeTimeout(GridNioSession ses, long handshakeTimeout) {
        assert handshakeTimeout > 0;

        Closeable timeoutTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                ses.close();

                U.warn(log, "Unable to perform handshake within timeout " +
                    "[timeout=" + handshakeTimeout + ", remoteAddr=" + ses.remoteAddress() + ']');
            }
        }, handshakeTimeout, -1);

        ses.addMeta(CONN_CTX_HANDSHAKE_TIMEOUT_TASK, timeoutTask);
    }

    /**
     * Cancel handshake timeout task execution.
     * @param ses Connection session.
     */
    private void cancelHandshakeTimeout(GridNioSession ses) {
        Closeable timeoutTask = ses.removeMeta(CONN_CTX_HANDSHAKE_TIMEOUT_TASK);

        try {
            if (timeoutTask != null)
                timeoutTask.close();
        } catch (Exception e) {
            U.warn(log, "Failed to cancel handshake timeout task " +
                "[remoteAddr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /**
     * Perform handshake.
     *
     * @param ses Session.
     * @param msg Message bytes.
     */
    private void onHandshake(GridNioSession ses, byte[] msg) {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextImpl(null, null));

        ctx.configure(marsh, new IgniteConfiguration());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx, new BinaryHeapInputStream(msg), null, true);

        byte cmd = reader.readByte();

        if (cmd != ClientListenerRequest.HANDSHAKE) {
            U.warn(log, "Unexpected client request (will close session): " + ses.remoteAddress());

            ses.close();

            return;
        }

        short verMajor = reader.readShort();
        short verMinor = reader.readShort();
        short verMaintenance = reader.readShort();

        ClientListenerProtocolVersion ver = ClientListenerProtocolVersion.create(verMajor, verMinor, verMaintenance);

        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(8), null, null);

        byte clientType = reader.readByte();

        ClientListenerConnectionContext connCtx = null;

        try {
            connCtx = prepareContext(clientType);

            ensureClientPermissions(clientType);

            if (connCtx.isVersionSupported(ver)) {
                connCtx.initializeFromHandshake(ses, ver, reader);

                ses.addMeta(CONN_CTX_META_KEY, connCtx);
            }
            else
                throw new IgniteCheckedException("Unsupported version: " + ver.asString());

            cancelHandshakeTimeout(ses);

            connCtx.handler().writeHandshake(writer);
        }
        catch (IgniteAccessControlException authEx) {
            writer.writeBoolean(false);

            writer.writeShort((short)0);
            writer.writeShort((short)0);
            writer.writeShort((short)0);

            writer.doWriteString(authEx.getMessage());

            if (ver.compareTo(ClientConnectionContext.VER_1_1_0) >= 0)
                writer.writeInt(ClientStatus.AUTH_FAILED);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Error during handshake [rmtAddr=" + ses.remoteAddress() + ", msg=" + e.getMessage() + ']');

            ClientListenerProtocolVersion currVer;

            if (connCtx == null)
                currVer = ClientListenerProtocolVersion.create(0, 0, 0);
            else
                currVer = connCtx.defaultVersion();

            writer.writeBoolean(false);

            writer.writeShort(currVer.major());
            writer.writeShort(currVer.minor());
            writer.writeShort(currVer.maintenance());

            writer.doWriteString(e.getMessage());

            if (ver.compareTo(ClientConnectionContext.VER_1_1_0) >= 0)
                writer.writeInt(ClientStatus.FAILED);
        }

        ses.send(writer.array());
    }

    /**
     * Prepare context.
     *
     * @param ses Session.
     * @param clientType Client type.
     * @return Context.
     * @throws IgniteCheckedException If failed.
     */
    private ClientListenerConnectionContext prepareContext(byte clientType) throws IgniteCheckedException {
        long connId = nextConnectionId();

        switch (clientType) {
            case ODBC_CLIENT:
                return new OdbcConnectionContext(ctx, busyLock, connId, maxCursors);

            case JDBC_CLIENT:
                return new JdbcConnectionContext(ctx, busyLock, connId, maxCursors);

            case THIN_CLIENT:
                return new ClientConnectionContext(ctx, connId, maxCursors, thinCfg);
        }

        throw new IgniteCheckedException("Unknown client type: " + clientType);
    }

    /**
     * Generate unique connection id.
     * @return connection id.
     */
    private long nextConnectionId() {
        return (ctx.discovery().localNode().order() << 32) + nextConnId.getAndIncrement();
    }

    /**
     * Ensures if the given type of client is enabled by config.
     *
     * @param clientType Client type.
     * @throws IgniteCheckedException If failed.
     */
    private void ensureClientPermissions(byte clientType) throws IgniteCheckedException {
        switch (clientType) {
            case ODBC_CLIENT: {
                if (!cliConnCfg.isOdbcEnabled())
                    throw new IgniteCheckedException("ODBC connection is not allowed, " +
                        "see ClientConnectorConfiguration.odbcEnabled.");
                break;
            }

            case JDBC_CLIENT: {
                if (!cliConnCfg.isJdbcEnabled())
                    throw new IgniteCheckedException("JDBC connection is not allowed, " +
                        "see ClientConnectorConfiguration.jdbcEnabled.");

                break;
            }

            case THIN_CLIENT: {
                if (!cliConnCfg.isThinClientEnabled())
                    throw new IgniteCheckedException("Thin client connection is not allowed, " +
                        "see ClientConnectorConfiguration.thinClientEnabled.");

                break;
            }

            default:
                throw new IgniteCheckedException("Unknown client type: " + clientType);
        }
    }
}
