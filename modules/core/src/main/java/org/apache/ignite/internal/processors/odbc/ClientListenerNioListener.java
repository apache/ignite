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
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.CONN_DISABLED_BY_ADMIN_ERR_MSG;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.clientTypeLabel;

/**
 * Client message listener.
 */
public class ClientListenerNioListener extends GridNioServerListenerAdapter<ClientMessage> {
    /** ODBC driver handshake code. */
    public static final byte ODBC_CLIENT = 0;

    /** JDBC driver handshake code. */
    public static final byte JDBC_CLIENT = 1;

    /** Thin client handshake code. */
    public static final byte THIN_CLIENT = 2;

    /** Client types. */
    public static final byte[] CLI_TYPES = {ODBC_CLIENT, JDBC_CLIENT, THIN_CLIENT};

    /** Connection handshake timeout task. */
    public static final int CONN_CTX_HANDSHAKE_TIMEOUT_TASK = GridNioSessionMetaKey.nextUniqueKey();

    /** Connection-related metadata key. */
    public static final int CONN_CTX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** {@code True} if a management client. Internal operations will be available. */
    public static final String MANAGEMENT_CLIENT_ATTR = "ignite.internal.management-client";

    /** Connection shifted ID for management clients. */
    public static final long MANAGEMENT_CONNECTION_SHIFTED_ID = -1;

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
    private final ClientConnectorConfiguration cliConnCfg;

    /** Thin client configuration. */
    private final ThinClientConfiguration thinCfg;

    /** Metrics. */
    private final ClientListenerMetrics metrics;

    /**
     * If return {@code true} then specifi protocol connections enabled.
     * Predicate checks distributed property value.
     *
     * @see ClientListenerNioListener#ODBC_CLIENT
     * @see ClientListenerNioListener#JDBC_CLIENT
     * @see ClientListenerNioListener#THIN_CLIENT
     */
    private final Predicate<Byte> newConnEnabled;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param busyLock Shutdown busy lock.
     * @param cliConnCfg Client connector configuration.
     * @param metrics Client listener metrics.
     * @param newConnEnabled Predicate to check if connection of specified type enabled.
     */
    public ClientListenerNioListener(
        GridKernalContext ctx,
        GridSpinBusyLock busyLock,
        ClientConnectorConfiguration cliConnCfg,
        ClientListenerMetrics metrics,
        Predicate<Byte> newConnEnabled
    ) {
        assert cliConnCfg != null;

        this.ctx = ctx;
        this.busyLock = busyLock;
        this.cliConnCfg = cliConnCfg;

        maxCursors = cliConnCfg.getMaxOpenCursorsPerConnection();
        log = ctx.log(getClass());

        thinCfg = cliConnCfg.getThinClientConfiguration() == null
            ? new ThinClientConfiguration()
            : new ThinClientConfiguration(cliConnCfg.getThinClientConfiguration());

        this.metrics = metrics;
        this.newConnEnabled = newConnEnabled;
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
    @Override public void onMessage(GridNioSession ses, ClientMessage msg) {
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
        ClientListenerRequestHandler hnd = connCtx.handler();

        ClientListenerRequest req;

        try {
            req = parser.decode(msg);
        }
        catch (Exception e) {
            try {
                hnd.unregisterRequest(parser.decodeRequestId(msg));
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
            long startTime;

            if (log.isTraceEnabled()) {
                startTime = System.nanoTime();

                log.trace("Client request received [reqId=" + req.requestId() + ", addr=" +
                    ses.remoteAddress() + ", req=" + req + ']');
            }
            else
                startTime = 0;

            ClientListenerResponse resp;

            try (OperationSecurityContext ignored = ctx.security().withContext(connCtx.securityContext())) {
                resp = hnd.handle(req);
            }

            if (resp != null) {
                if (resp instanceof ClientListenerAsyncResponse) {
                    ((ClientListenerAsyncResponse)resp).future().listen(fut -> {
                        try {
                            handleResponse(req, fut.get(), startTime, ses, parser);
                        }
                        catch (Throwable e) {
                            handleError(req, e, ses, parser, hnd);
                        }
                    });
                }
                else
                    handleResponse(req, resp, startTime, ses, parser);
            }
        }
        catch (Throwable e) {
            handleError(req, e, ses, parser, hnd);
        }
    }

    /** */
    private void handleResponse(
        ClientListenerRequest req,
        ClientListenerResponse resp,
        long startTime,
        GridNioSession ses,
        ClientListenerMessageParser parser
    ) {
        if (log.isTraceEnabled()) {
            long dur = (System.nanoTime() - startTime) / 1000;

            log.trace("Client request processed [reqId=" + req.requestId() + ", dur(mcs)=" + dur +
                ", resp=" + resp.status() + ']');
        }

        GridNioFuture<?> fut = ses.send(parser.encode(resp));

        fut.listen(() -> {
            if (fut.error() == null)
                resp.onSent();
        });
    }

    /** */
    private void handleError(
        ClientListenerRequest req,
        Throwable e,
        GridNioSession ses,
        ClientListenerMessageParser parser,
        ClientListenerRequestHandler hnd
    ) {
        hnd.unregisterRequest(req.requestId());

        if (e instanceof Error)
            U.error(log, "Failed to process client request [req=" + req + ", msg=" + e.getMessage() + "]", e);
        else
            U.warn(log, "Failed to process client request [req=" + req + ", msg=" + e.getMessage() + "]", e);

        ses.send(parser.encode(hnd.handleException(e, req)));

        if (e instanceof Error)
            throw (Error)e;
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void onFailure(FailureType failureType, Throwable failure) {
        if (failure instanceof OutOfMemoryError)
            ctx.failure().process(new FailureContext(failureType, failure));
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

                metrics.onHandshakeTimeout();

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
        }
        catch (Exception e) {
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
    private void onHandshake(GridNioSession ses, ClientMessage msg) {
        BinaryContext ctx = new BinaryContext(BinaryUtils.cachingMetadataHandler(), null, null, null, null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextImpl(null, null));

        ctx.configure(marsh);

        BinaryReaderEx reader = BinaryUtils.reader(ctx, BinaryStreams.inputStream(msg.payload()), null, true);

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

        BinaryWriterEx writer = BinaryUtils.writer(null, BinaryStreams.outputStream(8), null);

        byte clientType = reader.readByte();

        ClientListenerConnectionContext connCtx = null;

        try {
            connCtx = prepareContext(clientType, ses);

            ensureClientPermissions(clientType);

            if (connCtx.isVersionSupported(ver)) {
                connCtx.initializeFromHandshake(ses, ver, reader);

                ensureConnectionAllowed(connCtx);

                ses.addMeta(CONN_CTX_META_KEY, connCtx);
            }
            else
                throw new IgniteCheckedException("Unsupported version: " + ver.asString());

            cancelHandshakeTimeout(ses);

            connCtx.handler().writeHandshake(writer);

            metrics.onHandshakeAccept(clientType);

            if (log.isDebugEnabled()) {
                String login = connCtx.securityContext() == null ? null :
                    connCtx.securityContext().subject().login().toString();

                log.debug("Client handshake accepted [rmtAddr=" + ses.remoteAddress() +
                    ", type=" + clientTypeLabel(clientType) + ", ver=" + ver.asString() +
                    ", login=" + login + ", connId=" + connCtx.connectionId() + ']');
            }
        }
        catch (IgniteAccessControlException authEx) {
            metrics.onFailedAuth();

            if (log.isDebugEnabled()) {
                log.debug("Client authentication failed [rmtAddr=" + ses.remoteAddress() +
                    ", type=" + clientTypeLabel(clientType) + ", ver=" + ver.asString() +
                    ", err=" + authEx.getMessage() + ']');
            }

            writer.writeBoolean(false);

            writer.writeShort((short)0);
            writer.writeShort((short)0);
            writer.writeShort((short)0);

            writer.writeString(authEx.getMessage());

            if (ver.compareTo(ClientConnectionContext.VER_1_1_0) >= 0)
                writer.writeInt(ClientStatus.AUTH_FAILED);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Error during handshake [rmtAddr=" + ses.remoteAddress() +
                ", type=" + clientTypeLabel(clientType) + ", ver=" + ver.asString() + ", msg=" + e.getMessage() + ']');

            metrics.onGeneralReject();

            ClientListenerProtocolVersion currVer;

            if (connCtx == null)
                currVer = ClientListenerProtocolVersion.create(0, 0, 0);
            else
                currVer = connCtx.defaultVersion();

            writer.writeBoolean(false);

            writer.writeShort(currVer.major());
            writer.writeShort(currVer.minor());
            writer.writeShort(currVer.maintenance());

            writer.writeString(e.getMessage());

            if (ver.compareTo(ClientConnectionContext.VER_1_1_0) >= 0) {
                writer.writeInt(e instanceof ClientConnectionNodeRecoveryException
                    ? ClientStatus.NODE_IN_RECOVERY_MODE
                    : ClientStatus.FAILED);
            }
        }

        ses.send(new ClientMessage(writer.array()));
    }

    /**
     * Prepare context.
     *
     * @param ses Client's NIO session.
     * @param clientType Client type.
     * @return Context.
     * @throws IgniteCheckedException If failed.
     */
    private ClientListenerConnectionContext prepareContext(byte clientType, GridNioSession ses)
        throws IgniteCheckedException {
        long connId = nextConnectionId();

        switch (clientType) {
            case ODBC_CLIENT:
                return new OdbcConnectionContext(ctx, ses, busyLock, connId, maxCursors);

            case JDBC_CLIENT:
                return new JdbcConnectionContext(ctx, ses, busyLock, connId, maxCursors);

            case THIN_CLIENT:
                return new ClientConnectionContext(ctx, ses, connId, maxCursors, thinCfg);
        }

        throw new IgniteCheckedException("Unknown client type: " + clientType);
    }

    /**
     * Generate unique connection id.
     * @return connection id.
     */
    private long nextConnectionId() {
        long shiftedId = nodeInRecoveryMode() ? MANAGEMENT_CONNECTION_SHIFTED_ID : ctx.discovery().localNode().order();

        return (shiftedId << 32) + nextConnId.getAndIncrement();
    }

    /**
     * @return {@code True} if node in recovery mode and does not join topology yet.
     * {@link GridKernalContext#recoveryMode()} returns {@code true} before join topology
     * and some resources (local node etc.) are not available.
     */
    private boolean nodeInRecoveryMode() {
        return !ctx.discovery().localJoinFuture().isDone();
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

    /**
     * Ensures if the client are allowed to connect.
     *
     * @param connCtx Connection context.
     * @throws IgniteCheckedException If failed.
     */
    private void ensureConnectionAllowed(ClientListenerConnectionContext connCtx) throws IgniteCheckedException {
        boolean isControlUtility = connCtx.clientType() == THIN_CLIENT && connCtx.managementClient();

        if (nodeInRecoveryMode()) {
            if (!isControlUtility)
                throw new ClientConnectionNodeRecoveryException("Node in recovery mode.");

            return;
        }

        if (newConnEnabled.test(connCtx.clientType()))
            return;

        if (!isControlUtility)
            throw new IgniteAccessControlException(CONN_DISABLED_BY_ADMIN_ERR_MSG);

        // When security is enabled, only an administrator can connect and execute commands.
        if (connCtx.securityContext() != null) {
            try (OperationSecurityContext ignored = ctx.security().withContext(connCtx.securityContext())) {
                ctx.security().authorize(SecurityPermission.ADMIN_OPS);
            }
            catch (SecurityException e) {
                throw new IgniteAccessControlException("ADMIN_OPS permission required");
            }
        }
    }
}
