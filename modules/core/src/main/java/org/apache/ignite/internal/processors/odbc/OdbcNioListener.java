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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcMessageParser;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcRequestHandler;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ODBC message listener.
 */
public class OdbcNioListener extends GridNioServerListenerAdapter<byte[]> {
    /** Current version. */
    private static final SqlListenerProtocolVersion CURRENT_VER = SqlListenerProtocolVersion.create(2, 1, 0);

    /** Supported versions. */
    private static final Set<SqlListenerProtocolVersion> SUPPORTED_VERS = new HashSet<>();

    /** Connection-related metadata key. */
    private static final int CONN_CTX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Request ID generator. */
    private static final AtomicLong REQ_ID_GEN = new AtomicLong();

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Logger. */
    private final IgniteLogger log;

    static {
        SUPPORTED_VERS.add(CURRENT_VER);
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param busyLock Shutdown busy lock.
     * @param maxCursors Maximum allowed cursors.
     */
    public OdbcNioListener(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (log.isDebugEnabled())
            log.debug("SQL client connected: " + ses.remoteAddress());
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (log.isDebugEnabled()) {
            if (e == null)
                log.debug("SQL client disconnected: " + ses.remoteAddress());
            else
                log.debug("SQL client disconnected due to an error [addr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, byte[] msg) {
        assert msg != null;

        SqlListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

        if (connCtx == null) {
            onHandshake(ses, msg);

            return;
        }

        SqlListenerMessageParser parser = connCtx.parser();

        SqlListenerRequest req;

        try {
            req = parser.decode(msg);
        }
        catch (Exception e) {
            log.error("Failed to parse SQL client request [err=" + e + ']');

            ses.close();

            return;
        }

        assert req != null;

        req.requestId(REQ_ID_GEN.incrementAndGet());

        try {
            long startTime = 0;

            if (log.isDebugEnabled()) {
                startTime = System.nanoTime();

                log.debug("SQL client request received [reqId=" + req.requestId() + ", addr=" + ses.remoteAddress() +
                    ", req=" + req + ']');
            }

            SqlListenerRequestHandler handler = connCtx.handler();

            SqlListenerResponse resp = handler.handle(req);

            if (log.isDebugEnabled()) {
                long dur = (System.nanoTime() - startTime) / 1000;

                log.debug("SQL client request processed [reqId=" + req.requestId() + ", dur(mcs)=" + dur  +
                    ", resp=" + resp.status() + ']');
            }

            byte[] outMsg = parser.encode(resp);

            ses.send(outMsg);
        }
        catch (Exception e) {
            log.error("Failed to process SQL client request [reqId=" + req.requestId() + ", err=" + e + ']');

            ses.send(parser.encode(new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, e.toString())));
        }
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

        if (cmd != SqlListenerRequest.HANDSHAKE) {
            log.error("Unexpected SQL client request (will close session): " + ses.remoteAddress());

            ses.close();

            return;
        }

        short verMajor = reader.readShort();
        short verMinor = reader.readShort();
        short verMaintenance = reader.readShort();

        SqlListenerProtocolVersion ver = SqlListenerProtocolVersion.create(verMajor, verMinor, verMaintenance);

        String errMsg = null;

        if (SUPPORTED_VERS.contains(ver)) {
            // Prepare context.
            SqlListenerConnectionContext connCtx = prepareContext(ver, reader);

            ses.addMeta(CONN_CTX_META_KEY, connCtx);
        }
        else {
            log.warning("Unsupported version: " + ver.toString());

            errMsg = "Unsupported version.";
        }

        // Send response.
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(8), null, null);

        if (errMsg == null) {
            writer.writeBoolean(true);
        }
        else {
            writer.writeBoolean(false);
            writer.writeShort(CURRENT_VER.major());
            writer.writeShort(CURRENT_VER.minor());
            writer.writeShort(CURRENT_VER.maintenance());
            writer.doWriteString(errMsg);
        }

        ses.send(writer.array());
    }

    /**
     * Prepare context.
     *
     * @param ver Version.
     * @param reader Reader.
     * @return Context.
     */
    private SqlListenerConnectionContext prepareContext(SqlListenerProtocolVersion ver, BinaryReaderExImpl reader) {
        // TODO: Switch between ODBC and JDBC.
        boolean distributedJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();

        OdbcRequestHandler handler =
            new OdbcRequestHandler(ctx, busyLock, maxCursors, distributedJoins, enforceJoinOrder);

        OdbcMessageParser parser = new OdbcMessageParser(ctx);

        return new SqlListenerConnectionContext(handler, parser);
    }
}
