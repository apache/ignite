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
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * ODBC message listener.
 */
public class OdbcNioListener extends GridNioServerListenerAdapter<byte[]> {
    /** Connection-related metadata key. */
    private static final int CONNECTION_DATA_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

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
            log.debug("ODBC client connected: " + ses.remoteAddress());

        ses.addMeta(CONNECTION_DATA_META_KEY, new ConnectionData(ctx, busyLock));
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (log.isDebugEnabled()) {
            if (e == null)
                log.debug("ODBC client disconnected: " + ses.remoteAddress());
            else
                log.debug("ODBC client disconnected due to an error [addr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, byte[] msg) {
        assert msg != null;

        long reqId = REQ_ID_GEN.incrementAndGet();

        ConnectionData connData = ses.meta(CONNECTION_DATA_META_KEY);

        assert connData != null;

        OdbcMessageParser parser = connData.getParser();

        OdbcRequest req;

        try {
            req = parser.decode(msg);
        }
        catch (Exception e) {
            log.error("Failed to parse message [id=" + reqId + ", err=" + e + ']');

            ses.close();

            return;
        }

        assert req != null;

        try {
            long startTime = 0;

            if (log.isDebugEnabled()) {
                startTime = System.nanoTime();

                log.debug("ODBC request received [id=" + reqId + ", addr=" + ses.remoteAddress() +
                    ", req=" + req + ']');
            }

            OdbcRequestHandler handler = connData.getHandler();

            OdbcResponse resp = handler.handle(reqId, req);

            if (log.isDebugEnabled()) {
                long dur = (System.nanoTime() - startTime) / 1000;

                log.debug("ODBC request processed [id=" + reqId + ", dur(mcs)=" + dur  +
                    ", resp=" + resp.status() + ']');
            }

            byte[] outMsg = parser.encode(resp);

            ses.send(outMsg);
        }
        catch (Exception e) {
            log.error("Failed to process ODBC request [id=" + reqId + ", err=" + e + ']');

            ses.send(parser.encode(new OdbcResponse(OdbcResponse.STATUS_FAILED, e.toString())));
        }
    }

    /**
     * Connection-related data.
     */
    private class ConnectionData {
        /** Request handler. */
        private final OdbcRequestHandler handler;

        /** Message parser. */
        private final OdbcMessageParser parser;

        /**
         * @param ctx Context.
         * @param busyLock Shutdown busy lock.
         */
        public ConnectionData(GridKernalContext ctx, GridSpinBusyLock busyLock) {
            handler = new OdbcRequestHandler(ctx, busyLock, maxCursors);
            parser = new OdbcMessageParser(ctx);
        }

        /**
         * Handler getter.
         * @return Request handler for the connection.
         */
        public OdbcRequestHandler getHandler() {
            return handler;
        }

        /**
         * Parser getter
         * @return Message parser for the connection.
         */
        public OdbcMessageParser getParser() {
            return parser;
        }
    }
}
