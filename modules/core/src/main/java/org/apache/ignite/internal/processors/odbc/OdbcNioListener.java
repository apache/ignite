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
import org.apache.ignite.internal.processors.odbc.odbc.OdbcMessageParser;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcRequestHandler;
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

        OdbcRequestHandler handler = new OdbcRequestHandler(ctx, busyLock, maxCursors);
        OdbcMessageParser parser = new OdbcMessageParser(ctx);

        ses.addMeta(CONN_CTX_META_KEY, new SqlListenerConnectionContext(handler, parser));
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

        SqlListenerConnectionContext connData = ses.meta(CONN_CTX_META_KEY);

        assert connData != null;

        SqlListenerMessageParser parser = connData.parser();

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

            SqlListenerRequestHandler handler = connData.handler();

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
}
