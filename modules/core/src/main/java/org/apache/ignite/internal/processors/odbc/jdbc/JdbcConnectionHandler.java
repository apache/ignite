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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.GridSpinBusyLock;

/**
 * ODBC Connection Context.
 */
public class JdbcConnectionHandler {
    /** Connection ID sequence. */
    private static final AtomicLong CONN_ID_GEN = new AtomicLong();

    /** Request handlers map (connection Id -> handler). */
    private final ConcurrentHashMap<Long, JdbcConnectionContext> ctxs = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal Context.
     * @param busyLock Shutdown busy lock.
     * @param maxCursors Maximum allowed cursors.
     * @return JDBC connection context.
     */
    public JdbcConnectionContext createContext(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors) {
        long connId = CONN_ID_GEN.incrementAndGet();

        JdbcConnectionContext cctx = new JdbcConnectionContext(ctx, this, busyLock, maxCursors, connId);

        ctxs.put(connId, cctx);

        return cctx;
    }

    /**
     * @param connId Connection ID.
     * @return Handler for specified connection.
     */
    public JdbcRequestHandler handler(long connId) {
        JdbcConnectionContext ctx = ctxs.get(connId);

        return ctx == null ? null : (JdbcRequestHandler)ctx.handler();
    }

    /**
     * @param connId Connection ID of the disconnected client.
     */
    public void onDisconnect(long connId) {
        ctxs.remove(connId);
    }
}
