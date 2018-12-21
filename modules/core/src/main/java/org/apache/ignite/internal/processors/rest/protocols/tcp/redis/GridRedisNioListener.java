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

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis;

import java.util.EnumMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisConnectionCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.key.GridRedisDelCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.key.GridRedisExistsCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.key.GridRedisExpireCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.server.GridRedisDbSizeCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.server.GridRedisFlushCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisAppendCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisGetCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisGetRangeCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisGetSetCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisIncrDecrCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisMGetCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisMSetCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisSetCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisSetRangeCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.string.GridRedisStrlenCommandHandler;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for Redis protocol requests.
 */
public class GridRedisNioListener extends GridNioServerListenerAdapter<GridRedisMessage> {
    /** Logger. */
    private final IgniteLogger log;

    /** Redis-specific handlers. */
    protected final Map<GridRedisCommand, GridRedisCommandHandler> handlers = new EnumMap<>(GridRedisCommand.class);

    /** Connection-related metadata key. Used for cache name only. */
    public static final int CONN_CTX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /**
     * @param log Logger.
     * @param hnd REST protocol handler.
     * @param ctx Context.
     */
    public GridRedisNioListener(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        this.log = log;

        // connection commands.
        addCommandHandler(new GridRedisConnectionCommandHandler(log, hnd, ctx));

        // string commands.
        addCommandHandler(new GridRedisGetCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisSetCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisMSetCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisMGetCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisIncrDecrCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisAppendCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisGetSetCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisStrlenCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisSetRangeCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisGetRangeCommandHandler(log, hnd, ctx));

        // key commands.
        addCommandHandler(new GridRedisDelCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisExistsCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisExpireCommandHandler(log, hnd, ctx));

        // server commands.
        addCommandHandler(new GridRedisDbSizeCommandHandler(log, hnd, ctx));
        addCommandHandler(new GridRedisFlushCommandHandler(log, hnd, ctx));
    }

    /**
     * Adds Redis-specific command handlers.
     * <p>
     * Generic commands are treated by REST.
     *
     * @param hnd Redis-specific command handler.
     */
    private void addCommandHandler(GridRedisCommandHandler hnd) {
        assert !handlers.containsValue(hnd);

        if (log.isDebugEnabled())
            log.debug("Added Redis command handler: " + hnd);

        for (GridRedisCommand cmd : hnd.supportedCommands()) {
            assert !handlers.containsKey(cmd) : cmd;

            handlers.put(cmd, hnd);
        }
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        // No-op, never called.
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        // No-op, never called.
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void onMessage(final GridNioSession ses, final GridRedisMessage msg) {
        if (handlers.get(msg.command()) == null) {
            U.warn(log, "Cannot find the corresponding command (session will be closed) [ses=" + ses +
                ", command=" + msg.command().name() + ']');

            ses.close();

            return;
        }
        else {
            String cacheName = ses.meta(CONN_CTX_META_KEY);

            if (cacheName != null)
                msg.cacheName(cacheName);

            IgniteInternalFuture<GridRedisMessage> f = handlers.get(msg.command()).handleAsync(ses, msg);

            f.listen(new CIX1<IgniteInternalFuture<GridRedisMessage>>() {
                @Override public void applyx(IgniteInternalFuture<GridRedisMessage> f) throws IgniteCheckedException {
                    GridRedisMessage res = f.get();

                    sendResponse(ses, res);
                }
            });
        }
    }

    /**
     * Sends a response to be encoded and sent to the Redis client.
     *
     * @param ses NIO session.
     * @param res Response.
     * @return NIO send future.
     */
    private GridNioFuture<?> sendResponse(GridNioSession ses, GridRedisMessage res) {
        return ses.send(res);
    }
}
