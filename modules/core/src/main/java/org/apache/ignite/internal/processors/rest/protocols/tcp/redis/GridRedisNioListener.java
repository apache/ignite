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
import org.apache.ignite.internal.processors.rest.handlers.redis.server.GridRedisDbSizeCommandHandler;
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
import org.apache.ignite.internal.util.typedef.CIX1;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for Redis protocol requests.
 */
public class GridRedisNioListener extends GridNioServerListenerAdapter<GridRedisMessage> {
    /** Logger. */
    private final IgniteLogger log;

    /** Redis-specific handlers. */
    protected final Map<GridRedisCommand, GridRedisCommandHandler> handlers = new EnumMap<>(GridRedisCommand.class);

    /**
     * @param log Logger.
     * @param hnd REST protocol handler.
     * @param ctx Context.
     */
    public GridRedisNioListener(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        this.log = log;

        // connection commands.
        addCommandHandler(new GridRedisConnectionCommandHandler());

        // string commands.
        addCommandHandler(new GridRedisGetCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisSetCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisMSetCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisMGetCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisIncrDecrCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisAppendCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisGetSetCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisStrlenCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisSetRangeCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisGetRangeCommandHandler(ctx, hnd));

        // key commands.
        addCommandHandler(new GridRedisDelCommandHandler(ctx, hnd));
        addCommandHandler(new GridRedisExistsCommandHandler(ctx, hnd));

        // server commands.
        addCommandHandler(new GridRedisDbSizeCommandHandler(ctx, hnd));
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
        if (handlers.get(msg.command()) != null) {
            IgniteInternalFuture<GridRedisMessage> f = handlers.get(msg.command()).handleAsync(msg);

            f.listen(new CIX1<IgniteInternalFuture<GridRedisMessage>>() {
                @Override public void applyx(IgniteInternalFuture<GridRedisMessage> f) throws IgniteCheckedException {
                    GridRedisMessage res = f.get();

                    sendResponse(ses, res);
                }
            });
        }
    }

    /**
     * Sends a response to be decoded and sent to the Redis client.
     *
     * @param ses NIO session.
     * @param res Response.
     * @return NIO send future.
     */
    private GridNioFuture<?> sendResponse(GridNioSession ses, GridRedisMessage res) {
        return ses.send(res);
    }
}
