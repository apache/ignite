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

package org.apache.ignite.internal.processors.rest.handlers.redis;

import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisNioListener;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.ECHO;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.PING;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.QUIT;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SELECT;

/**
 * Redis connection handler.
 */
public class GridRedisConnectionCommandHandler implements GridRedisCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        PING,
        QUIT,
        ECHO,
        SELECT
    );

    /** Grid context. */
    private final GridKernalContext ctx;

    /** PONG response to PING. */
    private static final String PONG = "PONG";

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Context.
     */
    public GridRedisConnectionCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd,
        GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
        assert msg != null;

        switch (msg.command()) {
            case PING:
                msg.setResponse(GridRedisProtocolParser.toSimpleString(PONG));

                return new GridFinishedFuture<>(msg);

            case QUIT:
                msg.setResponse(GridRedisProtocolParser.oKString());

                return new GridFinishedFuture<>(msg);

            case ECHO:
                msg.setResponse(GridRedisProtocolParser.toSimpleString(msg.key()));

                return new GridFinishedFuture<>(msg);

            case SELECT:
                String cacheIdx = msg.key();

                if (F.isEmpty(cacheIdx))
                    msg.setResponse(GridRedisProtocolParser.toGenericError("No cache index specified"));
                else {
                    String cacheName = GridRedisMessage.CACHE_NAME_PREFIX + "-" + cacheIdx;

                    CacheConfiguration ccfg = ctx.cache().cacheConfiguration(GridRedisMessage.DFLT_CACHE_NAME);
                    ccfg.setName(cacheName);

                    ctx.grid().getOrCreateCache(ccfg);

                    ses.addMeta(GridRedisNioListener.CONN_CTX_META_KEY, cacheName);

                    msg.setResponse(GridRedisProtocolParser.oKString());
                }
                return new GridFinishedFuture<>(msg);
        }

        return new GridFinishedFuture<>();
    }
}
