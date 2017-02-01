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

package org.apache.ignite.internal.processors.rest.handlers.redis.string;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_IF_ABSENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REPLACE;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SET;

/**
 * Redis SET command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisSetCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        SET
    );

    /** Value position in Redis message. */
    private static final int VAL_POS = 2;

    /** Grid context. */
    private final GridKernalContext ctx;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Context.
     */
    public GridRedisSetCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd,
        GridKernalContext ctx) {
        super(log, hnd);

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        // check if an atomic long with the key exists (related to incr/decr).
        IgniteAtomicLong l = ctx.grid().atomicLong(msg.key(), 0, false);

        if (l != null) {
            try {
                l.close();
            }
            catch (IgniteException ignored) {
                U.warn(log, "Failed to remove atomic long for key [" + msg.key() + "]");
            }
        }

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());

        restReq.command(CACHE_PUT);

        if (msg.messageSize() < 3)
            throw new GridRedisGenericException("Wrong number of arguments");

        restReq.value(msg.aux(VAL_POS));

        if (msg.messageSize() >= 4) {
            List<String> params = msg.aux();

            // get rid of SET value.
            params.remove(0);

            if (isNx(params))
                restReq.command(CACHE_PUT_IF_ABSENT);
            else if (isXx(params))
                restReq.command(CACHE_REPLACE);

            // TODO: IGNITE-4226: Need properly handle expiration parameter.
        }

        return restReq;
    }

    /**
     * @param params Command parameters.
     * @return True if NX option is available, otherwise false.
     */
    private boolean isNx(List<String> params) {
        if (params.size() >= 3)
            return "nx".equalsIgnoreCase(params.get(0)) || "nx".equalsIgnoreCase(params.get(2));

        return "nx".equalsIgnoreCase(params.get(0));
    }

    /**
     * @param params Command parameters.
     * @return True if XX option is available, otherwise false.
     */
    private boolean isXx(List<String> params) {
        if (params.size() >= 3)
            return "xx".equalsIgnoreCase(params.get(0)) || "xx".equalsIgnoreCase(params.get(2));

        return "xx".equalsIgnoreCase(params.get(0));
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        Object resp = restRes.getResponse();

        if (resp == null)
            return GridRedisProtocolParser.nil();

        return (!(boolean)resp ? GridRedisProtocolParser.nil() : GridRedisProtocolParser.oKString());
    }
}
