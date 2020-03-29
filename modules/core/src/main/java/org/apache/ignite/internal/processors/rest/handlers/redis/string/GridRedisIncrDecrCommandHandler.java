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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisTypeException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.DataStructuresRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_DECREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_INCREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.DECR;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.DECRBY;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.INCR;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.INCRBY;

/**
 * Redis INCR/DECR command handler.
 */
public class GridRedisIncrDecrCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        INCR,
        DECR,
        INCRBY,
        DECRBY
    );

    /** Delta position in the message. */
    private static final int DELTA_POS = 2;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisIncrDecrCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        DataStructuresRequest restReq = new DataStructuresRequest();

        GridRestCacheRequest getReq = new GridRestCacheRequest();

        getReq.clientId(msg.clientId());
        getReq.key(msg.key());
        getReq.command(CACHE_GET);
        getReq.cacheName(msg.cacheName());

        GridRestResponse getResp = hnd.handle(getReq);

        if (getResp.getResponse() == null)
            restReq.initial(0L);
        else {
            if (getResp.getResponse() instanceof String) {
                long init;

                try {
                    init = Long.parseLong((String)getResp.getResponse());

                    restReq.initial(init);
                }
                catch (Exception e) {
                    U.error(log, "An initial value must be numeric and in range", e);

                    throw new GridRedisGenericException("An initial value must be numeric and in range");
                }

                if ((init == Long.MAX_VALUE && (msg.command() == INCR || msg.command() == INCRBY))
                    || (init == Long.MIN_VALUE && (msg.command() == DECR || msg.command() == DECRBY)))
                    throw new GridRedisGenericException("Increment or decrement would overflow");
            }
            else
                throw new GridRedisTypeException("Operation against a key holding the wrong kind of value");

            // remove from cache.
            GridRestCacheRequest rmReq = new GridRestCacheRequest();

            rmReq.clientId(msg.clientId());
            rmReq.key(msg.key());
            rmReq.command(CACHE_REMOVE);
            rmReq.cacheName(msg.cacheName());

            Object rmResp = hnd.handle(rmReq).getResponse();

            if (rmResp == null || !(boolean)rmResp)
                throw new GridRedisGenericException("Cannot incr/decr on the non-atomiclong key");
        }

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());
        restReq.delta(1L);

        if (msg.messageSize() > 2) {
            try {
                Long delta = Long.valueOf(msg.aux(DELTA_POS));

                // check if it can be safely added.
                safeAdd(restReq.initial(), delta);

                restReq.delta(delta);
            }
            catch (NumberFormatException | ArithmeticException e) {
                U.error(log, "An increment value must be numeric and in range", e);

                throw new GridRedisGenericException("An increment value must be numeric and in range");
            }
        }

        switch (msg.command()) {
            case INCR:
            case INCRBY:
                restReq.command(ATOMIC_INCREMENT);

                break;

            case DECR:
            case DECRBY:
                restReq.command(ATOMIC_DECREMENT);

                break;

            default:
                assert false : "Unexpected command received";
        }

        return restReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        if (restRes.getResponse() == null)
            return GridRedisProtocolParser.toGenericError("Failed to increment");

        if (restRes.getResponse() instanceof Long && (Long)restRes.getResponse() <= Long.MAX_VALUE)
            return GridRedisProtocolParser.toInteger(String.valueOf(restRes.getResponse()));
        else
            return GridRedisProtocolParser.toTypeError("Value is non-numeric or out of range");
    }

    /**
     * Safely add long values.
     *
     * @param left A long value.
     * @param right A long value.
     * @return An addition result or an exception is thrown when overflow occurs.
     */
    private static long safeAdd(long left, long right) {
        if (right > 0 ? left > Long.MAX_VALUE - right
            : left < Long.MIN_VALUE - right) {
            throw new ArithmeticException("Long overflow");
        }
        return left + right;
    }
}
