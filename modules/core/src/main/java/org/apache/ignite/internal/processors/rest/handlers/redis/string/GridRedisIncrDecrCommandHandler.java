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
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
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

    /** {@inheritDoc} */
    public GridRedisIncrDecrCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd) {
        super(log, hnd);
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

        GridRestResponse getResp = hnd.handle(getReq);

        if (getResp.getResponse() == null)
            restReq.initial(0L);
        else {
            if (getResp.getResponse() instanceof Long && (Long)getResp.getResponse() <= Long.MAX_VALUE)
                restReq.initial((Long)getResp.getResponse());
            else
                throw new GridRedisGenericException("An initial value must be numeric and in range");
        }

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());
        restReq.delta(1L);

        if (msg.messageSize() > 2) {
            try {
                restReq.delta(Long.valueOf(msg.aux(DELTA_POS)));
            }
            catch (NumberFormatException e) {
                U.error(log, "Wrong increment delta", e);
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
}
