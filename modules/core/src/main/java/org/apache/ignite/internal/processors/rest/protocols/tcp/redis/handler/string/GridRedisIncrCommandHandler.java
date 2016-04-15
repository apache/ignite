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

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis.handler.string;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.handler.GridRedisStringCommandHandler;
import org.apache.ignite.internal.processors.rest.request.DataStructuresRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_INCREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.INCR;

/**
 * Redis INCR command handler.
 */
public class GridRedisIncrCommandHandler extends GridRedisStringCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        INCR
    );

    /** {@inheritDoc} */
    public GridRedisIncrCommandHandler(GridRestProtocolHandler hnd) {
        super(hnd);
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
        GridRedisMessage getMsg = msg.copy(msg);

        getReq.clientId(getMsg.clientId());
        getReq.key(getMsg.key());
        getReq.command(CACHE_GET);

        GridRestResponse getResp = hnd.handle(getReq);

        if (getResp.getResponse() == null) {
            restReq.initial(0L);
        }
        else {
            if (getResp.getResponse() instanceof Long)
                restReq.initial((Long)getResp.getResponse());
            else
                throw new IgniteCheckedException("Failed to obtain an initial value!");
        }

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());
        restReq.delta(1L);
        restReq.command(ATOMIC_INCREMENT);

        return restReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes) {
        if (restRes.getResponse() == null)
            return GridRedisProtocolParser.toGenericError("Failed to increment!");

        Long val;
        if (restRes.getResponse() instanceof Long)
            val = (Long)restRes.getResponse();
        else
            return GridRedisProtocolParser.toTypeError("Non-numeric value!");

        return GridRedisProtocolParser.toInteger(new BigDecimal(val).intValueExact());
    }
}
