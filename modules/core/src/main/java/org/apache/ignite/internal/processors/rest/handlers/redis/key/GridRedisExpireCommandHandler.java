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

package org.apache.ignite.internal.processors.rest.handlers.redis.key;

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
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_UPDATE_TLL;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis EXPIRE/PEXPIRE command handler.
 */
public class GridRedisExpireCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        EXPIRE, PEXPIRE, TTL, PTTL, PERSIST
    );

    /** TTL position in Redis message. */
    private static final int TTL_POS = 2;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisExpireCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 2)
            throw new GridRedisGenericException("Wrong number of arguments (key is missing)");

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());
        restReq.command(CACHE_UPDATE_TLL);
        restReq.cacheName(msg.cacheName());

        switch (msg.command()) {
            case EXPIRE:
                if (msg.messageSize() < 3)
                    throw new GridRedisGenericException("Wrong number of arguments (timeout value is missing)");
                restReq.ttl(Long.valueOf(msg.aux(TTL_POS)) * 1000);
                break;
            case PEXPIRE:
                if (msg.messageSize() < 3)
                    throw new GridRedisGenericException("Wrong number of arguments (timeout value is missing)");
                // PEXPIRE
                restReq.ttl(Long.valueOf(msg.aux(TTL_POS)));
                break;
            case TTL:
            case PTTL:
                // no ttl update
                restReq.ttl(Long.MIN_VALUE);
                break;
            default:
                // PERSIST
                restReq.ttl(Long.MAX_VALUE);
        }

        return restReq;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer makeResponse(final GridRestResponse restRes, GridRedisMessage msg, List<String> params) {
        Long res = (Long)restRes.getResponse();
        if (params.size()==2 || msg.command()==PERSIST){
            return res>0? GridRedisProtocolParser.toInteger("1")
            : GridRedisProtocolParser.toInteger("0");
        }
        else{
            return msg.command()==TTL ? GridRedisProtocolParser.toInteger(String.valueOf(res/1000))
                    : GridRedisProtocolParser.toInteger(res.toString());
        }
    }
}
