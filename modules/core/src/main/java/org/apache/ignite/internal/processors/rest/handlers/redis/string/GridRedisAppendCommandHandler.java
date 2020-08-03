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
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_APPEND;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.APPEND;

/**
 * Redis APPEND command handler.
 */
public class GridRedisAppendCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        APPEND
    );

    /** Position of the value. */
    private static final int VAL_POS = 2;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisAppendCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 3)
            throw new GridRedisGenericException("Wrong syntax");

        GridRestCacheRequest appendReq = new GridRestCacheRequest();
        GridRestCacheRequest getReq = new GridRestCacheRequest();

        String val = msg.aux(VAL_POS);

        appendReq.clientId(msg.clientId());
        appendReq.key(msg.key());
        appendReq.value(val);
        appendReq.command(CACHE_APPEND);
        appendReq.cacheName(msg.cacheName());

        Object resp = hnd.handle(appendReq).getResponse();
        if (resp != null && !(boolean)resp) {
            // append on non-existing key.
            GridRestCacheRequest setReq = new GridRestCacheRequest();

            setReq.clientId(msg.clientId());
            setReq.key(msg.key());
            setReq.value(val);
            setReq.command(CACHE_PUT);
            setReq.cacheName(msg.cacheName());

            hnd.handle(setReq);
        }

        getReq.clientId(msg.clientId());
        getReq.key(msg.key());
        getReq.command(CACHE_GET);
        getReq.cacheName(msg.cacheName());

        return getReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        if (restRes.getResponse() == null)
            return GridRedisProtocolParser.nil();

        if (restRes.getResponse() instanceof String) {
            int resLen = ((String)restRes.getResponse()).length();

            return GridRedisProtocolParser.toInteger(String.valueOf(resLen));
        }
        else
            return GridRedisProtocolParser.toTypeError("Operation against a key holding the wrong kind of value");
    }
}
