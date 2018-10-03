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

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SETRANGE;

/**
 * Redis SETRANGE command handler.
 */
public class GridRedisSetRangeCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        SETRANGE
    );

    /** Offset position in Redis message among parameters. */
    private static final int OFFSET_POS = 2;

    /** Value position in Redis message. */
    private static final int VAL_POS = 3;

    /** Maximum offset. */
    private static final int MAX_OFFSET = 536870911;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisSetRangeCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        if (msg.messageSize() < 4)
            throw new GridRedisGenericException("Wrong number of arguments");

        int off;

        try {
            off = Integer.parseInt(msg.aux(OFFSET_POS));
        }
        catch (NumberFormatException e) {
            U.error(log, "Erroneous offset", e);
            throw new GridRedisGenericException("Offset is not an integer");
        }

        String val = String.valueOf(msg.aux(VAL_POS));

        GridRestCacheRequest getReq = new GridRestCacheRequest();

        getReq.clientId(msg.clientId());
        getReq.key(msg.key());
        getReq.command(CACHE_GET);
        getReq.cacheName(msg.cacheName());

        if (val.isEmpty())
            return getReq;

        Object resp = hnd.handle(getReq).getResponse();

        int totalLen = off + val.length();

        if (off < 0 || totalLen > MAX_OFFSET)
            throw new GridRedisGenericException("Offset is out of range");

        GridRestCacheRequest putReq = new GridRestCacheRequest();

        putReq.clientId(msg.clientId());
        putReq.key(msg.key());
        putReq.command(CACHE_PUT);
        putReq.cacheName(msg.cacheName());

        if (resp == null) {
            byte[] dst = new byte[totalLen];

            System.arraycopy(val.getBytes(), 0, dst, off, val.length());

            putReq.value(new String(dst));
        }
        else {
            if (!(resp instanceof String))
                return getReq;

            String cacheVal = String.valueOf(resp);

            cacheVal = cacheVal.substring(0, off) + val;

            putReq.value(cacheVal);
        }

        hnd.handle(putReq);

        return getReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
        if (restRes.getResponse() == null)
            return GridRedisProtocolParser.toInteger("0");

        if (restRes.getResponse() instanceof String) {
            int resLen = ((String)restRes.getResponse()).length();
            return GridRedisProtocolParser.toInteger(String.valueOf(resLen));
        }
        else
            return GridRedisProtocolParser.toTypeError("Operation against a key holding the wrong kind of value");
    }
}
