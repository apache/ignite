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
import java.util.Map;

import org.apache.ignite.*;
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

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.handlers.redis.list.GridRedisStreamCommandHandler.combineToLong;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis DEL command handler.
 */
public class GridRedisDelCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        DEL,XDEL
    );

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisDelCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;
        GridRedisCommand cmd = msg.command();
        if (msg.messageSize() < 2)
            throw new GridRedisGenericException("Wrong number of arguments");

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        restReq.key(msg.key());
        restReq.command(CACHE_REMOVE);
        restReq.cacheName(msg.cacheName());

        if(cmd==XDEL){
            List<String> keys = msg.aux();
            if(keys.size()>1) {
                restReq.command(CACHE_REMOVE_ALL);
                Map<Object, Object> mget = U.newHashMap(keys.size());
                for (String key : keys)
                    mget.put(combineToLong(key), key);

                restReq.values(mget);
            }
            else if(keys.size()==1){
                restReq.key(combineToLong(msg.key()));
            }
        }
        else{
            List<String> keys = msg.auxMKeys();
            if(keys.size()>1) {
                restReq.command(CACHE_REMOVE_ALL);
                Map<Object, Object> mget = U.newHashMap(keys.size());
                for (String key : keys)
                    mget.put(key, key);

                restReq.values(mget);
            }
        }
        return restReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, GridRedisMessage msg, List<String> params) {
        if(restRes.getResponse() == null || restRes.getResponse().equals(Boolean.FALSE)) {
            if(msg.command()==XDEL){
                if(params.isEmpty()){
                    ctx.grid().destroyCache(msg.cacheName());
                    return GridRedisProtocolParser.toInteger("1");
                }
                return GridRedisProtocolParser.toInteger("0");
            }
            IgniteAtomicLong l = ctx.grid().atomicLong(msg.key(), 0, false);
            if (l != null) {
                l.close();
                return GridRedisProtocolParser.toInteger("1");
            }
            String queueName = msg.cacheName()+"-"+msg.key();
            IgniteQueue list = ctx.grid().queue(queueName,0,null);
            if (list != null) {
                list.close();
                return GridRedisProtocolParser.toInteger("1");
            }
            IgniteSet set = ctx.grid().set(queueName,null);
            if (set != null) {
                set.close();
                return GridRedisProtocolParser.toInteger("1");
            }
            return GridRedisProtocolParser.toInteger("0");
        }
        else{
            // It has to respond with the number of removed entries...
            return GridRedisProtocolParser.toInteger(String.valueOf(params.size()));
        }
    }
}
