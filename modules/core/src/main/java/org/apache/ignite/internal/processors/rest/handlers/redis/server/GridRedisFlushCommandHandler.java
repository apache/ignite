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

package org.apache.ignite.internal.processors.rest.handlers.redis.server;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
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
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_CLEAR;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.NAME;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.BGSAVE;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.FLUSHALL;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.FLUSHDB;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.SAVE;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage.CACHE_NAME_PREFIX;

/**
 * Redis FLUSHDB/FLUSHALL command handler.
 */
public class GridRedisFlushCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        FLUSHDB, FLUSHALL, SAVE, BGSAVE
    );

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisFlushCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
        super(log, hnd, ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
        assert msg != null;

        GridRestCacheRequest restReq = new GridRestCacheRequest();

        restReq.clientId(msg.clientId());
        
        Date now = new Date();
    	DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd'T'HH_mm_ss");

        switch (msg.command()) {
            case FLUSHDB:
                restReq.command(CACHE_REMOVE_ALL);
                restReq.cacheName(msg.cacheName());

                break;
                
            case FLUSHALL:
                // CACHE_CLEAR
                Map<Object, Object> redisCaches = new HashMap<>();

                for (IgniteCacheProxy<?, ?> cache : ctx.cache().publicCaches()) {
                    if (cache.getName().startsWith(CACHE_NAME_PREFIX)) {
                        redisCaches.put(cache.getName(), cache.getName());
                    }
                }

                if (redisCaches.isEmpty())
                    throw new GridRedisGenericException("No Redis caches found");

                restReq.command(CACHE_CLEAR);
                restReq.values(redisCaches);
                break;
                
            case SAVE:
            	restReq.command(NAME);            	
            	
            	IgniteFuture<Void> fut = this.ctx.grid().snapshot().createDump(formatter.format(now),null);
            	
            	fut.get();
            	break;
            	
            case BGSAVE:
            	restReq.command(NAME);
            	
            	this.ctx.grid().snapshot().createDump(formatter.format(now),null);
            	break;
            	
            default:
            	restReq.command(NAME);
        }

        return restReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
    	if(restRes.getResponse() == null) {
    		return GridRedisProtocolParser.nil();
    	}
    	if(restRes.getResponse() instanceof Boolean) {
    		return ((Boolean)restRes.getResponse() ? GridRedisProtocolParser.oKString()
    	            : GridRedisProtocolParser.toGenericError("Failed to flush"));
    	}
    	if(restRes.getResponse() instanceof String) {
    		return GridRedisProtocolParser.toSimpleString(restRes.getResponse().toString());
    	}
    	return GridRedisProtocolParser.oKString();
    }
}
