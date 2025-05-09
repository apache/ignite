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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.exception.GridRedisGenericException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_KEYS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_SIZE;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis HKEYS command handler.
 */
public class GridRedisKeysCommandHandler extends GridRedisRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    	KEYS,HKEYS,SCAN,HSCAN
    );

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisKeysCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
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
        GridRedisCommand cmd = msg.command();
        
        restReq.clientId(msg.clientId());
       
        restReq.command(CACHE_GET_KEYS);
        restReq.cacheName(msg.cacheName());
        
        if(cmd==SCAN || cmd==HSCAN ) {      	
        	 // cmd, key, curser, MATCH pattem, COUNT 1000
        	String matchP = stringValue("MATCH",msg.auxMKeys());
        	if(matchP!=null) {
        		restReq.key(matchP);
        	}
        }
        else {
        	restReq.key(msg.key());
        }

        return restReq;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer makeResponse(final GridRestResponse restRes, List<String> params) {
    	if(restRes.getResponse() == null)
    		return GridRedisProtocolParser.nil();
    	
    	List<String> list = (List)restRes.getResponse();
    	if(params.size()>1) { 
	    	int offset = 0;
	    	if(params.get(1).matches("\\d")){ // cmd, key, curser, MATCH pattem, COUNT 1000
	    		offset = Integer.valueOf(params.get(1));
	    	}
	    	else if(params.get(0).matches("\\d")){ // cmd, curser, MATCH pattem, COUNT 1000
	    		offset = Integer.valueOf(params.get(0));
	    	}
	    	
	    	int count = 10;
	    	try {
				Long countP = longValue("COUNT",params);
				if(countP!=null) {
					count = countP.intValue();
				}
			} catch (GridRedisGenericException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	if(list.size()>offset+count) {
	    		list = list.subList(offset, offset+count);
	    	}
	    	else if(offset>0){
	    		list = list.subList(offset, list.size());
	    	}
    	}
    	
        return  GridRedisProtocolParser.toArray(list);
    }
    
    
}
