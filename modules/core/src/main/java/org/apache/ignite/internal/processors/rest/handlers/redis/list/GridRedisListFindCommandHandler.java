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

package org.apache.ignite.internal.processors.rest.handlers.redis.list;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.query.ScoredCacheEntry;
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
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.internal.FutureTask;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis List pop command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisListFindCommandHandler implements GridRedisCommandHandler {
    

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    		LPOS,LRANGE, SISMEMBER,SMEMBERS, ZRANGE
    );


    /** Logger. */
    protected final IgniteLogger log;

    /** Kernel context. */
    protected final GridKernalContext ctx;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisListFindCommandHandler(IgniteLogger log, GridKernalContext ctx) {
        this.log = log;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

   

	@Override
	public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
		assert msg != null;

        if (msg.messageSize() < 3) {            
        	msg.setResponse(GridRedisProtocolParser.toGenericError("Wrong number of arguments"));
        	return new GridFinishedFuture<>(msg);
        	// throw new GridRedisGenericException("Wrong number of arguments");
        }
        
        GridRedisCommand cmd = msg.command();
            
        String queueName = msg.cacheName()+"-"+msg.key();        
        
        if(cmd == LPOS) {         	
        	String value = msg.aux(2);
        	IgniteQueue<String> list = ctx.grid().queue(queueName,0,null);
        	  	
        	Iterator<String> it = list.iterator();
        	int n = 0;
        	while(it.hasNext()) {
        		String key = it.next(); 
        		if(key.equals(value)) {
        			break;      			
        		}
        		n++;
        	}        	
        	msg.setResponse(GridRedisProtocolParser.toInteger(n));
        	
        }
        
        else if(cmd == LRANGE) {        	
        	IgniteQueue<String> list = ctx.grid().queue(queueName,0,null);
        	Iterator<String> it = list.iterator();
        	int start = Integer.parseInt(msg.aux(2));
        	int end = Integer.parseInt(msg.aux(3));
        	if(start<0) start = list.size()+start;
        	if(end<0) end = list.size()+end;
        	int n = 0;
        	Collection<String> result = new ArrayList<>();
        	while(it.hasNext()) {
        		if(n>=end) {
        			break;
        		}
        		if(n>=start) {
        			String key = it.next();
        			result.add(key);
        		}        		
        		n++;
        	}
        	msg.setResponse(GridRedisProtocolParser.toArray(result));
        	
        }        
        else if(cmd == SISMEMBER) {
        	String query = msg.aux(2);
        	IgniteSet<String> list = ctx.grid().set(queueName, null);
        	if(list.contains(query)) {
        		msg.setResponse(GridRedisProtocolParser.toInteger(1));
        	}
        	else {
        		msg.setResponse(GridRedisProtocolParser.toInteger(0));
        	}
        }
        else if(cmd == SMEMBERS) {        	
        	IgniteSet<String> list = ctx.grid().set(queueName, null);
        	Iterator<String> it = list.iterator();
        	int n = 0;
        	Collection<String> result = new ArrayList<>();
        	while(it.hasNext()) {
        		String key = it.next();
    			result.add(key);   		
        		n++;
        	}
        	msg.setResponse(GridRedisProtocolParser.toArray(result));
        }
        else if(cmd == ZRANGE) {

        	IgniteSet<ScoredCacheEntry<String,String>> list = ctx.grid().set(queueName,null);
        	PriorityQueue<ScoredCacheEntry<String,String>> queue = new PriorityQueue<>(list.size());
        	queue.addAll(list);
        	Iterator<ScoredCacheEntry<String, String>> it = queue.iterator();
        	int start = Integer.parseInt(msg.aux(2));
        	int end = Integer.parseInt(msg.aux(3));

        	if(start<0) start = list.size()+start;
        	if(end<0) end = list.size()+end;
        	int n = 0;
        	Collection<String> result = new ArrayList<>();
        	while(it.hasNext()) {
        		if(n>=end) {
        			break;
        		}
        		if(n>=start) {
        			String key = it.next().getKey();
        			result.add(key);
        		}        		
        		n++;
        	}
        	msg.setResponse(GridRedisProtocolParser.toArray(result));
        }        
        
        return new GridFinishedFuture<>(msg);
	}
}
