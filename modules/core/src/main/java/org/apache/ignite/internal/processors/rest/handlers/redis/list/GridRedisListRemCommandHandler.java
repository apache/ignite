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
public class GridRedisListRemCommandHandler implements GridRedisCommandHandler {
    

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    		LSET,LREM,SREM,ZREM
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
    public GridRedisListRemCommandHandler(IgniteLogger log, GridKernalContext ctx) {
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
        
        if(cmd == LSET) { 
        	int pos = Integer.parseInt(msg.aux(2));
        	String value = msg.aux(3);
        	IgniteQueue<String> list = ctx.grid().queue(queueName,0,null);
        	if(pos<0) {
        		pos = list.size() + pos;
        	}    	
        	Iterator<String> it = list.iterator();
        	int n = 0;
        	while(it.hasNext()) {
        		String key = it.next(); 
        		if(n==pos) {
        			throw new UnsupportedOperationException("LSET not supported for ignite queue!");         			
        		}
        		n++;
        	}        	
        	msg.setResponse(GridRedisProtocolParser.toInteger(n));
        	
        }
        
        else if(cmd == LREM) { 
        	int count = Integer.parseInt(msg.aux(2));
        	String value = msg.aux(3);
        	IgniteQueue<String> list = ctx.grid().queue(queueName,0,null);
        	Iterator<String> it = list.iterator();
        	int n = 0;
        	while(it.hasNext()) {
        		String key = it.next();        		
        		if(key.equals(value)) {
        			it.remove();
        			n++;
        			if(count>0 && n>=count) break;
        		}
        	}
        	msg.setResponse(GridRedisProtocolParser.toInteger(n));
        	
        }        
        else if(cmd == SREM) {
        	List<String> keys = msg.aux();
        	IgniteSet<String> list = ctx.grid().set(queueName, null);
        	Iterator<String> it = list.iterator();
        	int n = 0;
        	while(it.hasNext()) {
        		String key = it.next();        		
        		if(keys.contains(key)) {
        			it.remove();
        			n++;
        		}
        	}
        	msg.setResponse(GridRedisProtocolParser.toInteger(n));
        }
        else if(cmd == ZREM) {
        	List<String> keys = msg.aux();
        	IgniteSet<ScoredCacheEntry<String,String>> list = ctx.grid().set(queueName,null);        	
        	Iterator<ScoredCacheEntry<String, String>> it = list.iterator();
        	int n = 0;
        	while(it.hasNext()) {
        		ScoredCacheEntry<String,String> item = it.next();
        		String key = item.getKey();
        		if(keys.contains(key)) {
        			it.remove();
        			n++;
        		}
        	}
        	msg.setResponse(GridRedisProtocolParser.toInteger(n));
        }        
        
        return new GridFinishedFuture<>(msg);
	}
}
