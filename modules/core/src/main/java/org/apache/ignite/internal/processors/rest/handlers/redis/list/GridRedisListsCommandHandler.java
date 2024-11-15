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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;



import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis List pop command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisListsCommandHandler implements GridRedisCommandHandler {
    

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    		LPOS,LRANGE,LINDEX,LLEN
    );


    /** Logger. */
    protected final IgniteLogger log;

    /** Kernel context. */
    protected final GridKernalContext ctx;
    
    protected CollectionConfiguration cfg = new CollectionConfiguration();

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisListsCommandHandler(IgniteLogger log, GridKernalContext ctx) {
        this.log = log;
        this.ctx = ctx;
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

   

	@Override
	public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
		assert msg != null;
        
        GridRedisCommand cmd = msg.command();
            
        String queueName = msg.cacheName()+"-"+msg.key();        
        IgniteQueue<String> list = ctx.grid().queue(queueName,0,cfg);
        
        if(cmd == LPOS) {         	
        	String value = msg.aux(2);
        	  	
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
        else if(cmd == LINDEX) {        	
        	
        	Iterator<String> it = list.iterator();
        	int start = Integer.parseInt(msg.aux(2));        	
        	if(start<0) start = list.size()+start;
        	
        	int n = 0;
        	String result = null;
        	while(it.hasNext()) {        		
        		if(n>=start) {
        			result = it.next();
        			break;
        		}        		
        		n++;
        	}
        	if(result==null)
        		msg.setResponse(GridRedisProtocolParser.nil());
        	else
        		msg.setResponse(GridRedisProtocolParser.toSimpleString(result));
        	
        }
        else if(cmd == LLEN) {
        	msg.setResponse(GridRedisProtocolParser.toInteger(list.size()));
        }
        return new GridFinishedFuture<>(msg);
	}	
}
