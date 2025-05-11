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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
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
public class GridRedisSetsCommandHandler implements GridRedisCommandHandler {
    

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    		SISMEMBER,SMEMBERS,SDIFF,SINTER,SSCAN,SCARD
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
    public GridRedisSetsCommandHandler(IgniteLogger log, GridKernalContext ctx) {
        this.log = log;
        this.ctx = ctx;
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setBackups(1);
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
        IgniteSet<String> list = ctx.grid().set(queueName, null);
        if(list==null && cmd != SCARD) {
    		msg.setResponse(GridRedisProtocolParser.nil());
    		return new GridFinishedFuture<>(msg);
    	}
        
        if(cmd == SISMEMBER) {
        	String query = msg.aux(2);
        	
        	if(list.contains(query)) {
        		msg.setResponse(GridRedisProtocolParser.toInteger(1));
        	}
        	else {
        		msg.setResponse(GridRedisProtocolParser.toInteger(0));
        	}
        }
        else if(cmd == SMEMBERS) {     	
        	
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
        else if(cmd == SSCAN) {
        	// cmd, key, offset, MATCH pattem, COUNT 1000
        	int offset = Integer.valueOf(msg.aux(2));
        	int count = 10;
        	String countP = stringValue("COUNT",msg.auxMKeys()); 
        	if(countP!=null) {
        		count = Integer.parseInt(countP);
        	}
        	PathMatcher pathMatcher = null;
        	String matchP = stringValue("MATCH",msg.auxMKeys());
        	if(matchP!=null && !matchP.equals("*")) {
        		pathMatcher = FileSystems.getDefault().getPathMatcher("glob:"+matchP);
        	}
    		
        	Iterator<String> it = list.iterator();
        	int n = 0;
        	Collection<String> result = new ArrayList<>();
        	while(it.hasNext()) {
        		String iKey = it.next();
        		if(pathMatcher==null || pathMatcher.matches(Path.of(iKey))){
        			if(n<offset) {
            			continue;
            		}
            		if(result.size()>count) {
            			break;
            		}
        			result.add(iKey);   		
            		n++;
				}        		
        	}
        	msg.setResponse(GridRedisProtocolParser.toArray(List.of(n,result)));
        }
        else if(cmd == SDIFF) {
        	List<String> othersKeys = msg.aux();
        	HashSet<String> result = new HashSet<>(list);
        	for(String key2: othersKeys) {
        		String queueName2 = msg.cacheName()+"-"+key2;
        		IgniteSet<String> list2 = ctx.grid().set(queueName2, cfg);
        		if(list2!=null) {
        			result.removeAll(list2);
        		}
        	}        	
        	msg.setResponse(GridRedisProtocolParser.toArray(result));
        }
        else if(cmd == SINTER) {
        	List<String> othersKeys = msg.aux();
        	HashSet<String> result = new HashSet<>(list);
        	for(String key2: othersKeys) {
        		String queueName2 = msg.cacheName()+"-"+key2;
        		IgniteSet<String> list2 = ctx.grid().set(queueName2, cfg);
        		if(list2!=null) {
        			result.retainAll(list2);
        		}
        	}        	
        	msg.setResponse(GridRedisProtocolParser.toArray(result));
        }
        else if(cmd == SCARD) {
        	msg.setResponse(GridRedisProtocolParser.toInteger(list!=null?list.size():0));     	
        }
        
        return new GridFinishedFuture<>(msg);
	}	
}
