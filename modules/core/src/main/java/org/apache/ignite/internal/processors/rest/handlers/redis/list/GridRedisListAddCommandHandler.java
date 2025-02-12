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
import java.util.List;
import java.util.PriorityQueue;


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
 * Redis List add command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisListAddCommandHandler implements GridRedisCommandHandler {
    

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        LPUSH,LPUSHX,RPUSH,RPUSHX,SADD,ZADD
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
    public GridRedisListAddCommandHandler(IgniteLogger log, GridKernalContext ctx) {
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

        if (msg.messageSize() < 3) {            
        	msg.setResponse(GridRedisProtocolParser.toGenericError("Wrong number of arguments"));
        	return new GridFinishedFuture<>(msg);        	
        }
        
        GridRedisCommand cmd = msg.command();
            
        String queueName = msg.cacheName()+"-"+msg.key();        
        
        if(cmd == LPUSH || cmd == LPUSHX) {        	
        	IgniteQueue<String> list = ctx.grid().queue(queueName,0,cfg);
        	
        	List<String> params = msg.aux();
        	Collections.reverse(params);
        	for(String data: params) {
        		list.addFirst(data);
        	}
            msg.setResponse(GridRedisProtocolParser.toInteger(list.size()));
            return new GridFinishedFuture<>(msg);
        }
        else if(cmd == RPUSH || cmd == RPUSHX) {        	
        	IgniteQueue<String> list = ctx.grid().queue(queueName,0,cfg);
        	
        	List<String> params = msg.aux();
            list.addAll(params);   
            msg.setResponse(GridRedisProtocolParser.toInteger(list.size()));
            return new GridFinishedFuture<>(msg);
        }
        else if(cmd == SADD) {
        	IgniteSet<String> list = ctx.grid().set(queueName, cfg);
        	
        	List<String> params = msg.aux();
            list.addAll(params);      
            msg.setResponse(GridRedisProtocolParser.toInteger(list.size()));
            return new GridFinishedFuture<>(msg);
        }
        else if(cmd == ZADD) {
        	IgniteSet<ScoredItem<String>> list = ctx.grid().set(queueName,cfg);
        	
        	List<String> params = msg.aux();        	
        	for(int i=0;i<params.size();i+=2) {
        		double score = Double.parseDouble(params.get(i));
        		String value = params.get(i+1);
        		ScoredItem<String> entry = new ScoredItem<>(value,score);
        		list.add(entry);
        	}  
            msg.setResponse(GridRedisProtocolParser.toInteger(list.size()));
            return new GridFinishedFuture<>(msg);
        }    
       
        msg.setResponse(GridRedisProtocolParser.nil());
        return new GridFinishedFuture<>(msg);
	}
}
