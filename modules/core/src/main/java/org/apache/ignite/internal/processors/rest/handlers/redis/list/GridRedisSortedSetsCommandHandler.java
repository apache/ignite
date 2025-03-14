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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.ignite.IgniteLogger;
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
public class GridRedisSortedSetsCommandHandler implements GridRedisCommandHandler {
    

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    		ZRANK,ZREVRANK,ZRANGE,ZREVRANGE,ZRANGEBYSCORE,ZREVRANGEBYSCORE,ZSCAN,ZCARD 
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
    public GridRedisSortedSetsCommandHandler(IgniteLogger log, GridKernalContext ctx) {
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
        
        GridRedisCommand cmd = msg.command();
            
        String queueName = msg.cacheName()+"-"+msg.key(); 
        
        IgniteSet<ScoredItem<String>> list = ctx.grid().set(queueName,null);
        if(list==null && cmd != ZCARD) {
    		msg.setResponse(GridRedisProtocolParser.nil());
    		return new GridFinishedFuture<>(msg);
    	}  
        
        if(cmd==ZRANK || cmd==ZREVRANK) {
        	
        	Comparator<ScoredItem<String>> c = GridRedisSortedSetsCommandHandler::scoreCompareTo;
        	PriorityQueue<ScoredItem<String>> queue = new PriorityQueue<>(1+list.size(),cmd==ZREVRANK? c.reversed() : c);
        	queue.addAll(list);
        	Iterator<ScoredItem<String>> it = queue.iterator();
        	String member = msg.aux(3);        	
        	boolean found = false;
        	int n = 0;        	
        	while(it.hasNext()) {
        		ScoredItem<String> item = it.next();
        		if(item.getValue().equals(member)) {
        			found = true;
        			break;     			
        		}        		
        		n++;
        	}
        	if(found)
        		msg.setResponse(GridRedisProtocolParser.toInteger(n));
        	else
        		msg.setResponse(GridRedisProtocolParser.nil());
        }        
        else if(cmd==ZRANGE || cmd==ZREVRANGE ) {        	
        	
        	Comparator<ScoredItem<String>> c = GridRedisSortedSetsCommandHandler::scoreCompareTo;
        	PriorityQueue<ScoredItem<String>> queue = new PriorityQueue<>(1+list.size(),cmd==ZREVRANGE? c.reversed() : c);
        	queue.addAll(list);
        	Iterator<ScoredItem<String>> it = queue.iterator();
        	int start = Integer.parseInt(msg.aux(2));
        	int end = Integer.parseInt(msg.aux(3));

        	if(start<0) start = list.size()+start;
        	if(end<0) end = list.size()+end;
        	
        	boolean withScore = false;
        	if(msg.messageSize()>4) {
        		if("WITHSCORES".equals(msg.aux(4).toUpperCase())) {
        			withScore = true;
        		}
        	}
        	
        	int n = 0;
        	Collection<String> result = new ArrayList<>();
        	while(it.hasNext()) {
        		if(n>=end) {
        			break;
        		}
        		if(n>=start) {
        			ScoredItem<String> item = it.next();
        			result.add(item.getValue());
        			if(withScore) {
        				result.add(String.valueOf(item.getScore()));
        			}        			
        		}        		
        		n++;
        	}
        	msg.setResponse(GridRedisProtocolParser.toArray(result));
        }
        else if(cmd == ZRANGEBYSCORE || cmd == ZREVRANGEBYSCORE) {        	
        	
        	Iterator<ScoredItem<String>> it = list.iterator();
        	double min = Double.parseDouble(msg.aux(2));
        	double max = Double.parseDouble(msg.aux(3));
        	
        	boolean withScore = false;
        	if(msg.messageSize()>4) {
        		if("WITHSCORES".equals(msg.aux(4).toUpperCase())) {
        			withScore = true;
        		}
        	}
        	
        	Comparator<ScoredItem<String>> c = GridRedisSortedSetsCommandHandler::scoreCompareTo;
        	
        	PriorityQueue<ScoredItem<String>> sorted = new PriorityQueue<>((8+list.size()/8),cmd==ZREVRANGEBYSCORE? c.reversed() : c);
        	while(it.hasNext()) {
        		ScoredItem<String> item = it.next();
        		if(item.getScore()>=min && item.getScore()<=max) {
        			sorted.add(item);
        		}
        	}
        	
        	Iterator<ScoredItem<String>> it2 = sorted.iterator();        	
        	Collection<String> result = new ArrayList<>();
        	while(it2.hasNext()) {
        		ScoredItem<String> item = it2.next();
        		result.add(item.getValue());
        		if(withScore) {    				
    				result.add(String.valueOf(item.getScore()));
    			}
    			
        	}
        	msg.setResponse(GridRedisProtocolParser.toArray(result));
        }
        else if(cmd == ZSCAN) {
        	
        	Iterator<ScoredItem<String>> it = list.iterator();
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
        	
        	int n = 0;
        	Collection<String> result = new ArrayList<>();
        	while(it.hasNext()) {
        		ScoredItem<String> item = it.next();
        		String iKey = item.getValue();
        		if(pathMatcher==null || pathMatcher.matches(Path.of(iKey))){
        			if(n<offset) {
            			continue;
            		}
            		if(result.size()>count) {
            			break;
            		}
        			result.add(iKey);
        			result.add(String.valueOf(item.getScore()));
            		n++;
				}        		
        	}
        	msg.setResponse(GridRedisProtocolParser.toArray(List.of(n,result)));
        }
        else if(cmd == ZCARD) {
        	msg.setResponse(GridRedisProtocolParser.toInteger(list!=null?list.size():0));
        }
        return new GridFinishedFuture<>(msg);
	}
	
	static int scoreCompareTo(ScoredItem<?> o1,ScoredItem<?> o2) {
		double score1 = o1.getScore();
		double score2 = o2.getScore();
		return Double.compare(score1, score2);		
	}
}
