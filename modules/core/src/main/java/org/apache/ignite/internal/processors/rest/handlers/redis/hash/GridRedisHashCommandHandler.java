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

package org.apache.ignite.internal.processors.rest.handlers.redis.hash;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisRestCommandHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis Hash command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisHashCommandHandler extends GridRedisRestCommandHandler implements GridRedisCommandHandler {

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
			HGET,HMGET,HGETALL,
			HSET,HSETNX,HMSET,
			HINCRBY,
			HLEN,
			HKEYS,HSCAN,
			HDEL,
			HEXISTS
    );

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param ctx Kernal context.
     */
    public GridRedisHashCommandHandler(IgniteLogger log, GridRestProtocolHandler hnd, GridKernalContext ctx) {
		super(log,hnd,ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }


	@Override
	public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
		assert msg != null;

        GridRedisCommand cmd = msg.command();
		IgniteCache<String, Map<String,String>> hash = ctx.grid().cache(msg.cacheName());
		if(hash!=null) {
			trySuspendTransaction(ses,msg); // hash maybe not support transaction
		}
		if(cmd == HSET || cmd == HSETNX || cmd == HMSET) {
			String key = msg.key();
			List<String> params = msg.aux();
			Map<String,String> data = hash.get(key);
			if(data==null)
				data = new HashMap<>(params.size());
			for(int i=0; i<params.size()-1; i+=2) {
				String field = params.get(i);
				String value = params.get(i+1);
				if(cmd == HSETNX)
					data.putIfAbsent(field,value);
				else
					data.put(field,value);
			}
			hash.put(key,data);
			msg.setResponse(GridRedisProtocolParser.oKString());
			return new GridFinishedFuture<>(msg);
		}
		if(cmd == HDEL) {
			int n = 0;
			String key = msg.key();
			List<String> params = msg.aux();
			if(params.isEmpty()){
				if(hash.remove(key)){
					n++;
				}
			}
			else{
				Map<String,String> data = hash.get(key);
				if(data!=null){
					for(int i=0; i<params.size(); i++) {
						String field = params.get(i);
						Object old = data.remove(field);
						if(old!=null) n++;
					}
					hash.put(key,data);
				}
			}
			msg.setResponse(GridRedisProtocolParser.toInteger(n));
			return new GridFinishedFuture<>(msg);
		}
		if(cmd == HEXISTS) {
			int n = 0;
			String key = msg.key();
			List<String> params = msg.aux();
			if(params.isEmpty()){
				if(hash.containsKey(key)){
					n++;
				}
			}
			else{
				Map<String,String> data = hash.get(key);
				if(data!=null) {
					for (int i = 0; i < params.size(); i++) {
						String field = params.get(i);
						if (data.containsKey(field))
							n++;
					}
				}
			}
			msg.setResponse(GridRedisProtocolParser.toInteger(n));
			return new GridFinishedFuture<>(msg);
		}
		if(cmd == HGET || cmd == HMGET || cmd == HGETALL) {
			int n = 0;
			String key = msg.key();
			List<String> params = msg.aux();
			Map<String,String> data = hash.get(key);
			if(data==null) {
				msg.setResponse(GridRedisProtocolParser.nil());
				return new GridFinishedFuture<>(msg);
			}

			if(cmd == HGET){
				String output = data.get(params.get(0));
				msg.setResponse(GridRedisProtocolParser.toBulkString(output));
			}
			else if(cmd == HGETALL){
				List<String> result = new ArrayList<>(data.size()*2);
				data.forEach((k,v)->{
					if(v!=null) {
						result.add(k);
						result.add(v);
					}
				});
				msg.setResponse(GridRedisProtocolParser.toArray(result));
			}
			else{
				Map<Object,Object> result = new HashMap<>(params.size());
				for(int i=0; i<params.size(); i++) {
					String field = params.get(i);
					String value = data.get(field);
					if(value!=null){
						result.put(field,value);
					}
				}
				msg.setResponse(GridRedisProtocolParser.toOrderedArray(result, params));
			}
			return new GridFinishedFuture<>(msg);
		}
        else if(cmd==HKEYS) {
			String key = msg.key();
			Map<String,String> data = hash.get(key);
			if(data==null) {
				msg.setResponse(GridRedisProtocolParser.nil());
				return new GridFinishedFuture<>(msg);
			}
        	msg.setResponse(GridRedisProtocolParser.toArray(data.keySet()));
        }
		else if(cmd==HSCAN) { // HSCAN key cursor [MATCH pattern] [COUNT count]
			String key = msg.key();
			String matchP = stringValue("MATCH",msg.auxMKeys());

			Map<String,String> data = hash.get(key);
			if(data==null) {
				msg.setResponse(GridRedisProtocolParser.nil());
				return new GridFinishedFuture<>(msg);
			}
			List<String> result = new ArrayList<>(data.size());
			for(String field: data.keySet()){
				if(matchP!=null) {
					if(!field.matches(matchP)) continue;
				}
				result.add(field);
			}
			msg.setResponse(GridRedisProtocolParser.toArray(List.of(0,result)));
		}
        else if(cmd == HINCRBY) { // HINCRBY key field increment
			String key = msg.key();
			String field = msg.aux(2);
			try {
				Long delta = Long.valueOf(msg.aux(3));
				Long newValue = delta;
				Map<String, String> data = hash.get(key);
				if (data == null)
					data = new HashMap<>();
				String value = data.get(field);
				if (value == null)
					data.put(field, delta.toString());
				else {
					Long old = Long.parseLong(value);
					newValue = old + delta;
					data.put(field, newValue.toString());
				}
				hash.put(key, data);
				msg.setResponse(GridRedisProtocolParser.toInteger(newValue.toString()));
			}
			catch (NumberFormatException e){
				msg.setResponse(GridRedisProtocolParser.toTypeError("param value of field value is non-numeric"));
			}
			return new GridFinishedFuture<>(msg);
        }
		else if(cmd == HLEN) {
			if(hash==null){
				msg.setResponse(GridRedisProtocolParser.toInteger(0));
			}
			else {
				int n = 0;
				String key = msg.key();
				Map<String,String> data = hash.get(key);
				n = data.size();
				msg.setResponse(GridRedisProtocolParser.toInteger(n));
			}
		}
		if(hash!=null) {
			tryResumeTransaction(ses,msg);
		}
        return new GridFinishedFuture<>(msg);
	}

	@Override
	public GridRestRequest asRestRequest(GridRedisMessage msg) throws IgniteCheckedException {
		return null;
	}
}
