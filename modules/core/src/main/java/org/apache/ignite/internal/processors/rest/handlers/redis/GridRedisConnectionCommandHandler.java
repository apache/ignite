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

package org.apache.ignite.internal.processors.rest.handlers.redis;

import java.util.Collection;
import java.util.List;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.client.message.GridClientAuthenticationRequest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.GridTcpRestNioListener;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisNioListener;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityCredentials;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.ECHO;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.PING;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.QUIT;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis connection handler.
 */
public class GridRedisConnectionCommandHandler implements GridRedisCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
        PING,
        QUIT,
        ECHO,
        SELECT,
        AUTH,
        CLIENT,
        INFO
    );

    /** Grid context. */
    private final GridKernalContext ctx;

    /** PONG response to PING. */
    private static final String PONG = "PONG";
    
    
    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Context.
     */
    public GridRedisConnectionCommandHandler(final IgniteLogger log, final GridRestProtocolHandler hnd,
        GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
        assert msg != null;
        switch (msg.command()) {
            case PING:
                msg.setResponse(GridRedisProtocolParser.toSimpleString(PONG));

                return new GridFinishedFuture<>(msg);

            case QUIT:
                msg.setResponse(GridRedisProtocolParser.oKString());
                
                return new GridFinishedFuture<>(msg);

            case ECHO:
                msg.setResponse(GridRedisProtocolParser.toSimpleString(msg.key()));

                return new GridFinishedFuture<>(msg);

            case SELECT:
                String cacheIdx = msg.key();

                if (F.isEmpty(cacheIdx))
                    msg.setResponse(GridRedisProtocolParser.toGenericError("No cache index specified"));
                else {
                    String cacheName = GridRedisMessage.CACHE_NAME_PREFIX + "-" + cacheIdx;

                    CacheConfiguration<String,String> ccfg0 = ctx.cache().cacheConfiguration(GridRedisMessage.DFLT_CACHE_NAME);
                    CacheConfiguration<String,String> ccfg = new CacheConfiguration<>(ccfg0);
                    ccfg.setName(cacheName);
                    

                    IgniteCache<String, String> cache = ctx.grid().getOrCreateCache(ccfg);

                    ses.addMeta(GridRedisNioListener.CONN_CTX_META_KEY, cache.getName());

                    msg.setResponse(GridRedisProtocolParser.oKString());
                }
                return new GridFinishedFuture<>(msg);
                
            case AUTH:
			    // add@byron
            	// format auth user password
            	List<String> password = msg.auxMKeys();
            	IgniteSecurity security = ctx.security();
            	GridClientAuthenticationRequest authMsg = new GridClientAuthenticationRequest();
				
            	try {
					if(password.size()>1) {
						authMsg.credentials(new SecurityCredentials(password.get(0),password.get(1)));
					}
					else {
						authMsg.credentials(new SecurityCredentials(ctx.igniteInstanceName(),password.get(0)));
					}					
					authMsg.clientId(msg.clientId());
					
					ses.addMeta(GridTcpRestNioListener.CREDS_KEY, authMsg.credentials());
					ses.addMeta(GridTcpRestNioListener.USER_ATTR_KEY, authMsg.userAttributes());					
					
				} 
				catch (Exception e) {
					
					ctx.log(getClass()).warning("auth default user name is igniteInstanceName");
					e.printStackTrace();
				}
				
            	msg.setResponse(GridRedisProtocolParser.oKString());            	
                return new GridFinishedFuture<>(msg);
                
            case CLIENT:
            	// add@byron
            	String op = msg.aux(1).toUpperCase();
            	msg.setResponse(GridRedisProtocolParser.oKString());
            	
            	if(op.equals("SETNAME")) {
            		ses.addMeta(GridRedisNioListener.CONN_NAME_META_KEY, msg.aux(2));
            	}
            	else if(op.equals("GETNAME")) {
            		Object name = ses.meta(GridRedisNioListener.CONN_NAME_META_KEY);
            		if(name!=null) {
            			msg.setResponse(GridRedisProtocolParser.toSimpleString(name.toString()));
            		}
            		else {
            			msg.setResponse(GridRedisProtocolParser.nil());
            		}
            	}
                return new GridFinishedFuture<>(msg);
                
            case INFO:
            	// add@byron
            	String section = msg.aux(1);
            	if(section==null) {
            		section = "default";
            	}
            	
            	StringBuilder sb = new StringBuilder();
            	if(section.equals("server") || section.equals("default") || section.equals("everything")) {
            		sb.append("\n# Server");
            		sb.append("\nredis_version:6.0.0");
            		sb.append("\nredis_mode:standalone");
            		sb.append("\nos:"+System.getenv("os.name"));
            		sb.append("\nprocess_id:"+1);
            	}
            	if(section.equals("memory") || section.equals("default")  || section.equals("everything")) {
            		sb.append("\n# Memory");
            		sb.append("\nused_memory:"); sb.append(ctx.metric().heapMemoryUsage().getUsed());
            		sb.append("\nused_memory_human:"); sb.append(ctx.metric().nonHeapMemoryUsage().getUsed());
            		sb.append("\ntotal_system_memory:"); sb.append(ctx.metric().heapMemoryUsage().getInit());
            		sb.append("\ntotal_system_memory_human:"); sb.append(ctx.metric().nonHeapMemoryUsage().getInit());
            		sb.append("\nmaxmemory:"); sb.append(ctx.metric().heapMemoryUsage().getMax());
            		sb.append("\nmaxmemory_human:"); sb.append(ctx.metric().nonHeapMemoryUsage().getMax());
            	}
            	if(section.equals("stats")  || section.equals("everything")) {
            		sb.append("\n# Stats");
            		sb.append("\ntotal_connections_received:"); sb.append(GridRedisNioListener.total_connections_received);
            		sb.append("\ntotal_commands_processed:"); sb.append(GridRedisNioListener.total_commands_processed);
            		sb.append("\ntotal_net_input_bytes:"); sb.append(GridRedisNioListener.total_net_input_bytes);
            		sb.append("\ntotal_net_output_bytes:"); sb.append(GridRedisNioListener.total_net_output_bytes);
            		sb.append("\npubsub_channels:"); sb.append(GridRedisNioListener.pubsub_channels);
            		
            	}
            	
            	msg.setResponse(GridRedisProtocolParser.toBulkString(sb.toString()));
                return new GridFinishedFuture<>(msg);
        }

        return new GridFinishedFuture<>();
    }
}
