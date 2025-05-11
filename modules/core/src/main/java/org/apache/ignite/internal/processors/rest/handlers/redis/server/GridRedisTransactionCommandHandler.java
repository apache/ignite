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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisNioListener.SESS_TX_META_KEY;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisNioListener.SESS_TX_QUEUED_META_KEY;

/**
 * Redis FLUSHDB/FLUSHALL command handler.
 */
public class GridRedisTransactionCommandHandler implements GridRedisCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    	MULTI, EXEC, DISCARD 
    );
    
    /** Logger. */
    protected final IgniteLogger log;

    /** Kernel context. */
    protected final GridKernalContext ctx;
    
    private Map<GridRedisCommand, GridRedisCommandHandler> handlers;

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisTransactionCommandHandler(IgniteLogger log, GridKernalContext ctx, Map<GridRedisCommand, GridRedisCommandHandler> handlers) {
    	 this.log = log;
         this.ctx = ctx;
         this.handlers = handlers;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRedisCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }


	@Override
	public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg) {
		assert msg != null;
		GridRedisCommand cmd = msg.command();
		msg.setResponse(GridRedisProtocolParser.nil());
		
		IgniteCache<?, ?> cache = ctx.grid().cache(msg.cacheName());
        if(cache==null) {        	
        	return new GridFinishedFuture<>(msg);
        }
        boolean isTransCache = true;
        CacheConfiguration<?,?> cfg = cache.getConfiguration(CacheConfiguration.class);
        if(cfg.getAtomicityMode()!=CacheAtomicityMode.TRANSACTIONAL) {
        	this.log.warning("IgniteTransactions is only enable on CacheAtomicityMode.TRANSACTIONAL cache. "+cache.getName()+" is not!");        	
        	isTransCache = false;        	
        }
        
        if(cmd == MULTI) { 
    		ses.addMeta(SESS_TX_QUEUED_META_KEY, new ArrayList<GridRedisMessage>(5));
    		
    		if(isTransCache) {
    			IgniteTransactions tn = ctx.grid().transactions();
    			if(tn.tx()!=null) {
    				this.log.warning("Transaction already exists! commit it.");
            		tn.tx().commit();
            	}
            	Transaction t = tn.txStart();
            	ses.addMeta(SESS_TX_META_KEY, t);
    		}
    		
    		msg.setResponse(GridRedisProtocolParser.oKString());
    	}
    	else if(cmd == DISCARD) {
    		ses.removeMeta(SESS_TX_QUEUED_META_KEY);
    		
    		if(isTransCache) {
    			Transaction t = ses.meta(SESS_TX_META_KEY);
            	if(t!=null) {
            		ses.removeMeta(SESS_TX_META_KEY);
            		t.rollback();            		
            	}            	
    		}
    		msg.setResponse(GridRedisProtocolParser.oKString());
    	}
    	else { // EXEC
    		
    		List<GridRedisMessage> queued = ses.meta(SESS_TX_QUEUED_META_KEY);
    		ses.removeMeta(SESS_TX_QUEUED_META_KEY);
    		
    		try {
    			List<ByteBuffer> resps = new ArrayList<>(queued.size());
    			
	    		for(GridRedisMessage msgPart: queued) {
	    			GridRedisMessage res = handlers.get(msgPart.command()).handleAsync(ses, msgPart).get();	    			
					resps.add(res.getResponse());					
	    		}
	    		
	    		if(isTransCache) {
	    			Transaction t = ses.meta(SESS_TX_META_KEY);
	            	if(t!=null) {
	            		ses.removeMeta(SESS_TX_META_KEY);
	            		if(t.isRollbackOnly()) {
	            			t.rollback();
	            			msg.setResponse(GridRedisProtocolParser.nil());
	            			return new GridFinishedFuture<>(msg);
	            		}
	            		else {
	            			t.commit();	            			
	            		}
	            	}	            	
	    		}
	    		
	    		msg.setResponse(GridRedisProtocolParser.toArray(resps));
	    		
    		} catch (IgniteCheckedException e) {
				log.error("Fail to atom exec cmd "+ cmd,e);
				if(isTransCache) {
					Transaction t = ses.meta(SESS_TX_META_KEY);
					if(t!=null) {
						t.rollback();
					}					
				}
				msg.setResponse(GridRedisProtocolParser.nil());
			}
    	}
        
        return new GridFinishedFuture<>(msg);
	}
}
