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

package org.apache.ignite.internal.processors.rest.handlers.redis.pubsub;

import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.query.ScoredCacheEntry;
import org.apache.ignite.internal.processors.rest.handlers.redis.GridRedisCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.redis.pubsub.GridRedisSubscribeCommandHandler.ChanelInfo;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisProtocolParser;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand.*;

/**
 * Redis List add command handler.
 * <p>
 * No key expiration is currently supported.
 */
public class GridRedisSubscribeCommandHandler implements GridRedisCommandHandler {
    

	/** Supported commands. */
    private static final Collection<GridRedisCommand> SUPPORTED_COMMANDS = U.sealList(
    		SUBSCRIBE,PSUBSCRIBE,UNSUBSCRIBE,PUNSUBSCRIBE,PUBLISH,PUBSUB
    );
    
    /** Logger. */
    protected final IgniteLogger log;

    /** Kernel context. */
    protected final GridKernalContext ctx;
    
    public static class ChanelInfo implements java.io.Serializable{    	
		private static final long serialVersionUID = 1L;
		String clientId; // client addr
    	String chanel;
    	long lastReceiveTime;
    	long lastSendTime;
    	long createTime;
    	int clients; // 客户端数量
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((chanel == null) ? 0 : chanel.hashCode());
			result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ChanelInfo other = (ChanelInfo) obj;
			if (chanel == null) {
				if (other.chanel != null)
					return false;
			} else if (!chanel.equals(other.chanel))
				return false;
			if (clientId == null) {
				if (other.clientId != null)
					return false;
			} else if (!clientId.equals(other.clientId))
				return false;
			return true;
		}
    	
    }
    
    public IgniteSet<ChanelInfo> topicsMap = null;
    

    /**
     * Handler constructor.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     * @param ctx Kernal context.
     */
    public GridRedisSubscribeCommandHandler(IgniteLogger log, GridKernalContext ctx) {
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
        
        if(topicsMap==null) {
	        
	        try {
	        	CollectionConfiguration cfg = new CollectionConfiguration();
		        cfg.setBackups(1);
		        
				topicsMap = ctx.dataStructures().set(GridRedisMessage.CACHE_NAME_PREFIX+"-chanel-info-sets",null,cfg);
			} 
	        catch (IgniteCheckedException e) {
				log.error("create chanel-sets failes!",e);
			}
        }
        
        GridRedisCommand cmd = msg.command();
        IgniteBiPredicate<UUID, ByteBuffer> p = ses.messageListener();
        
        if(cmd == SUBSCRIBE) {
        	List<String> topics = msg.auxMKeys();
        	Collection<Object[]> result = new ArrayList<>();
        	int n = 0;
        	for(String topic: topics) {
        		ctx.grid().message().localListen(topic, p);
        		ChanelInfo cInfo = addChanelInfo(topic, ses.remoteAddress().toString());
        		
        		Object[] info = new Object[3];        		
        		info[0] = "subscribe";
        		info[1] = topic;
        		info[2] = cInfo.clients;
        		result.add(info);
        	}        	    
            msg.setResponse(GridRedisProtocolParser.toBulkList(result));
            return new GridFinishedFuture<>(msg);
        }
        if(cmd == PSUBSCRIBE) {
        	List<String> topics = msg.auxMKeys();
        	Collection<Object[]> result = new ArrayList<>();
        	int n = 0;
        	for(String topicPatten: topics) {
        		for(String topic: findMatchedTopic(topicPatten)) {
	        		ctx.grid().message().localListen(topic, p);
	        		ChanelInfo cInfo = addChanelInfo(topic, ses.remoteAddress().toString());
	        		Object[] info = new Object[3];        		
	        		info[0] = "subscribe";
	        		info[1] = topic;
	        		info[2] = cInfo.clients;
	        		result.add(info);
        		}
        	}        	    
            msg.setResponse(GridRedisProtocolParser.toBulkList(result));
            return new GridFinishedFuture<>(msg);
        }
        if(cmd == UNSUBSCRIBE) {
        	List<String> topics = msg.auxMKeys();
        	if(topics.isEmpty()) {
        		topics = new ArrayList<>(allTopics());
        	}
        	Collection<Object[]> result = new ArrayList<>();
        	int n = 0;
        	for(String topic: topics) {
        		ChanelInfo cInfo = removeChanelInfo(topic,ses.remoteAddress().toString());
        		if(cInfo!=null) {
	        		ctx.grid().message().stopLocalListen(topic,p);
	        		Object[] info = new Object[3];        		
	        		info[0] = "unsubscribe";
	        		info[1] = topic;
	        		info[2] = cInfo.clients;
	        		result.add(info);
        		}
        	}        	    
            msg.setResponse(GridRedisProtocolParser.toBulkList(result));
            return new GridFinishedFuture<>(msg);
        }
        
        if(cmd == PUNSUBSCRIBE) {
        	List<String> topics = msg.auxMKeys();
        	Collection<Object[]> result = new ArrayList<>();
        	int n = 0;
        	for(String topicPatten: topics) {
        		for(String topic: findMatchedTopic(topicPatten)) {
        			ChanelInfo cInfo = removeChanelInfo(topic,ses.remoteAddress().toString());
            		if(cInfo!=null) {
    	        		ctx.grid().message().stopLocalListen(topic,p);
    	        		Object[] info = new Object[3];        		
    	        		info[0] = "unsubscribe";
    	        		info[1] = topic;
    	        		info[2] = cInfo.clients;
    	        		result.add(info);
            		}
        		}
        	}        	    
            msg.setResponse(GridRedisProtocolParser.toBulkList(result));
            return new GridFinishedFuture<>(msg);
        }
        
        if(cmd == PUBLISH) {
        	String topic = msg.aux(1);
        	String value = msg.aux(2);
        	List<ChanelInfo> cInfo = getChanelInfo(topic);        	
        	int n = cInfo.size();
        	
        	Collection<String> message = new ArrayList<>();
		    message.add("message");
		    message.add(topic);
		    message.add(value);
		    ByteBuffer buf = GridRedisProtocolParser.toArray(message);
		    
        	ctx.grid().message().sendOrdered(topic, buf, 0);
            msg.setResponse(GridRedisProtocolParser.toInteger(n));
            return new GridFinishedFuture<>(msg);
        }
        
        if(cmd == PUBSUB) {
        	String subCmd = msg.aux(1);
        	
        	int n = 0;
        	if("CHANNELS".equals(subCmd)) {
        		String pattern = msg.aux(2);
        		List<String> result = findMatchedTopic(pattern);
        		msg.setResponse(GridRedisProtocolParser.toArray(result));
                return new GridFinishedFuture<>(msg);
        	}
        	if("NUMSUB".equals(subCmd)) {
        		List<String> topics = msg.aux();
        		Collection<Object[]> result = new ArrayList<>();
        		for(String topic: topics) {
        			List<ChanelInfo> cInfo = getChanelInfo(topic);
        			Object[] subInfo = new Object[2];
        			subInfo[0] = topic;
        			subInfo[1] = cInfo.size();
        			result.add(subInfo);
        		}
                msg.setResponse(GridRedisProtocolParser.toBulkList(result));
                return new GridFinishedFuture<>(msg);
        	}       	
            
        }
       
        msg.setResponse(GridRedisProtocolParser.nil());
        return new GridFinishedFuture<>(msg);
	}
	
	public List<String> findMatchedTopic(String glob) {
		List<String> result = new ArrayList<>();
		if(glob==null || glob.isEmpty()) {
			result.addAll(allTopics());
		}
		else {
			PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:"+glob);		
			for(String topic : allTopics()) {
				if(pathMatcher.matches(Path.of(topic))){
					result.add(topic);
				}
			}
		}
		return result;
	}
	
	public ChanelInfo addChanelInfo(String chanel,String clientId) {
		ChanelInfo cInfo = new ChanelInfo();
		cInfo.clientId = clientId;
		cInfo.chanel = chanel;
		cInfo.lastReceiveTime = System.currentTimeMillis();
		cInfo.createTime = cInfo.lastReceiveTime;		
		topicsMap.add(cInfo);
		List<ChanelInfo> consumes = getChanelInfo(chanel);
		cInfo.clients = consumes.size();
		return cInfo;
	}
	
	public Set<String> allTopics() {
		Iterator<ChanelInfo> it = topicsMap.iterator();    	
    	Set<String> result = new HashSet<>();
    	while(it.hasNext()) {
    		ChanelInfo info = it.next();
    		result.add(info.chanel);    		
    	}		
		return result;
	}
	
	public List<ChanelInfo> getChanelInfoOfClient(String clientId) {
		Iterator<ChanelInfo> it = topicsMap.iterator();
    	int n = 0;
    	List<ChanelInfo> result = new ArrayList<>();
    	while(it.hasNext()) {
    		ChanelInfo info = it.next();
			if(info.clientId.equals(clientId)) {
				result.add(info);
			}
    		n++;
    	}		
		return result;
	}
	
	public List<ChanelInfo> getChanelInfo(String chanel) {
		Iterator<ChanelInfo> it = topicsMap.iterator();    	
    	List<ChanelInfo> result = new ArrayList<>();
    	while(it.hasNext()) {
    		ChanelInfo info = it.next();
			if(info.chanel.equals(chanel)) {
				result.add(info);
			}	
    	}		
		return result;
	}
	
	public ChanelInfo getChanelInfo(String chanel,String clientId) {
		ChanelInfo cInfo = null;
		Iterator<ChanelInfo> it = topicsMap.iterator();
    	int n = 0;    	
    	while(it.hasNext()) {
    		ChanelInfo info = it.next();
			if(info.chanel.equals(chanel)) {
				if(info.clientId.equals(clientId)) {
					cInfo = info;
				}
				n++;
			}    		
    	}
    	if(cInfo!=null)
    		cInfo.clients = n; // 客户端数量
		return cInfo;
	}
	
	public ChanelInfo removeChanelInfo(String chanel,String clientId) {
		ChanelInfo cInfo = getChanelInfo(chanel,clientId);		
    	if(cInfo!=null) {
    		topicsMap.remove(cInfo);
    		cInfo.clients--;
    	}
		return cInfo;
	}
	
	public int removeChanelInfoOfClient(String clientAddr) {
		if(topicsMap==null) {
			return 0;
		}
		List<ChanelInfo> consumes = getChanelInfoOfClient(clientAddr);
		topicsMap.removeAll(consumes);
		return consumes.size();
	}
}
