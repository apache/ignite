package org.apache.ignite.console.agent.service;

import java.util.*;


import io.vertx.webmvc.mcp.McpSchema.*;
import io.vertx.webmvc.mcp.ToolExecutor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.resources.IgniteInstanceResource;

import io.swagger.annotations.ApiOperation;



@ApiOperation("Clear cache data from cluster")
public class CacheClearDataService implements CacheAgentService,McpService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

	@Override
	public List<ToolExecutor> toolExecutors(){
		ToolExecutor toolX = ToolExecutor.builder()
				.name("clear-cache")
				.description("Clear cache data from cluster")
				.parameter("cacheName","string","Ignie cache name",true)
				.executor((Map<String,Object> param)->{
					return this.call((String)param.get("cacheName"),param);
				 })
				.build();

		return List.of(toolX);
	}
    
	@Override
	public ServiceResult call(String cache,Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;		
		
		List<String> message = result.getMessages();
		
		try {
			IgniteCache<?,?> igcache = ignite.cache(cache);
			count = igcache.size(CachePeekMode.ALL);
			igcache.withSkipStore().clear();
			
		}
		catch(Exception e) {
			result.setAcknowledged(false);
			message.add(e.getMessage());
		}
		result.put("deletedCount", count);
		return result;
	}

}
