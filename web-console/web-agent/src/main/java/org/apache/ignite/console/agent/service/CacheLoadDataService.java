package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;

@ApiOperation("load dbstore data to cache")
public class CacheLoadDataService implements CacheAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public Map<String, ? extends Object> call(Map<String,Object> payload) {
		Map<String,Object> result = new HashMap<>();
		int count = 0;
		JsonObject args = new JsonObject((Map)payload.get("args"));	
		List<String> message = new ArrayList<>();
		List<String> caches = ClusterAgentServiceUtil.cacheSelectList(ignite,args);
		for(String cache: caches) {
			try {
				IgniteCache<?,?> igcache = ignite.cache(cache);
					
				igcache.loadCache(null);
				count++;
			}
			catch(Exception e) {
				message.add(e.getMessage());
			}
		}
		result.put("message", message);
		result.put("caches", ignite.cacheNames());
		result.put("count", count);
		return result;
	}

}
