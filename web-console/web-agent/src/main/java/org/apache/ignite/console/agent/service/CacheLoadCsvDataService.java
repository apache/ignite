package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;

@ApiOperation("Load csv or zip of csv data to cache")
public class CacheLoadCsvDataService implements CacheAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public ServiceResult call(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;
		JsonObject args = new JsonObject(payload);	
		List<String> message = result.getMessages();
		List<String> caches = cacheNameSelectList(ignite,args);
		for(String cache: caches) {
			try {
				IgniteCache<?,?> igcache = ignite.cache(cache);
					
				IgniteFuture<Void> fut = igcache.loadCacheAsync(null, null);
				count++;
				
				fut.get(1000);				
				
			}
			catch(Exception e) {
				message.add(e.getMessage());
			}
		}
		if(message.isEmpty()) {
			message.add("Finish load data successfull!");
		}
		
		result.put("caches", caches);
		result.put("count", count);
		return result;
	}

}
