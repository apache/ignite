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


@ApiOperation("Destroy cache from cluster")
public class CacheDestroyService implements CacheAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public ServiceResult call(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;		
		JsonObject args = new JsonObject(payload);	
		List<String> message = result.messages;	
		List<String> caches = cacheNameSelectList(ignite,args);
		for(String cache: caches) {
			try {
				IgniteCache<?,?> igcache = ignite.cache(cache);
					
				igcache.destroy();
				count++;
			}
			catch(Exception e) {
				message.add(e.getMessage());
			}
		}
		if(message.isEmpty()) {
			message.add("Finish destroy cache successfull!");
		}
		
		result.put("caches", caches);
		result.put("count", count);
		return result;
	}

}
