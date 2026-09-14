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
	public ServiceResult call(String cache,Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;
		List<String> message = result.getMessages();

		try {
			IgniteCache<?,?> igcache = ignite.cache(cache);
				
			igcache.destroy();
			count++;
		}
		catch(Exception e) {
			result.setAcknowledged(false);
			message.add(e.getMessage());
		}
		result.put("deletedCount", count);
		return result;
	}

}
