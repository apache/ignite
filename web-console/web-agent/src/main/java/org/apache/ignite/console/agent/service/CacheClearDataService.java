package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.resources.IgniteInstanceResource;

import io.swagger.annotations.ApiOperation;



@ApiOperation("Clear cache data to cluster")
public class CacheClearDataService implements CacheAgentService {
   
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
