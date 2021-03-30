package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;


public class ClusterClearDataService implements ClusterAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public Map<String, ? extends Object> call(Map<String,Object> payload) {
		Map<String,Object> result = new HashMap<>();
		int count = 0;
		JsonObject args = (JsonObject)payload.get("args");
		String cacheId = null;
		JsonArray selectCaches = args.getJsonArray("caches");
		if(args.get("cache")!=null) {
			cacheId = args.getJsonObject("cache").getString("id");
		}		
		List<String> message = new ArrayList<>();
		for(String cache: ignite.cacheNames()) {
			try {
				IgniteCache<?,?> igcache = ignite.cache(cache);
				if(cacheId!=null) {
					if(!igcache.getName().equals(cacheId)) {
						continue;
					}
				}
				if(selectCaches!=null) {
					if(!selectCaches.contains(igcache.getName())) {
						continue;
					}
				}				
				igcache.clear();
				count++;
			}
			catch(Exception e) {
				message.add(e.getMessage());
			}
		}
		result.put("errors", message);
		result.put("caches", ignite.cacheNames());
		result.put("count", count);
		return result;
	}

	public String toString() {
		return "clear cache data to cluster";
	}
}
