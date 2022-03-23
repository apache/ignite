package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;




public class ClusterAgentServiceUtil  {
	
	
	public static List<String> cacheSelectList(Ignite ignite,JsonObject args) {		
		int count = 0;		
		String cacheName = null;
			
		JsonArray selectCaches = args.getJsonArray("caches");
		if(args.get("cache")!=null) {
			cacheName = args.getJsonObject("cache").getString("name");
		}		
		List<String> list = new ArrayList<>();
		for(String cache: ignite.cacheNames()) {			
			IgniteCache<?,?> igcache = ignite.cache(cache);
			if(cacheName!=null && !cacheName.isEmpty()) {
				if(!igcache.getName().equals(cacheName)) {
					continue;
				}
			}
			if(selectCaches!=null) {
				if(!selectCaches.contains(igcache.getName())) {
					//-continue;
				}
			}				
			list.add(cache);
			count++;			
		}
		
		return list;
	}
	
}
