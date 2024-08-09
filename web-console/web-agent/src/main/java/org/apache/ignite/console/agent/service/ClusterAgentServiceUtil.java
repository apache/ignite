package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;





public class ClusterAgentServiceUtil  {
	
	public static List<JsonObject> cacheSelectList(JsonObject args) {
		JsonObject cacheOne = null;
		if(args.containsKey("cache")) {
			cacheOne = args.getJsonObject("cache");
			return List.of(cacheOne);
		}
			
		JsonArray selectCaches = args.getJsonArray("caches");				
		List<JsonObject> list = new ArrayList<>();
		for(int i=0;i<selectCaches.size();i++) {
			list.add(selectCaches.getJsonObject(i));					
		}		
		return list;
	}
	
	public static List<String> cacheNameSelectList(Ignite ignite,JsonObject args) {
		String cacheName = null;
			
		JsonArray selectCaches = args.getJsonArray("caches");
		if(args.containsKey("cache")) {
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
					continue;
				}
			}				
			list.add(cache);					
		}		
		return list;
	}
	
}
