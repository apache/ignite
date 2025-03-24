package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;



public interface CacheAgentService extends Service {   
	
	public abstract ServiceResult call(Map<String,Object> payload);
	
	public default List<JsonObject> cacheSelectList(JsonObject args) {
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
}
