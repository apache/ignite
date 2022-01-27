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



public interface ClusterAgentService extends Service {
	
    static Map<String,JsonObject> serviceList = new HashMap<>();

	@Override
	default void cancel(ServiceContext ctx) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	default void init(ServiceContext ctx) throws Exception {
		JsonObject info = new JsonObject();
		info.add("name", ctx.name());
		// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
		info.add("mode", "NodeSingleton"); 
		info.add("description", this.toString());
		info.add("cacheName", ctx.cacheName());
		info.add("affinityKey", ctx.affinityKey());
		serviceList.put(ctx.name(), info);
	}
	
	@Override
	default void execute(ServiceContext ctx) throws Exception {
		// TODO Auto-generated method stub
		
	}   
   
	
	public abstract Map<String, ? extends Object> call(Map<String,Object> payload);
	
	
}
