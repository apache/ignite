package org.apache.ignite.console.agent.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDescriptor;

import io.swagger.annotations.ApiOperation;

@ApiOperation("get list of servcie of cluster")
public class ClusterAgentServiceList implements ClusterAgentService {
   
	@IgniteInstanceResource
    private Ignite ignite;
	
	@Override
	public Map<String, ? extends Object> call(Map<String,Object> payload) {
		Map<String,Object> result = new HashMap<>();
		Collection<ServiceDescriptor> descs = ignite.services().serviceDescriptors();
		
		JsonObject args = new JsonObject(payload);
		String type = args!=null?args.getString("type"):null;
		
		for(ServiceDescriptor ctx: descs) {
			JsonObject info = new JsonObject();
			info.add("name", ctx.name());			
			
			info.add("cacheName", ctx.cacheName());
			info.add("affinityKey", ctx.affinityKey());
			info.add("totalCount", ctx.totalCount());
			info.add("maxPerNodeCount", ctx.maxPerNodeCount());
			ApiOperation api = ctx.serviceClass().getAnnotation(ApiOperation.class);
			if(api!=null) {
				info.add("description", api.value());
				info.add("notes", api.notes());				
			}			
			else {
				info.add("description", ctx.serviceClass().getSimpleName());
				info.add("notes","");
			}
			
			if(ClusterAgentService.class.isAssignableFrom(ctx.serviceClass())){
				info.add("type","ClusterAgentService");
				// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
				info.add("mode", "ClusterSingleton"); 
			}
			else if(CacheAgentService.class.isAssignableFrom(ctx.serviceClass())){
				info.add("type","CacheAgentService");
				// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
				info.add("mode", "NodeSingleton"); 
			}
			else {
				info.add("mode", "ClusterSingleton"); 
				info.add("type","Unknown");
			}
			if(type!=null && !info.getString("type").equals(type)) {
				continue;
			}
			result.put(ctx.name(), info);
		}
		return result;
	}
}
