package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;

import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDescriptor;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@ApiOperation("get list of servcie of cluster")
public class ClusterAgentServiceManager {
   
	@IgniteInstanceResource
    private Ignite ignite;	
		
	public ClusterAgentServiceManager(Ignite ignite){
		this.ignite = ignite;
	}	
	
	public ServiceResult serviceList(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		Collection<ServiceDescriptor> descs = ignite.services().serviceDescriptors();
		
		JsonObject args = new JsonObject(payload);
		String type = args!=null? args.getString("type"):null;
		
		for(ServiceDescriptor ctx: descs) {
			JsonObject info = new JsonObject();
			info.put("name", ctx.name());			
			
			info.put("cacheName", ctx.cacheName());
			info.put("affinityKey", ctx.affinityKey());
			info.put("totalCount", ctx.totalCount());
			info.put("maxPerNodeCount", ctx.maxPerNodeCount());
			ApiOperation api = ctx.serviceClass().getAnnotation(ApiOperation.class);
			if(api!=null) {
				info.put("description", api.value());
				info.put("notes", api.notes());				
			}			
			else {
				info.put("description", ctx.serviceClass().getSimpleName());
				info.put("notes","");
			}
			
			if(ClusterAgentService.class.isAssignableFrom(ctx.serviceClass())){
				info.put("type","ClusterAgentService");
				// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
				info.put("mode", "ClusterSingleton"); 
			}
			else if(CacheAgentService.class.isAssignableFrom(ctx.serviceClass())){
				info.put("type","CacheAgentService");
				// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
				info.put("mode", "NodeSingleton"); 
			}
			else {
				info.put("mode", "ClusterSingleton"); 
				info.put("type","Unknown");
			}
			if(type!=null && !info.getString("type").equals(type)) {
				continue;
			}
			result.put(ctx.name(), info);
		}
		return result;
	}	
	
	public ServiceResult redeployService(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		Collection<ServiceDescriptor> descs = ignite.services().serviceDescriptors();		
		JsonObject args = new JsonObject(payload);		
		JsonArray services = args.getJsonArray("services");
		List<String> messages = new ArrayList<>();
		for(ServiceDescriptor desc: descs) {
			for(Object service: services) {
				if(desc.name().equals(service)) {
					ServiceConfiguration cfg = new ServiceConfiguration();
				    cfg.setName(desc.name());
				    JsonObject info = new JsonObject();
					info.put("name", desc.name());
					info.put("cacheName", desc.cacheName());
					
				    try {
						cfg.setService(desc.serviceClass().newInstance());
						ignite.services().deployNodeSingleton(cfg.getName(),cfg.getService());						
						
					} catch (InstantiationException | IllegalAccessException e) {
						messages.add(e.getMessage());
					}
				    result.put(desc.name(), info);					
				}
			}
		}
		return result;
	}
	
	public ServiceResult undeployService(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();	
		JsonObject args = new JsonObject(payload);		
		JsonArray services = args.getJsonArray("services");
		List<String> messages = result.messages;
		for(Object service: services) {
			if(service!=null) {
				
			    JsonObject info = new JsonObject();
				info.put("name", service.toString());				
			    try {						
					ignite.services().cancel(service.toString());					
					
				} catch (Exception e) {
					messages.add(e.getMessage());
				}
			    result.put(service.toString(), info);					
			}
		}	
		return result;
	}
	
	public ServiceResult cancelService(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();	
		JsonObject args = new JsonObject(payload);		
		JsonArray services = args.getJsonArray("services");
		List<String> messages = result.messages;
		for(Object service: services) {
			if(service!=null) {
				
			    JsonObject info = new JsonObject();
				info.put("name", service.toString());				
			    try {						
					Service svc = ignite.services().service(service.toString());
					svc.cancel();
					
				} catch (Exception e) {
					messages.add(e.getMessage());
				}
			    result.put(service.toString(), info);					
			}
		}	
		return result;
	}
}
