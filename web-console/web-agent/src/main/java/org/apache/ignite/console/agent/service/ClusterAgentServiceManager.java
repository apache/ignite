package org.apache.ignite.console.agent.service;

import java.lang.reflect.InvocationTargetException;
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

@ApiOperation(value = "Deploy Service to the cluster", notes = "部署Service到ignite集群中。")
public class ClusterAgentServiceManager implements ClusterAgentService {
   
	@IgniteInstanceResource
    private Ignite ignite;
	
	public ClusterAgentServiceManager(){		
	}
		
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
			
			if(ctx.affinityKey()!=null) {
				info.put("mode", "KeyaffinitySingleton"); 
			}
			else if(ctx.maxPerNodeCount()>1) {
				info.put("mode", "Multiple"); 
			}
			result.put(ctx.name(), info);
		}
		return result;
	}	
	
	public ServiceResult redeployService(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		List<String> messages = result.messages;
		Collection<ServiceDescriptor> descs = ignite.services().serviceDescriptors();		
		JsonObject args = new JsonObject(payload);		
		JsonArray services = args.getJsonArray("services");
		
		for(ServiceDescriptor desc: descs) {
			for(Object service: services) {
				if(desc.name().equals(service)) {
					ServiceConfiguration cfg = new ServiceConfiguration();
				    cfg.setName(desc.name());
				    cfg.setCacheName(desc.cacheName());
				    cfg.setAffinityKey(desc.affinityKey());
				    cfg.setMaxPerNodeCount(desc.maxPerNodeCount());
				    cfg.setTotalCount(desc.totalCount());
				    
				    JsonObject info = new JsonObject();
					info.put("name", desc.name());
					info.put("cacheName", desc.cacheName());
					
				    try {
						cfg.setService(desc.serviceClass().getDeclaredConstructor().newInstance());
						ignite.services().deploy(cfg);						
						
					} catch (Exception e) {
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

	/**
	 * deploy service
	 */
	@Override
	public ServiceResult call(String cluterId, Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();
		List<String> messages = result.messages;
		JsonObject args = new JsonObject(payload);
		if(args.containsKey("service")) {
			JsonObject service = args.getJsonObject("service");
			ServiceConfiguration cfg = new ServiceConfiguration();
		    cfg.setName(service.getString("name"));
		    cfg.setCacheName(service.getString("cache"));
		    cfg.setAffinityKey(service.getString("affinityKey"));
		    cfg.setMaxPerNodeCount(service.getInteger("maxPerNodeCount"));
		    cfg.setTotalCount(service.getInteger("totalCount"));		    
		    
		    try {
		    	String serviceCls = service.getString("service");
		    	// must be java bean
		    	Class<? extends Service> serviceClass = (Class) Class.forName(serviceCls);
				cfg.setService(serviceClass.getDeclaredConstructor().newInstance());
				ignite.services().deploy(cfg);				
				
			} catch (Exception e) {
				messages.add(e.getMessage());
			}
			result.setStatus("success");
			return result;
		}
		// 依次部署多个class，信息从ApiOperation获取
		JsonArray services = args.getJsonArray("servicesClass");
		// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
		String mode = args.getString("mode","NodeSingleton");
		
		for(Object service: services) {
			if(service instanceof String) {				
			    try {
			    	Class<? extends Service> ctx = (Class)Class.forName(service.toString());
			    	Service svc = ctx.getDeclaredConstructor().newInstance();
			    	
				    JsonObject info = new JsonObject();
					info.put("name", ctx.getSimpleName());
					info.put("cacheName", "");
					ApiOperation api = ctx.getAnnotation(ApiOperation.class);					

					if (api != null) {
						info.put("description", api.value());
						info.put("notes", api.notes());
					} else {
						info.put("description", ctx.getName());
						info.put("notes", "");
					}
					info.put("mode", mode);
					if(mode.equals("ClusterSingleton")) {
						ignite.services().deployClusterSingleton(ctx.getSimpleName(),svc);
					}
					else if(mode.equals("NodeSingleton")) {
						ignite.services().deployNodeSingleton(ctx.getSimpleName(),svc);
					}
					else if(mode.equals("Multiple")) {
						int totalCnt = args.getInteger("totalCnt",ignite.cluster().nodes().size());
						int maxPerNodeCnt = args.getInteger("maxPerNodeCnt",1);
						ignite.services().deployMultiple(ctx.getSimpleName(), svc, totalCnt, maxPerNodeCnt);
					}
					else {
						String cacheName = args.getString("cacheName");
						Object affKey = args.getValue("affKey");
						info.put("cacheName", cacheName);
						ignite.services().deployKeyAffinitySingleton(ctx.getSimpleName(), svc, cacheName, affKey);
					}					
					result.put(ctx.getSimpleName(), info);	
					
				} catch (InstantiationException | IllegalAccessException e) {
					messages.add(e.getMessage());
				} catch (Exception e) {
					messages.add(e.getMessage());
				}			    				
			}
		}
		return result;
	}
}
