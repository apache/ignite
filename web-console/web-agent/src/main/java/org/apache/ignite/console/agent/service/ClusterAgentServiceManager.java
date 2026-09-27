package org.apache.ignite.console.agent.service;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.webmvc.mcp.ToolExecutor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.console.agent.ServiceDeployment;
import org.apache.ignite.console.agent.handlers.RestClusterHandler;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.internal.plugin.IgniteVertxPlugin;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDescriptor;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@ApiOperation(nickname="serviceManager", value = "Deploy Service to the cluster", notes = "列出、部署、卸载Service到ignite集群中。")
public class ClusterAgentServiceManager implements ClusterAgentService,McpService {
   
	@IgniteInstanceResource
    private Ignite ignite;
	
	public ClusterAgentServiceManager(){		
	}
		
	public ClusterAgentServiceManager(Ignite ignite){
		this.ignite = ignite;
	}
	
	public static Ignite getIgniteByName(String clusterName,ServiceResult stat) {
    	Ignite ignite = null;    	
    	String clusterId = Utils.escapeFileName(clusterName);
    	String gridName = RestClusterHandler.clusterNameMap.get(clusterId);
		if(gridName!=null) {
			try {
        		ignite = Ignition.ignite(gridName);	    		
	    		stat.setStatus("started");
	    		clusterName = null;
    		}
	    	catch(IgniteIllegalStateException e) {	
	    		stat.addMessage(e.getMessage());
	    		stat.setStatus("stoped");
	    	}
		}        
        if(ignite!=null && clusterName!=null) {
        	try {
        		ignite = Ignition.ignite(clusterName);	    		
	    		stat.setStatus("started");
    		}
	    	catch(IgniteIllegalStateException e) {	
	    		stat.addMessage(e.getMessage());
	    		stat.setStatus("stoped");
	    	}
    	}
        return ignite;
    }

	@Override
	public List<ToolExecutor> toolExecutors(){
		ToolExecutor list = ToolExecutor.builder()
				.name("serviceList")
				.description("Get deployed services from cluster")
				.outputField("*","object","Service attributes and toolList")
				.executor((Map<String,Object> param)->{
					ServiceResult result = this.call("serviceList",param);
					return result;
				})
				.build();

		ToolExecutor deploy = ToolExecutor.builder()
				.name("deployService")
				.description("Deploy Service into cluster")
				.parameter("service","object","Ignite service object",false)
				.parameter("name","string","Ignite service name",true)
				.parameter("serviceClass","string","Ignite service class",true)
				.outputField("*","object","Service attributes and toolList")
				.executor((Map<String,Object> param)->{
					ServiceResult result = this.call("deployService",param);
					return result;
				})
				.build();

		ToolExecutor redeploy = ToolExecutor.builder()
				.name("redeployService")
				.description("ReDeploy Service into cluster")
				.parameter("services","array","Ignite service name list",true)
				.outputField("*","object","Service attributes")
				.executor((Map<String,Object> param)->{
					ServiceResult result = this.call("redeployService",param);
					return result;
				})
				.build();

		ToolExecutor undeploy = ToolExecutor.builder()
				.name("undeployService")
				.description("UnDeploy Service from cluster")
				.parameter("services","array","Ignite service name list",true)
				.outputField("*","object","Service attributes")
				.executor((Map<String,Object> param)->{
					ServiceResult result = this.call("undeployService",param);
					return result;
				})
				.build();

		ToolExecutor cancel = ToolExecutor.builder()
				.name("cancelService")
				.description("Cancel Service execute in cluster")
				.parameter("services","array","Ignite service name list",true)
				.outputField("*","object","Service attributes")
				.executor((Map<String,Object> param)->{
					ServiceResult result = this.call("cancelService",param);
					return result;
				})
				.build();

		return List.of(list,deploy,redeploy,undeploy,cancel);
	}
	
	@Override
	public ServiceResult call(String serviceName, Map<String, Object> payload) {
		if(serviceName==null || serviceName.isBlank()) {
			serviceName = "listService";
		}
		if(serviceName.equals("listService") || serviceName.equals("serviceList")) {    		
			return this.serviceList(payload);
    	}
    	if(serviceName.equals("deployService")) {    		
    		return this.deployService(payload);	
    	}
    	if(serviceName.equals("redeployService")) {
    		return this.redeployService(payload);		
    	}
    	if(serviceName.equals("undeployService")) {    		
    		return this.undeployService(payload);		
    	}
    	if(serviceName.equals("cancelService")) {    		
    		return this.cancelService(payload);
    	}
    	return ServiceResult.fail("Wrong serviceName:"+serviceName);
	}
	
	public ServiceResult serviceList(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		Collection<ServiceDescriptor> descs = ignite.services().serviceDescriptors();
		
		String type = payload!=null? (String)payload.get("type"):null;
		
		for(ServiceDescriptor desc: descs) {
			JsonObject info = new JsonObject();
			info.put("name", desc.name());
			
			info.put("cacheName", desc.cacheName());
			info.put("affinityKey", desc.affinityKey());
			info.put("totalCount", desc.totalCount());
			info.put("maxPerNodeCount", desc.maxPerNodeCount());
			ApiOperation api = desc.serviceClass().getAnnotation(ApiOperation.class);
			if(api!=null) {
				info.put("description", api.value());
				info.put("notes", api.notes());				
			}			
			else {
				info.put("description", desc.serviceClass().getSimpleName());
				info.put("notes","");
			}

			info.put("tools",ServiceDeployment.getToolList(desc.name()));
			
			if(ClusterAgentService.class.isAssignableFrom(desc.serviceClass())){
				info.put("type","ClusterAgentService");
				// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
				info.put("mode", "ClusterSingleton"); 
			}
			else if(CacheAgentService.class.isAssignableFrom(desc.serviceClass())){
				info.put("type","CacheAgentService");
				// KeyaffinitySingleton,Multiple,NodeSingleton,ClusterSingleton
				info.put("mode", "NodeSingleton"); 
			}
			else if(desc.totalCount()==1){
				info.put("mode", "ClusterSingleton"); 
				info.put("type",desc.serviceClass().getSimpleName());
			}
			else if(desc.totalCount()==0 && desc.maxPerNodeCount()==1){
				info.put("mode", "NodeSingleton");
				info.put("type",desc.serviceClass().getSimpleName());
			}
			else{
				info.put("mode", "Multiple");
				info.put("type",desc.serviceClass().getSimpleName());
			}
			
			if(type!=null && !info.getString("type").equals(type)) {
				continue;
			}
			result.put(desc.name(), info);
		}
		return result;
	}	
	
	public ServiceResult redeployService(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		List<String> messages = result.getMessages();
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
						ignite.services().cancel(service.toString());
						ServiceDeployment.deployService(ignite,cfg,desc.serviceClass().getName(),null);
						info.put("success",true);
						
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
		List<String> messages = result.getMessages();
		for(Object service: services) {
			if(service!=null) {
				
			    JsonObject info = new JsonObject();
				info.put("name", service.toString());				
			    try {						
					ignite.services().cancel(service.toString());
					info.put("success",true);
					
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
		List<String> messages = result.getMessages();
		for(Object service: services) {
			if(service!=null) {
				
			    JsonObject info = new JsonObject();
				info.put("name", service.toString());				
			    try {						
					Service svc = ignite.services().service(service.toString());
					svc.cancel();
					info.put("success",true);
					
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
	public ServiceResult deployService(Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();
		JsonObject args = new JsonObject(payload);
		JsonObject service;
		if(args.containsKey("service")) {
			service = args.getJsonObject("service");
		}
		else{
			service = args;
		}

		if(service.containsKey("name")) {
			ServiceConfiguration cfg = new ServiceConfiguration();
		    cfg.setName(service.getString("name"));
		    cfg.setCacheName(service.getString("cache"));
		    cfg.setAffinityKey(service.getString("affinityKey"));

			String mode = args.getString("mode");
			if(mode==null) {
				cfg.setMaxPerNodeCount(service.getInteger("maxPerNodeCount"));
				cfg.setTotalCount(service.getInteger("totalCount"));
			}
			else{
				if(mode.equals("ClusterSingleton")) {
					cfg.setTotalCount(1);
					cfg.setMaxPerNodeCount(1);
				}
				else if(mode.equals("NodeSingleton")) {
					cfg.setMaxPerNodeCount(1);
					cfg.setTotalCount(0);
				}
				else if(mode.equals("Multiple")) {
					int maxPerNodeCnt = args.getInteger("maxPerNodeCount",2);
					cfg.setMaxPerNodeCount(maxPerNodeCnt);
					cfg.setTotalCount(0);
				}
			}
		    
		    try {
		    	String serviceCls = service.getString("serviceClass");
				result = ServiceDeployment.deployService(ignite,cfg,serviceCls,mode);
				
			} catch (Exception e) {
				result.addMessage(e.getMessage());
			}
			result.setStatus("success");
			return result;
		}
		return result;
	}
}
