package org.apache.ignite.console.agent.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.console.agent.service.CacheAgentService;
import org.apache.ignite.console.agent.service.ServiceResult;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.TaskContinuousMapperResource;
import org.jetbrains.annotations.NotNull;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * This task demonstrates how continuous mapper is used. The passed in phrase is
 * split into multiple words and next word is sent out for processing only when
 * the result for the previous word was received.
 * <p>
 * Note that annotation {@link ComputeTaskNoResultCache} is optional and tells
 * Ignite not to accumulate results from individual jobs. In this example we
 * increment total character count directly in
 * {@link #result(ComputeJobResult, List)} method, and therefore don't need to
 * accumulate them be be processed at reduction step.
 */

public class CacheServiceMapperTask extends ComputeTaskSplitAdapter<JsonObject, ServiceResult> {
	
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
	
	public static List<String> cacheNameSelectList(JsonObject args,Ignite ignite) {		
		List<String> list = new ArrayList<>();
		
		if(args.containsKey("caches")) {
			JsonArray selectCaches = args.getJsonArray("caches");
			for(Object cache: selectCaches) {
				if(cache instanceof JsonObject) {
					list.add(((JsonObject)cache).getString("name"));
				}
				else if(cache!=null) {
					list.add(cache.toString());		
				}
			}
		}		
		else if(args.containsKey("cache")) {
			String cacheName = args.getJsonObject("cache").getString("name");
			if(cacheName!=null && !cacheName.isEmpty()) {
				list.add(cacheName);
			}
		}
		else if(args.containsKey("cacheName")) {
			String obj = args.getString("cacheName");
			list.add(obj.toString());
		}
		else {
			list.addAll(ignite.cacheNames().stream().filter(n-> !n.startsWith("__") && !n.startsWith("igfs-internal-") && !n.startsWith("INDEXES.")).collect(Collectors.toList()));
		}
		
		return list;
	}

	static class CacheServiceAdapterJob implements ComputeJob {

		@IgniteInstanceResource
		private Ignite ignite;

		final String serviceName;
		final String cacheName;
		final Map<String, Object> args;

		public CacheServiceAdapterJob(String service,String cacheName, Map<String, Object> args) {

			this.serviceName = service;
			this.cacheName = cacheName;
			this.args = args;

		}

		@Override
		public void cancel() {

			CacheAgentService agentSeervice = ignite.services().serviceProxy(serviceName, CacheAgentService.class,true);
			if (agentSeervice != null) {
				agentSeervice.cancel();
			}
		}

		@Override
		public ServiceResult execute() throws IgniteException {
			try {

				CacheAgentService agentSeervice = ignite.services().serviceProxy(serviceName, CacheAgentService.class,true);
				if (agentSeervice == null) {
					ServiceResult stat = new ServiceResult();
					stat.setStatus("404");
					stat.put("message", "service not found!");
					stat.setCacheName(cacheName);
					return stat;
				}
				ServiceResult result = agentSeervice.call(cacheName,args);
				result.setCacheName(cacheName);
				return result;

			} catch (Exception e) {
				throw new IgniteException(e);
			}
		}

	}
	
	@IgniteInstanceResource
	private Ignite ignite;

	
	@Override
	protected Collection<? extends ComputeJob> split(int gridSize, JsonObject payload) {
		List<CacheServiceAdapterJob> jobs = new ArrayList<>(gridSize);
		String serviceName = payload.getString("serviceName", "");
		JsonObject args = payload.getJsonObject("args");		
		
		List<String> cacheNames = cacheNameSelectList(args,ignite);

		for (int i = 0; i < cacheNames.size(); i++) {
			String cacheName = cacheNames.get(i);			
			jobs.add(new CacheServiceAdapterJob(serviceName,cacheName, args.getMap()));
		}

		return jobs;
	}

	// Aggregate results into one compound result.
	public ServiceResult reduce(List<ComputeJobResult> results) {
		ServiceResult result = new ServiceResult();
		StringBuilder errorMessage = new StringBuilder();
		for (ComputeJobResult res : results) {
			if(!res.isCancelled()) {
				if(res.getException()!=null) {
					result.addMessage(res.getException().getMessage());
					result.setAcknowledged(false);
				}
				else {
					ServiceResult partResult = res.getData();
					String cacheName = partResult.getCacheName();
					if(partResult.isAcknowledged()) {						
						result.put(cacheName, partResult.getResult());
					}
					else {
						result.put(cacheName, partResult.getResult());
						result.getMessages().addAll(partResult.getMessages());
						if(partResult.getErrorType()!=null) {
							result.setAcknowledged(false);
							errorMessage.append(cacheName);
							errorMessage.append(":");
							errorMessage.append(partResult.getErrorType());
							errorMessage.append("\n");
						}
					}
				}
			}
		}
		result.setErrorType(errorMessage.toString());
		return result;
	}
}
