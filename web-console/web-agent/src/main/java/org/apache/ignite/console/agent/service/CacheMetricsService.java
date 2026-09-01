package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;


@ApiOperation("Get cache metrics from cluster")
public class CacheMetricsService implements CacheAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public ServiceResult call(String cache,Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();

		List<String> message = result.getMessages();
		
		try {				
			IgniteCache<?,?> igcache = ignite.cache(cache);
			if(igcache!=null) {
				CacheMetrics metric = igcache.metrics();
				result.getResult().putAll(JsonObject.mapFrom(metric).getMap());				
			}		

		}
		catch(Exception e) {
			message.add(e.getMessage());
		}
		return result;
	}

}
