package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;


@ApiOperation("delete cache table entity from cluster")
public class CacheDeleteTableService implements CacheAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public ServiceResult call(String cache,Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;		
		JsonObject args = new JsonObject(payload);	
		List<String> message = result.getMessages();	
		IgniteEx igniteEx = (IgniteEx) ignite;
		GridKernalContext ctx = igniteEx.context();
		String schema = args.getString("sqlSchema");		
		
		try {
			
			SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM "+cache);
			qry.setLocal(true);
			if(schema!=null && !schema.isBlank()) {
				qry.setSchema(schema);
			}
			IgniteCache<?,?> igcache = ignite.cache(cache);
			if(igcache!=null) {
				count = igcache.query(qry).getAll().size();
			}
			else {
				count = ctx.query().querySqlFields(qry, true).getAll().size();
			}
			
		}
		catch(Exception e) {
			result.setAcknowledged(false);
			message.add(e.getMessage());
		}
		
		result.put("deletedCount", count);
		return result;
	}

}
