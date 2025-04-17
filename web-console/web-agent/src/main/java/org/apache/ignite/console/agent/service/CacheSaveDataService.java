package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.cache.Cache;
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheWriter;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;

@ApiOperation("Save ignite cache data to dbstore")
public class CacheSaveDataService implements CacheAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public ServiceResult call(Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;
		JsonObject args = new JsonObject(payload);	
		List<String> message = result.getMessages();
		List<String> caches = cacheNameSelectList(ignite,args);
		for(String cache: caches) {
			try {
				IgniteCache<Object, Object> igcache = ignite.cache(cache);					
				
				CacheConfiguration<Object, Object> cfg = igcache.getConfiguration(CacheConfiguration.class);
				
				if(cfg.isWriteThrough()) {
					message.add("Cache WriteThrough is true, It may not be necessary to call this service!");
				}
				
				if(cfg.getCacheWriterFactory()!=null) {
					CacheWriter<Object, Object> writer = cfg.getCacheWriterFactory().create();
					
					Iterable<Cache.Entry<Object, Object>> it = (Iterable)igcache.localEntries(CachePeekMode.PRIMARY);
					for(Cache.Entry<Object, Object> row: it) {
						writer.write(row);
					}
					
				}
				else if(cfg.getCacheStoreFactory()!=null) {
					CacheStore<Object, Object> store =  cfg.getCacheStoreFactory().create();
					Iterable<Cache.Entry<Object, Object>> it = (Iterable)igcache.localEntries(CachePeekMode.PRIMARY);
					for(Cache.Entry<Object, Object> row: it) {
						store.write(row);
					}
					store.sessionEnd(true);
				}
				
				count++;
			}
			catch(Exception e) {
				message.add(e.getMessage());
			}
		}
		if(message.isEmpty()) {
			message.add("Finish save data successfull!");
		}
		
		result.put("caches", caches);
		result.put("count", count);
		return result;
	}

}
