package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.stream.StreamVisitor;


public class ClusterWriteDataService implements ClusterAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public Map<String, ? extends Object> call(Map<String,Object> payload) {
		Map<String,Object> result = new HashMap<>();
		int count = 0;		
		JsonObject args = new JsonObject((Map)payload.get("args"));	
		List<String> caches = ClusterAgentServiceUtil.cacheSelectList(ignite,args);
		
		String destClusterName = args.getString("dest");
		
		Ignite igniteDest = Ignition.ignite(destClusterName);
		
		JsonObject cacheInfo = new JsonObject();
		List<String> message = new ArrayList<>();
		for(String cache: caches) {
			try {
				IgniteCache<?,?> igcache = ignite.cache(cache);
					
				long totalRows = transformLoadedData(igniteDest, igcache);
				if(totalRows==0) {
				   totalRows = transformData(igniteDest, igcache);
				}
				
				cacheInfo.put(cache, totalRows);
				count++;
			}
			catch(Exception e) {
				message.add(e.getMessage());
			}
		}
		
		result.put("metric", cacheInfo);
		result.put("errors", message);
		result.put("caches", ignite.cacheNames());
		result.put("count", count);
		return result;
	}

	public String toString() {
		return "Write data to other cluster";
	}
	
	/**
	 * 传输数据到目的集群
	 * @param igniteDest
	 * @param igcache Cache for market data ticks streamed into the system.
	 * @return
	 */
	public long transformData(Ignite igniteDest,IgniteCache<?,?> igcache) {
		LongAdder rows = new LongAdder();
		try  {
			String destCacheName = igcache.getName();

	        // Cache for financial instruments.
	        IgniteCache instCache = igniteDest.getOrCreateCache(destCacheName);

	        IgniteDataStreamer<?, ?> mktStmr = ignite.dataStreamer(destCacheName);
            // Note that we do not populate the 'marketData' cache (it remains empty).
            // Instead we update the 'instruments' cache based on the latest market price.
            mktStmr.receiver(StreamVisitor.from((k, v) -> {
               
            	rows.increment();
                // Update the dest cache.
                instCache.put(k, v);
            }));

           
            igcache.loadCache(null);
	        
	    }
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		return rows.longValue();		
	}
	
	public long transformLoadedData(Ignite igniteDest,IgniteCache<?,?> igcache) {
		LongAdder rows = new LongAdder();
		try  {
			String destCacheName = igcache.getName();

	        // Cache for financial instruments.
	        IgniteCache instCache = igniteDest.getOrCreateCache(destCacheName);

	        IgniteDataStreamer mktStmr = ignite.dataStreamer(destCacheName);
            

            ScanQuery scan = new ScanQuery();
            QueryCursor<Cache.Entry> cursor = igcache.query(scan, null);
            for(Cache.Entry entry: cursor) {
            	mktStmr.addData(entry.getKey(),entry.getValue());
            	rows.increment();
            }	        
	    }
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		return rows.longValue();		
	}
}
