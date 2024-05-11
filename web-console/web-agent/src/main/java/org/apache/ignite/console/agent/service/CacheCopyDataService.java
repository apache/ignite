package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.stream.StreamVisitor;

import io.swagger.annotations.ApiOperation;

/**
 * Cache和Cache之间进行数据传输，在目标端的cluste执行
 * @author zjf
 *
 */
@ApiOperation(value="传输接受数据到集群内Cache",notes="这个操作是同步的")
public class CacheCopyDataService implements CacheAgentService {
   
	 /** Target Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
    public Ignite getIgniteByName(String clusterName,Map<String,Object> stat) {
    	Ignite ignite = null;
    	String nodeId;
    	nodeId = Utils.escapeFileName(clusterName);        
        if(nodeId!=null) {        	
        	try {
        		ignite = Ignition.ignite(UUID.fromString(nodeId));	    		
	    		stat.put("status", "started");
	    		clusterName = null;
    		}
	    	catch(IgniteIllegalStateException e) {	
	    		stat.put("message", e.getMessage());
	    		stat.put("status", "stoped");
	    	}
        }
        if(clusterName!=null) {
        	try {
        		ignite = Ignition.ignite(clusterName);	    		
	    		stat.put("status", "started");
    		}
	    	catch(IgniteIllegalStateException e) {	
	    		stat.put("message", e.getMessage());
	    		stat.put("status", "stoped");
	    	}
    	}
        return ignite;
    }
    
	@Override
	public Map<String, ? extends Object> call(Map<String,Object> payload) {
		Map<String,Object> result = new HashMap<>();
		int count = 0;		
		JsonObject args = new JsonObject((Map)payload.get("args"));	
		String targetCache = args.getString("target");
		String sourceCache = args.getString("source");	
		
		
		String sourceClusterName = args.getString("sourceCluster");
				
		Ignite igniteSource = getIgniteByName(sourceClusterName,result);
		if(igniteSource==null) {
			return result;
		}
		
		JsonObject cacheInfo = new JsonObject();
		String message = null;

		try {
			IgniteCache<?,?> destCache = ignite.cache(targetCache);
			
			IgniteCache<?,?> srcCache = igniteSource.cache(targetCache);
				
			long totalRows = transformExistedData(igniteSource, srcCache,destCache);
			cacheInfo.put("existedData", totalRows);
			if(totalRows==0) {
			   totalRows = transformData(igniteSource, srcCache,destCache);
			   cacheInfo.put("loadedData", totalRows);
			}			
			
			count++;
		}
		catch(Exception e) {
			message = e.getMessage();
		}
		
		result.put("metric", cacheInfo);
		result.put("message", message);		
		result.put("count", count);
		return result;
	}
	
	
	/**
	 * 传输数据到目的集群
	 * @param igniteDest
	 * @param igcache Cache for market data ticks streamed into the system.
	 * @return
	 */
	public long transformData(Ignite igniteSrc,IgniteCache<?,?> srcCache, IgniteCache<?,?> destCache0) {
		LongAdder rows = new LongAdder();
		try  {
			String srcCacheName = srcCache.getName();
			IgniteCache<Object,Object> destCache = (IgniteCache)destCache0;
	        IgniteDataStreamer<Object,Object> mktStmr = igniteSrc.dataStreamer(srcCacheName);
            // Note that we do not populate the 'marketData' cache (it remains empty).
            // Instead we update the 'instruments' cache based on the latest market price.
            mktStmr.receiver(StreamVisitor.from((k, v) -> {
               
            	rows.increment();
                // Update the dest cache.
            	destCache.put(k, v);
            }));

           
            srcCache.loadCache(null);
	        
	    }
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		return rows.longValue();		
	}
	
	public long transformExistedData(Ignite igniteSrc,IgniteCache<?,?> srcCache, IgniteCache<?,?> destCache) {
		LongAdder rows = new LongAdder();
		try  {
			String destCacheName = destCache.getName();

	        IgniteDataStreamer<Object,Object> mktStmr = ignite.dataStreamer(destCacheName);            
	        mktStmr.allowOverwrite();
	        
            ScanQuery<?,?> scan = new ScanQuery<>();
            QueryCursor<Cache.Entry<?,?>> cursor = srcCache.query(scan, null);
            for(Cache.Entry<?,?> entry: cursor) {
            	mktStmr.addData(entry.getKey(),entry.getValue());
            	rows.increment();
            }	
            
            mktStmr.close();
	    }
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		return rows.longValue();		
	}
}
