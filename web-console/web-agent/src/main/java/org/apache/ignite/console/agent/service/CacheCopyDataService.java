package org.apache.ignite.console.agent.service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.agent.db.DataSourceManager;
import org.apache.ignite.console.agent.handlers.RestClusterHandler;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.stream.StreamVisitor;

import io.netty.util.internal.StringUtil;
import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Cache和Cache之间进行数据传输，在目标端的cluster执行
 * @author zjf
 *
 */
@ApiOperation(value="传输接受数据到集群内Cache",notes="这个操作是同步的")
public class CacheCopyDataService implements CacheAgentService {
   
	 /** Target Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;    
    
    
	@Override
	public ServiceResult call(String targetCache,Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;
		int nInserts = 0;
		JsonObject args = new JsonObject(payload);
		String clusterId = args.getString("clusterId");
		JsonArray taskFlows = DataSourceManager.getTaskFlows(clusterId, targetCache);
		for(int i=0;i<taskFlows.size();i++) {
			JsonObject task = taskFlows.getJsonObject(i);
			nInserts += copyFrom(result, task);	
			count++;
		}

		result.put("insertedCount", nInserts);	
		result.put("sourceCount", count);		
		return result;
	}
	
	public long copyFrom(ServiceResult result, JsonObject args) {
			
		long nInserts = 0;
		String targetCache = args.getString("target");
		String sourceCache = args.getString("source");
		
		String sourceClusterName = args.getString("sourceCluster"); // is cluster uuid
				
		Ignite igniteSource = ClusterAgentServiceManager.getIgniteByName(sourceClusterName,result);
		if(igniteSource==null) {
			result.getMessages().add("SourceCluster " + sourceClusterName + " is not existed!");
			return nInserts;
		}		

		
		JsonObject cacheInfo = new JsonObject();		

		try {
			IgniteCache<Object,BinaryObject> destCache = ignite.cache(targetCache).withKeepBinary();
			
			IgniteCache<Object,BinaryObject> srcCache = igniteSource.cache(sourceCache).withKeepBinary();
				
			long totalRows = transformExistedData(igniteSource,srcCache,destCache);
			cacheInfo.put("existedData", totalRows);
			if(totalRows==0) {
			   totalRows = transformData(igniteSource,srcCache,destCache);
			   cacheInfo.put("loadedData", totalRows);
			}
			
			nInserts = totalRows;
			
		}
		catch(Exception e) {
			result.setAcknowledged(false);
			result.getMessages().add(e.getMessage());
		}
		
		result.put("metric_"+sourceCache, cacheInfo);		
		
		return nInserts;
	}
	
	
	/**
	 * 传输数据到目的集群
	 * @param igniteDest
	 * @param igcache Cache for market data ticks streamed into the system.
	 * @return
	 */
	public long transformData(Ignite igniteSrc,IgniteCache<Object,BinaryObject> srcCache, IgniteCache<Object,BinaryObject> destCache) {
		LongAdder rows = new LongAdder();
		try  {
			String srcCacheName = srcCache.getName();
			
			String typeName = typeName(destCache);
	        
	        BinaryObjectBuilder bb = ignite.binary().builder(typeName);
			
	        IgniteDataStreamer<Object,BinaryObject> mktStmr = igniteSrc.dataStreamer(srcCacheName);
            // Note that we do not populate the 'marketData' cache (it remains empty).
            // Instead we update the 'instruments' cache based on the latest market price.
            mktStmr.receiver(StreamVisitor.from((k, entry) -> {
               
            	BinaryObject binaryObj = entry.getValue();
            	if(binaryObj instanceof BinaryObjectImpl) {
            		BinaryObjectImpl bo = (BinaryObjectImpl)binaryObj;
            		for(String field: bo.type().fieldNames()) {
            			bb.setField(field,bo.<Object>field(field));
            		}
            		
            		destCache.put(entry.getKey(),bb.build());
                	rows.increment();
            	}          	
            	
            }));
           
            srcCache.loadCache(null);
	        
	    }
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		return rows.longValue();		
	}
	
	public long transformExistedData(Ignite igniteSrc,IgniteCache<Object,BinaryObject> srcCache, IgniteCache<Object,BinaryObject> destCache) {
		LongAdder rows = new LongAdder();
		try  {
			String destCacheName = destCache.getName();

	        IgniteDataStreamer<Object,Object> mktStmr = ignite.dataStreamer(destCacheName);            
	        mktStmr.allowOverwrite(false);
	        String typeName = typeName(destCache);
	        
	        BinaryObjectBuilder bb = ignite.binary().builder(typeName);
	        
            ScanQuery<Object,BinaryObject> scan = new ScanQuery<>();
            QueryCursor<Cache.Entry<Object,BinaryObject>> cursor = srcCache.query(scan);
            for(Cache.Entry<Object,BinaryObject> entry: cursor) {
            	
            	BinaryObject binaryObj = entry.getValue();
            	if(binaryObj instanceof BinaryObjectImpl) {
            		BinaryObjectImpl bo = (BinaryObjectImpl)binaryObj;
            		for(String field: bo.type().fieldNames()) {
            			bb.setField(field,bo.<Object>field(field));
            		}
            		
            		mktStmr.addData(entry.getKey(),bb.build());
                	rows.increment();
            	}            	
            }
            
            mktStmr.close();
	    }
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		return rows.longValue();		
	}
	
	public String typeName(IgniteCache<Object,BinaryObject> dataMap) {    	
    	String typeName = tableOfCache(dataMap.getName());    	
    	int pos = typeName.lastIndexOf('.');
    	String shortName = pos>0? typeName.substring(pos+1): typeName;
    	
    	CacheConfiguration<Object,BinaryObject> cfg = dataMap.getConfiguration(CacheConfiguration.class);
    	
    	if(!cfg.getQueryEntities().isEmpty()) {
    		Iterator<QueryEntity> qeit = cfg.getQueryEntities().iterator();
    		while(qeit.hasNext()) {
	    		QueryEntity entity = qeit.next();
	    		if(typeName.equalsIgnoreCase(entity.getValueType()) || shortName.equalsIgnoreCase(entity.getTableName())){
	    			break;
	    		}
	    		else {
	    			typeName = entity.getValueType();
	    		}
    		}
    	}    	  	
    	return typeName;
    }
	
	public static String tableOfCache(String cacheName) {
		if(cacheName.startsWith("SQL_")) {
			int pos = cacheName.lastIndexOf('_',5);
			if(pos>0)
				return cacheName.substring(pos+1);
		}
		return cacheName;
	}
	
}
