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
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.stream.StreamVisitor;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;


@ApiOperation(value="Backup cluster data to snapshot",notes="这个操作是异步的")
public class ClusterSnapshotDataService implements ClusterAgentService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public ServiceResult call(String cluterId,Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();
		int count = 0;		
		JsonObject args = new JsonObject(payload);	
		
		String destClusterName = args.getString("dest");
		
		IgniteFuture<Void> future = ignite.snapshot().createSnapshot(destClusterName);
		
		result.put("result", "created");
		return result;
	}	
}
