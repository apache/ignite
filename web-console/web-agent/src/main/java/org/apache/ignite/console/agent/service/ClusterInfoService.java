package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import javax.cache.Cache;

import io.vertx.webmvc.mcp.ToolExecutor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.stream.StreamVisitor;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


@ApiOperation(value="Get cluster node attributes and metrics data",notes="获取集群节点的属性和统计信息")
public class ClusterInfoService implements ClusterAgentService,McpService {
   
	 /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

	@Override
	public List<ToolExecutor> toolExecutors(){
		ToolExecutor toolX = ToolExecutor.builder()
				.name("cluster-info")
				.description("Get cluster node attributes from cluster")
				.parameter("clusterName","string","Ignite cluster name",true)
				.outputField("nodes","array","node attributes")
				.outputField("state","string","cluster state {actived,inactived}")
				.executor((Map<String,Object> param)->{
					ServiceResult result = this.call((String)param.get("clusterName"),param);
					return result;
				})
				.build();

		return List.of(toolX);
	}

	@Override
	public ServiceResult call(String cluterId,Map<String,Object> payload) {
		ServiceResult result = new ServiceResult();		
		
		IgniteCluster cluster = ignite.cluster();
		
		JsonArray nodes = new JsonArray();
		for(ClusterNode node: cluster.nodes()) {
			JsonObject attr = new JsonObject();			
			attr.put("node.isClient", node.isClient());
			attr.put("node.consistentId", node.consistentId());
			attr.put("node.addresses", node.addresses());
			attr.put("node.hostNames", node.hostNames());
			
			node.attributes().forEach((k,v)->{
				if(v instanceof CharSequence && v.toString().length()<256) {
					attr.put(k,v);
				}
				else if(v instanceof Number) {
					attr.put(k,v);
				}
				else if(v!=null && v.getClass().getName().startsWith("java.")) {
					attr.put(k,v);
				}
			});
			nodes.add(attr);
		}
		
		result.put("metrics", JsonObject.mapFrom(cluster.metrics()));
		result.put("nodes", nodes);		
		result.put("state", cluster.state().name());
		return result;
	}	
}
