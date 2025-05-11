/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent.handlers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Optional;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.GremlinExecutor;
import org.apache.ignite.console.agent.rest.GridTaskExecutor;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.agent.service.LangflowApiClient;
import org.apache.ignite.console.agent.service.ServiceResult;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.plugin.IgniteVertxPlugin;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.processors.rest.handlers.top.GridTopologyCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestTopologyRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;

import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.TOPOLOGY;

/**
 * API to transfer topology from vertx cluster to Web Console.
 */
public class VertxClusterHandler implements ClusterHandler{
	/** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(VertxClusterHandler.class));
	
    /** Vertx cluster name. */
    public static final String VERTX_CLUSTER_NAME_PREFIX = "Vertx-";
    /** ignite name -> vertx */
    public static final Map<String, Vertx> clusterVertxMap = U.newHashMap(4);
    /** ignite name -> exception */
    public static final Map<String, Throwable> lastErrors = U.newLinkedHashMap(4);
    /** ignite cluster id -> ignite name */
    public static final Map<String, String> clusterNameMap = U.newHashMap(4);
    
    public static final Map<String, Integer> deactivedCluster = new ConcurrentHashMap<>();

    private GremlinExecutor gremlinExecutor;
    
    private GridTaskExecutor gridTaskExecutor;
    
    private final LangflowApiClient langflowClient;
    
    /** Agent configuration. */
    protected final AgentConfiguration cfg;
    
    /**
     * @param cfg Config.
     */
    public VertxClusterHandler(AgentConfiguration cfg) {
    	this.cfg = cfg;
        gremlinExecutor = new GremlinExecutor(F.first(cfg.nodeURIs()),cfg.gremlinPort());
        gridTaskExecutor = new GridTaskExecutor();
        this.langflowClient = new LangflowApiClient();
    }

    /** {@inheritDoc} */
    @Override public RestResult restCommand(String clusterId,JsonObject params) throws Throwable {
    	String cmd = params.getString("cmd");    	
    	String code = params.getString("qry");
    	Optional<String> clusterNameOpt = getClusterName(clusterId);
    	if (clusterNameOpt.isPresent()) {
    		String clusterName = clusterNameOpt.get();
    		
    		if("text2gremlin".equals(cmd)) {
    			
    			JsonArray list = new JsonArray();
    			params.put("input_value", params.getString("text"));
    			String endpoint = params.getString("endpoint","text2gremlin");
            	ServiceResult r = langflowClient.call(endpoint,params);
            	if(r.getStatus().equals("200")) {
            		try {
    		    		JsonArray outputs = r.getResult().getJsonArray("outputs").getJsonObject(0).getJsonArray("outputs");
    		    		for(int i=0;i<outputs.size();i++) {
    			    		String sql = outputs.getJsonObject(i).getJsonObject("results").getJsonObject("message").getString("text");
    						list.add(sql);
    		    		}
            		}
            		catch(Exception e) {
            			
            		}
    			}
    			return RestResult.success(list.encode(), params.getString("sessionToken"));
    		}
    		
    		if("qrygremlinexe".equals(cmd) || "qrygroovyexe".equals(cmd)) {
    			
    			if(code.indexOf("vertx.")<0) {
            		// Gremlin Query
    				Map<String,Object> context = new HashMap<>();
    				context.put("graphName", clusterName);
            		return gremlinExecutor.sendRequest(context, clusterId, params);
        		}
        		
                if (cfg.disableVertx())
                    return RestResult.fail(404, "Vertx disabled by administrator.");
                
                
                lastErrors.clear();
                Vertx vertx = clusterVertxMap.get(clusterName);
                if (vertx==null) {
                	vertx = startVertxCluster(clusterName,cfg);                	
                }            

                try {
        			// Gremlin Query
            		Ignite ignite = Ignition.ignite(clusterName);
            		return gremlinExecutor.execRequest(ignite, vertx, clusterId, params);
        		}
        		catch(Exception e) {
        			log.error("gremlin rest call fail!",e);
        			lastErrors.put(clusterName, e);
        		}
                
                if (lastErrors.containsKey(clusterName)) {
                	return RestResult.fail(GridRestResponse.STATUS_FAILED, lastErrors.get(clusterName).getMessage());
                }
    		}
    		else {
    			IgniteEx igniteEx = (IgniteEx)Ignition.ignite(clusterName);
    			return gridTaskExecutor.execRequest(igniteEx,clusterId, params);
    		}
    		         
        }    	
        return RestResult.fail(404, "Failed to send request because of embedded node for vertx is not started yet.");
        
    }
    
    private Vertx startVertxCluster(String clusterName, AgentConfiguration cfg) {
        
    	Ignite ignite = Ignition.ignite(clusterName);
    	IgniteVertxPlugin vertxPlugin = ignite.plugin("Vertx");

        Vertx vertx = vertxPlugin.vertx();
        if(vertx!=null) {
            
            clusterVertxMap.put(clusterName, vertx); 
            clusterNameMap.put(ignite.cluster().id().toString(), clusterName);
            
        }else{
            //失败的时候做什么！
        	lastErrors.put(clusterName, new RuntimeException("Vertx cluster start failed!"));
        	
        }
        return vertx;
    }
    
    public boolean isVertxCluster(String clusterId) {
		if(clusterId!=null && clusterNameMap.containsKey(clusterId)) {
			String clusterName = clusterNameMap.get(clusterId);
			return clusterVertxMap.containsKey(clusterName);
		}
		return false;
	}
    
    public Optional<String> getClusterName(String clusterId) {
		if(clusterNameMap.containsKey(clusterId)) {
			String clusterName = clusterNameMap.get(clusterId);
			return Optional.of(clusterName);
		}
		if(RestClusterHandler.clusterNameMap.containsKey(clusterId)) {
			String clusterName = RestClusterHandler.clusterNameMap.get(clusterId);
			return Optional.of(clusterName);
		}
		return Optional.empty();
	}

    /**
     * @return Topology snapshot for demo cluster.
     */
    public List<TopologySnapshot> topologySnapshot() {
        if (cfg.disableVertx())
            return null;
        
        List<TopologySnapshot> tops = new LinkedList<>();    
        
        for (Entry<String, String> ent: clusterNameMap.entrySet()) {
        	TopologySnapshot top;
        	String clusterId = ent.getKey();
        	String clusterName = ent.getValue();
        	try {
	        	IgniteEx ignite = (IgniteEx)Ignition.ignite(clusterName);
	
	            Collection<GridClientNodeBean> nodes = collectNodes(ignite.context());
	            top = new TopologySnapshot(nodes);
	            top.setId(clusterId);
	            top.setName(VERTX_CLUSTER_NAME_PREFIX+clusterName);
	            top.setDemo(false);
	            top.setClusterVersion(VER_STR);
	            top.setActive(ignite.active());
        	}
        	catch(IgniteIllegalStateException e) {
        		top = new TopologySnapshot();            
        		
	            top.setId(clusterId);
	            top.setName(VERTX_CLUSTER_NAME_PREFIX+clusterName);
	            top.setDemo(false);
	            top.setClusterVersion(VER_STR);
	            top.setActive(false);
	            deactivedCluster.put(clusterId, 1);
	            clusterVertxMap.remove(clusterName);
	            
        	}
            tops.add(top);
        }
        
        for(String nodeId: deactivedCluster.keySet()) {        	
    		clusterNameMap.remove(nodeId);
    	}
    	deactivedCluster.clear();
    	
        return tops;
    }

    /**
     * @param ctx Context.
     */
    private Collection<GridClientNodeBean> collectNodes(GridKernalContext ctx) {
        try {
            GridTopologyCommandHandler hnd = new GridTopologyCommandHandler(ctx);

            GridRestTopologyRequest req = new GridRestTopologyRequest();

            req.command(TOPOLOGY);
            req.includeAttributes(true);

            GridRestResponse res = hnd.handleAsync(req).getUninterruptibly();

            return (Collection<GridClientNodeBean>)res.getResponse();
        }
        catch (IgniteCheckedException e) {
            return Collections.emptyList();
        }
    }
    
    public void close() {
    	for (Entry<String, Vertx> ent: clusterVertxMap.entrySet()) {
    		ent.getValue().close();
    	}
    }
}
