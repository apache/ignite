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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.GremlinExecutor;
import org.apache.ignite.console.agent.rest.GridTaskExecutor;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;


import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
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

    private GremlinExecutor gremlinExecutor;
    
    private GridTaskExecutor gridTaskExecutor;
    
    /** Agent configuration. */
    protected final AgentConfiguration cfg;
    
    private String lastClusterId = "";
    
    /**
     * @param cfg Config.
     */
    public VertxClusterHandler(AgentConfiguration cfg) {
    	this.cfg = cfg;
        gremlinExecutor = new GremlinExecutor(cfg.gremlinPort());
        gridTaskExecutor = new GridTaskExecutor();
    }

    /** {@inheritDoc} */
    @Override public RestResult restCommand(String clusterId,JsonObject params) throws Throwable {
    	String cmd = params.getString("cmd");    	
    	String code = params.getString("qry");
    	Optional<String> clusterNameOpt = getClusterName(clusterId);
    	if (clusterNameOpt.isPresent()) {
    		String clusterName = clusterNameOpt.get();
    		
    		if("qrygremlinexe".equals(cmd) || "qrygroovyexe".equals(cmd)) {
    			
    			if(code.indexOf("vertx.")<0) {
            		// Gremlin Query
        			Ignite ignite = Ignition.ignite(clusterName);        		
            		return gremlinExecutor.sendRequest(ignite, clusterId, params);
        		}
        		
                if (cfg.disableVertx())
                    return RestResult.fail(404, "Vertx disabled by administrator.");
                
                
                if (!clusterVertxMap.containsKey(clusterName)) {
                	startVertxCluster(clusterName,cfg);
                	if(!lastErrors.containsKey(clusterName) && !clusterVertxMap.containsKey(clusterName)) {
                		Thread.sleep(200);
                	}
                }            

                if (clusterVertxMap.containsKey(clusterName)) {
                	try {
            			// Gremlin Query
                		Ignite ignite = Ignition.ignite(clusterName);      		
                		Vertx vertx = clusterVertxMap.get(clusterName);
                		return gremlinExecutor.execRequest(ignite, vertx, clusterId, params);
            		}
            		catch(Exception e) {
            			log.error("Vertx cluster rest call fail!",e);        			
            		}
                }
                
                if (lastErrors.containsKey(clusterName)) {
                	return RestResult.fail(500, lastErrors.get(clusterName).getMessage());
                }
    		}
    		else {
    			IgniteEx igniteEx = (IgniteEx)Ignition.ignite(clusterName);
    			return gridTaskExecutor.execRequest(igniteEx,clusterId, params);
    		}
    		         
        }    	
        return RestResult.fail(404, "Failed to send request because of embedded node for vertx is not started yet.");
        
    }
    
    void startVertxCluster(String clusterName, AgentConfiguration cfg) {
        
    	Ignite ignite = Ignition.ignite(clusterName);
    	IgniteClusterManager clusterManager = new IgniteClusterManager(ignite);
        
        VertxOptions options = new VertxOptions();
        options.setClusterManager(clusterManager);
        
        Vertx.clusteredVertx(options,res->{
            if(res.succeeded()) {
                Vertx vertx = res.result();
                clusterVertxMap.put(clusterName, vertx); 
                clusterNameMap.put(ignite.cluster().id().toString(), clusterName);
                
            }else{
                //失败的时候做什么！
            	lastErrors.put(clusterName, res.cause());
            	log.error("Failt to start Vertx cluster.",res.cause());
            }
        });

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
        
        for (Entry<String, Vertx> ent: clusterVertxMap.entrySet()) {
        	TopologySnapshot top;
        	String clusterName = ent.getKey();
        	try {
	        	IgniteEx ignite = (IgniteEx)Ignition.ignite(clusterName);
	
	            Collection<GridClientNodeBean> nodes = collectNodes(ignite.context());
	            top = new TopologySnapshot(nodes);            
	            lastClusterId = ignite.cluster().id().toString();
	            top.setId(lastClusterId);
	            top.setName(VERTX_CLUSTER_NAME_PREFIX+clusterName);
	            top.setDemo(false);
	            top.setClusterVersion(VER_STR);
	            top.setActive(ignite.active());
        	}
        	catch(Exception e) {
        		top = new TopologySnapshot();            
        		
	            top.setId(lastClusterId);
	            top.setName(VERTX_CLUSTER_NAME_PREFIX+clusterName);
	            top.setDemo(false);
	            top.setClusterVersion(VER_STR);
	            top.setActive(false);
        	}
            tops.add(top);
        }
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
