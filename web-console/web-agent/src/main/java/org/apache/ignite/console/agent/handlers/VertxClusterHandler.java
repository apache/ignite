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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentConfiguration;
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
public class VertxClusterHandler extends AbstractClusterHandler{
	/** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(VertxClusterHandler.class));
	
    /** Vertx cluster ID. */
    public static final String VERTX_CLUSTER_ID = UUID.randomUUID().toString();

    /** Vertx cluster name. */
    public static final String VERTX_CLUSTER_NAME = "vertx-cluster";
    
    public static final Map<String, Vertx> clusterVertxMap = U.newHashMap(2);

    
    public static final Map<String, String> clusterNameMap = RestClusterHandler.clusterNameMap;

    /**
     * @param cfg Config.
     */
    VertxClusterHandler(AgentConfiguration cfg) {
        super(cfg, null);
    }

    /** {@inheritDoc} */
    @Override public RestResult restCommand(String clusterId,JsonObject params) throws Throwable {
        if (clusterNameMap.containsKey(clusterId)) {
            if (cfg.disableVertx())
                return RestResult.fail(404, "Vertx disabled by administrator.");

            String clusterName = clusterNameMap.get(clusterId);
            
            if (!clusterVertxMap.containsKey(clusterName)) {
            	startVertxCluster(clusterName,cfg);
            	Thread.sleep(400);
            }
            

            if (clusterVertxMap.containsKey(clusterName)) {
            	List<String> nodeURIs = this.cfg.nodeURIs();
            	for(String url: nodeURIs) {
            		try {
            			return restExecutor.sendRequest(url, params);
            		}
            		catch(Exception e) {
            			log.error("Vertx cluster rest call fail!",e);
            		}
            	}
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
                
            }else{
                //失败的时候做什么！
            	log.error("Failt to start Vertx cluster.",res.cause());
            }
        });

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
        	IgniteEx ignite = (IgniteEx)Ignition.ignite(clusterName);

            Collection<GridClientNodeBean> nodes = collectNodes(ignite.context());
            top = new TopologySnapshot(nodes);            

            top.setId(VERTX_CLUSTER_ID.toString());
            top.setName("Vertx-"+VERTX_CLUSTER_NAME);
            top.setDemo(false);
            top.setClusterVersion(VER_STR);
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
}
