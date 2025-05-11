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

import static org.apache.ignite.console.agent.AgentUtils.nid8;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientResponse.STATUS_FAILED;
import static org.apache.ignite.lang.IgniteProductVersion.fromString;

import java.net.ConnectException;
import java.net.http.HttpResponse;
import java.nio.channels.AsynchronousCloseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLException;

import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.util.StringUtil;

import com.fasterxml.jackson.core.type.TypeReference;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * API to transfer topology from Ignite cluster to Web Console.
 */
public class RestClusterHandler extends AbstractClusterHandler {


    /** Index of alive node URI. */
    private final Map<List<String>, Integer> startIdxs = U.newHashMap(2);
    
    /** Map of clusterId ->  node URI. */
    public static final Map<String, List<String>> clusterUrlMap = U.newHashMap(2);
    /** Map of cluster id -> cluster name */
    public static final Map<String, String> clusterNameMap = U.newHashMap(2);
    
    public static final Map<String, Integer> deactivedCluster = new ConcurrentHashMap<>();    
    /** */
    private static final String EXPIRED_SES_ERROR_MSG = "Failed to handle request - unknown session token (maybe expired session)";


    /** Session token. */
    private String sesTok;

    /** Latest topology snapshot. */
    private TopologySnapshot latestTop;
    
    private String serverUri;

    /**
     * @param cfg Web agent configuration.
     */
    public RestClusterHandler(AgentConfiguration cfg) {
        super(cfg, createNodeSslFactory(cfg));
        clusterUrlMap.put("",cfg.nodeURIs());
        serverUri = cfg.serverUri();
    }
    
    public static void registerNodeUrl(String clusterId,String url,String clusterName) {
    	List<String> urls = clusterUrlMap.get(clusterId);
    	if(urls==null) {
    		urls = new ArrayList<>(1);
    		urls.add(url);
    		clusterUrlMap.put(clusterId, urls);
    		clusterNameMap.put(clusterId, clusterName);
    	}
    	else if(!urls.contains(url)){    		
    		urls.add(url);
    	}
    }

    /** {@inheritDoc} */
    @Override public RestResult restCommand(String clusterId,JsonObject params) throws Throwable {
        List<String> nodeURIs = clusterUrlMap.get(clusterId);
        if(nodeURIs==null) {
        	nodeURIs = this.cfg.nodeURIs();
        }

        Integer startIdx = startIdxs.getOrDefault(nodeURIs, 0);

        int urlsCnt = nodeURIs.size();       
        
        String cmd = params.getString("cmd");
        String token = (String)params.remove("token");
        for (int i = 0;  i < urlsCnt; i++) {
            int currIdx = (startIdx + i) % urlsCnt;

            String nodeUrl = nodeURIs.get(currIdx);

            try {
                RestResult res = restExecutor.sendRequest(nodeUrl, params);

                // If first attempt failed then throttling should be cleared.
                if (i > 0 || !startIdxs.containsKey(nodeURIs))
                    log.info("Connected to node [url=" + nodeUrl + "]");

                startIdxs.put(nodeURIs, currIdx);
                
                if(res.getStatus()==0 && "metadata".equals(cmd)) {
                	try {
                		String cacheName = params.getString("cacheName");
                		
                		JsonObject models = restExecutor.getCachedMetadata(serverUri,clusterId,token);
	                	
	                	JsonArray response = new JsonArray(res.getData());
	                	for(int n=0;n<response.size();n++) {
	                		JsonObject meta = response.getJsonObject(i);
	                		JsonObject comments = new JsonObject();
	                		
	                		JsonObject fields = meta.getJsonObject("fields");
	                		for(String type: fields.fieldNames()) {
	                			JsonObject typeConfig = models.getJsonObject(type);
	                			if(typeConfig!=null) {
	                				JsonArray metaFields = typeConfig.getJsonArray("fields");
	                				JsonObject typeComment = new JsonObject();
	                				for(int j=0;j<metaFields.size();j++) {
	                					String comment = metaFields.getJsonObject(j).getString("comment");
	                					String name = metaFields.getJsonObject(j).getString("name");
	                					typeComment.put(name.toLowerCase(), comment);
	                				}
	                				comments.put(type, typeConfig.getString("tableComment"));
	                				JsonObject queryFields = fields.getJsonObject(type);
	                				for(String fieldName: queryFields.fieldNames()) {
	                					String aClass = queryFields.getString(fieldName);
	                					String comment = typeComment.getString(fieldName.toLowerCase());
	                					queryFields.put(fieldName, !StringUtil.isBlank(comment)? aClass+" //"+comment: aClass);   
	                				}
	                			}
	                		}
	                		
	                		meta.put("comments", comments);
	                	}
	                	
	                	return RestResult.success(response.encodePrettily(), res.getSessionToken());
                	}catch(Exception e) {
                		LT.error(log, e, "Failed execute meta request on server [url=" + serverUri + ", parameters=" + params + "]");
                	}               	
                	
                }

                return res;
            }
            catch (InterruptedException | TimeoutException | HttpResponseException | AsynchronousCloseException ignored) {
                // No-op.
            	throw new TimeoutException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]"+ " Cause by "+ignored.getMessage());
            }
            catch (ConnectException ignored) {
                // No-op.            	
            }
            catch (Throwable e) {
                LT.error(log, e, "Failed execute request on node [url=" + nodeUrl + ", parameters=" + params + "]");

                if (e instanceof SSLException) {
                    LT.warn(log, "Check that connection to cluster node configured correctly.");
                    LT.warn(log, "Options to check: --node-uri, --node-key-store, --node-key-store-password, --node-trust-store, --node-trust-store-password.");
                }
            }
        }

        LT.warn(log, "Failed to connect to cluster.");
        LT.warn(log, "Check that '--node-uri' configured correctly.");
        LT.warn(log, "Ensure that cluster nodes have [ignite-rest-http] module in classpath " +
            "(was copied from libs/optional to libs folder).");

        throw new ConnectException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]");
    }
    
    /**
     * Execute REST command under agent user.
     *
     * @param params Command params.
     * @return Command result.
     * @throws Exception If failed to execute.
     */
    public RestResult authRestCommand(String clusterId, JsonObject params) throws Throwable {
        if (!F.isEmpty(sesTok))
            params.put("sessionToken", sesTok);
        else if (!F.isEmpty(cfg.nodeLogin()) && !F.isEmpty(cfg.nodePassword())) {
            params.put("user", cfg.nodeLogin());
            params.put("password", cfg.nodePassword());
        }

        RestResult res = restCommand(clusterId,params);

        switch (res.getStatus()) {
            case STATUS_SUCCESS:
                sesTok = res.getSessionToken();

                return res;

            case STATUS_FAILED:
                if (res.getError().startsWith(EXPIRED_SES_ERROR_MSG)) {
                    sesTok = null;

                    params.remove("sessionToken");

                    return authRestCommand(clusterId, params);
                }

            default:
                return res;
        }
    }


    /**
     * @param ver Cluster version.
     * @param nid Node ID.
     * @return Cluster active state.
     * @throws Exception If failed to collect cluster active state.
     */
    private boolean active(IgniteProductVersion ver, String clusterId, UUID nid) throws Throwable {
        // 1.x clusters are always active.
        if (ver.compareTo(ClustersWatcher.IGNITE_2_0) < 0)
            return true;

        JsonObject params = new JsonObject();

        boolean v23 = ver.compareTo(ClustersWatcher.IGNITE_2_3) >= 0;

        if (v23)
            params.put("cmd", "currentState");
        else {
            params.put("cmd", "top");           
            params.put("id", nid);
            params.put("attr", false);
            params.put("mtr", false);
            params.put("caches", false);
        }

        RestResult res = restCommand(clusterId,params);

        if (res.getStatus() == STATUS_SUCCESS)
            return v23 ? Boolean.valueOf(res.getData()) : res.getData().contains("\"nodeId\"");

        return false;
    }

    /**
     * Callback on disconnect from cluster.
     */
    private void onFailedClusterRequest(Throwable e) {
        String msg = latestTop == null ? "Failed to establish connection to node" : "Connection to cluster was lost";

        boolean failed = X.hasCause(e, IllegalStateException.class);

        if (X.hasCause(e, ConnectException.class))
            LT.info(log, msg);
        else if (failed && "Failed to handle request - session token not found or invalid".equals(e.getMessage()))
            LT.error(log, null, "Failed to establish connection to secured cluster - missing credentials. Please pass '--node-login' and '--node-password' options");
        else if (failed && e.getMessage().startsWith("Failed to authenticate remote client (invalid credentials?):"))
            LT.error(log, null, "Failed to establish connection to secured cluster - invalid credentials. Please check '--node-login' and '--node-password' options");
        else if (failed)
            LT.error(log, null, msg + ". " + e.getMessage());
        else
            LT.error(log, e, msg);
    }

    /**
     * Collect topology.
     *
     * @return REST result.
     * @throws Exception If failed to collect cluster topology.
     */
    private RestResult topology(String nid) throws Throwable {
        JsonObject params = new JsonObject()
            .put("cmd", "top")
            .put("attr", true)
            .put("mtr", false)
            .put("caches", false);

        return authRestCommand(nid,params);
    }
    
    public List<TopologySnapshot> topologySnapshot() {
    	List<TopologySnapshot> tops = new LinkedList<>(); 
    	
    	for(String cluster: clusterUrlMap.keySet()) {
            try {
            	
                RestResult res = topology(cluster);

                if (res.getStatus() != STATUS_SUCCESS)
                    throw new IllegalStateException(res.getError());

                List<GridClientNodeBean> nodes = fromJson(
                    res.getData(),
                    new TypeReference<List<GridClientNodeBean>>() {}
                );

                TopologySnapshot newTop = new TopologySnapshot(nodes);

                if (!newTop.sameNodes(latestTop)) {
                	if(log.isDebugEnabled())
                		log.info("Connection successfully established to cluster with nodes: " + nid8(newTop.nids()));
                }
                else if (!Objects.equals(latestTop.nids(), newTop.nids())) {
                    log.info("Cluster topology changed, new topology: " + nid8(newTop.nids()));
                }
                
                if(cluster.isEmpty()) { // external node
                	cluster = F.first(nodes).getNodeId().toString();
                	String clusterName = F.first(nodes).getConsistentId().toString();
                	if(!clusterNameMap.containsKey(cluster)) {
                		clusterNameMap.put(cluster, clusterName);
                	}
                }

                boolean active = active(fromString(newTop.getClusterVersion()), cluster, F.first(newTop.nids()));
                newTop.setId(cluster);
                newTop.setDemo(false);
                newTop.setActive(active);
                newTop.setSecured(!F.isEmpty(res.getSessionToken()));
                newTop.setName(clusterNameMap.getOrDefault(cluster, cluster));

                latestTop = newTop;

                tops.add(newTop);	
                
            }
            catch (TimeoutException | IllegalStateException e) {
                onFailedClusterRequest(e);
                latestTop = null;
            }
            catch (Throwable e) {
                onFailedClusterRequest(e);
                
                TopologySnapshot dieTop = new TopologySnapshot();
                dieTop.setId(cluster);
                dieTop.setActive(false);
                dieTop.setName(RestClusterHandler.clusterNameMap.getOrDefault(cluster, cluster));
                tops.add(dieTop);
                
                deactivedCluster.put(cluster,1);
                latestTop = null;
            }
        }
    	
    	for(String clusterId: deactivedCluster.keySet()) {
    		clusterUrlMap.remove(clusterId);
    		clusterNameMap.remove(clusterId);
    	}
    	deactivedCluster.clear();
    	return tops;
    }
}
