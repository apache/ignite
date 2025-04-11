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


import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.configureProxy;
import static org.apache.ignite.console.agent.AgentUtils.entriesToMap;
import static org.apache.ignite.console.agent.AgentUtils.entry;
import static org.apache.ignite.console.agent.AgentUtils.secured;
import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;
import static org.apache.ignite.console.agent.handlers.DemoClusterHandler.DEMO_CLUSTER_ID;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.websocket.AgentHandshakeRequest.CURRENT_VER;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_CALL_CLUSTER_COMMAND;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_CALL_CLUSTER_SERVICE;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_START_CLUSTER;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_STOP_CLUSTER;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.AgentUtils;
import org.apache.ignite.console.agent.IgniteClusterLauncher;
import org.apache.ignite.console.agent.db.DataSourceManager;
import org.apache.ignite.console.agent.rest.GremlinExecutor;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestRequest;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.agent.service.CacheAgentService;
import org.apache.ignite.console.agent.service.ClusterAgentService;
import org.apache.ignite.console.agent.service.ClusterAgentServiceManager;
import org.apache.ignite.console.agent.service.ClusterAgentServiceUtil;
import org.apache.ignite.console.agent.service.ServiceResult;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.jackson.IgniteObjectMapper;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.services.Service;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketFrame;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.api.extensions.Frame;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
@WebSocket(maxTextMessageSize = 8 * 1024 * 1024, maxBinaryMessageSize = 8 * 1024 * 1024)
public class WebSocketRouter implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketRouter.class));

    /** Pong message. */
    private static final ByteBuffer PONG_MSG = UTF_8.encode("PONG");

    /** Default error messages. */
    private static final Map<String, String> ERROR_MSGS = Collections.unmodifiableMap(Stream.of(
        entry(SCHEMA_IMPORT_DRIVERS, "Failed to collect list of JDBC drivers"),
        entry(SCHEMA_IMPORT_SCHEMAS, "Failed to collect database schemas"),
        entry(SCHEMA_IMPORT_METADATA, "Failed to collect database metadata"),
        entry(NODE_REST, "Failed to handle REST request"),
        entry(NODE_VISOR, "Failed to handle Visor task request")).
        collect(entriesToMap()));

    /** Close agent latch. */
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    /** Agent configuration. */
    private final AgentConfiguration cfg;

    /** Websocket Client. */
    private WebSocketClient client;

	/** Schema import handler. */
    private final DatabaseHandler dbHnd;

    /** Cluster handler. */
    private final RestClusterHandler clusterHnd;

    /** Demo cluster handler. */
    private final DemoClusterHandler demoClusterHnd;
    
    /** Vertx cluster handler. */
    private final VertxClusterHandler vertxClusterHnd;

    /** Cluster watcher. */
    private final ClustersWatcher watcher;

    /** Reconnect count. */
    private AtomicInteger reconnectCnt = new AtomicInteger();

    /** Active tokens after handshake. */
    private Collection<String> validTokens;     
    
    private HttpClient httpClient;
    
    private static Map<String,Service> runningServices = new ConcurrentHashMap<>();
    
    /**
     * @param cfg Configuration.
     */
    public WebSocketRouter(AgentConfiguration cfg) {
        this.cfg = cfg;

        dbHnd = new DatabaseHandler(cfg);
        clusterHnd = new RestClusterHandler(cfg);
        demoClusterHnd = new DemoClusterHandler(cfg);
        vertxClusterHnd = new VertxClusterHandler(cfg);

        watcher = new ClustersWatcher(cfg, clusterHnd,demoClusterHnd,dbHnd,vertxClusterHnd);
        
        // createServerSslFactory(cfg)
        httpClient = new HttpClient();
        httpClient.setMaxConnectionsPerDestination(2);
        httpClient.setConnectBlocking(false);
        httpClient.setConnectTimeout(1000);
        httpClient.setFollowRedirects(false);
        
        
        // TODO GG-18379 Investigate how to establish native websocket connection with proxy.
        configureProxy(httpClient, cfg.serverUri());

        try {
			httpClient.start();
		} catch (Exception e) {
			log.error("Failed to start http client: ", e);
		}
    }

    /**
     * @param cfg Config.
     */
    private static SslContextFactory createServerSslFactory(AgentConfiguration cfg) {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.serverTrustStore())) {
            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to Web server.");

            trustAll = false;
        }

        return sslContextFactory(
            cfg.serverKeyStore(),
            cfg.serverKeyStorePassword(),
            trustAll,
            cfg.serverTrustStore(),
            cfg.serverTrustStorePassword(),
            cfg.cipherSuites()
        );
    }

    /**
     * Start websocket client.
     */
    public void start() {
        log.info("Starting Web Console Agent...");

        Runtime.getRuntime().addShutdownHook(new Thread(closeLatch::countDown));
        
        connect();
    }

    /**
     * Stop websocket client.
     */
    private void stopClient() {
        LT.clear();        

        if (client != null) {
            try {
                client.stop();
                client.destroy();
                client = null;
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.info("Stopping Web Console Agent...");
        watcher.stop();
        vertxClusterHnd.close();
        dbHnd.close();
        demoClusterHnd.close();
        clusterHnd.close();

        stopClient();
        
        httpClient.destroy();
        
        watcher.close();
    }

    /**
     * Connect to backend.
     */
    private void connect0() {
        boolean connecting = reconnectCnt.getAndIncrement() == 0;

        try {
            stopClient();

            if (!isRunning())
                return;

            if (connecting)
                log.info("Connecting to server: " + cfg.serverUri());

            Thread.sleep((reconnectCnt.get() - 1) * 1000);

            if (!isRunning())
                return;

            
            client = new WebSocketClient(httpClient); 
                     
            
            client.start();
            Session session = client.connect(this, URI.create(cfg.serverUri()).resolve(AGENTS_PATH)).get(5L, TimeUnit.SECONDS);         
                        
            session.setIdleTimeout(3600*1000);
            
            reconnectCnt.set(0);
            
        }
        catch (InterruptedException e) {
            closeLatch.countDown();
        }
        catch (TimeoutException | ExecutionException | CancellationException ignored) {
            // No-op.
        	log.error("Failed to establish websocket connection with server: " + cfg.serverUri(), ignored);
        }
        catch (Exception e) {
            if (connecting)
                log.error("Failed to establish websocket connection with server: " + cfg.serverUri(), e);
            closeLatch.countDown();
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        //-connectorPool.submit(this::connect0);
        this.connect0();
    }

    /**
     * @throws InterruptedException If await failed.
     */
    public void awaitClose() throws InterruptedException {
        closeLatch.await();

        AgentClusterDemo.stop();
    }

    /**
     * @return {@code true} If web agent is running.
     */
    private boolean isRunning() {
        return closeLatch.getCount() > 0;
    }

    /**
     * @param ses Session.
     */
    @OnWebSocketConnect
    public void onConnect(Session ses) {
        AgentHandshakeRequest req = new AgentHandshakeRequest(CURRENT_VER, cfg.tokens());

        try {
            AgentUtils.send(ses, new WebSocketResponse(AGENT_HANDSHAKE, req), 10L, TimeUnit.SECONDS);
        }
        catch (Throwable e) {
            log.error("Failed to send handshake to server", e);

            connect();
        }
    }

    /**
     * @param res Response from server.
     */
    private void processHandshakeResponse(AgentHandshakeResponse res) {
        if (F.isEmpty(res.getError())) {
            validTokens = res.getTokens();

            List<String> missedTokens = new ArrayList<>(cfg.tokens());

            missedTokens.removeAll(validTokens);

            if (!F.isEmpty(missedTokens)) {
                log.warning("Failed to validate token(s): " + secured(missedTokens) + "." +
                    " Please reload agent archive or check settings");
            }

            if (F.isEmpty(validTokens)) {
                log.warning("Valid tokens not found. Stopping agent...");

                closeLatch.countDown();
            }
            else {
            	DataSourceManager.init(getHttpClient(),cfg.serverUri(),validTokens);
            }

            log.info("Successfully completes handshake with server");
        }
        else {
            log.error(res.getError() + " Please reload agent or check settings");

            closeLatch.countDown();
        }
    }

    /**
     * @param tok Token to revoke.
     */
    private void processRevokeToken(String tok) {
        log.warning("Security token has been revoked: " + tok);

        validTokens.remove(tok);

        if (F.isEmpty(validTokens)) {
            log.warning("Web Console Agent will be stopped because no more valid tokens available");

            closeLatch.countDown();
        }
    }
    

    /**
     * @param tok Token to revoke.
     */
    private Ignite getIgnite(JsonObject json,JsonObject stat) {                
        String clusterId = json.getString("id");        
        String clusterName = json.getString("name");        
        Ignite ignite = null;
        stat.put("status", "stoped");
        if(clusterName!=null) {
        	clusterName = Utils.escapeFileName(clusterName);
    	}
        else {        	
        	String gridName = RestClusterHandler.clusterNameMap.get(clusterId);
    		if(gridName!=null) {
    			try {
            		ignite = Ignition.ignite(gridName);	    		
    	    		stat.put("status", "started");
    	    		clusterName = null;
        		}
    	    	catch(IgniteIllegalStateException e) {	
    	    		stat.put("message", e.getMessage());
    	    		stat.put("status", "stoped");
    	    	}
    		}        
        }
        
        if(ignite==null && clusterName!=null) {
        	try {
        		ignite = Ignition.ignite(clusterName);	    		
	    		stat.put("status", "started");
    		}
	    	catch(IgniteIllegalStateException e) {	
	    		stat.put("message", e.getMessage());
	    		stat.put("status", "stoped");	    		
	    	}
    	}
        else {
        	stat.put("message", "ignite cluster not found!");
        }
        return ignite;
    }

    /**
     * @param tok Token to revoke.
     */
    private JsonObject processClusterStart(WebSocketRequest evt) {
        log.info("Cluster start msg has been revoked: " + evt.getNodeSeq());
        JsonObject stat = new JsonObject();
        JsonObject json = fromJson(evt.getPayload());
        
        boolean isLastNode = evt.isLastNode();       
        String clusterId = json.getString("id");
        if(clusterId.equals(DEMO_CLUSTER_ID) || AgentClusterDemo.SRV_NODE_NAME.equals(clusterId)) {
        	json.put("demo", true);
        }
        String clusterName = Utils.escapeFileName(json.getString("name"));
		try {			

			String unzipDest = IgniteClusterLauncher.saveBlobToFile(json);
			
			Boolean restart = json.getBoolean("restart",false);
			if(restart) {			
	        	IgniteClusterLauncher.stopIgnite(clusterName,clusterId);
			}
			else if(unzipDest!=null) {
				stat.put("message", "Deploy server.xml to "+unzipDest);
				stat.put("status", "stoped");
				return stat;
			}
			
			Ignite ignite = null;
			// 不是演示环境
			if(!json.getBoolean("demo",false)) {
				// 如果包含hosts，ports则远程启动节点。
				
				File startIniFile = new File(U.getIgniteHome()+ "/config/clusters/"+clusterName+"-start-nodes.ini");				
				
				File configFile = new File(U.getIgniteHome()+ "/config/clusters/"+clusterName+"-config.xml");
				
				if(startIniFile.exists()) {
					if(isLastNode) {
						try {						
			        		ignite = Ignition.allGrids().get(0);
			        		ignite.cluster().startNodes(startIniFile, restart, 60*1000, 1);
				    		stat.put("status", "started");
			    		}
				    	catch(IgniteIllegalStateException e) {	
				    		stat.put("message", e.getMessage());
				    		stat.put("status", "stoped");
				    		return stat;
				    	}
					}
				}
				else {
				
					String work = U.workDirectory(null, null);			
		        	String cfgFile = String.format("%s/config/%s/src/main/resources/META-INF/%s-server.xml", work, clusterName,clusterName);
		        	File configWorkFile = new File(cfgFile);
		        	
		        	if(configWorkFile.exists() && configFile.exists()) {
		        		// 合并并启动一个服务端已经配置好的node
		        		ignite = IgniteClusterLauncher.trySingleStart(clusterId,clusterName,cfg.serverId(),isLastNode,cfgFile,configFile.toString());
		        	}					
		        	else if(configWorkFile.exists()) {
		        		// 启动一个独立的node，jvm内部的node之间相互隔离
		        		ignite = IgniteClusterLauncher.trySingleStart(clusterId,clusterName,cfg.serverId(),isLastNode,cfgFile);
		        	}		        	
		        	else if(configFile.exists()) {
		        		// 启动一个服务端已经配置好的node
		        		ignite = IgniteClusterLauncher.trySingleStart(clusterId,clusterName,cfg.serverId(),isLastNode,null,configFile.toString());
		        	}
		        	else {		        		
		        		stat.put("message", "not found cfg file, please deploy cfg first!");		        		
		        	}
					
					if(ignite!=null) {
						IgniteClusterLauncher.registerNodeUrl(ignite,clusterId);
						
			        	stat.put("status", "started");
			        	stat.put("message","Ignite started successfully.");
					}
					else {
						stat.put("status", "stoped");
						return stat;
					}
				}
				
			}
			else {
				// 启动一个内存型的node，有多少个Agent就有多少个Demo Node
				String work = U.workDirectory(null, null);
				String cfgFile = String.format("%s/config/%s/src/main/resources/META-INF/%s-server.xml", work, clusterName,clusterName);
	        	File configWorkFile = new File(cfgFile);
	        	
				IgniteConfiguration icfg = new IgniteConfiguration();
				if(configWorkFile.exists()) {
					IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap=null;
			    	if(ignite==null) {
						URL springPreCfgUrl = U.resolveSpringUrl(configWorkFile.toString());			
						cfgMap = IgnitionEx.loadConfigurations(springPreCfgUrl);
						
						Collection<IgniteConfiguration> cfgList = cfgMap.get1();
						for(IgniteConfiguration cfg0: cfgList) {
							if(clusterName.equals(cfg0.getIgniteInstanceName())){
								icfg = cfg0;
							}				
						}
					}
				}
				
				// 启动Demo节点，并且在最后一个节点部署服务
	        	ignite = AgentClusterDemo.tryStart(icfg,cfg.serverId(),isLastNode);
	        	if(ignite!=null) {
	        		stat.put("status", "started");
	        	}
	        	else {
	        		stat.put("message","Demo Ignite already started.");
	        	}
	        	return stat;
			}			
			
			
		} catch (Exception e) {			
			e.printStackTrace();
			stat.put("message", e.getMessage());
			stat.put("status", "stoped");
		}
        
        return stat;
    }
    
    /**
     * @param tok Token to revoke.
     */
    private JsonObject processClusterStop(WebSocketRequest evt) {
        log.info("Cluster stop msg has been revoked: " + evt.getPayload());
        JsonObject stat = new JsonObject();
        JsonObject json = fromJson(evt.getPayload());
        boolean isLastNode = evt.isLastNode();
        String clusterName = Utils.escapeFileName(json.getString("name"));
        if(json.getBoolean("demo",false)) {
        	AgentClusterDemo.stop();
    	}
        else {
        	String id = json.getString("id");
        	
        	File startIniFile = new File(U.getIgniteHome()+ "/config/clusters/"+clusterName+"-start-nodes.ini");
			
			if(isLastNode && startIniFile.exists()) {
				try {						
	        		Ignite ignite = Ignition.ignite(clusterName);	        		
	        		ignite.cluster().stopNodes();
	    		}
		    	catch(IgniteIllegalStateException e) {	
		    		stat.put("message", e.getMessage());
		    		stat.put("status", "stoped");
		    		return stat;
		    	}				
			}
			
        	IgniteClusterLauncher.stopIgnite(clusterName,id);
        }
        stat.put("status", "stoped");
        return stat;
    }
    
    /**
     * @param tok Token to revoke.
     */
    private JsonObject processCallClusterService(String msg) {
        log.info("Cluster status msg has been revoked: " + msg);
        JsonObject stat = new JsonObject();
        JsonObject json = fromJson(msg);
        String cluterId = json.getString("id");
        String serviceName = json.getString("serviceName","");        
        
        if(serviceName.equals("datasourceTest")) {
        	JsonObject args = json.getJsonObject("args");
        	try (Connection conn = dbHnd.connect(args)) {
                String catalog = conn.getCatalog();

                log.info("Collected database catalog:" + catalog);            
                stat.put("message","Connect success!"+(catalog==null?"":" Catalog:"+catalog));
                stat.put("status", "connected");
                return stat;
            }
        	catch(SQLException e) {	
	    		stat.put("message", e.getMessage());
	    		stat.put("status", "fail");
	    		return stat;
	    	}
        	
        }
        if(serviceName.equals("datasourceDisconnect")) {        	
        	boolean rv = dbHnd.getDatabaseListener().deactivedCluster(cluterId);
        	if(!rv) {
        		stat.put("message", "Try again to remove from List.");
        	}
        	else {
        		stat.put("message", "Remove this connection from List.");
        	}
    		stat.put("status", rv);
    		return stat;
        }
        
        Ignite ignite = getIgnite(json,stat);
        if(ignite!=null && !serviceName.isEmpty()) {
        	JsonObject args = json.getJsonObject("args");
        	if(args==null) {
        		args = new JsonObject();
        	}
        	Service serviceObject = null;
        	String serviceType = json.getString("serviceType","ClusterAgentService");
        	
        	if(serviceName.equals("serviceList")) {
        		ClusterAgentServiceManager serviceList = new ClusterAgentServiceManager(ignite);
        		ServiceResult result = serviceList.serviceList(args.getMap());
        		return result.toJson();   		
        	}
        	if(serviceName.equals("redeployService")) {
        		ClusterAgentServiceManager serviceList = new ClusterAgentServiceManager(ignite);
        		ServiceResult result = serviceList.redeployService(args.getMap());
        		return result.toJson();   		
        	}
        	if(serviceName.equals("undeployService")) {
        		ClusterAgentServiceManager serviceList = new ClusterAgentServiceManager(ignite);
        		ServiceResult result = serviceList.undeployService(args.getMap());
        		return result.toJson();     		
        	}
        	if(serviceName.equals("cancelService")) {
        		ClusterAgentServiceManager serviceList = new ClusterAgentServiceManager(ignite);
        		ServiceResult result = serviceList.cancelService(args.getMap());
        		return result.toJson();    		
        	}
        	
        	try {
        		
        		if(runningServices.containsKey(serviceName)) {
        			stat.put("message", String.format("%s already running!", serviceName));
        			return stat;
        		}
        		if(serviceType.equals("CacheAgentService")) {
            		serviceObject = ignite.services().serviceProxy(serviceName,CacheAgentService.class,true);
            		if(serviceObject==null) {
        				stat.put("message", "service not found!");
        				return stat;
        			}
            		
            		CacheAgentService agentSeervice = (CacheAgentService)(serviceObject);
            		runningServices.put(serviceName,agentSeervice);            		
            		ServiceResult result = agentSeervice.call(args.getMap());
            		runningServices.remove(serviceName);            		
            		return result.toJson();
            		
            	}
            	else {
            		serviceObject = ignite.services().serviceProxy(serviceName,ClusterAgentService.class,true);
            		if(serviceObject==null) {
        				stat.put("message", "service not found!");
        				return stat;
        			}
            		
            		ClusterAgentService agentSeervice = (ClusterAgentService)(serviceObject);
            		runningServices.put(serviceName,agentSeervice);
            		ServiceResult result = agentSeervice.call(cluterId,args.getMap());
            		runningServices.remove(serviceName);
            		return result.toJson();
            	}	        	
	        }
			catch(Exception e) {
				stat.put("message", e.getMessage());
				stat.put("errorType", e.getClass().getSimpleName());
				runningServices.remove(serviceName);
			}
        }
        return stat;
    }

    
    /**
     * @param tok Token to revoke.
     */
    private JsonObject processCallClusterCommand(String msg) {
        log.info("Cluster cmd has been revoked: " + msg);        
        JsonObject json = fromJson(msg);
        
        String cmdName = json.getString("cmdName","");
        JsonObject state = new JsonObject();
        Ignite ignite = getIgnite(json,state);
        if(ignite!=null) {
        	state = IgniteClusterLauncher.callClusterCommand(ignite,cmdName,json);
        }
        return state;
    }

    /**
     * @param msg Message.
     */
    @OnWebSocketMessage
    public void onMessage(Session ses, String msg) {
        WebSocketRequest evt = null;
        JsonObject msgRet;
        try {
            evt = fromJson(msg, WebSocketRequest.class);

            switch (evt.getEventType()) {
                case AGENT_HANDSHAKE:
                    AgentHandshakeResponse req0 = fromJson(evt.getPayload(), AgentHandshakeResponse.class);

                    processHandshakeResponse(req0);

                    if (closeLatch.getCount() > 0)
                        watcher.startWatchTask(ses);

                    break;

                case AGENT_REVOKE_TOKEN:
                    processRevokeToken(evt.getPayload());

                    return;
                    
                case AGENT_START_CLUSTER:
                	msgRet = processClusterStart(evt);
                	send(ses, evt.response(msgRet));
                	break;
                	
                case AGENT_STOP_CLUSTER:
                	msgRet = processClusterStop(evt);
                	send(ses, evt.response(msgRet));
                	break;
                	
                case AGENT_CALL_CLUSTER_SERVICE:
                	msgRet = processCallClusterService(evt.getPayload());
                	send(ses, evt.response(msgRet));
                	break;
                	
                case AGENT_CALL_CLUSTER_COMMAND:
                	msgRet = processCallClusterCommand(evt.getPayload());
                	send(ses, evt.response(msgRet));
                	break;

                case SCHEMA_IMPORT_DRIVERS:
                    send(ses, evt.response(dbHnd.collectJdbcDrivers()));

                    break;

                case SCHEMA_IMPORT_SCHEMAS:
                    send(ses, evt.response(dbHnd.collectDbSchemas(evt)));

                    break;

                case SCHEMA_IMPORT_METADATA:
                    send(ses, evt.response(dbHnd.collectDbMetadata(evt)));

                    break;

                case NODE_REST:
                case NODE_VISOR:
                    if (log.isDebugEnabled())
                        log.debug("Processing REST request: " + evt);

                    RestRequest reqRest = fromJson(evt.getPayload(), RestRequest.class);

                    JsonObject params = new JsonObject(reqRest.getParams());
                    String cmd = params.getString("cmd");
                    RestResult res;

                    try {
                    	
                    	if("qryscanexe".equals(cmd) && params.containsKey("className")) {
                    		// Scan Query or Text Query
                    		String className = params.getString("className");
                    		if(className.startsWith("$text:")) {
                    			params.put("qry", className.substring(6));
                    			params.remove("className");
                    		}
                		}
                    	
                    	if("qrygremlinexe".equals(cmd) || "qrygroovyexe".equals(cmd) || "text2gremlin".equals(cmd)) {
                    		// Gremlin Query or Execute groovy code
                    		res = vertxClusterHnd.restCommand(reqRest.getClusterId(),params);
                		}                    	
                    	else if(DEMO_CLUSTER_ID!=null && DEMO_CLUSTER_ID.equals(reqRest.getClusterId())) {
                    		// demo grid cmd
                    		res = demoClusterHnd.restCommand(reqRest.getClusterId(),params);
                    	}
                    	else if(dbHnd.isDBCluster(reqRest.getClusterId())) {
                    		// rdms cmd
                    		res = dbHnd.restCommand(reqRest.getClusterId(), params);
                    	}
                    	else if(vertxClusterHnd.isVertxCluster(reqRest.getClusterId())) {
                    		// inner task call
                    		res = vertxClusterHnd.restCommand(reqRest.getClusterId(),params);
                		} 
                    	else {                    		
                    		res = clusterHnd.restCommand(reqRest.getClusterId(),params);
                    	}
                    	
                    	if(cmd.equals("top") && params.getBoolean("attr",false)) {
                    		JsonArray nodes = new JsonArray(res.getData());
                    		for(int i=0;i<nodes.size();i++) {
                    			JsonObject node = nodes.getJsonObject(i);
                    			JsonObject attr = node.getJsonObject("attributes");
                    			if(attr!=null) {
                    				attr.remove("java.library.path");
                    				attr.remove("java.class.path");
                    			}
                    		}
                    		res = RestResult.success(nodes.toString(), res.getSessionToken());
                    	}
                    	else if(cmd.equals("qryscanexe") && res.getData()!=null) {                    		       				
                    		JsonObject queryResult = new JsonObject(res.getData());
                    		queryResult = GremlinExecutor.parseKeyValueResponse(queryResult, reqRest.getClusterId());
                    		if(queryResult.containsKey("rows")) {
                    			res = RestResult.success(queryResult.toString(), res.getSessionToken());
                    		}
                    		
                    	}
                    }
                    catch (Throwable e) {
                        res = RestResult.fail(HTTP_INTERNAL_ERROR, e.getMessage());
                    }

                    send(ses, evt.response(res));

                    break;

                default:
                    log.warning("Unknown event: " + evt);
            }
        }
        catch (Throwable e) {
            if (evt == null) {
                log.error("Failed to process message: " + msg, e);

                return;
            }
            
            log.error("Failed to process message: " + evt, e);

            try {
                send(ses, evt.withError(ERROR_MSGS.get(evt.getEventType()), e));
            }
            catch (Exception ex) {
                log.error("Failed to send response with error", e);

                connect();
            }
        }
    }

    /**
     * @param ses Session.
     * @param frame Frame.
     */
    @OnWebSocketFrame
    public void onFrame(Session ses, Frame frame) {
        if (isRunning() && frame.getType() == Frame.Type.PING) {
            if (log.isTraceEnabled())
                log.trace("Received ping message [socket=" + ses + ", msg=" + frame + "]");

            try {
                ses.getRemote().sendPong(PONG_MSG);
            }
            catch (Throwable e) {
                log.error("Failed to send pong to: " + ses, e);
            }
        }
    }

    /**
     * @param ignored Error.
     */
    @OnWebSocketError
    public void onError(Throwable ignored) {
        if (reconnectCnt.get() == 1)
            log.error("Failed to establish websocket connection with server: " + cfg.serverUri());

        if (reconnectCnt.get() >= 1)
            connect();
    }

    /**
     * @param statusCode Close status code.
     * @param reason Close reason.
     */
    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        if (reconnectCnt.get() == 0) {
            log.info("Websocket connection closed with code: " + statusCode+ ", reason:"+reason);

            connect();
        }
        else {
        	closeLatch.countDown();
        }
    }

    /**
     * Send event to websocket.
     *
     * @param ses Websocket session.
     * @param evt Event.
     * @throws Exception If failed to send event.
     */
    private static void send(Session ses, WebSocketResponse evt) throws Exception {
        AgentUtils.send(ses, evt, 60L, TimeUnit.SECONDS);
    }
    
    public HttpClient getHttpClient() {
		return client.getHttpClient();
	}
}
