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


import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentClusterLauncher;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.AgentUtils;
import org.apache.ignite.console.agent.db.DataSourceManager;
import org.apache.ignite.console.agent.db.DbSchema;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.agent.service.CacheAgentService;
import org.apache.ignite.console.agent.service.ClusterAgentService;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.CommonArgParser;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.java.JavaLogger;
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

import com.fasterxml.jackson.databind.DeserializationConfig;

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
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;
import static org.apache.ignite.console.websocket.WebSocketEvents.*;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
@WebSocket(maxTextMessageSize = 10 * 1024 * 1024, maxBinaryMessageSize = 10 * 1024 * 1024)
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
    private final ClusterHandler clusterHnd;

    /** Demo cluster handler. */
    private final DemoClusterHandler demoClusterHnd;

    /** Cluster watcher. */
    private final ClustersWatcher watcher;

    /** Reconnect count. */
    private AtomicInteger reconnectCnt = new AtomicInteger();

    /** Active tokens after handshake. */
    private Collection<String> validTokens;

    /** Connector pool. */
    private ExecutorService connectorPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "Connect thread"));

    GridJettyObjectMapper objectMapper = new GridJettyObjectMapper();
    
    /**
     * @param cfg Configuration.
     */
    public WebSocketRouter(AgentConfiguration cfg) {
        this.cfg = cfg;

        dbHnd = new DatabaseHandler(cfg);
        clusterHnd = new ClusterHandler(cfg);
        demoClusterHnd = new DemoClusterHandler(cfg);

        watcher = new ClustersWatcher(cfg, clusterHnd, demoClusterHnd,dbHnd);
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

        watcher.stop();

        if (client != null) {
            try {
                client.stop();
                client.destroy();
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.info("Stopping Web Console Agent...");

        stopClient();

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

            Thread.sleep(Math.min(10, reconnectCnt.get() - 1) * 1000);

            if (!isRunning())
                return;

            HttpClient httpClient = new HttpClient(createServerSslFactory(cfg));
            httpClient.setMaxConnectionsPerDestination(4);
            httpClient.setConnectBlocking(false);
            httpClient.setFollowRedirects(false);
            
            // TODO GG-18379 Investigate how to establish native websocket connection with proxy.
            configureProxy(httpClient, cfg.serverUri());

            client = new WebSocketClient(httpClient);            

            httpClient.start();
            client.start();
            client.connect(this, URI.create(cfg.serverUri()).resolve(AGENTS_PATH)).get(5L, TimeUnit.SECONDS);

            reconnectCnt.set(0);
        }
        catch (InterruptedException e) {
            closeLatch.countDown();
        }
        catch (TimeoutException | ExecutionException | CancellationException ignored) {
            // No-op.
        }
        catch (Exception e) {
            if (connecting)
                log.error("Failed to establish websocket connection with server: " + cfg.serverUri(), e);
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        connectorPool.submit(this::connect0);
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
    private Map<String,Object> processClusterStart(String msg) {
        log.info("Cluster start msg has been revoked: " + msg);
        JsonObject stat = new JsonObject();
        JsonObject json = fromJson(msg);
       
        String clusterId = json.getString("id");
        if(DEMO_CLUSTER_ID.equals(clusterId)) {
        	json.add("demo", true);
        }
        String clusterName = Utils.escapeFileName(json.getString("name"));
		try {
			
			AgentClusterLauncher.saveBlobToFile(json);
			
			Boolean restart = json.getBoolean("restart");
			if(restart!=null && restart) {
				String id = json.getString("id");
	        	AgentClusterLauncher.stop(clusterName,id);
			}
			
			if(!json.getBoolean("demo",false)) {	
				// 启动一个独立的node，jvm内部的node之间相互隔离
				Ignite ignite = AgentClusterLauncher.trySingleStart(json);
				
				if(ignite!=null) {
					AgentClusterLauncher.registerNodeUrl(ignite);
					
					AgentClusterLauncher.deployServices(ignite.services(ignite.cluster().forServers()));
		        	stat.put("status", "started");
				}
				else {
					stat.put("status", "started");
				}
				
			}
			else {
			   // 启动一个共享型的node，jvm内部的grid node会组成集群
				IgniteConfiguration cfg = new IgniteConfiguration();
				cfg.setIgniteInstanceName(clusterName);
				cfg.setConsistentId(clusterId);
				cfg.setNodeId(UUID.fromString(clusterId));
				if(json.getBoolean("demo",false)) {
					// 启动多个节点，并且在最后一个节点部署服务
		        	boolean rv = AgentClusterDemo.tryStart(cfg).await(60,TimeUnit.SECONDS);
		        	if(rv) {
		        		stat.put("status", "started");
		        	}
		    	}
		        else {
		        	// 启动一个节点，最后再部署服务
		        	Ignite ignite = AgentClusterLauncher.tryStart(cfg);
		        	if(ignite!=null) {
		        		//ignite.cluster().tag(clusterName);
			        	AgentClusterLauncher.registerNodeUrl(ignite);
			        	
			        	AgentClusterLauncher.deployServices(ignite.services(ignite.cluster().forServers()));
		        	}
		        	stat.put("status", "started");
		        }
			}
			
			stat.put("message","Ignite started successfully.");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			stat.put("message", e.getMessage());
			stat.put("status", "stoped");
		}
        
        return stat;
    }
    
    /**
     * @param tok Token to revoke.
     */
    private Map<String,Object> processClusterStop(String msg) {
        log.info("Cluster stop msg has been revoked: " + msg);
        JsonObject stat = new JsonObject();
        JsonObject json = fromJson(msg);
        String clusterName = Utils.escapeFileName(json.getString("name"));
        if(json.getBoolean("demo",false)) {
        	AgentClusterDemo.stop();
    	}
        else {
        	String id = json.getString("id");
        	AgentClusterLauncher.stop(clusterName,id);
        }
        stat.put("status", "stoped");
        return stat;
    }
    
    /**
     * @param tok Token to revoke.
     */
    private Map<String,Object> processCallClusterService(String msg) {
        log.info("Cluster status msg has been revoked: " + msg);
        JsonObject stat = new JsonObject();
        JsonObject json = fromJson(msg);
        String nodeId = json.getString("id");
        String serviceName = json.getString("serviceName","");
        String clusterName = null;
        stat.put("status", "stoped");
        Ignite ignite = null;
        if(json.getString("name",null)!=null) {
        	clusterName = Utils.escapeFileName(json.getString("name"));
    	}
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
        	JsonObject args = json.getJsonObject("args");
        	boolean rv = dbHnd.getDatabaseListener().deactivedCluster(nodeId);
        	if(!rv) {
        		stat.put("message", "Try again to remove from List.");
        	}
        	else {
        		stat.put("message", "Remove this connection from List.");
        	}
    		stat.put("status", rv);
    		return stat;
        }
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
        
        if(ignite!=null && !serviceName.isEmpty()) {
        	Service serviceObject = ignite.services().service(serviceName);
        	if(serviceObject==null) {
				stat.put("message", "service not found");
				return stat;
			}
        	try {
	        	if(serviceObject instanceof ClusterAgentService){
		        	ClusterAgentService agentSeervice = (ClusterAgentService)(serviceObject);
	        		Map<String,? extends Object> result = agentSeervice.call(json);
	        		stat.put("result", result);	        		
		        	
	        	}        
	        	else if(serviceObject instanceof CacheAgentService){
	        		CacheAgentService agentSeervice = (CacheAgentService)(serviceObject);
	        		Map<String,? extends Object> result = agentSeervice.call(json);
	        		stat.put("result", result);	        	
		    	}
	        }
			catch(Exception e) {
				stat.put("message", e.getMessage());
				stat.put("errorType", e.getClass().getSimpleName());
			}	
			
        }
        return stat;
    }

    
    /**
     * @param tok Token to revoke.
     */
    private Map<String,Object> processCallClusterCommand(String msg) {
        log.info("Cluster cmd has been revoked: " + msg);
        JsonObject stat = new JsonObject();
        JsonObject json = fromJson(msg);
        String nodeId = json.getString("id");
        String cmdName = json.getString("cmdName","");
        String clusterName = null;
        stat.put("status", "stoped");        
        
        JsonArray args = json.getJsonArray("args");
        
        
        Ignite ignite = null;
        if(json.getString("name",null)!=null) {
        	clusterName = Utils.escapeFileName(json.getString("name"));
    	}
        if(nodeId!=null) {        	
        	try {
        		ignite = Ignition.ignite(UUID.fromString(nodeId));	    		
	    		stat.put("status", "started");
	    		clusterName = null;
    		}
	    	catch(IgniteIllegalStateException e) {	
	    		stat.put("message", e.getMessage());
	    		stat.put("status", "stoped");
	    		return stat;
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
	    		return stat;
	    	}
    	}
        
        List<String> argsList = new ArrayList<>();
        
        String host = ignite.configuration().getConnectorConfiguration().getHost();
        if(host!=null && !host.isBlank()) {
	        argsList.add("--host");
	        argsList.add(host);
        }
        argsList.add("--port");
        argsList.add(""+ignite.configuration().getConnectorConfiguration().getPort());
        //-argsList.add(cmdName);
        if(args!=null) {
        	args.forEach(e-> argsList.add(e.toString()));
        }
        
        StringStreamHandler outHandder = new StringStreamHandler();        
        Logger logger = Logger.getLogger(CommandHandler.class.getName() + "Log");
        logger.addHandler(outHandder);
        JavaLogger javaLogger = new JavaLogger(logger);
        
        CommandHandler hnd = new CommandHandler(javaLogger);
        hnd.console = null;
        boolean experimentalEnabled = true;
        if(cmdName.equals("commandList")) {        	
        	List<JsonObject> results = new ArrayList<>(10);
        	Arrays.stream(CommandList.values())
            .filter(c -> experimentalEnabled || !c.command().experimental())
            .forEach(c -> {
            	c.command().printUsage(javaLogger);
            	JsonObject cmd = new JsonObject();
            	cmd.put("name", c.name());
            	cmd.put("text", c.text());
            	cmd.put("usage", outHandder.getOutput());
            	cmd.put("experimental", c.command().experimental());
            	results.add(cmd);
            });
        	
        	stat.put("result", results);
        	return stat;
        }
        int code = hnd.execute(argsList);
        stat.put("code",code);
        if(code==CommandHandler.EXIT_CODE_OK) {
        	stat.put("result", hnd.getLastOperationResult());
        	stat.put("message", outHandder.getOutput());
        }
        else {
        	stat.put("message", outHandder.getOutput());
        }
        return stat;
    }

    /**
     * @param msg Message.
     */
    @OnWebSocketMessage
    public void onMessage(Session ses, String msg) {
        WebSocketRequest evt = null;
        Map<String,Object> msgRet;
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
                	msgRet = processClusterStart(evt.getPayload());
                	send(ses, evt.response(msgRet));
                	break;
                	
                case AGENT_STOP_CLUSTER:
                	msgRet = processClusterStop(evt.getPayload());
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

                    JsonObject params = reqRest.getParams();

                    RestResult res;

                    try {
                    	if(DEMO_CLUSTER_ID.equals(reqRest.getClusterId())) {
                    		res = demoClusterHnd.restCommand(reqRest.getClusterId(),params);
                    	}
                    	else if(dbHnd.isDBCluster(reqRest.getClusterId())) {
                    		res = dbHnd.restCommand(reqRest.getClusterId(), params);
                    	}
                    	else {
                    		res = clusterHnd.restCommand(reqRest.getClusterId(),params);
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
            log.info("Websocket connection closed with code: " + statusCode);

            connect();
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
