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

package org.apache.ignite.console.web.socket;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Announcement;

import org.apache.ignite.console.web.AbstractSocketHandler;
import org.apache.ignite.console.web.model.VisorTaskDescriptor;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.jetty.JettyWebSocketSession;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;

import static org.apache.ignite.console.websocket.WebSocketEvents.*;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;
import static org.springframework.web.util.UriComponentsBuilder.fromUri;
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;

/**
 * Browsers web sockets handler.
 */
@Service
public class BrowsersService extends AbstractSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(BrowsersService.class);

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** Max text message size. */
    private static final int MAX_TEXT_MESSAGE_SIZE = 8 * 1024 * 1024;

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks = new HashMap<>();

    /** */
    private final Map<UserKey, Collection<WebSocketSession>> locBrowsers;

    /** */
    private final Map<String, WebSocketSession> locRequests;

    /** */
    private volatile WebSocketEvent<Announcement> lastAnn;

    /** */
    private final AgentsService agentsSrvc;

    /** */
    private final TransitionService transitionSrvc;

    /**
     * @param agentsSrvc Agents service.
     * @param transitionSrvc Service for transfering messages between backends.
     */
    public BrowsersService(AgentsService agentsSrvc, TransitionService transitionSrvc) {
        this.agentsSrvc = agentsSrvc;
        this.transitionSrvc = transitionSrvc;

        locBrowsers = new ConcurrentHashMap<>();
        locRequests = new ConcurrentHashMap<>();

        registerVisorTasks();
    }

    /**
     * Periodically ping connected clients to keep connections alive.
     */
    @Scheduled(fixedRate = 3_000)
    public void heartbeat() {
        locBrowsers.values().stream().flatMap(Collection::stream).collect(toList()).forEach(this::ping);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Browser session opened [socket=" + ws + "]");
        JettyWebSocketSession ses = (JettyWebSocketSession) ws;
        ses.getNativeSession().getPolicy().setMaxTextMessageSize(MAX_TEXT_MESSAGE_SIZE);        
        ses.setTextMessageSizeLimit(MAX_TEXT_MESSAGE_SIZE);

        UserKey id = getId(ses);

        locBrowsers.compute(id, (key, sessions) -> {
            if (sessions == null)
                sessions = new HashSet<>();

            sessions.add(ses);

            return sessions;
        });

        if (lastAnn != null)
            sendMessageQuiet(ses, lastAnn);

        sendMessageQuiet(ses, agentsSrvc.collectAgentStats(id));
    }

    /**
     * @param key Key.
     * @param evt Event.
     */
    public void sendToAgent(AgentKey key, WebSocketRequest evt) {
        transitionSrvc.sendToAgent(key, evt);
    }

    /** {@inheritDoc} */
    @Override public void handleEvent(WebSocketSession ses, WebSocketRequest evt) {
        try {
        	JsonObject payload = null;
        	String clusterId;
            UUID accId = getAccountId(ses);

            switch (evt.getEventType()) {
                case SCHEMA_IMPORT_DRIVERS:
                case SCHEMA_IMPORT_SCHEMAS:
                case SCHEMA_IMPORT_METADATA:

                    sendToAgent(new AgentKey(accId), evt);

                    break;

                case NODE_REST:
                case NODE_VISOR:
                    payload = fromJson(evt.getPayload());

                    clusterId = payload.getString("clusterId");

                    if (F.isEmpty(clusterId))
                        throw new IllegalStateException(messages.getMessage("err.missing-cluster-id-param"));

                    if (evt.getEventType().equals(NODE_VISOR))
                        evt.setPayload(toJson(fillVisorGatewayTaskParams(payload)));

                    sendToAgent(new AgentKey(accId, clusterId), evt);

                    break;
                    
                case AGENT_START_CLUSTER:
                case AGENT_STOP_CLUSTER:
                	try {
                		payload = fromJson(evt.getPayload());

                        clusterId = payload.getString("id");

                        if (F.isEmpty(clusterId))
                            throw new IllegalStateException(messages.getMessage("err.missing-cluster-id-param"));
                        
                        // last node will active cluster
                        int nodeIndex = 0;
                        AgentKey key = new AgentKey(accId);
                        List<WebSocketSession> nids = agentsSrvc.findLocalAgents(key);
                        if (!nids.isEmpty()) {
                        	for(WebSocketSession sess: nids) {
                        		evt.setNodeSeq(nodeIndex);
                        		if(nodeIndex==nids.size()-1) {
                        			evt.setLastNode(true);
                        			agentsSrvc.sendMessageWithResponse(sess, evt, this.transitionSrvc.localNodeId());
                        		}
                        		else {
                        			agentsSrvc.sendMessage(sess, evt);
                        		}
                        		nodeIndex++;
                        	}
                        }                        	
                        else {
                        	log.warn("Not found any cluster agent for : " + evt);
                        	sendMessageQuiet(ses, evt.response("Failed to send event to agent: Not found any cluster agent"));
                        }
                    }
                    catch (ClusterGroupEmptyException ignored) {
                        // No-op.
                    }
                    catch (Exception e) {
                        log.warn("Failed to send response to browser: " + evt, e);
                    }
                    break;
                    
                case AGENT_CALL_CLUSTER_SERVICE:
                case AGENT_CALL_CLUSTER_COMMAND:
                	try {
                		payload = fromJson(evt.getPayload());

                        clusterId = payload.getString("id");

                        if (F.isEmpty(clusterId))
                            throw new IllegalStateException(messages.getMessage("err.missing-cluster-id-param"));
                        
                        
                        AgentKey key = new AgentKey(accId, clusterId);
                        Optional<WebSocketSession> nid = agentsSrvc.findLocalAgent(key);
                        if (nid.isPresent())
                        	sendToAgent(key, evt);
                        else {
                        	key = new AgentKey(accId);
                            nid = agentsSrvc.findLocalAgent(key);
                            if (nid.isPresent()) {
                            	 sendToAgent(key, evt);
                            }
                            else {
                            	log.warn("Not found any cluster agent for : " + evt);
                            	sendMessageQuiet(ses, evt.response("Failed to send event to agent: Not found any cluster agent"));
                            }
                        }
                    }
                    catch (ClusterGroupEmptyException ignored) {
                        // No-op.
                    }
                    catch (Exception e) {
                        log.warn("Failed to send response to browser: " + evt, e);
                    }
                    break;
                    
                case "BROADCAST":
                	try {
                        
                		for (Collection<WebSocketSession> sessions : locBrowsers.values()) {
                            for (WebSocketSession bses : sessions) {
                            	if(bses!=ses) {
                            		sendMessageQuiet(bses, evt);
                            	}
                            }
                        }
                    }                    
                    catch (Exception e) {
                        log.warn("Failed to send response to browser: " + evt, e);
                    }
                    break;
                	

                default:
                    throw new IllegalStateException(messages.getMessageWithArgs("err.unknown-evt", evt));
            }
            if(evt.getRequestId()!=null)
            	locRequests.put(evt.getRequestId(), ses);
        }
        catch (IllegalStateException e) {
            log.warn(e.toString());

            sendMessageQuiet(ses, evt.withError("Failed to send event to agent: ", e));
        }
        catch (Throwable e) {
            String errMsg = "Failed to send event to agent: " + evt.getPayload();

            log.error(errMsg, e);

            sendMessageQuiet(ses, evt.withError(errMsg, e));
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ses, CloseStatus status) {
        log.info("Browser session closed [socket=" + ses + ", status=" + status + "]");

        locBrowsers.computeIfPresent(getId(ses), (key, sessions) -> {
            sessions.remove(ses);

            return sessions;
        });

        locRequests.values().removeAll(Collections.singleton(ses));
    }

    /**
     * Get session ID.
     *
     * @param ses Session.
     * @return User ID.
     */
    protected UserKey getId(WebSocketSession ses) {
        return new UserKey(
            getAccountId(ses),
            Boolean.parseBoolean(fromUri(ses.getUri()).build().getQueryParams().getFirst("demoMode"))
        );
    }

    /**
     * @param ses Session.
     */
    protected UUID getAccountId(WebSocketSession ses) {
        Principal p = ses.getPrincipal();

        if (p instanceof Authentication) {
            Authentication t = (Authentication)p;

            Object tp = t.getPrincipal();

            if (tp instanceof Account)
                return ((Account)tp).getId();
        }

        throw new IllegalStateException(messages.getMessageWithArgs("err.account-cant-be-found-in-ws-session", ses));
    }

    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    protected String igniteVisor(String shortName) {
        return VISOR_IGNITE + shortName;
    }
    
    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    protected String igniteCmd(String shortName) {
        return shortName;
    }

    /**
     * @param taskId Task ID.
     * @param taskCls Task class name.
     * @param argCls Arguments classes names.
     */
    protected void registerVisorTask(String taskId, String taskCls, String... argCls) {
        visorTasks.put(taskId, new VisorTaskDescriptor(taskCls, argCls));
    }

    /**
     * Register Visor tasks.
     */
	protected void registerVisorTasks() {

		registerVisorTask("querySqlX2", igniteCmd("qryfldexe"), Map.class.getName());
		// scanf
		registerVisorTask("queryScanX2", igniteCmd("qryscanexe"), Map.class.getName());		
		// gremlin
		registerVisorTask("queryGremlin", igniteCmd("qrygremlinexe"), Map.class.getName());		
		// text2sql
		registerVisorTask("text2sql", igniteCmd("text2sql"), Map.class.getName());		
		// text2gremlin
		registerVisorTask("text2gremlin", igniteCmd("text2gremlin"), Map.class.getName());

		registerVisorTask("queryFetch", igniteCmd("qryfetch"), Map.class.getName());

		registerVisorTask("queryFetchFirstPage", igniteCmd("qryfetch"), Map.class.getName());

		registerVisorTask("queryClose", igniteCmd("qrycls"), Map.class.getName());

		registerVisorTask("toggleClusterState", igniteCmd("setstate"), Map.class.getName());

		registerVisorTask("cacheNamesCollectorTask", igniteVisor("cache.VisorCacheNamesCollectorTask"),
				Void.class.getName());

		registerVisorTask("cacheNodesTaskX2", igniteVisor("cache.VisorCacheNodesTask"),
				igniteVisor("cache.VisorCacheNodesTaskArg"));
	}

    /**
     * Prepare task event for execution on agent.
     *
     * @param payload Task event.
     */
    protected JsonObject fillVisorGatewayTaskParams(JsonObject payload) {
        JsonObject params = payload.getJsonObject("params");

        String taskId = params.getString("taskId");

        if (F.isEmpty(taskId))
            throw new IllegalStateException(messages.getMessageWithArgs("err.not-specified-task-id", payload));

        String nids = params.getString("nids");

        VisorTaskDescriptor desc = visorTasks.get(taskId);

        if (desc == null)
            throw new IllegalStateException(messages.getMessageWithArgs("err.unknown-task", taskId, payload));
        
        // not visor task
        if(!desc.getTaskClass().startsWith(VISOR_IGNITE)) {
        	
        	JsonObject exeParams =  new JsonObject();
        	exeParams.put("cmd", desc.getTaskClass());            

            JsonObject args = params.getJsonObject("args");

            if (!F.isEmpty(args))
                args.getMap().entrySet().forEach(arg -> exeParams.put(arg.getKey(),arg.getValue()));

            Stream.of("user", "password", "sessionToken").forEach(p -> exeParams.put(p, params.getString(p)));

            payload.put("params", exeParams);

            return payload;
        	
        }
        else {
        	JsonObject exeParams =  new JsonObject()
                    .put("cmd", "exe")
                    .put("name", "org.apache.ignite.internal.visor.compute.VisorGatewayTask")
                    .put("p1", nids)
                    .put("p2", desc.getTaskClass());

            AtomicInteger idx = new AtomicInteger(3);

            Arrays.stream(desc.getArgumentsClasses()).forEach(arg ->  exeParams.put("p" + idx.getAndIncrement(), arg));

            JsonArray args = params.getJsonArray("args");

            if (!F.isEmpty(args))
                args.forEach(arg -> exeParams.put("p" + idx.getAndIncrement(), arg));

            Stream.of("user", "password", "sessionToken").forEach(p -> exeParams.put(p, params.getString(p)));

            payload.put("params", exeParams);

            return payload;
        	
        }
        
    }

    /**
     * @param evt Event.
     * @return {@code true} if response was processed.
     */
    public boolean processResponse(WebSocketEvent evt) {
        WebSocketSession ses = locRequests.remove(evt.getRequestId());

        if (ses != null) {
            try {
                sendMessage(ses, evt);
            }
            catch (Throwable e) {
                log.error("Failed to send event [session=" + ses + ", event=" + evt + "]", e);

                return false;
            }

            return true;
        }

        return false;
    }

    /**
     * @param evt Announcement.
     */
    void sendToBrowsers(UserKey id, WebSocketEvent evt) {
        Collection<WebSocketSession> sessions = locBrowsers.get(id);

        if (!F.isEmpty(sessions)) {
            for (WebSocketSession ses : sessions)
                sendMessageQuiet(ses, evt);
        }
    }

    /**
     * @param ann Announcement.
     */
    void sendAnnouncement(WebSocketEvent<Announcement> ann) {
        lastAnn = ann;

        for (Collection<WebSocketSession> sessions : locBrowsers.values()) {
            for (WebSocketSession ses : sessions)
                sendMessageQuiet(ses, lastAnn);
        }
    }
}
