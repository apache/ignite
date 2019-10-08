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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.ADMIN_ANNOUNCEMENT;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;
import static org.springframework.web.util.UriComponentsBuilder.fromUri;

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
    private static final int MAX_TEXT_MESSAGE_SIZE = 10 * 1024 * 1024;

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks = new HashMap<>();

    /** */
    private final Map<UserKey, Collection<WebSocketSession>> locBrowsers;

    /** */
    private final Map<String, WebSocketSession> locRequests;

    /** */
    private volatile WebSocketEvent lastAnn;

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
    @Override public void afterConnectionEstablished(WebSocketSession ses) {
        log.info("Browser session opened [socket=" + ses + "]");

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
            UUID accId = getAccountId(ses);

            switch (evt.getEventType()) {
                case SCHEMA_IMPORT_DRIVERS:
                case SCHEMA_IMPORT_SCHEMAS:
                case SCHEMA_IMPORT_METADATA:

                    sendToAgent(new AgentKey(accId), evt);

                    break;

                case NODE_REST:
                case NODE_VISOR:
                    JsonObject payload = fromJson(evt.getPayload());

                    String clusterId = payload.getString("clusterId");

                    if (F.isEmpty(clusterId))
                        throw new IllegalStateException(messages.getMessage("err.missing-cluster-id-param"));

                    if (evt.getEventType().equals(NODE_VISOR))
                        evt.setPayload(toJson(fillVisorGatewayTaskParams(payload)));

                    sendToAgent(new AgentKey(accId, clusterId), evt);

                    break;

                default:
                    throw new IllegalStateException(messages.getMessageWithArgs("err.unknown-evt", evt));
            }

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
        registerVisorTask(
            "querySql",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryArg"));

        registerVisorTask("querySqlV2",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryArgV2"));

        registerVisorTask("querySqlV3",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryArgV3"));

        registerVisorTask("querySqlX2",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryTaskArg"));

        registerVisorTask("queryScanX2",
            igniteVisor("query.VisorScanQueryTask"),
            igniteVisor("query.VisorScanQueryTaskArg"));

        registerVisorTask("queryFetch",
            igniteVisor("query.VisorQueryNextPageTask"),
            IgniteBiTuple.class.getName(), String.class.getName(), Integer.class.getName());

        registerVisorTask("queryFetchX2",
            igniteVisor("query.VisorQueryNextPageTask"),
            igniteVisor("query.VisorQueryNextPageTaskArg"));

        registerVisorTask("queryFetchFirstPage",
            igniteVisor("query.VisorQueryFetchFirstPageTask"),
            igniteVisor("query.VisorQueryNextPageTaskArg"));

        registerVisorTask("queryClose",
            igniteVisor("query.VisorQueryCleanupTask"),
            Map.class.getName(), UUID.class.getName(), Set.class.getName());

        registerVisorTask("queryCloseX2",
            igniteVisor("query.VisorQueryCleanupTask"),
            igniteVisor("query.VisorQueryCleanupTaskArg"));

        registerVisorTask("queryPing",
            igniteVisor("query.VisorQueryPingTask"),
            igniteVisor("query.VisorQueryNextPageTaskArg"));

        registerVisorTask("toggleClusterState",
            igniteVisor("misc.VisorChangeGridActiveStateTask"),
            igniteVisor("misc.VisorChangeGridActiveStateTaskArg"));

        registerVisorTask("cacheNamesCollectorTask",
            igniteVisor("cache.VisorCacheNamesCollectorTask"),
            Void.class.getName());

        registerVisorTask("cacheNodesTask",
            igniteVisor("cache.VisorCacheNodesTask"),
            String.class.getName());

        registerVisorTask("cacheNodesTaskX2",
            igniteVisor("cache.VisorCacheNodesTask"),
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

        JsonObject exeParams =  new JsonObject()
            .add("cmd", "exe")
            .add("name", "org.apache.ignite.internal.visor.compute.VisorGatewayTask")
            .add("p1", nids)
            .add("p2", desc.getTaskClass());

        AtomicInteger idx = new AtomicInteger(3);

        Arrays.stream(desc.getArgumentsClasses()).forEach(arg ->  exeParams.put("p" + idx.getAndIncrement(), arg));

        JsonArray args = params.getJsonArray("args");

        if (!F.isEmpty(args))
            args.forEach(arg -> exeParams.put("p" + idx.getAndIncrement(), arg));

        Stream.of("user", "password", "sessionToken").forEach(p -> exeParams.add(p, params.get(p)));

        payload.put("params", exeParams);

        return payload;
    }

    /**
     * @param evt Event.
     */
    void processResponse(WebSocketEvent evt) {
        WebSocketSession ses = locRequests.remove(evt.getRequestId());

        if (ses != null)
            sendMessageQuiet(ses, evt);
    }

    /**
     * @param id Browsers identifier.
     * @param evt Event.
     */
    void sendToBrowsers(UserKey id, WebSocketEvent evt) {
        if (id == null) {
            if (evt.getEventType().equals(ADMIN_ANNOUNCEMENT))
                lastAnn = evt;

            for (Collection<WebSocketSession> sessions : locBrowsers.values()) {
                for (WebSocketSession ses : sessions)
                    sendMessageQuiet(ses, evt);
            }
        }
        else {
            Collection<WebSocketSession> sessions = locBrowsers.getOrDefault(id, Collections.emptyList());

            for (WebSocketSession ses : sessions)
                sendMessageQuiet(ses, evt);
        }
    }
}
