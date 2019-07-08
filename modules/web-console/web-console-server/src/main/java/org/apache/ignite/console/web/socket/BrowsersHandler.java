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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;
import org.apache.ignite.console.web.AbstractHandler;
import org.apache.ignite.console.web.model.VisorTaskDescriptor;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;

/**
 * Browsers web sockets handler.
 */
@Service
public class BrowsersHandler extends AbstractHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(BrowsersHandler.class);

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks = new HashMap<>();

    /** */
    private final WebSocketsManager wsm;

    /** Messages accessor. */
    private WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /**
     * @param wsm Web sockets manager.
     */
    public BrowsersHandler(WebSocketsManager wsm) {
        this.wsm = wsm;

        registerVisorTasks();
    }

    /**
     * Extract account from session.
     *
     * @param ws Websocket.
     * @return Account.
     */
    protected Account extractAccount(WebSocketSession ws) {
        Principal p = ws.getPrincipal();

        if (p instanceof Authentication) {
            Authentication t = (Authentication)p;

            Object tp = t.getPrincipal();

            if (tp instanceof Account)
                return (Account)tp;
        }

        throw new IllegalStateException(messages.getMessageWithArgs("err.account-cant-be-found-in-ws-session", ws));
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Browser session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024);

        Account acc = extractAccount(ws);

        wsm.onBrowserConnect(ws, acc.getId());
    }

    /** {@inheritDoc} */
    @Override public void handleEvent(WebSocketSession ws, WebSocketRequest evt) {
        try {
            switch (evt.getEventType()) {
                case SCHEMA_IMPORT_DRIVERS:
                case SCHEMA_IMPORT_SCHEMAS:
                case SCHEMA_IMPORT_METADATA:
                    wsm.sendToFirstAgent(ws, evt.response());

                    break;

                case NODE_REST:
                case NODE_VISOR:
                    JsonObject payload = fromJson(evt.getPayload());

                    String clusterId = payload.getString("clusterId");

                    if (F.isEmpty(clusterId))
                        throw new IllegalStateException(messages.getMessage("err.missing-cluster-id-param"));

                    WebSocketResponse reqEvt = evt.getEventType().equals(NODE_REST) ?
                        evt.response() : evt.withPayload(prepareNodeVisorParams(payload));

                    wsm.sendToNode(ws, clusterId, reqEvt);

                    break;

                default:
                    throw new IllegalStateException(messages.getMessageWithArgs("err.unknown-evt", evt));
            }
        }
        catch (IllegalStateException e) {
            log.warn(e.toString());

            sendError(ws, evt, "Failed to send event to agent: " + evt.getPayload(), e);
        }
        catch (Throwable e) {
            String errMsg = "Failed to send event to agent: " + evt.getPayload();

            log.error(errMsg, e);

            sendError(ws, evt, errMsg, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        log.info("Browser session closed [socket=" + ws + ", status=" + status + "]");

        wsm.onBrowserConnectionClosed(ws);
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
    private void registerVisorTasks() {
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
    private JsonObject prepareNodeVisorParams(JsonObject payload) {
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
}
