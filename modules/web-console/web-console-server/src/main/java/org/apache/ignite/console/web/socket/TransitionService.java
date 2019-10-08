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

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.messaging.MessagingListenActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.websocket.WebSocketEvents.ADMIN_ANNOUNCEMENT;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;

/**
 * Service for transition request/response between backends.
 */
@Service
public class TransitionService {
    /** */
    private static final Logger log = LoggerFactory.getLogger(TransitionService.class);

    /** */
    private static final String SEND_TO_AGENT = "SEND_TO_AGENT";

    /** */
    private static final String SEND_TO_BROWSER = "SEND_TO_BROWSER";

    /** */
    public static final String SEND_RESPONSE = "SEND_RESPONSE";

    /** Messages accessor. */
    private WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** */
    private Ignite ignite;

    /** */
    private AgentsRepository agentsRepo;

    /** */
    private AgentsService agentsSrvc;

    /** */
    private BrowsersService browsersSrvc;

    /**
     * @param ignite Ignite.
     * @param agentsRepo Agents repository.
     * @param clustersRepo Clusters repository.
     * @param agentsSrvc Agents service.
     * @param browsersSrvc Browsers service.
     */
    public TransitionService(
        Ignite ignite,
        AgentsRepository agentsRepo,
        ClustersRepository clustersRepo,
        @Lazy AgentsService agentsSrvc,
        @Lazy BrowsersService browsersSrvc
    ) {
        this.ignite = ignite;
        this.agentsRepo = agentsRepo;
        this.agentsSrvc = agentsSrvc;
        this.browsersSrvc = browsersSrvc;

        registerListeners();

        ignite.events().enableLocal(EVTS_DISCOVERY);

        ignite.events().localListen((event) -> {
            clustersRepo.cleanupClusterIndex();
            agentsRepo.cleanupBackendIndex();

            return true;
        }, EVTS_DISCOVERY);
    }

    /**
     * @param ann Announcement.
     */
    public void broadcastToBrowsers(Announcement ann) {
        sendToBrowser(null, new WebSocketResponse(ADMIN_ANNOUNCEMENT, ann));
    }

    /**
     * @param userKey User key.
     * @param evt Event to send.
     */
    public void sendToBrowser(UserKey userKey, WebSocketResponse evt) {
        ignite.message().send(SEND_TO_BROWSER, new UserEvent(userKey, evt));
    }

    /**
     * @param key Key.
     * @param evt Event.
     */
    public void sendToAgent(AgentKey key, WebSocketRequest evt) {
        sendToAgent(new AgentRequest(ignite.cluster().localNode().id(), key, evt));
    }

    /**
     * @param nid Node ID.
     * @param evt Event to send.
     */
    public void sendResponse(UUID nid, WebSocketRequest evt) {
        ignite.message(ignite.cluster().forNodeId(nid)).send(SEND_RESPONSE, evt);
    }

    /**
     * Listen events on transition.
     */
    protected void registerListeners() {
        ignite.message().localListen(SEND_TO_AGENT, new MessagingListenActor<AgentRequest>() {
            @Override protected void receive(UUID nodeId, AgentRequest req) {
                sendToAgent(req);
            }
        });

        ignite.message().localListen(SEND_RESPONSE, new MessagingListenActor<WebSocketEvent>() {
            @Override protected void receive(UUID nodeId, WebSocketEvent evt) {
                browsersSrvc.processResponse(evt);
            }
        });

        ignite.message().localListen(SEND_TO_BROWSER, new MessagingListenActor<UserEvent>() {
            @Override protected void receive(UUID nodeId, UserEvent res) {
                browsersSrvc.sendToBrowsers(res.getKey(), res.getEvt());
            }
        });
    }

    /**
     * @param req Request.
     * @param prefix Prefix.
     * @param e Exception.
     */
    private void responseWithError(AgentRequest req, String prefix, Throwable e) {
        try {
            ignite.message(ignite.cluster().forNodeId(req.getSrcNid()))
                .send(SEND_RESPONSE, req.getEvent().withError(prefix, e));
        }
        catch (Exception e1) {
            log.warn("Failed to send response to browser: " + req.getEvent(), e1);
        }
    }

    /**
     * @param req Agent request.
     */
    private void sendToAgent(AgentRequest req) {
        try {
            agentsSrvc.sendLocally(req);
        }
        catch (IllegalStateException ignored) {
            resendToOtherBackend(req);
        }
        catch (Exception e) {
            responseWithError(req, messages.getMessage("err.failed-to-send-to-agent"), e);
        }
    }

    /**
     * @param req Request to agent.
     */
    private void resendToOtherBackend(AgentRequest req) {
        Set<UUID> nids = agentsRepo.get(req.getKey());

        if (nids.isEmpty()) {
            responseWithError(req, messages.getMessage("err.agent-not-found"), null);

            return;
        }

        UUID targetNid = F.rand(nids);

        try {
            ignite.message(ignite.cluster().forNodeId(targetNid)).send(SEND_TO_AGENT, req);
        }
        catch (ClusterGroupEmptyException ignored) {
            agentsRepo.remove(req.getKey(), targetNid);

            resendToOtherBackend(req);
        }
        catch (Exception e) {
            responseWithError(req, messages.getMessage("err.failed-to-send-to-agent"), e);
        }
    }
}
