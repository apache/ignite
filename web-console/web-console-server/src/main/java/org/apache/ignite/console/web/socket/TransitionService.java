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
    private static final String SEND_RESPONSE = "SEND_RESPONSE";

    /** */
    private static final String SEND_TO_USER_BROWSER = "SEND_TO_USER_BROWSER";

    /** */
    private static final String SEND_ANNOUNCEMENT = "SEND_ANNOUNCEMENT";

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
     * @param req Request.
     */
    private void resendToRemote(AgentRequest req) {
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

            resendToRemote(req);
        }
        catch (Exception e) {
            responseWithError(req, messages.getMessage("err.failed-to-send-to-agent"), e);
        }
    }

    /**
     * Listen events on transition.
     */
    private void registerListeners() {
        ignite.message().localListen(SEND_TO_AGENT, new MessagingListenActor<AgentRequest>() {
            @Override protected void receive(UUID nodeId, AgentRequest req) {
                try {
                    agentsSrvc.sendLocally(req);
                }
                catch (IllegalStateException ignored) {
                    resendToRemote(req);
                }
                catch (Exception e) {
                    responseWithError(req, messages.getMessage("err.failed-to-send-to-agent"), e);
                }
            }
        });

        ignite.message().localListen(SEND_RESPONSE, new MessagingListenActor<WebSocketEvent>() {
            @Override protected void receive(UUID nodeId, WebSocketEvent evt) {
                boolean processed = browsersSrvc.processResponse(evt);

                if (!processed)
                    processed = agentsSrvc.processResponse(evt);

                if (!processed)
                    log.warn("Event was not processed: " + evt);
            }
        });

        ignite.message().localListen(SEND_TO_USER_BROWSER, new MessagingListenActor<UserEvent>() {
            @Override protected void receive(UUID nodeId, UserEvent res) {
                browsersSrvc.sendToBrowsers(res.getKey(), res.getEvt());
            }
        });

        ignite.message().localListen(SEND_ANNOUNCEMENT, new MessagingListenActor<WebSocketEvent<Announcement>>() {
            @Override protected void receive(UUID nodeId, WebSocketEvent<Announcement> ann) {
                browsersSrvc.sendAnnouncement(ann);
            }
        });
    }

    /**
     * @param ann Announcement.
     */
    public void broadcastAnnouncement(Announcement ann) {
        ignite.message().send(SEND_ANNOUNCEMENT, new WebSocketResponse(ADMIN_ANNOUNCEMENT, ann));
    }

    /**
     * @param key Key.
     * @param evt Event.
     */
    public void sendToAgent(AgentKey key, WebSocketRequest evt) {
        ignite.message(ignite.cluster().forLocal())
            .send(SEND_TO_AGENT, new AgentRequest(ignite.cluster().localNode().id(), key, evt));
    }

    /**
     * @param userKey User key.
     * @param evt Event to send.
     */
    public void sendToBrowser(UserKey userKey, WebSocketResponse evt) {
        ignite.message().send(SEND_TO_USER_BROWSER, new UserEvent(userKey, evt));
    }

    /**
     * @param nid Node ID.
     * @param evt Event to send.
     */
    public void sendResponse(UUID nid, WebSocketRequest evt) {
        ignite.message(ignite.cluster().forNodeId(nid)).send(SEND_RESPONSE, evt);
    }
    
    public UUID localNodeId() {
    	return ignite.cluster().localNode().id();
    }
}
