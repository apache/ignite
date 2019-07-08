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

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.web.AbstractHandler;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.websocket.AgentHandshakeRequest.SUPPORTED_VERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketEvents.CLUSTER_TOPOLOGY;

/**
 * Agents web sockets handler.
 */
@Service
public class AgentsHandler extends AbstractHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentsHandler.class);

    /** */
    private AccountsRepository accRepo;

    /** */
    private WebSocketsManager wsm;

    /** Messages accessor. */
    private WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /**
     * @param accRepo Repository to work with accounts.
     * @param wsm Web sockets manager.
     */
    public AgentsHandler(AccountsRepository accRepo, WebSocketsManager wsm) {
        this.accRepo = accRepo;
        this.wsm = wsm;
    }

    /**
     * @param req Agent handshake.
     */
    private void validateAgentHandshake(AgentHandshakeRequest req) {
        if (F.isEmpty(req.getTokens()))
            throw new IllegalArgumentException(messages.getMessage("err.tokens-no-specified-in-agent-handshake-req"));

        if (!SUPPORTED_VERS.contains(req.getVersion()))
            throw new IllegalArgumentException(messages.getMessageWithArgs("err.agent-unsupport-version", req.getVersion()));
    }

    /**
     * @param tokens Tokens.
     */
    private Collection<Account> loadAccounts(Set<String> tokens) {
        Collection<Account> accounts = accRepo.getAllByTokens(tokens);

        if (accounts.isEmpty())
            throw new IllegalArgumentException(messages.getMessageWithArgs("err.failed-auth-with-tokens", tokens));

        return accounts;
    }

    /** {@inheritDoc} */
    @Override public void handleEvent(WebSocketSession ws, WebSocketRequest evt) throws IOException {
        switch (evt.getEventType()) {
            case AGENT_HANDSHAKE:
                try {
                    AgentHandshakeRequest req = fromJson(evt.getPayload(), AgentHandshakeRequest.class);

                    validateAgentHandshake(req);

                    Collection<Account> accounts = loadAccounts(req.getTokens());

                    sendResponse(ws, evt, new AgentHandshakeResponse(accounts.stream().map(Account::getToken).collect(toList())));

                    wsm.onAgentConnect(ws, mapToSet(accounts, Account::getId));

                    log.info("Agent connected: " + req);
                }
                catch (IllegalArgumentException e) {
                    log.warn("Failed to establish connection in handshake. " + e.getMessage());

                    sendResponse(ws, evt, new AgentHandshakeResponse(e));

                    ws.close();
                }
                catch (Exception e) {
                    log.warn("Failed to establish connection in handshake: " + evt, e);

                    sendResponse(ws, evt, new AgentHandshakeResponse(e));

                    ws.close();
                }

                break;

            case CLUSTER_TOPOLOGY:
                try {
                    Collection<TopologySnapshot> tops = fromJson(
                        evt.getPayload(),
                        new TypeReference<Collection<TopologySnapshot>>() {}
                    );

                    wsm.processTopologyUpdate(ws, tops);
                }
                catch (Exception e) {
                    log.warn("Failed to process topology update: " + evt, e);
                }

                break;

            default:
                wsm.sendResponseToBrowser(evt);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Agent session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        log.info("Agent session closed [socket=" + ws + ", status=" + status + "]");

        wsm.onAgentConnectionClosed(ws);
    }
}
