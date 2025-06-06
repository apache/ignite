

package org.apache.ignite.console.web.socket;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;
import static org.apache.ignite.console.websocket.AgentHandshakeRequest.CURRENT_VER;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_STATUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *  Transition service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class AgentsServiceSelfTest {
    /** Agents service. */
    @Autowired
    private AgentsService agentsSrvc;

    /** Accounts repository. */
    @MockBean
    private AccountsRepository accRepo;

    /** Browsers service. */
    @MockBean
    private BrowsersService browsersSrvc;

    /** */
    @Test
    public void testHandshake() throws Exception {
        Account acc = new Account("", "", "", "", "", "", "");

        Set<String> toks = Collections.singleton(acc.getToken());

        when(accRepo.getAllByTokens(anySet())).thenReturn(Collections.singleton(acc));

        AgentHandshakeRequest payload = new AgentHandshakeRequest(CURRENT_VER, toks);

        WebSocketRequest req = new WebSocketRequest();
        
        req.setRequestId(UUID.randomUUID().toString());
        req.setEventType(AGENT_HANDSHAKE);
        req.setPayload(toJson(payload));

        WebSocketSession ses = mock(WebSocketSession.class);

        agentsSrvc.handleEvent(ses, req);

        ArgumentCaptor<TextMessage> msgCaptor = ArgumentCaptor.forClass(TextMessage.class);

        verify(ses, times(1)).sendMessage(msgCaptor.capture());

        WebSocketRequest res = fromJson(msgCaptor.getValue().getPayload(), WebSocketRequest.class);

        assertEquals(req.getRequestId(), res.getRequestId());
        assertEquals(req.getEventType(), res.getEventType());

        AgentHandshakeResponse handshakeRes = fromJson(res.getPayload(), AgentHandshakeResponse.class);

        assertNull(handshakeRes.getError());
        assertEquals(toks, handshakeRes.getTokens());

        ArgumentCaptor<UserKey> keyCaptor = ArgumentCaptor.forClass(UserKey.class);
        ArgumentCaptor<WebSocketEvent> evtCaptor = ArgumentCaptor.forClass(WebSocketEvent.class);

        verify(browsersSrvc, times(2)).sendToBrowsers(keyCaptor.capture(), evtCaptor.capture());

        assertEquals(acc.getId(), keyCaptor.getValue().getAccId());
        assertEquals(AGENT_STATUS, evtCaptor.getValue().getEventType());
    }
}
