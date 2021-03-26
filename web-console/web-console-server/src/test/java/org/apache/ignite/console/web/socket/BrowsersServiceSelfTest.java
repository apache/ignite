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

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.apache.ignite.console.utils.Utils.entriesToMap;
import static org.apache.ignite.console.utils.Utils.entry;
import static org.apache.ignite.console.utils.Utils.toJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *  Transition service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class BrowsersServiceSelfTest {
    /** Browsers service. */
    @Autowired
    private BrowsersService browsersSrvc;

    /** Browsers service. */
    @MockBean
    private AgentsService agentsSrvc;

    /** Ignite instance. */
    @Autowired
    private Ignite ignite;

    /**
     *
     */
    @Test
    public void testSendToAgent() throws Exception {
        String clusterId = UUID.randomUUID().toString();

        WebSocketRequest req = new WebSocketRequest();

        req.setRequestId(UUID.randomUUID().toString());
        req.setEventType(NODE_REST);

        req.setPayload(toJson(new JsonObject(
            Stream.<Map.Entry<String, Object>>of(
                entry("clusterId", clusterId)
            ).collect(entriesToMap()))
        ));

        AgentKey key = new AgentKey(UUID.randomUUID(), clusterId);

        ReflectionTestUtils.invokeMethod(browsersSrvc, "sendToAgent", key, req);

        ArgumentCaptor<AgentRequest> captor = ArgumentCaptor.forClass(AgentRequest.class);

        verify(agentsSrvc, times(1)).sendLocally(captor.capture());

        assertEquals(ignite.cluster().localNode().id(), captor.getValue().getSrcNid());
        assertEquals(key, captor.getValue().getKey());
        assertEquals(req, captor.getValue().getEvent());
    }
}
