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

package org.apache.ignite.websocket;

import java.io.IOException;
import java.util.Collections;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * Test for Jackson serialization/deserialization.
 */
public class SerializationTests {
    /**
     * Test with agent handshake error object.
     */
    @Test
    public void handshakeError() throws IOException {
        AgentHandshakeResponse exp = new AgentHandshakeResponse(new IllegalAccessException("Failed to authenticate with token(s): [bff05108-404d-4f84-b743-6cd90b0ae205]. Please reload agent or check settings."));

        String payload = toJson(exp);

        AgentHandshakeResponse actual = fromJson(payload, AgentHandshakeResponse.class);

        Assert.assertEquals(exp.getError(), actual.getError());
        Assert.assertEquals(exp.getTokens(), actual.getTokens());
    }

    /**
     * Test with agent handshake.
     */
    @Test
    public void handshakeTokens() throws IOException {
        AgentHandshakeResponse exp = new AgentHandshakeResponse(Collections.singleton("bff05108-404d-4f84-b743-6cd90b0ae205"));

        String payload = toJson(exp);

        AgentHandshakeResponse actual = fromJson(payload, AgentHandshakeResponse.class);

        Assert.assertEquals(exp.getError(), actual.getError());
        Assert.assertEquals(exp.getTokens(), actual.getTokens());
    }
}
