

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
