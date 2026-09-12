

package org.apache.ignite.console.web.socket;

import java.util.UUID;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Wrapper for websocket request.
 */
public class AgentRequest {
    /** Source nid. */
    private UUID srcNid;

    /** Agent key. */
    private AgentKey key;

    /** Event. */
    private WebSocketRequest evt;

    /**
     * @param key Cluster.
     * @param evt Event.
     */
    public AgentRequest(UUID srcNid, AgentKey key, WebSocketRequest evt) {
        this.srcNid = srcNid;
        this.key = key;
        this.evt = evt;
    }

    /**
     * @return value of source nid
     */
    public UUID getSrcNid() {
        return srcNid;
    }

    /**
     * @return value of agent key.
     */
    public AgentKey getKey() {
        return key;
    }

    /**
     * @return Event.
     */
    public WebSocketRequest getEvent() {
        return evt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AgentRequest.class, this);
    }
}
