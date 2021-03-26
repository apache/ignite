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
