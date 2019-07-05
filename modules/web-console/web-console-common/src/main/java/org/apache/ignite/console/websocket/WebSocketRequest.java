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

package org.apache.ignite.console.websocket;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.ignite.console.json.RawContentDeserializer;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.console.websocket.WebSocketEvents.ERROR;

/**
 * Websocket event POJO.
 */
public class WebSocketRequest implements WebSocketEvent<String> {
    /** */
    private String reqId;

    /** */
    private String evtType;

    /** */
    private String payload;

    /**
     * Default constructor for deserialization.
     */
    public WebSocketRequest() {
        // No-op.
    }

    /**
     * @return Request ID.
     */
    public String getRequestId() {
        return reqId;
    }

    /**
     * @param reqId New request ID.
     */
    public void setRequestId(String reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Event type.
     */
    public String getEventType() {
        return evtType;
    }

    /**
     * @param evtType New event type.
     */
    public void setEventType(String evtType) {
        this.evtType = evtType;
    }

    /**
     * @return Payload.
     */
    @JsonRawValue
    public String getPayload() {
        return payload;
    }

    /**
     * @param payload New payload.
     */
    @JsonDeserialize(using = RawContentDeserializer.class)
    public void setPayload(String payload) {
        this.payload = payload;
    }

    /**
     * Create event with payload for response with same ID.
     *
     * @param payload Payload.
     */
    public WebSocketResponse withPayload(Object payload) {
        return new WebSocketResponse(this.reqId, evtType, payload);
    }

    /**
     * Create event with payload for response with same ID.
     */
    public WebSocketResponse response() {
        return new WebSocketResponse(this.reqId, evtType, payload);
    }

    /**
     * Create event with error for response with same ID.
     *
     * @param msg Message.
     */
    public WebSocketResponse withError(String msg) {
        Map<String, String> err = new HashMap<>();

        err.put("message", msg);

        return new WebSocketResponse(this.reqId, ERROR, err);
    }
    
    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSocketRequest.class, this);
    }
}
