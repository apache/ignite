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
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.console.utils.Utils.toJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.ERROR;

/**
 * Websocket event POJO.
 */
public class WebSocketEvent {
    /** */
    private String reqId;

    /** */
    private String evtType;

    /** */
    private String payload;

    /**
     * Default constructor for serialization.
     */
    public WebSocketEvent() {
        // No-op.
    }

    /**
     * Constructor with auto generated ID.
     *
     * @param evtType Event type.
     * @param payload Payload.
     */
    public WebSocketEvent(String evtType, Object payload) {
        this(UUID.randomUUID().toString(), evtType, toJson(payload));
    }

    /**
     * Constructor.
     *
     * @param reqId Request ID.
     * @param evtType Event type.
     * @param payload Payload.
     */
    private WebSocketEvent(String reqId, String evtType, String payload) {
        this.reqId = reqId;
        this.evtType = evtType;
        this.payload = payload;
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
    public String getPayload() {
        return payload;
    }

    /**
     * @param payload New payload.
     */
    public void setPayload(String payload) {
        this.payload = payload;
    }

    /**
     * Create event with payload for response with same ID.
     *
     * @param payload Payload.
     */
    public WebSocketEvent withPayload(Object payload) {
        return new WebSocketEvent(this.reqId, evtType, toJson(payload));
    }

    /**
     * Create event with error for response with same ID.
     *
     * @param msg Message.
     */
    public WebSocketEvent withError(String msg) {
        Map<String, String> err = new HashMap<>();

        err.put("message", msg);

        return new WebSocketEvent(this.reqId, ERROR, toJson(err));
    }
    
    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSocketEvent.class, this);
    }
}
