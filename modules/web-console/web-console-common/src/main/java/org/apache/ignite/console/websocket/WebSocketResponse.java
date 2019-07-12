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

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Websocket response POJO.
 */
public class WebSocketResponse implements WebSocketEvent<Object> {
    /** */
    private String reqId;

    /** */
    private String evtType;

    /** */
    private Object payload;

    /**
     * Constructor with auto generated ID.
     *
     * @param evtType Event type.
     * @param payload Payload.
     */
    public WebSocketResponse(String evtType, Object payload) {
        this(UUID.randomUUID().toString(), evtType, payload);
    }

    /**
     * Constructor.
     *
     * @param reqId Request ID.
     * @param evtType Event type.
     * @param payload Payload.
     */
    WebSocketResponse(String reqId, String evtType, Object payload) {
        this.reqId = reqId;
        this.evtType = evtType;
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override public String getRequestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public void setRequestId(String reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public String getEventType() {
        return evtType;
    }

    /** {@inheritDoc} */
    @Override public void setEventType(String evtType) {
        this.evtType = evtType;
    }

    /** {@inheritDoc} */
    @Override public Object getPayload() {
        return payload;
    }

    /** {@inheritDoc} */
    @Override public void setPayload(Object payload) {
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSocketResponse.class, this);
    }
}
