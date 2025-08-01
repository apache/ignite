/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.query.messages;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Query kill response.
 */
public class GridQueryKillResponse implements Message {
    /** */
    public static final short TYPE_CODE = 173;

    /** Error text. */
    @Order(value = 0, method = "error")
    private String errMsg;

    /** Request id.*/
    @Order(value = 1, method = "requestId")
    private long reqId;

    /**
     * Default constructor.
     */
    public GridQueryKillResponse() {
        // No-op.
    }

    /**
     * @param reqId Request id.
     * @param errMsg Error message.
     */
    public GridQueryKillResponse(long reqId, String errMsg) {
        this.reqId = reqId;
        this.errMsg = errMsg;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param reqId New request id.
     */
    public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Error text or {@code null} if no error.
     */
    public String error() {
        return errMsg;
    }

    /**
     * @param errMsg New error text.
     */
    public void error(String errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryKillResponse.class, this);
    }
}
