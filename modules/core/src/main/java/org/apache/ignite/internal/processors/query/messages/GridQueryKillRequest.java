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
 * Query kill request.
 */
public class GridQueryKillRequest implements Message {
    /** */
    public static final short TYPE_CODE = 172;

    /** Request id. */
    @Order(value = 0, method = "requestId")
    private long reqId;

    /** Query id on a node. */
    @Order(1)
    private long nodeQryId;

    /** Async response flag. */
    @Order(value = 2, method = "asyncResponse")
    private boolean asyncRes;

    /**
     * Default constructor.
     */
    public GridQueryKillRequest() {
        // No-op.
    }

    /**
     * @param reqId Request id.
     * @param nodeQryId Query ID on a node.
     * @param asyncRes {@code true} in case reposnse should be send asynchronous.
     */
    public GridQueryKillRequest(long reqId, long nodeQryId, boolean asyncRes) {
        this.reqId = reqId;
        this.nodeQryId = nodeQryId;
        this.asyncRes = asyncRes;
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
     * @return Query id on a node.
     */
    public long nodeQryId() {
        return nodeQryId;
    }

    /**
     * @param nodeQryId New query id on a node.
     */
    public void nodeQryId(long nodeQryId) {
        this.nodeQryId = nodeQryId;
    }

    /**
     * @return {@code true} in case response should be send back asynchronous.
     */
    public boolean asyncResponse() {
        return asyncRes;
    }

    /**
     * @param asyncRes New async response flag.
     */
    public void asyncResponse(boolean asyncRes) {
        this.asyncRes = asyncRes;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryKillRequest.class, this);
    }
}
