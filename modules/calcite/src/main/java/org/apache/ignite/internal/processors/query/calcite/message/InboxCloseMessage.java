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
 */

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.UUID;
import org.apache.ignite.internal.Order;

/**
 *
 */
public class InboxCloseMessage implements CalciteMessage {
    /** */
    @Order(value = 0, method = "queryId")
    private UUID qryId;

    /** */
    @Order(1)
    private long fragmentId;

    /** */
    @Order(2)
    private long exchangeId;

    /** */
    public InboxCloseMessage() {
        // No-op.
    }

    /** */
    public InboxCloseMessage(UUID qryId, long fragmentId, long exchangeId) {
        this.qryId = qryId;
        this.fragmentId = fragmentId;
        this.exchangeId = exchangeId;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return qryId;
    }

    /**
     * @param queryId New query ID.
     */
    public void queryId(UUID qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     * @param fragmentId New fragment ID.
     */
    public void fragmentId(long fragmentId) {
        this.fragmentId = fragmentId;
    }

    /**
     * @return Exchange ID.
     */
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * @param exchangeId New exchange ID.
     */
    public void exchangeId(long exchangeId) {
        this.exchangeId = exchangeId;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_INBOX_CANCEL_MESSAGE;
    }
}
