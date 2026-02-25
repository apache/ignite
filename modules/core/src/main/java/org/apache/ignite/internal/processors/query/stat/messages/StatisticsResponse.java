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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Response for statistics request.
 */
public class StatisticsResponse implements Message {
    /** */
    public static final short TYPE_CODE = 188;

    /** Request id. */
    @Order(0)
    UUID reqId;

    /** Requested statistics. */
    @Order(1)
    StatisticsObjectData data;

    /**
     * Constructor.
     */
    public StatisticsResponse() {
    }

    /**
     * Constructor.
     *
     * @param reqId Request id
     * @param data Statistics data.
     */
    public StatisticsResponse(
        UUID reqId,
        StatisticsObjectData data
    ) {
        this.reqId = reqId;
        this.data = data;
    }

    /**
     * @return Request id.
     */
    public UUID reqId() {
        return reqId;
    }

    /**
     * @return Statitics data.
     */
    public StatisticsObjectData data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

}
