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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Request for single partition info.
 */
public class GridDhtPartitionsSingleRequest extends GridDhtPartitionsAbstractMessage {
    /** */
    @Order(value = 6, method = "restoreExchangeId")
    private GridDhtPartitionExchangeId restoreExchId;

    /**
     * Empty constructor.
     */
    public GridDhtPartitionsSingleRequest() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     */
    GridDhtPartitionsSingleRequest(GridDhtPartitionExchangeId id) {
        super(id, null);
    }

    /**
     * @param msgExchId Exchange ID for message.
     * @param restoreExchId Initial exchange ID for current exchange.
     * @return Message.
     */
    static GridDhtPartitionsSingleRequest restoreStateRequest(GridDhtPartitionExchangeId msgExchId,
        GridDhtPartitionExchangeId restoreExchId) {
        GridDhtPartitionsSingleRequest msg = new GridDhtPartitionsSingleRequest(msgExchId);

        msg.restoreState(true);

        msg.restoreExchId = restoreExchId;

        return msg;
    }

    /**
     * @return ID of current exchange on new coordinator.
     */
    public GridDhtPartitionExchangeId restoreExchangeId() {
        return restoreExchId;
    }

    /**
     * @param restoreExchId ID of current exchange on new coordinator.
     */
    public void restoreExchangeId(GridDhtPartitionExchangeId restoreExchId) {
        this.restoreExchId = restoreExchId;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 48;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsSingleRequest.class, this, super.toString());
    }
}
