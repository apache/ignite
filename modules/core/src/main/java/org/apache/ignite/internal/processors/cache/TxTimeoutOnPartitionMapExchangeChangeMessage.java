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

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Discovery message for changing transaction timeout on partition map exchange.
 */
public class TxTimeoutOnPartitionMapExchangeChangeMessage extends DiscoveryCustomMessage {
    /** Request ID. */
    @Order(0)
    UUID reqId;

    /** Transaction timeout on partition map exchange in milliseconds. */
    @Order(1)
    long timeout;

    /** Init flag. */
    @Order(2)
    boolean isInit;

    /** */
    public TxTimeoutOnPartitionMapExchangeChangeMessage() {
        // No-op.
    }

    /**
     * Constructor for response.
     *
     * @param req Request message.
     */
    private TxTimeoutOnPartitionMapExchangeChangeMessage(TxTimeoutOnPartitionMapExchangeChangeMessage req) {
        super(IgniteUuid.randomUuid());

        reqId = req.reqId;
        timeout = req.timeout;
        isInit = false;
    }

    /**
     * Constructor.
     *
     * @param reqId Request ID.
     * @param timeout Transaction timeout on partition map exchange in milliseconds.
     */
    public TxTimeoutOnPartitionMapExchangeChangeMessage(UUID reqId, long timeout) {
        super(IgniteUuid.randomUuid());

        this.reqId = reqId;
        this.timeout = timeout;
        isInit = true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return isInit() ? new TxTimeoutOnPartitionMapExchangeChangeMessage(this) : null;
    }

    /**
     * Gets request ID.
     *
     * @return Request ID.
     */
    public UUID getRequestId() {
        return reqId;
    }

    /**
     * Gets transaction timeout on partition map exchange in milliseconds.
     *
     * @return Transaction timeout on partition map exchange in milliseconds.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Gets init flag.
     *
     * @return Init flag.
     */
    public boolean isInit() {
        return isInit;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxTimeoutOnPartitionMapExchangeChangeMessage.class, this);
    }
}
