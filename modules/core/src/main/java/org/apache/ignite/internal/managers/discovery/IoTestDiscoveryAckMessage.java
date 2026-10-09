/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.Nullable;

/** Acknowledgement for {@link IoTestDiscoveryMessage}. */
public class IoTestDiscoveryAckMessage extends DiscoveryServerOnlyCustomMessage {
    /** Request message ID. */
    @Order(0)
    IgniteUuid requestId;

    /** Ordered server node path. */
    @Order(1)
    List<UUID> path;

    /** Estimated one-way delay for every completed ring hop. */
    @Order(2)
    List<Long> hopTimesMillis;

    /** Request ring latency from submission to local acknowledgement processing, measured on the coordinator. */
    long ringTimeNanos;

    /** Empty constructor for {@link MessageFactory}. */
    public IoTestDiscoveryAckMessage() {
        // No-op.
    }

    /** @param msg Request. */
    public IoTestDiscoveryAckMessage(IoTestDiscoveryMessage msg) {
        super(IgniteUuid.randomUuid());

        requestId = msg.id();
        path = new ArrayList<>(msg.path);
        hopTimesMillis = new ArrayList<>(msg.hopTimesMillis);
    }

    /** @return Request message ID. */
    public IgniteUuid requestId() {
        return requestId;
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoverySpiCustomMessage ackMessage() {
        return null;
    }
}
