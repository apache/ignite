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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.Nullable;

/** Mutable message used to record a Discovery SPI ring path. */
public class IoTestDiscoveryMessage extends DiscoveryServerOnlyCustomMessage implements MarshallableMessage {
    /** Payload. */
    @Order(0)
    byte[] payload;

    /** Ordered server node path. */
    @Order(1)
    List<UUID> path;

    /** Current hop serialization-start timestamp from the sender wall clock. */
    @Order(2)
    long hopSendTsMillis;

    /** Estimated one-way delay from serialization start to deserialization completion for every completed ring hop. */
    @Order(3)
    List<Long> hopTimesMillis;

    /** Empty constructor for {@link MessageFactory}. */
    public IoTestDiscoveryMessage() {
        // No-op.
    }

    /** @param payload Payload. */
    public IoTestDiscoveryMessage(byte[] payload) {
        super(IgniteUuid.randomUuid());

        this.payload = payload;
        path = new ArrayList<>();
        hopTimesMillis = new ArrayList<>();
    }

    /** @param nodeId Node that processed this message. */
    public void onProcessed(UUID nodeId) {
        path.add(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        hopSendTsMillis = System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        hopTimesMillis.add(System.currentTimeMillis() - hopSendTsMillis);
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoverySpiCustomMessage ackMessage() {
        return new IoTestDiscoveryAckMessage(this);
    }
}
