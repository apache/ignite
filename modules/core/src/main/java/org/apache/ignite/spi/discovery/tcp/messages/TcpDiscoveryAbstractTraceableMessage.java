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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.io.Externalizable;
import java.util.UUID;
import org.apache.ignite.internal.processors.tracing.messages.SpanContainer;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;

/**
 * Abstract traceable message for TCP discovery.
 */
public abstract class TcpDiscoveryAbstractTraceableMessage extends TcpDiscoveryAbstractMessage implements TraceableMessage {
    /** Container. */
    private SpanContainer spanContainer = new SpanContainer();

    /**
     * Default no-arg constructor for {@link Externalizable} interface.
     */
    protected TcpDiscoveryAbstractTraceableMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     */
    protected TcpDiscoveryAbstractTraceableMessage(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /**
     * @param msg Message.
     */
    protected TcpDiscoveryAbstractTraceableMessage(TcpDiscoveryAbstractTraceableMessage msg) {
        super(msg);

        this.spanContainer = msg.spanContainer;
    }

    /**
     * Restores spanContainer field to non-null value after deserialization.
     * This is needed for compatibility between nodes having Tracing SPI and not.
     */
    public Object readResolve() {
        if (spanContainer == null)
            spanContainer = new SpanContainer();

        return this;
    }

    /** {@inheritDoc} */
    @Override public SpanContainer spanContainer() {
        return spanContainer;
    }
}
