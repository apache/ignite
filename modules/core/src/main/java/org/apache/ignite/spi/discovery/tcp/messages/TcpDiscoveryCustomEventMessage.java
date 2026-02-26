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

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.managers.discovery.IncompleteDeserializationException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for custom message.
 */
@TcpDiscoveryRedirectToClient
@TcpDiscoveryEnsureDelivery
public class TcpDiscoveryCustomEventMessage extends TcpDiscoveryAbstractTraceableMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private volatile DiscoveryCustomMessage msg;

    /** Serialized message bytes. */
    // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-27627
    @Order(6)
    volatile @Nullable byte[] msgBytes;

    /** {@link Message} representation of original message. */
    // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-27627
    @Order(7)
    volatile @Nullable Message serMsg;

    /**
     * Constructor for {@link DiscoveryMessageFactory}.
     */
    public TcpDiscoveryCustomEventMessage() {
        // No-op.
    }

    /**
     * @param creatorNodeId Creator node id.
     * @param msg Message.
     */
    public TcpDiscoveryCustomEventMessage(UUID creatorNodeId, DiscoveryCustomMessage msg) {
        super(creatorNodeId);

        this.msg = msg;
    }

    /**
     * Copy constructor.
     * @param msg Message.
     */
    public TcpDiscoveryCustomEventMessage(TcpDiscoveryCustomEventMessage msg) {
        super(msg);

        msgBytes = msg.msgBytes;
        serMsg = msg.serMsg;
        this.msg = msg.msg;
    }

    /**
     * Clear deserialized form of wrapped message.
     */
    public void clearMessage() {
        msg = null;
    }

    /**
     * @return Original message.
     */
    public DiscoveryCustomMessage message() {
        return msg;
    }

    /**
     * Prepare message for serialization.
     *
     * @param marsh Marshaller.
     */
    // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-27627
    public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (msg instanceof Message)
            serMsg = (Message)msg;
        else {
            assert msgBytes == null || msg.isMutable() : "Message bytes are not null for immutable message: msg =" + msg;

            msgBytes = U.marshal(marsh, msg);
        }
    }

    /**
     * Finish deserialization.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     */
    // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-27627
    public void finishUnmarhal(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (msg != null)
            return;

        if (serMsg != null)
            msg = (DiscoveryCustomMessage)serMsg;
        else {
            try {
                msg = U.unmarshal(marsh, msgBytes, ldr);
            }
            catch (IgniteCheckedException e) {
                // Try to resurrect a message in a case of deserialization failure
                if (e.getCause() instanceof IncompleteDeserializationException) {
                    msg = ((IncompleteDeserializationException)e.getCause()).message();

                    return;
                }

                throw e;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return super.equals(obj) &&
            obj instanceof TcpDiscoveryCustomEventMessage &&
            Objects.equals(((TcpDiscoveryCustomEventMessage)obj).verifierNodeId(), verifierNodeId());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryCustomEventMessage.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 21;
    }
}
