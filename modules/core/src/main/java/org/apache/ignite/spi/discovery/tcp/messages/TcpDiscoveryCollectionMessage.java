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

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: Remove/revise after https://issues.apache.org/jira/browse/IGNITE-25883
 * Message to transfer a collection of {@link TcpDiscoveryAbstractMessage} with the original order.
 * Several of them might be a {@link Message}, several may not and require the original marshalling.
 */
public class TcpDiscoveryCollectionMessage implements TcpDiscoveryMarshallableMessage {
    /** Marshallable or Java-serializable pending messages which are not a {@link Message}. */
    @Nullable private Collection<TcpDiscoveryAbstractMessage> marshallableMsgs;

    /** Marshalled {@link #marshallableMsgs}. */
    @Order(0)
    @GridToStringExclude
    @Nullable byte[] marshallableMsgsBytes;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryCollectionMessage() {
        // No-op.
    }

    /** @param msgs Discovery messages to hold. */
    public TcpDiscoveryCollectionMessage(Collection<TcpDiscoveryAbstractMessage> msgs) {
        messages(msgs);
    }

    /** @param marsh marshaller. */
    @Override public void prepareMarshal(Marshaller marsh) {
        if (marshallableMsgs != null && marshallableMsgsBytes == null) {
            try {
                marshallableMsgsBytes = U.marshal(marsh, marshallableMsgs);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal marshallable pending messages.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (marshallableMsgsBytes != null && marshallableMsgs == null) {
            try {
                marshallableMsgs = U.unmarshal(marsh, marshallableMsgsBytes, clsLdr);

                marshallableMsgsBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal marshallable pending messages.", e);
            }
        }
    }

    /**
     * Gets pending messages sent to new node by its previous.
     *
     * @return Pending messages from previous node.
     */
    public Collection<TcpDiscoveryAbstractMessage> messages() {
        if(F.isEmpty(marshallableMsgs))
            return Collections.emptyList();

        return marshallableMsgs;
    }

    /**
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     */
    public void messages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs) {
        marshallableMsgsBytes = null;
        marshallableMsgs = null;

        if (F.isEmpty(msgs))
            return;

        marshallableMsgs = msgs;
    }

    /** */
    private boolean filterWritableMessage(TcpDiscoveryAbstractMessage m) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -108;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryCollectionMessage.class, this, "super", super.toString());
    }
}
