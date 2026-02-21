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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
    /** {@link TcpDiscoveryAbstractMessage} pending messages which are a {@link Message}. */
    @Order(value = 0, method = "writableMessages")
    @Nullable private Map<Integer, Message> writableMsgs;

    /** Marshallable or Java-serializable pending messages which are not a {@link Message}. */
    @Nullable private Map<Integer, TcpDiscoveryAbstractMessage> marshallableMsgs;

    /** Marshalled {@link #marshallableMsgs}. */
    @Order(value = 1, method = "marshallableMessagesBytes")
    @GridToStringExclude
    @Nullable private byte[] marshallableMsgsBytes;

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
        assert marshallableMsgsBytes != null || marshallableMsgs != null || writableMsgs != null;

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
     * @return Writable messages by their order.
     * @see #messages()
     */
    public @Nullable Map<Integer, Message> writableMessages() {
        return writableMsgs;
    }

    /** @param msgs Writable messages by their order. */
    public void writableMessages(@Nullable Map<Integer, Message> msgs) {
        writableMsgs = msgs;
    }

    /** @return Bytes of {@link #marshallableMsgs}. */
    public @Nullable byte[] marshallableMessagesBytes() {
        return marshallableMsgsBytes;
    }

    /** @param marshallableMsgsBytes Bytes of {@link #marshallableMsgs}. */
    public void marshallableMessagesBytes(@Nullable byte[] marshallableMsgsBytes) {
        this.marshallableMsgsBytes = marshallableMsgsBytes;
    }

    /**
     * Gets pending messages sent to new node by its previous.
     *
     * @return Pending messages from previous node.
     */
    public Collection<TcpDiscoveryAbstractMessage> messages() {
        assert !F.isEmpty(writableMsgs) || !F.isEmpty(marshallableMsgs);

        int totalSz = (F.isEmpty(writableMsgs) ? 0 : writableMsgs.size())
            + (F.isEmpty(marshallableMsgs) ? 0 : marshallableMsgs.size());

        List<TcpDiscoveryAbstractMessage> res = new ArrayList<>(totalSz);

        for (int i = 0; i < totalSz; ++i) {
            Message m = F.isEmpty(writableMsgs) ? null : writableMsgs.get(i);

            if (m == null) {
                TcpDiscoveryAbstractMessage sm = marshallableMsgs.get(i);

                assert sm != null;

                res.add(sm);
            }
            else {
                assert marshallableMsgs == null || marshallableMsgs.get(i) == null;
                assert m instanceof TcpDiscoveryAbstractMessage;

                res.add((TcpDiscoveryAbstractMessage)m);
            }
        }

        return res;
    }

    /**
     * Sets pending messages to send to new node.
     *
     * @param msgs Pending messages to send to new node.
     */
    public void messages(@Nullable Collection<TcpDiscoveryAbstractMessage> msgs) {
        marshallableMsgsBytes = null;

        if (F.isEmpty(msgs)) {
            marshallableMsgs = null;
            writableMsgs = null;

            return;
        }

        // Keeps the original message order.
        int idx = 0;

        for (TcpDiscoveryAbstractMessage m : msgs) {
            if (m instanceof Message) {
                if (writableMsgs == null)
                    writableMsgs = U.newHashMap(msgs.size());

                writableMsgs.put(idx++, (Message)m);

                continue;
            }

            if (marshallableMsgs == null)
                marshallableMsgs = U.newHashMap(msgs.size());

            marshallableMsgs.put(idx++, m);
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -111;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryCollectionMessage.class, this, "super", super.toString());
    }
}
