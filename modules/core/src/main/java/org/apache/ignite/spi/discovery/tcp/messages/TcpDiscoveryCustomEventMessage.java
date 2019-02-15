/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapped for custom message.
 */
@TcpDiscoveryRedirectToClient
@TcpDiscoveryEnsureDelivery
public class TcpDiscoveryCustomEventMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private transient volatile DiscoverySpiCustomMessage msg;

    /** */
    private byte[] msgBytes;

    /**
     * @param creatorNodeId Creator node id.
     * @param msg Message.
     * @param msgBytes Serialized message.
     */
    public TcpDiscoveryCustomEventMessage(UUID creatorNodeId, @Nullable DiscoverySpiCustomMessage msg,
        @NotNull byte[] msgBytes) {
        super(creatorNodeId);

        this.msg = msg;
        this.msgBytes = msgBytes;
    }

    /**
     * Copy constructor.
     * @param msg Message.
     */
    public TcpDiscoveryCustomEventMessage(TcpDiscoveryCustomEventMessage msg) {
        super(msg);

        this.msgBytes = msg.msgBytes;
        this.msg = msg.msg;
    }

    /**
     * Clear deserialized form of wrapped message.
     */
    public void clearMessage() {
        msg = null;
    }

    /**
     * @return Serialized message.
     */
    public byte[] messageBytes() {
        return msgBytes;
    }

    /**
     * @param msg Message.
     * @param msgBytes Serialized message.
     */
    public void message(@Nullable DiscoverySpiCustomMessage msg, @NotNull byte[] msgBytes) {
        this.msg = msg;
        this.msgBytes = msgBytes;
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Classloader.
     * @return Deserialized message,
     * @throws java.lang.Throwable if unmarshal failed.
     */
    @Nullable public DiscoverySpiCustomMessage message(@NotNull Marshaller marsh, ClassLoader ldr) throws Throwable {
        if (msg == null) {
            msg = U.unmarshal(marsh, msgBytes, ldr);

            assert msg != null;
        }

        return msg;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return super.equals(obj) &&
            obj instanceof TcpDiscoveryCustomEventMessage &&
            F.eq(
                ((TcpDiscoveryCustomEventMessage)obj).verifierNodeId(),
                verifierNodeId());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryCustomEventMessage.class, this, "super", super.toString());
    }
}
