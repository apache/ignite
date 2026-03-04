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

package org.apache.ignite.internal.managers.discovery;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/** Custom message wrapper with ID of security subject that initiated the current message. */
public class SecurityAwareCustomMessageWrapper extends DiscoverySpiCustomMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Security subject ID. */
    @Order(0)
    UUID secSubjId;

    /** Original message. */
    private DiscoveryCustomMessage delegate;

    /** */
    @Order(1)
    // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-27627
    Message msg;

    /** Serialized message bytes. */
    // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-27627
    @Order(value = 2, method = "messageBytes")
    byte[] msgBytes;

    /** Default constructor for {@link MessageFactory}. */
    public SecurityAwareCustomMessageWrapper() {
        // No-op.
    }

    /** */
    public SecurityAwareCustomMessageWrapper(DiscoveryCustomMessage delegate, UUID secSubjId) {
        this.delegate = delegate;
        this.secSubjId = secSubjId;

        if (delegate instanceof Message)
            msg = (Message)delegate;
    }

    /** Gets security Subject ID. */
    public UUID securitySubjectId() {
        return secSubjId;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return delegate().isMutable();
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return delegate().stopProcess();
    }

    /**
     * @return Delegate.
     */
    public DiscoveryCustomMessage delegate() {
        return msg != null ? (DiscoveryCustomMessage)msg : delegate;
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoveryCustomMessage ackMessage() {
        DiscoveryCustomMessage ack = delegate().ackMessage();

        return ack == null ? null : new SecurityAwareCustomMessageWrapper(ack, secSubjId);
    }

    /** */
    public byte[] messageBytes() {
        if (delegate instanceof Message)
            return null;

        if (msgBytes != null)
            return msgBytes;

        try {
            return msgBytes = U.marshal(jdk(), delegate);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public void messageBytes(byte[] msgBytes) {
        if (F.isEmpty(msgBytes))
            return;

        try {
            delegate = U.unmarshal(jdk(), msgBytes, U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 501;
    }
}
