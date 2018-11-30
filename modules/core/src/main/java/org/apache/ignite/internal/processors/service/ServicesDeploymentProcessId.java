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

package org.apache.ignite.internal.processors.service;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Services deployment process id.
 */
public class ServicesDeploymentProcessId implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    @Nullable private AffinityTopologyVersion topVer;

    /** Request's id. */
    @Nullable private IgniteUuid reqId;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServicesDeploymentProcessId() {
    }

    /**
     * @param topVer Topology version.
     */
    ServicesDeploymentProcessId(@NotNull AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @param reqId Request's id.
     */
    ServicesDeploymentProcessId(@NotNull IgniteUuid reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Requests id.
     */
    public IgniteUuid requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("reqId", reqId))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                reqId = reader.readIgniteUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ServicesDeploymentProcessId.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 167;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServicesDeploymentProcessId id = (ServicesDeploymentProcessId)o;

        return F.eq(topVer, id.topVer) && F.eq(reqId, id.reqId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(topVer, reqId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServicesDeploymentProcessId.class, this);
    }
}
