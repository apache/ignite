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
package org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message is used to send acks for {@link Latch} instances management.
 */
public class LatchAckMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Latch id. */
    private String latchId;

    /** Latch topology version. */
    private AffinityTopologyVersion topVer;

    /** Flag indicates that ack is final. */
    private boolean isFinal;

    /**
     * Constructor.
     *
     * @param latchId Latch id.
     * @param topVer Latch topology version.
     * @param isFinal Final acknowledgement flag.
     */
    public LatchAckMessage(String latchId, AffinityTopologyVersion topVer, boolean isFinal) {
        this.latchId = latchId;
        this.topVer = topVer;
        this.isFinal = isFinal;
    }

    /**
     * Empty constructor for marshalling purposes.
     */
    public LatchAckMessage() {
    }

    /**
     * @return Latch id.
     */
    public String latchId() {
        return latchId;
    }

    /**
     * @return Latch topology version.
     */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    /**
     * @return {@code} if ack is final.
     */
    public boolean isFinal() {
        return isFinal;
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
                if (!writer.writeBoolean("isFinal", isFinal))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("latchId", latchId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
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
                isFinal = reader.readBoolean("isFinal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                latchId = reader.readString("latchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(LatchAckMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 135;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
