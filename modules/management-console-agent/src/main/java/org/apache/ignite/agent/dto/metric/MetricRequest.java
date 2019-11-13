/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.metric;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Metrics request message. Sent from Management Console agent to any arbitrary node to request latest metrics.
 */
public class MetricRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Protocol version 1. */
    public static final short PROTO_VER_1 = 1;

    /** */
    private short protoVer = PROTO_VER_1;

    /** */
    private int schemaVer;

    /** */
    private boolean schemaOnly;

    /**
     * Default metrics request.
     */
    public MetricRequest() {
        this(-1);
    }

    /**
     * @param schemaVer Schema version.
     */
    public MetricRequest(int schemaVer) {
        this(schemaVer, false);
    }

    /**
     * @param schemaVer Schema version.
     * @param schemaOnly Schema only flag.
     */
    public MetricRequest(int schemaVer, boolean schemaOnly) {
        this.schemaVer = schemaVer;
        this.schemaOnly = schemaOnly;
    }

    /**
     * @return Protocol version.
     */
    public short protocolVersion() {
        return protoVer;
    }

    /**
     * @return Schema version.
     */
    public int schemaVersion() {
        return schemaVer;
    }

    /**
     * @return Schema only flag.
     */
    public boolean schemaOnly() {
        return schemaOnly;
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
                if (!writer.writeShort("protoVer", protoVer))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean("schemaOnly", schemaOnly))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("schemaVer", schemaVer))
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
                protoVer = reader.readShort("protoVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                schemaOnly = reader.readBoolean("schemaOnly");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                schemaVer = reader.readInt("schemaVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MetricRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -63;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetricRequest.class, this);
    }
}
