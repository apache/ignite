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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class RequestSnapshotMessage implements Message {
    /** Snapshot request message type (value is {@code 177}). */
    public static final short TYPE_CODE = 177;

    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Unique snapshot name. */
    private String snpName;

    /** Map of requested partitions to be snapshotted. */
    @GridDirectMap(keyType = Integer.class, valueType = GridIntList.class)
    private Map<Integer, GridIntList> parts;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public RequestSnapshotMessage() {
        // No-op.
    }

    /**
     * @param snpName Unique snapshot name.
     * @param parts Map of requested partitions to be snapshotted.
     */
    public RequestSnapshotMessage(
        String snpName,
        Map<Integer, GridIntList> parts
    ) {
        assert parts != null && !parts.isEmpty();

        this.snpName = snpName;
        this.parts = U.newHashMap(parts.size());

        for (Map.Entry<Integer, GridIntList> e : parts.entrySet())
            this.parts.put(e.getKey(), e.getValue().copy());
    }

    /**
     * @return Unique snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return The demanded cache group partions per each cache group.
     */
    public Map<Integer, GridIntList> parts() {
        return parts;
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
                if (!writer.writeMap("parts", parts, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("snpName", snpName))
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
                parts = reader.readMap("parts", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                snpName = reader.readString("snpName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(RequestSnapshotMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
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
    @Override public String toString() {
        return S.toString(RequestSnapshotMessage.class, this);
    }
}
