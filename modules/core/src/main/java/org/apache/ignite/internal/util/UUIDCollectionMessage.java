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

package org.apache.ignite.internal.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Collection of UUIDs.
 */
public class UUIDCollectionMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectCollection(UUID.class)
    private Collection<UUID> uuids;

    /**
     * Empty constructor required for direct marshalling.
     */
    public UUIDCollectionMessage() {
        // No-op.
    }

    /**
     * @param uuids UUIDs to wrap.
     */
    public UUIDCollectionMessage(Collection<UUID> uuids) {
        this.uuids = uuids;
    }

    /**
     * @param uuids UUIDs.
     * @return Message.
     */
    public static UUIDCollectionMessage of(UUID... uuids) {
        if (uuids == null || uuids.length == 0)
            return null;

        List<UUID> list = uuids.length == 1 ? Collections.singletonList(uuids[0]) : Arrays.asList(uuids);

        return new UUIDCollectionMessage(list);
    }

    /**
     * @return The collection of UUIDs that was wrapped.
     */
    public Collection<UUID> uuids() {
        return uuids;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
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
                if (!writer.writeCollection("uuids", uuids, MessageCollectionItemType.UUID))
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
                uuids = reader.readCollection("uuids", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(UUIDCollectionMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 115;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        UUIDCollectionMessage that = (UUIDCollectionMessage)o;

        return uuids == that.uuids || (uuids != null && uuids.equals(that.uuids));
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return uuids != null ? uuids.hashCode() : 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UUIDCollectionMessage.class, this, "uuidsSize", uuids == null ? null : uuids.size());
    }
}
