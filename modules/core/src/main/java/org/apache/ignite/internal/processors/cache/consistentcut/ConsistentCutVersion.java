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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Composite version of Consistent Cut. It consists of two fields: incremental version and timestamp of starting
 * Consistent Cut. Both fields set on the coordinator node.
 */
public class ConsistentCutVersion implements Message, Comparable<ConsistentCutVersion> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 190;

    /** Incremental version. */
    @GridToStringInclude
    private long ver;

    /** Consistent Cut coordinator node ID. */
    @GridToStringInclude
    private UUID cutCrdNodeId;

    /** */
    public ConsistentCutVersion() {}

    /** */
    public ConsistentCutVersion(long ver, UUID cutCrdNodeId) {
        this.ver = ver;
        this.cutCrdNodeId = cutCrdNodeId;
    }

    /** */
    public long version() {
        return ver;
    }

    /** Consistent Cut coordinator node ID. */
    public UUID cutCrdNodeId() {
        return cutCrdNodeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCutVersion.class, this);
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
                if (!writer.writeLong("ver", ver))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid("cutCrdNodeId", cutCrdNodeId))
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
                ver = reader.readLong("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                cutCrdNodeId = reader.readUuid("cutCrdNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ConsistentCutVersion.class);
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
    @Override public boolean equals(Object o) {
        return o instanceof ConsistentCutVersion && ver == ((ConsistentCutVersion)o).version();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Long.hashCode(ver);
    }

    /** {@code null} means the least possible version. */
    public int compareToNullable(@Nullable ConsistentCutVersion o) {
        return o == null ? 1 : compareTo(o);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull ConsistentCutVersion o) {
        return Long.compare(ver, o.ver);
    }
}
