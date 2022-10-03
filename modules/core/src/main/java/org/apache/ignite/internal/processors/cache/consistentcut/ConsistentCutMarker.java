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
import java.util.Objects;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 * Marker that inites {@link ConsistentCut}.
 */
public class ConsistentCutMarker implements Message, Comparable<ConsistentCutMarker> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 201;

    /** Incremental version. */
    @GridToStringInclude
    private long ts;

    /** */
    @GridToStringInclude
    private AffinityTopologyVersion topVer;

    /** */
    public ConsistentCutMarker() {}

    /** */
    public ConsistentCutMarker(long ts, AffinityTopologyVersion topVer) {
        this.ts = ts;
        this.topVer = topVer;
    }

    /** */
    public long timestamp() {
        return ts;
    }

    /** */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCutMarker.class, this);
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
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("ts", ts))
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
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ts = reader.readLong("ts");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ConsistentCutMarker.class);
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
        return o instanceof ConsistentCutMarker
            && topVer.equals(((ConsistentCutMarker)o).topVer)
            && ts == ((ConsistentCutMarker)o).ts;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(topVer, ts);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull ConsistentCutMarker o) {
        int cmp;

        if ((cmp = topVer.compareTo(o.topVer)) != 0)
            return cmp;

        return Long.compare(ts, o.ts);
    }
}
