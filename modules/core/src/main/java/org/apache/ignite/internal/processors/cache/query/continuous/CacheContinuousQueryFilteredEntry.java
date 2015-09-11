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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.nio.ByteBuffer;
import javax.cache.event.EventType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous query entry.
 */
public class CacheContinuousQueryFilteredEntry extends CacheContinuousQueryEntry {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private EventType evtType;

    /** Cache name. */
    private int cacheId;

    /** Partition. */
    private int part;

    /** Update index. */
    private long updateIdx;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private AffinityTopologyVersion topVer;

    /**
     * Required by {@link Message}.
     */
    public CacheContinuousQueryFilteredEntry() {
        // No-op.
    }

    /**
     * @param e Cache continuous query entry.
     */
    CacheContinuousQueryFilteredEntry(CacheContinuousQueryEntry e) {
        this.cacheId = e.cacheId();
        this.evtType = e.eventType();
        this.part = e.partition();
        this.updateIdx = e.updateIndex();
        this.topVer = e.topologyVersion();
    }

    /**
     * @return Topology version if applicable.
     */
    @Nullable AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache ID.
     */
    int cacheId() {
        return cacheId;
    }

    /**
     * @return Event type.
     */
    EventType eventType() {
        return evtType;
    }

    /**
     * @return Partition.
     */
    int partition() {
        return part;
    }

    /**
     * @return Update index.
     */
    long updateIndex() {
        return updateIdx;
    }

    /** {@inheritDoc} */
    @Override boolean filtered() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 115;
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
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte("evtType", evtType != null ? (byte)evtType.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("updateIdx", updateIdx))
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
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                byte evtTypeOrd;

                evtTypeOrd = reader.readByte("evtType");

                if (!reader.isLastRead())
                    return false;

                evtType = CacheContinuousQueryEntry.eventTypeFromOrdinal(evtTypeOrd);

                reader.incrementState();

            case 2:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                updateIdx = reader.readLong("updateIdx");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(CacheContinuousQueryFilteredEntry.class);
    }

    /** {@inheritDoc} */
    @Override void prepareMarshal(GridCacheContext cctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override void unmarshal(GridCacheContext cctx, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryFilteredEntry.class, this);
    }
}