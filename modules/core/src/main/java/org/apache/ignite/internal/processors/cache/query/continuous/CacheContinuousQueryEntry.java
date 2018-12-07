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
import org.apache.ignite.internal.GridCodegenConverter;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous query entry.
 */
public class CacheContinuousQueryEntry implements GridCacheDeployable, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final byte BACKUP_ENTRY = 0b0001;

    /** */
    private static final byte FILTERED_ENTRY = 0b0010;

    /** */
    private static final byte KEEP_BINARY = 0b0100;

    /** */
    private static final EventType[] EVT_TYPE_VALS = EventType.values();

    /**
     * @param ord Event type ordinal value.
     * @return Event type.
     */
    @Nullable public static EventType eventTypeFromOrdinal(int ord) {
        return ord >= 0 && ord < EVT_TYPE_VALS.length ? EVT_TYPE_VALS[ord] : null;
    }

    /** */
    @GridCodegenConverter(
        type = byte.class,
        get = "evtType != null ? (byte)evtType.ordinal() : -1",
        set = "eventTypeFromOrdinal($val$)"
    )
    private EventType evtType;

    /** Key. */
    @GridToStringInclude
    @GridCodegenConverter(get = "isFiltered() ? null : key")
    private KeyCacheObject key;

    /** New value. */
    @GridToStringInclude
    @GridCodegenConverter(get = "isFiltered() ? null : newVal")
    private CacheObject newVal;

    /** Old value. */
    @GridToStringInclude
    @GridCodegenConverter(get = "isFiltered() ? null : oldVal")
    private CacheObject oldVal;

    /** Cache name. */
    private int cacheId;

    /** Deployment info. */
    @GridToStringExclude
    @GridDirectTransient
    private GridDeploymentInfo depInfo;

    /** Partition. */
    private int part;

    /** Update counter. */
    private long updateCntr;

    /** Flags. */
    private byte flags;

    /** */
    @GridToStringInclude
    private AffinityTopologyVersion topVer;

    /** */
    private long filteredCnt;

    /**
     * Required by {@link Message}.
     */
    public CacheContinuousQueryEntry() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param evtType Event type.
     * @param key Key.
     * @param newVal New value.
     * @param oldVal Old value.
     * @param keepBinary Keep binary flag.
     * @param part Partition.
     * @param updateCntr Update partition counter.
     * @param topVer Topology version if applicable.
     * @param flags Flags.
     */
    CacheContinuousQueryEntry(
        int cacheId,
        EventType evtType,
        KeyCacheObject key,
        @Nullable CacheObject newVal,
        @Nullable CacheObject oldVal,
        boolean keepBinary,
        int part,
        long updateCntr,
        @Nullable AffinityTopologyVersion topVer,
        byte flags) {
        this.cacheId = cacheId;
        this.evtType = evtType;
        this.key = key;
        this.newVal = newVal;
        this.oldVal = oldVal;
        this.part = part;
        this.updateCntr = updateCntr;
        this.topVer = topVer;
        this.flags = flags;

        if (keepBinary)
            this.flags |= KEEP_BINARY;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
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
     * @return Update counter.
     */
    long updateCounter() {
        return updateCntr;
    }

    /**
     * Mark that entry create on backup.
     */
    void markBackup() {
        flags |= BACKUP_ENTRY;
    }

    /**
     * Mark that entry filtered.
     */
    void markFiltered() {
        flags |= FILTERED_ENTRY;
        depInfo = null;
    }

    /**
     * @param topVer Topology version.
     */
    void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @param filteredCnt Number of entries filtered before this entry.
     */
    void filteredCount(long filteredCnt) {
        assert filteredCnt >= 0 : filteredCnt;

        this.filteredCnt = filteredCnt;
    }

    /**
     * @return Number of entries filtered before this entry.
     */
    long filteredCount() {
        return filteredCnt;
    }

    /**
     * @return If entry filtered then will return light-weight <i><b>new entry</b></i> without values and key
     * (avoid to huge memory consumption), otherwise {@code this}.
     */
    CacheContinuousQueryEntry copyWithDataReset() {
        if (!isFiltered())
            return this;

        return new CacheContinuousQueryEntry(
            cacheId,
            null,
            null,
            null,
            null,
            false,
            part,
            updateCntr,
            topVer,
            flags);
    }

    /**
     * @return {@code True} if entry sent by backup node.
     */
    boolean isBackup() {
        return (flags & BACKUP_ENTRY) != 0;
    }

    /**
     * @return {@code True} if entry was filtered.
     */
    boolean isFiltered() {
        return (flags & FILTERED_ENTRY) != 0;
    }

    /**
     * @return Keep binary flag.
     */
    boolean isKeepBinary() {
        return (flags & KEEP_BINARY) != 0;
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException In case of error.
     */
    void prepareMarshal(GridCacheContext cctx) throws IgniteCheckedException {
        if (key != null)
            key.prepareMarshal(cctx.cacheObjectContext());

        if (newVal != null)
            newVal.prepareMarshal(cctx.cacheObjectContext());

        if (oldVal != null)
            oldVal.prepareMarshal(cctx.cacheObjectContext());
    }

    /**
     * @param cctx Cache context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException In case of error.
     */
    void unmarshal(GridCacheContext cctx, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        if (!isFiltered()) {
            if (key != null)
                key.finishUnmarshal(cctx.cacheObjectContext(), ldr);

            if (newVal != null)
                newVal.finishUnmarshal(cctx.cacheObjectContext(), ldr);

            if (oldVal != null)
                oldVal.finishUnmarshal(cctx.cacheObjectContext(), ldr);
        }
    }

    /**
     * @return Key.
     */
    KeyCacheObject key() {
        return key;
    }

    /**
     * @return New value.
     */
    CacheObject value() {
        return newVal;
    }

    /**
     * @return Old value.
     */
    CacheObject oldValue() {
        return oldVal;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void prepare(GridDeploymentInfo depInfo) {
        this.depInfo = depInfo;
    }

    /** {@inheritDoc} */
    @Override public GridDeploymentInfo deployInfo() {
        return depInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 96;
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
                if (!writer.writeLong("filteredCnt", filteredCnt))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("key", isFiltered() ? null : key))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("newVal", isFiltered() ? null : newVal))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("oldVal", isFiltered() ? null : oldVal))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeLong("updateCntr", updateCntr))
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
                evtType = eventTypeFromOrdinal(reader.readByte("evtType"));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                filteredCnt = reader.readLong("filteredCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                newVal = reader.readMessage("newVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                oldVal = reader.readMessage("oldVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                updateCntr = reader.readLong("updateCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheContinuousQueryEntry.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryEntry.class, this);
    }
}
