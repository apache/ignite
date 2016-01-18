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
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.GridLongList;
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
    private static final EventType[] EVT_TYPE_VALS = EventType.values();

    /**
     * @param ord Event type ordinal value.
     * @return Event type.
     */
    @Nullable public static EventType eventTypeFromOrdinal(int ord) {
        return ord >= 0 && ord < EVT_TYPE_VALS.length ? EVT_TYPE_VALS[ord] : null;
    }

    /** */
    private EventType evtType;

    /** Key. */
    @GridToStringInclude
    private KeyCacheObject key;

    /** New value. */
    @GridToStringInclude
    private CacheObject newVal;

    /** Old value. */
    @GridToStringInclude
    private CacheObject oldVal;

    /** Cache name. */
    private int cacheId;

    /** Deployment info. */
    @GridToStringExclude
    @GridDirectTransient
    private GridDeploymentInfo depInfo;

    /** Partition. */
    private int _part;

    /** Update counter. */
    private long _updateCntr;

    /** Flags. */
    private byte flags;

    /** */
    @GridToStringInclude
    private AffinityTopologyVersion _topVer;

    /** Filtered events. */
    private GridLongList filteredEvts;

    /** Keep binary. */
    private boolean keepBinary;

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
     * @param part Partition.
     * @param updateCntr Update partition counter.
     * @param topVer Topology version if applicable.
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
        @Nullable AffinityTopologyVersion topVer) {
        this.cacheId = cacheId;
        this.evtType = evtType;
        this.key = key;
        this.newVal = newVal;
        this.oldVal = oldVal;
        this._part = part;
        this._updateCntr = updateCntr;
        this._topVer = topVer;
        this.keepBinary = keepBinary;
    }

    /**
     * @return Topology version if applicable.
     */
    @Nullable AffinityTopologyVersion topologyVersion() {
        return _topVer;
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
        return _part;
    }

    /**
     * @return Update counter.
     */
    long updateCounter() {
        return _updateCntr;
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
        newVal = null;
        oldVal = null;
        key = null;
        depInfo = null;
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
        return keepBinary;
    }

    /**
     * @param cntrs Filtered events.
     */
    void filteredEvents(GridLongList cntrs) {
        filteredEvts = cntrs;
    }

    /**
     * @return previous filtered events.
     */
    long[] filteredEvents() {
        return filteredEvts == null ? null : filteredEvts.array();
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException In case of error.
     */
    void prepareMarshal(GridCacheContext cctx) throws IgniteCheckedException {
        assert key != null;

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
    @Override public void prepare(GridDeploymentInfo depInfo) {
        this.depInfo = depInfo;
    }

    /** {@inheritDoc} */
    @Override public GridDeploymentInfo deployInfo() {
        return depInfo;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
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
                if (!writer.writeInt("_part", _part))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("_topVer", _topVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("_updateCntr", _updateCntr))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByte("evtType", evtType != null ? (byte)evtType.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("filteredEvts", filteredEvts))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("keepBinary", keepBinary))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("newVal", newVal))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("oldVal", oldVal))
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
                _part = reader.readInt("_part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                _topVer = reader.readMessage("_topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                _updateCntr = reader.readLong("_updateCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                byte evtTypeOrd;

                evtTypeOrd = reader.readByte("evtType");

                if (!reader.isLastRead())
                    return false;

                evtType = eventTypeFromOrdinal(evtTypeOrd);

                reader.incrementState();

            case 5:
                filteredEvts = reader.readMessage("filteredEvts");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                keepBinary = reader.readBoolean("keepBinary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                newVal = reader.readMessage("newVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                oldVal = reader.readMessage("oldVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheContinuousQueryEntry.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 11;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryEntry.class, this);
    }
}
