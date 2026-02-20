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

import javax.cache.event.EventType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridCodegenConverter;
import org.apache.ignite.internal.Order;
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
import org.jetbrains.annotations.Nullable;

/**
 * Continuous query entry.
 */
public class CacheContinuousQueryEntry implements GridCacheDeployable, Message {
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
    @Order(value = 0, method = "eventType")
    @GridCodegenConverter(
        type = byte.class,
        get = "evtType != null ? (byte)evtType.ordinal() : -1",
        set = "eventTypeFromOrdinal($val$)"
    )
    private EventType evtType;

    /** Key. */
    @GridToStringInclude
    @Order(1)
    @GridCodegenConverter(get = "isFiltered() ? null : key")
    private KeyCacheObject key;

    /** New value. */
    @GridToStringInclude
    @Order(value = 2, method = "newValue")
    @GridCodegenConverter(get = "isFiltered() ? null : newVal")
    private CacheObject newVal;

    /** Old value. */
    @GridToStringInclude
    @Order(value = 3, method = "oldValue")
    @GridCodegenConverter(get = "isFiltered() ? null : oldVal")
    private CacheObject oldVal;

    /** Cache name. */
    @Order(4)
    private int cacheId;

    /** Deployment info. */
    @GridToStringExclude
    private GridDeploymentInfo depInfo;

    /** Partition. */
    @Order(value = 5, method = "partition")
    private int part;

    /** Update counter. */
    @Order(value = 6, method = "updateCounter")
    private long updateCntr;

    /** Flags. */
    @Order(7)
    private byte flags;

    /** */
    @GridToStringInclude
    @Order(value = 8, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** */
    @Order(value = 9, method = "filteredCount")
    private long filteredCnt;

    /**
     * Empty constructor.
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
        byte flags
    ) {
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
     * @param cacheId Cache id.
     * @param partId Partition id entry related to.
     * @param topVer Topology version.
     * @param cntr Update counter entry related to.
     * @param filtered Number of filtered entries prior to current one.
     * @return Entry instance.
     */
    public static CacheContinuousQueryEntry createFilteredEntry(
        int cacheId,
        int partId,
        AffinityTopologyVersion topVer,
        long cntr,
        long filtered
    ) {
        CacheContinuousQueryEntry e = new CacheContinuousQueryEntry(cacheId,
            null,
            null,
            null,
            null,
            false,
            partId,
            cntr,
            topVer,
            (byte)0);

        e.markFiltered();
        e.filteredCount(filtered);

        return e;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Topology version if applicable.
     */
    @Nullable public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @param cacheId Cache ID.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /**
     * @return Event type.
     */
    public EventType eventType() {
        return evtType;
    }

    /**
     * @param evtType Event type.
     */
    public void eventType(EventType evtType) {
        this.evtType = evtType;
    }

    /**
     * @return Partition.
     */
    public int partition() {
        return part;
    }

    /**
     * @param part Partition.
     */
    public void partition(int part) {
        this.part = part;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updateCntr;
    }

    /**
     * @param updateCntr Update counter.
     */
    public void updateCounter(long updateCntr) {
        this.updateCntr = updateCntr;
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
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @param filteredCnt Number of entries filtered before this entry.
     */
    public void filteredCount(long filteredCnt) {
        assert filteredCnt >= 0 : filteredCnt;

        this.filteredCnt = filteredCnt;
    }

    /**
     * @return Number of entries filtered before this entry.
     */
    public long filteredCount() {
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
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return New value.
     */
    public CacheObject newValue() {
        return newVal;
    }

    /**
     * @param newVal New value.
     */
    public void newValue(CacheObject newVal) {
        this.newVal = newVal;
    }

    /**
     * @return Old value.
     */
    public CacheObject oldValue() {
        return oldVal;
    }

    /**
     * @param oldVal Old value.
     */
    public void oldValue(CacheObject oldVal) {
        this.oldVal = oldVal;
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
    @Override public String toString() {
        return S.toString(CacheContinuousQueryEntry.class, this);
    }
}
