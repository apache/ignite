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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.SKIP_STORE_FLAG_MASK;

/**
 *
 */
public abstract class GridDhtAtomicAbstractUpdateRequest extends GridCacheMessage implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID. */
    @GridDirectTransient
    protected UUID nodeId;

    /** Future version. */
    protected GridCacheVersion futVer;

    /** Write version. */
    protected GridCacheVersion writeVer;

    /** Write synchronization mode. */
    protected CacheWriteSynchronizationMode syncMode;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** Force transform backups flag. */
    protected boolean forceTransformBackups;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name hash. */
    protected int taskNameHash;

    /**
     * Additional flags.
     */
    protected byte flags;

    /** On response flag. Access should be synced on future. */
    @GridDirectTransient
    private boolean onRes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    protected GridDhtAtomicAbstractUpdateRequest() {
        // N-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param writeVer Write version for cache values.
     * @param syncMode Cache write synchronization mode.
     * @param topVer Topology version.
     * @param forceTransformBackups Force transform backups flag.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    protected GridDhtAtomicAbstractUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        boolean forceTransformBackups,
        UUID subjId,
        int taskNameHash,
        boolean skipStore) {
        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.writeVer = writeVer;
        this.syncMode = syncMode;
        this.topVer = topVer;
        this.forceTransformBackups = forceTransformBackups;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        setFlag(skipStore, SKIP_STORE_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Version assigned on primary node.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Write version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Keep binary flag.
     */
    public abstract boolean keepBinary();

    /**
     * @return Skip write-through to a persistent storage.
     */
    public boolean skipStore() {
        return isFlag(SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return {@code True} if on response flag changed.
     */
    public boolean onResponse() {
        return !onRes && (onRes = true);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Force transform backups flag.
     */
    public boolean forceTransformBackups() {
        return forceTransformBackups;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        cleanup();
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} adds previous value.
     * @param partId Partition.
     * @param prevVal Previous value.
     * @param updateCntr Update counter.
     */
    public abstract void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        int partId,
        @Nullable CacheObject prevVal,
        @Nullable Long updateCntr
    );

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public abstract void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime);

    /**
     * Cleanup values not needed after message was sent.
     */
    protected abstract void cleanup();

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                forceTransformBackups = reader.readBoolean("forceTransformBackups");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 8:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicAbstractUpdateRequest.class);
    }

    /**
     * @return Keys size.
     */
    public abstract int size();

    /**
     * @return Keys size.
     */
    public abstract int nearSize();

    /**
     * @param key Key to check.
     * @return {@code true} if request keys contain key.
     */
    public abstract boolean hasKey(KeyCacheObject key);

    /**
     * @param idx Key index.
     * @return Key.
     */
    public abstract KeyCacheObject key(int idx);

    /**
     * @param idx Partition index.
     * @return Partition id.
     */
    public abstract int partitionId(int idx);

    /**
     * @param updCntr Update counter.
     * @return Update counter.
     */
    public abstract Long updateCounter(int updCntr);

    /**
     * @param idx Near key index.
     * @return Key.
     */
    public abstract KeyCacheObject nearKey(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject previousValue(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @Nullable public abstract EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Near key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject nearValue(int idx);

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    @Nullable public abstract EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx);

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public abstract GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return TTL.
     */
    public abstract long ttl(int idx);

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public abstract long nearTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public abstract long conflictExpireTime(int idx);

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public abstract long nearExpireTime(int idx);

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public abstract Object[] invokeArguments();

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    protected void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    protected boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeBoolean("forceTransformBackups", forceTransformBackups))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("writeVer", writeVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override public byte fieldsCount() {
        return 11;
    }
}
