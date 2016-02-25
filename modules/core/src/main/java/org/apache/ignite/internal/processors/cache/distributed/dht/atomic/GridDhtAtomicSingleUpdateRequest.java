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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import javax.cache.processor.EntryProcessor;
import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

public class GridDhtAtomicSingleUpdateRequest extends GridCacheMessage implements GridCacheDeployable, GridDhtAtomicUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID. */
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Write version. */
    private GridCacheVersion writeVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Key. */
    @GridToStringInclude
    private KeyCacheObject key;

    /** Value. */
    @GridToStringInclude
    private CacheObject val;

    /** Previous value. */
    @GridToStringInclude
    private CacheObject prevVal;

    /** Conflict version. */
    @GridToStringInclude
    private GridCacheVersion conflictVer;

    /** TTL. */
    private long ttl = CU.TTL_NOT_CHANGED;

    /** Conflict expire time. */
    private long conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;

    /** Near TTL. */
    private long nearTtl = CU.TTL_NOT_CHANGED;

    /** Near expire time. */
    private long nearExpireTime = CU.EXPIRE_TIME_CALCULATE;

    /** Near key. */
    @GridToStringInclude
    private KeyCacheObject nearKey;

    /** Near value. */
    @GridToStringInclude
    private CacheObject nearVal;

    /** Entry processor. */
    @GridDirectTransient
    private EntryProcessor<Object, Object, Object> entryProcessor;

    /** Entry processor bytes. */
    private byte[] entryProcessorBytes;

    /** Near entry processor. */
    @GridDirectTransient
    private EntryProcessor<Object, Object, Object> nearEntryProcessor;

    /** Near entry processor bytes. */
    private byte[] nearEntryProcessorBytes;

    /** Update counter. */
    private long updateCntr = -1;

    /** Partition ID. */
    @GridDirectTransient
    private int partId;

    /** Local previous value. */
    @GridDirectTransient
    private CacheObject locPrevVal;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Force transform backups flag. */
    private boolean forceTransformBackups;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** On response flag. Access should be synced on future. */
    @GridDirectTransient
    private boolean onRes;

    /** Keep binary flag. */
    private boolean keepBinary;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicSingleUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param writeVer Write version for cache values.
     * @param invokeArgs Optional arguments for entry processor.
     * @param syncMode Cache write synchronization mode.
     * @param topVer Topology version.
     * @param forceTransformBackups Force transform backups flag.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param addDepInfo Deployment info.
     */
    public GridDhtAtomicSingleUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        boolean forceTransformBackups,
        UUID subjId,
        int taskNameHash,
        Object[] invokeArgs,
        boolean addDepInfo,
        boolean keepBinary
    ) {
        assert invokeArgs == null || forceTransformBackups;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.writeVer = writeVer;
        this.syncMode = syncMode;
        this.topVer = topVer;
        this.forceTransformBackups = forceTransformBackups;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.invokeArgs = invokeArgs;
        this.addDepInfo = addDepInfo;
        this.keepBinary = keepBinary;
    }

    /**
     * @return Force transform backups flag.
     */
    @Override public boolean forceTransformBackups() {
        return forceTransformBackups;
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} adds previous value.
     * @param prevVal Previous value.
     */
    @Override public void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        int partId,
        @Nullable CacheObject prevVal,
        @Nullable Long updateIdx,
        boolean storeLocPrevVal) {
        this.key = key;

        this.partId = partId;

        this.locPrevVal = storeLocPrevVal ? prevVal : null;

        if (forceTransformBackups) {
            assert entryProcessor != null;

            this.entryProcessor = entryProcessor;
        }
        else
            this.val = val;

        if (addPrevVal)
            this.prevVal = prevVal;

        if (updateIdx != null)
            updateCntr = updateIdx;

        this.conflictVer = conflictVer;

        if (ttl >= 0)
            this.ttl = ttl;

        if (conflictExpireTime >= 0)
            this.conflictExpireTime = conflictExpireTime;
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    @Override public void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {

        nearKey = key;

        if (forceTransformBackups) {
            assert entryProcessor != null;

            nearEntryProcessor = entryProcessor;
        }
        else
            nearVal = val;

        if (ttl >= 0)
            nearTtl = ttl;

        if (expireTime >= 0)
            nearExpireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID.
     */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Subject ID.
     */
    @Override public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name.
     */
    @Override public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Keys size.
     */
    @Override public int size() {
        return key != null ? 1 : 0;
    }

    /**
     * @return Keys size.
     */
    @Override public int nearSize() {
        return nearKey != null ? 1 : 0;
    }

    /**
     * @return Version assigned on primary node.
     */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Write version.
     */
    @Override public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    @Override public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Keys.
     */
    @Override public Collection<KeyCacheObject> keys() {
        return Collections.singletonList(key);
    }

    /**
     * @param idx Key index.
     * @return Key.
     */
    @Override public KeyCacheObject key(int idx) {
        assert idx == 0;

        return key;
    }

    /**
     * @param idx Partition index.
     * @return Partition id.
     */
    @Override public int partitionId(int idx) {
        assert idx == 0;

        return partId;
    }

    /**
     * @param updCntr Update counter.
     * @return Update counter.
     */
    @Override public Long updateCounter(int updCntr) {
        if (updCntr != 0)
            return null;

        if (updateCntr == -1)
            return null;

        return updateCntr;
    }

    /**
     * @param idx Near key index.
     * @return Key.
     */
    @Override public KeyCacheObject nearKey(int idx) {
        assert idx == 0;

        return nearKey;
    }

    /**
     * @return Keep binary flag.
     */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Override @Nullable public CacheObject value(int idx) {
        assert idx == 0;

        return val;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Override @Nullable public CacheObject previousValue(int idx) {
        assert idx == 0;

        return prevVal;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Override @Nullable public CacheObject localPreviousValue(int idx) {
        assert idx == 0;

        return locPrevVal;
    }

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @Override @Nullable public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert idx == 0;

        return entryProcessor;
    }

    /**
     * @param idx Near key index.
     * @return Value.
     */
    @Override @Nullable public CacheObject nearValue(int idx) {
        assert idx == 0;

        return nearVal;
    }

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    @Override @Nullable public EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx) {
        assert idx == 0;

        return nearEntryProcessor;
    }

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Override @Nullable public GridCacheVersion conflictVersion(int idx) {
        assert idx == 0;

        return conflictVer;
    }

    /**
     * @param idx Index.
     * @return TTL.
     */
    @Override public long ttl(int idx) {
        assert idx == 0;

        return ttl;
    }

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    @Override public long nearTtl(int idx) {
        assert idx == 0;

        return nearTtl;
    }

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    @Override public long conflictExpireTime(int idx) {
        assert idx == 0;

        return conflictExpireTime;
    }

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    @Override public long nearExpireTime(int idx) {
        assert idx == 0;

        return nearExpireTime;
    }

    /**
     * @return {@code True} if on response flag changed.
     */
    @Override public boolean onResponse() {
        return !onRes && (onRes = true);
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Override @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObject(key, cctx);

        prepareMarshalCacheObject(val, cctx);

        prepareMarshalCacheObject(nearKey, cctx);

        prepareMarshalCacheObject(nearVal, cctx);

        prepareMarshalCacheObject(prevVal, cctx);

        if (forceTransformBackups) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);

            if (entryProcessorBytes == null)
                entryProcessorBytes = marshal(entryProcessor, cctx);

            if (nearEntryProcessorBytes == null)
                nearEntryProcessorBytes = marshal(nearEntryProcessor, cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObject(key, cctx, ldr);

        finishUnmarshalCacheObject(val, cctx, ldr);

        finishUnmarshalCacheObject(nearKey, cctx, ldr);

        finishUnmarshalCacheObject(nearVal, cctx, ldr);

        finishUnmarshalCacheObject(prevVal, cctx, ldr);

        if (forceTransformBackups) {
            if (entryProcessor == null)
                entryProcessor = unmarshal(entryProcessorBytes, ctx, ldr);

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);

            if (nearEntryProcessor == null)
                nearEntryProcessor = unmarshal(nearEntryProcessorBytes, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
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
                if (!writer.writeLong("conflictExpireTime", conflictExpireTime))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("conflictVer", conflictVer))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("entryProcessorBytes", entryProcessorBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeBoolean("forceTransformBackups", forceTransformBackups))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("keepBinary", keepBinary))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeByteArray("nearEntryProcessorBytes", nearEntryProcessorBytes))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeLong("nearExpireTime", nearExpireTime))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMessage("nearKey", nearKey))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeLong("nearTtl", nearTtl))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("nearVal", nearVal))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMessage("prevVal", prevVal))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeLong("ttl", ttl))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeLong("updateCntr", updateCntr))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMessage("val", val))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMessage("writeVer", writeVer))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                conflictExpireTime = reader.readLong("conflictExpireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                conflictVer = reader.readMessage("conflictVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                entryProcessorBytes = reader.readByteArray("entryProcessorBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                forceTransformBackups = reader.readBoolean("forceTransformBackups");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageCollectionItemType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                keepBinary = reader.readBoolean("keepBinary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                nearEntryProcessorBytes = reader.readByteArray("nearEntryProcessorBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                nearExpireTime = reader.readLong("nearExpireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                nearKey = reader.readMessage("nearKey");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                nearTtl = reader.readLong("nearTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                nearVal = reader.readMessage("nearVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                prevVal = reader.readMessage("prevVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 19:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                ttl = reader.readLong("ttl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                updateCntr = reader.readLong("updateCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicSingleUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        cleanup();
    }

    /**
     * Cleanup values not needed after message was sent.
     */
    private void cleanup() {
        nearVal = null;
        prevVal = null;

        // Do not keep values if they are not needed for continuous query notification.
        if (locPrevVal == null) {
            key = null;
            val = null;
            locPrevVal = null;
        }
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -25;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicSingleUpdateRequest.class, this, "super", super.toString());
    }
}