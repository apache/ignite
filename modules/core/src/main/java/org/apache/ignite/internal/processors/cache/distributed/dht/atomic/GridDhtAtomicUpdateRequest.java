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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.SKIP_STORE_FLAG_MASK;

/**
 * Lite dht cache backup update request.
 */
public class GridDhtAtomicUpdateRequest extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Write version. */
    private GridCacheVersion writeVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Keys to update. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** Values to update. */
    @GridToStringInclude
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> vals;

    /** Previous values. */
    @GridToStringInclude
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> prevVals;

    /** Conflict versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> conflictVers;

    /** TTLs. */
    private GridLongList ttls;

    /** Conflict expire time. */
    private GridLongList conflictExpireTimes;

    /** Near TTLs. */
    private GridLongList nearTtls;

    /** Near expire times. */
    private GridLongList nearExpireTimes;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Near cache keys to update. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> nearKeys;

    /** Values to update. */
    @GridToStringInclude
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> nearVals;

    /** Force transform backups flag. */
    private boolean forceTransformBackups;

    /** Entry processors. */
    @GridDirectTransient
    private List<EntryProcessor<Object, Object, Object>> entryProcessors;

    /** Entry processors bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> entryProcessorsBytes;

    /** Near entry processors. */
    @GridDirectTransient
    private List<EntryProcessor<Object, Object, Object>> nearEntryProcessors;

    /** Near entry processors bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> nearEntryProcessorsBytes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Partition. */
    private GridLongList updateCntrs;

    /** On response flag. Access should be synced on future. */
    @GridDirectTransient
    private boolean onRes;

    /** */
    @GridDirectTransient
    private List<Integer> partIds;

    /** Keep binary flag. */
    private boolean keepBinary;

    /**
     * Additional flags.
     */
    private byte flags;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicUpdateRequest() {
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
    public GridDhtAtomicUpdateRequest(
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
        boolean keepBinary,
        boolean skipStore
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

        if (skipStore)
            flags = (byte)(flags | SKIP_STORE_FLAG_MASK);

        keys = new ArrayList<>();
        partIds = new ArrayList<>();

        if (forceTransformBackups) {
            entryProcessors = new ArrayList<>();
            entryProcessorsBytes = new ArrayList<>();
        }
        else
            vals = new ArrayList<>();
    }

    /**
     * @return Force transform backups flag.
     */
    public boolean forceTransformBackups() {
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
     * @param partId Partition.
     * @param prevVal Previous value.
     * @param updateCntr Update counter.
     */
    public void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        int partId,
        @Nullable CacheObject prevVal,
        @Nullable Long updateCntr
    ) {
        keys.add(key);

        partIds.add(partId);

        if (forceTransformBackups) {
            assert entryProcessor != null;

            entryProcessors.add(entryProcessor);
        }
        else
            vals.add(val);

        if (addPrevVal) {
            if (prevVals == null)
                prevVals = new ArrayList<>();

            prevVals.add(prevVal);
        }

        if (updateCntr != null) {
            if (updateCntrs == null)
                updateCntrs = new GridLongList();

            updateCntrs.add(updateCntr);
        }

        // In case there is no conflict, do not create the list.
        if (conflictVer != null) {
            if (conflictVers == null) {
                conflictVers = new ArrayList<>();

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictVers.add(null);
            }

            conflictVers.add(conflictVer);
        }
        else if (conflictVers != null)
            conflictVers.add(null);

        if (ttl >= 0) {
            if (ttls == null) {
                ttls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    ttls.add(CU.TTL_NOT_CHANGED);
            }
        }

        if (ttls != null)
            ttls.add(ttl);

        if (conflictExpireTime >= 0) {
            if (conflictExpireTimes == null) {
                conflictExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
            }
        }

        if (conflictExpireTimes != null)
            conflictExpireTimes.add(conflictExpireTime);
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {
        if (nearKeys == null) {
            nearKeys = new ArrayList<>();

            if (forceTransformBackups) {
                nearEntryProcessors = new ArrayList<>();
                nearEntryProcessorsBytes = new ArrayList<>();
            }
            else
                nearVals = new ArrayList<>();
        }

        nearKeys.add(key);

        if (forceTransformBackups) {
            assert entryProcessor != null;

            nearEntryProcessors.add(entryProcessor);
        }
        else
            nearVals.add(val);

        if (ttl >= 0) {
            if (nearTtls == null) {
                nearTtls = new GridLongList(nearKeys.size());

                for (int i = 0; i < nearKeys.size() - 1; i++)
                    nearTtls.add(CU.TTL_NOT_CHANGED);
            }
        }

        if (nearTtls != null)
            nearTtls.add(ttl);

        if (expireTime >= 0) {
            if (nearExpireTimes == null) {
                nearExpireTimes = new GridLongList(nearKeys.size());

                for (int i = 0; i < nearKeys.size() - 1; i++)
                    nearExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
            }
        }

        if (nearExpireTimes != null)
            nearExpireTimes.add(expireTime);
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
     * @return Keys size.
     */
    public int size() {
        return keys.size();
    }

    /**
     * @return Keys size.
     */
    public int nearSize() {
        return nearKeys != null ? nearKeys.size() : 0;
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
     * @return Keys.
     */
    public Collection<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param idx Key index.
     * @return Key.
     */
    public KeyCacheObject key(int idx) {
        return keys.get(idx);
    }

    /**
     * @param idx Partition index.
     * @return Partition id.
     */
    public int partitionId(int idx) {
        return partIds.get(idx);
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    public boolean skipStore() {
        return (flags & SKIP_STORE_FLAG_MASK) == SKIP_STORE_FLAG_MASK;
    }

    /**
     * @param updCntr Update counter.
     * @return Update counter.
     */
    public Long updateCounter(int updCntr) {
        if (updateCntrs != null && updCntr < updateCntrs.size())
            return updateCntrs.get(updCntr);

        return null;
    }

    /**
     * @param idx Near key index.
     * @return Key.
     */
    public KeyCacheObject nearKey(int idx) {
        return nearKeys.get(idx);
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public CacheObject value(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public CacheObject previousValue(int idx) {
        if (prevVals != null)
            return prevVals.get(idx);

        return null;
    }

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @Nullable public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        return entryProcessors == null ? null : entryProcessors.get(idx);
    }

    /**
     * @param idx Near key index.
     * @return Value.
     */
    @Nullable public CacheObject nearValue(int idx) {
        if (nearVals != null)
            return nearVals.get(idx);

        return null;
    }

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    @Nullable public EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx) {
        return nearEntryProcessors == null ? null : nearEntryProcessors.get(idx);
    }

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public GridCacheVersion conflictVersion(int idx) {
        if (conflictVers != null) {
            assert idx >= 0 && idx < conflictVers.size();

            return conflictVers.get(idx);
        }

        return null;
    }

    /**
     * @param idx Index.
     * @return TTL.
     */
    public long ttl(int idx) {
        if (ttls != null) {
            assert idx >= 0 && idx < ttls.size();

            return ttls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public long nearTtl(int idx) {
        if (nearTtls != null) {
            assert idx >= 0 && idx < nearTtls.size();

            return nearTtls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public long conflictExpireTime(int idx) {
        if (conflictExpireTimes != null) {
            assert idx >= 0 && idx < conflictExpireTimes.size();

            return conflictExpireTimes.get(idx);
        }

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public long nearExpireTime(int idx) {
        if (nearExpireTimes != null) {
            assert idx >= 0 && idx < nearExpireTimes.size();

            return nearExpireTimes.get(idx);
        }

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /**
     * @return {@code True} if on response flag changed.
     */
    public boolean onResponse() {
        return !onRes && (onRes = true);
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);

        prepareMarshalCacheObjects(vals, cctx);

        prepareMarshalCacheObjects(nearKeys, cctx);

        prepareMarshalCacheObjects(nearVals, cctx);

        prepareMarshalCacheObjects(prevVals, cctx);

        if (forceTransformBackups) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);

            if (entryProcessorsBytes == null)
                entryProcessorsBytes = marshalCollection(entryProcessors, cctx);

            if (nearEntryProcessorsBytes == null)
                nearEntryProcessorsBytes = marshalCollection(nearEntryProcessors, cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        finishUnmarshalCacheObjects(vals, cctx, ldr);

        finishUnmarshalCacheObjects(nearKeys, cctx, ldr);

        finishUnmarshalCacheObjects(nearVals, cctx, ldr);

        finishUnmarshalCacheObjects(prevVals, cctx, ldr);

        if (forceTransformBackups) {
            if (entryProcessors == null)
                entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);

            if (nearEntryProcessors == null)
                nearEntryProcessors = unmarshalCollection(nearEntryProcessorsBytes, ctx, ldr);
        }

        if (partIds != null && !partIds.isEmpty()) {
            assert partIds.size() == keys.size();

            for (int i = 0; i < keys.size(); i++)
                keys.get(i).partition(partIds.get(i));
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
                if (!writer.writeMessage("conflictExpireTimes", conflictExpireTimes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection("conflictVers", conflictVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("entryProcessorsBytes", entryProcessorsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("forceTransformBackups", forceTransformBackups))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean("keepBinary", keepBinary))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection("nearEntryProcessorsBytes", nearEntryProcessorsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMessage("nearExpireTimes", nearExpireTimes))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection("nearKeys", nearKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("nearTtls", nearTtls))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeCollection("nearVals", nearVals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection("prevVals", prevVals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMessage("ttls", ttls))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMessage("updateCntrs", updateCntrs))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection("vals", vals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 25:
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
                conflictExpireTimes = reader.readMessage("conflictExpireTimes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                conflictVers = reader.readCollection("conflictVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                entryProcessorsBytes = reader.readCollection("entryProcessorsBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                forceTransformBackups = reader.readBoolean("forceTransformBackups");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageCollectionItemType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                keepBinary = reader.readBoolean("keepBinary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                nearEntryProcessorsBytes = reader.readCollection("nearEntryProcessorsBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                nearExpireTimes = reader.readMessage("nearExpireTimes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                nearKeys = reader.readCollection("nearKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                nearTtls = reader.readMessage("nearTtls");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                nearVals = reader.readCollection("nearVals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                prevVals = reader.readCollection("prevVals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 20:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                ttls = reader.readMessage("ttls");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                updateCntrs = reader.readMessage("updateCntrs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                vals = reader.readCollection("vals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        cleanup();
    }

    /**
     * Cleanup values not needed after message was sent.
     */
    private void cleanup() {
        nearVals = null;
        prevVals = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 38;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 26;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateRequest.class, this, "super", super.toString());
    }
}
