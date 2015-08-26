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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.nio.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicUpdateRequest extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Target node ID. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Fast map flag. */
    private boolean fastMap;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    private GridCacheVersion updateVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    private boolean topLocked;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    private GridCacheOperation op;

    /** Keys to update. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** Values to update. */
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> vals;

    /** Entry processors. */
    @GridDirectTransient
    private List<EntryProcessor<Object, Object, Object>> entryProcessors;

    /** Entry processors bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> entryProcessorsBytes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Conflict versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> conflictVers;

    /** Conflict TTLs. */
    private GridLongList conflictTtls;

    /** Conflict expire times. */
    private GridLongList conflictExpireTimes;

    /** Return value flag. */
    private boolean retval;

    /** Expiry policy. */
    @GridDirectTransient
    private ExpiryPolicy expiryPlc;

    /** Expiry policy bytes. */
    private byte[] expiryPlcBytes;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    /** Flag indicating whether request contains primary keys. */
    private boolean hasPrimary;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Skip write-through to a persistent storage. */
    private boolean skipStore;

    /** */
    private boolean clientReq;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param fastMap Fast map scheme flag.
     * @param updateVer Update version set if fast map is performed.
     * @param topVer Topology version.
     * @param topLocked Topology locked flag.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param retval Return value required flag.
     * @param expiryPlc Expiry policy.
     * @param invokeArgs Optional arguments for entry processor.
     * @param filter Optional filter for atomic check.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip write-through to a persistent storage.
     * @param clientReq Client node request flag.
     */
    public GridNearAtomicUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        boolean fastMap,
        @Nullable GridCacheVersion updateVer,
        @NotNull AffinityTopologyVersion topVer,
        boolean topLocked,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        boolean retval,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Object[] invokeArgs,
        @Nullable CacheEntryPredicate[] filter,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean clientReq
    ) {
        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.fastMap = fastMap;
        this.updateVer = updateVer;

        this.topVer = topVer;
        this.topLocked = topLocked;
        this.syncMode = syncMode;
        this.op = op;
        this.retval = retval;
        this.expiryPlc = expiryPlc;
        this.invokeArgs = invokeArgs;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.clientReq = clientReq;

        keys = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Mapped node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    public boolean fastMap() {
        return fastMap;
    }

    /**
     * @return Update version for fast-map request.
     */
    public GridCacheVersion updateVersion() {
        return updateVer;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Topology locked flag.
     */
    public boolean topologyLocked() {
        return topLocked;
    }

    /**
     * @return {@code True} if request sent from client node.
     */
    public boolean clientRequest() {
        return clientReq;
    }

    /**
     * @return Cache write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Expiry policy.
     */
    public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /**
     * @return Return value flag.
     */
    public boolean returnValue() {
        return retval;
    }

    /**
     * @return Filter.
     */
    @Nullable public CacheEntryPredicate[] filter() {
        return filter;
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    public boolean skipStore() {
        return skipStore;
    }

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param primary If given key is primary on this mapping.
     */
    public void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean primary) {
        EntryProcessor<Object, Object, Object> entryProcessor = null;

        if (op == TRANSFORM) {
            assert val instanceof EntryProcessor : val;

            entryProcessor = (EntryProcessor<Object, Object, Object>) val;
        }

        assert val != null || op == DELETE;

        keys.add(key);

        if (entryProcessor != null) {
            if (entryProcessors == null)
                entryProcessors = new ArrayList<>();

            entryProcessors.add(entryProcessor);
        }
        else if (val != null) {
            assert val instanceof CacheObject : val;

            if (vals == null)
                vals = new ArrayList<>();

            vals.add((CacheObject)val);
        }

        hasPrimary |= primary;

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

        if (conflictTtl >= 0) {
            if (conflictTtls == null) {
                conflictTtls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictTtls.add(CU.TTL_NOT_CHANGED);
            }

            conflictTtls.add(conflictTtl);
        }

        if (conflictExpireTime >= 0) {
            if (conflictExpireTimes == null) {
                conflictExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
            }

            conflictExpireTimes.add(conflictExpireTime);
        }
    }

    /**
     * @return Keys for this update request.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @return Values for this update request.
     */
    public List<?> values() {
        return op == TRANSFORM ? entryProcessors : vals;
    }

    /**
     * @return Update operation.
     */
    public GridCacheOperation operation() {
        return op;
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @SuppressWarnings("unchecked")
    public CacheObject value(int idx) {
        assert op == UPDATE : op;

        return vals.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @SuppressWarnings("unchecked")
    public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert op == TRANSFORM : op;

        return entryProcessors.get(idx);
    }

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    public CacheObject writeValue(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /**
     * @return Conflict versions.
     */
    @Nullable public List<GridCacheVersion> conflictVersions() {
        return conflictVers;
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
     * @return Conflict TTL.
     */
    public long conflictTtl(int idx) {
        if (conflictTtls != null) {
            assert idx >= 0 && idx < conflictTtls.size();

            return conflictTtls.get(idx);
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
     * @return Flag indicating whether this request contains primary keys.
     */
    public boolean hasPrimary() {
        return hasPrimary;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);

        if (op == TRANSFORM)
            entryProcessorsBytes = marshalCollection(entryProcessors, ctx);
        else
            prepareMarshalCacheObjects(vals, cctx);

        if (filter != null) {
            boolean hasFilter = false;

            for (CacheEntryPredicate p : filter) {
                if (p != null) {
                    hasFilter = true;

                    p.prepareMarshal(cctx);
                }
            }

            if (!hasFilter)
                filter = null;
        }

        invokeArgsBytes = marshalInvokeArguments(invokeArgs, ctx);

        if (expiryPlc != null)
            expiryPlcBytes = CU.marshal(ctx, new IgniteExternalizableExpiryPolicy(expiryPlc));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        if (op == TRANSFORM)
            entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);
        else
            finishUnmarshalCacheObjects(vals, cctx, ldr);

        if (filter != null) {
            for (CacheEntryPredicate p : filter) {
                if (p != null)
                    p.finishUnmarshal(cctx, ldr);
            }
        }

        invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);

        if (expiryPlcBytes != null)
            expiryPlc = ctx.marshaller().unmarshal(expiryPlcBytes, ldr);
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
                if (!writer.writeBoolean("clientReq", clientReq))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("conflictExpireTimes", conflictExpireTimes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("conflictTtls", conflictTtls))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection("conflictVers", conflictVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection("entryProcessorsBytes", entryProcessorsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByteArray("expiryPlcBytes", expiryPlcBytes))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("fastMap", fastMap))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeObjectArray("filter", filter, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBoolean("hasPrimary", hasPrimary))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeBoolean("retval", retval))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeBoolean("skipStore", skipStore))
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
                if (!writer.writeBoolean("topLocked", topLocked))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMessage("updateVer", updateVer))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection("vals", vals, MessageCollectionItemType.MSG))
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
                clientReq = reader.readBoolean("clientReq");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                conflictExpireTimes = reader.readMessage("conflictExpireTimes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                conflictTtls = reader.readMessage("conflictTtls");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                conflictVers = reader.readCollection("conflictVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                entryProcessorsBytes = reader.readCollection("entryProcessorsBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                expiryPlcBytes = reader.readByteArray("expiryPlcBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                fastMap = reader.readBoolean("fastMap");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                filter = reader.readObjectArray("filter", MessageCollectionItemType.MSG, CacheEntryPredicate.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                hasPrimary = reader.readBoolean("hasPrimary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageCollectionItemType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 16:
                retval = reader.readBoolean("retval");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                skipStore = reader.readBoolean("skipStore");

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
                topLocked = reader.readBoolean("topLocked");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                updateVer = reader.readMessage("updateVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                vals = reader.readCollection("vals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateRequest.class, this, "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
