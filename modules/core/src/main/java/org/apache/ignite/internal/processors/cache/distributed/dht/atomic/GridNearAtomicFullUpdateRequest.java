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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicFullUpdateRequest extends GridNearAtomicAbstractUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Target node ID. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    private GridCacheVersion updateVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    private GridCacheOperation op;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name hash. */
    protected int taskNameHash;

    /** */
    @GridDirectTransient
    private GridNearAtomicUpdateResponse res;

    /** Fast map flag. */
    protected boolean fastMap;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    protected boolean topLocked;

    /** Flag indicating whether request contains primary keys. */
    protected boolean hasPrimary;

    /** Skip write-through to a persistent storage. */
    protected boolean skipStore;

    /** */
    protected boolean clientReq;

    /** Keep binary flag. */
    protected boolean keepBinary;

    /** Return value flag. */
    protected boolean retval;

    /** Keys to update. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** Values to update. */
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> vals;

    /** Partitions of keys. */
    @GridDirectCollection(int.class)
    private List<Integer> partIds;

    /** Entry processors. */
    @GridDirectTransient
    private List<EntryProcessor<Object, Object, Object>> entryProcessors;

    /** Entry processors bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> entryProcessorsBytes;

    /** Conflict versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> conflictVers;

    /** Conflict TTLs. */
    private GridLongList conflictTtls;

    /** Conflict expire times. */
    private GridLongList conflictExpireTimes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Expiry policy. */
    @GridDirectTransient
    private ExpiryPolicy expiryPlc;

    /** Expiry policy bytes. */
    private byte[] expiryPlcBytes;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    /** Maximum possible size of inner collections. */
    @GridDirectTransient
    private int initSize;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicFullUpdateRequest() {
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
     * @param keepBinary Keep binary flag.
     * @param clientReq Client node request flag.
     * @param addDepInfo Deployment info flag.
     * @param maxEntryCnt Maximum entries count.
     */
    GridNearAtomicFullUpdateRequest(
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
        boolean keepBinary,
        boolean clientReq,
        boolean addDepInfo,
        int maxEntryCnt
    ) {
        assert futVer != null;

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
        this.keepBinary = keepBinary;
        this.clientReq = clientReq;
        this.addDepInfo = addDepInfo;

        // By default ArrayList expands to array of 10 elements on first add. We cannot guess how many entries
        // will be added to request because of unknown affinity distribution. However, we DO KNOW how many keys
        // participate in request. As such, we know upper bound of all collections in request. If this bound is lower
        // than 10, we use it.
        initSize = Math.min(maxEntryCnt, 10);

        keys = new ArrayList<>(initSize);

        partIds = new ArrayList<>(initSize);
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public UUID subjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @Override public int taskNameHash() {
        return taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion updateVersion() {
        return updateVer;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /** {@inheritDoc} */
    @Override public GridCacheOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override public boolean onResponse(GridNearAtomicUpdateResponse res) {
        if (this.res == null) {
            this.res = res;

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridNearAtomicUpdateResponse response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean primary) {
        EntryProcessor<Object, Object, Object> entryProcessor = null;

        if (op == TRANSFORM) {
            assert val instanceof EntryProcessor : val;

            entryProcessor = (EntryProcessor<Object, Object, Object>)val;
        }

        assert val != null || op == DELETE;

        keys.add(key);
        partIds.add(key.partition());

        if (entryProcessor != null) {
            if (entryProcessors == null)
                entryProcessors = new ArrayList<>(initSize);

            entryProcessors.add(entryProcessor);
        }
        else if (val != null) {
            assert val instanceof CacheObject : val;

            if (vals == null)
                vals = new ArrayList<>(initSize);

            vals.add((CacheObject)val);
        }

        hasPrimary |= primary;

        // In case there is no conflict, do not create the list.
        if (conflictVer != null) {
            if (conflictVers == null) {
                conflictVers = new ArrayList<>(initSize);

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

    /** {@inheritDoc} */
    @Override public List<KeyCacheObject> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return keys != null ? keys.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key(int idx) {
        return keys.get(idx);
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return op == TRANSFORM ? entryProcessors : vals;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public CacheObject value(int idx) {
        assert op == UPDATE : op;

        return vals.get(idx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert op == TRANSFORM : op;

        return entryProcessors.get(idx);
    }

    /** {@inheritDoc} */
    @Override public CacheObject writeValue(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public List<GridCacheVersion> conflictVersions() {
        return conflictVers;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheVersion conflictVersion(int idx) {
        if (conflictVers != null) {
            assert idx >= 0 && idx < conflictVers.size();

            return conflictVers.get(idx);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public long conflictTtl(int idx) {
        if (conflictTtls != null) {
            assert idx >= 0 && idx < conflictTtls.size();

            return conflictTtls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        if (conflictExpireTimes != null) {
            assert idx >= 0 && idx < conflictExpireTimes.size();

            return conflictExpireTimes.get(idx);
        }

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @Override public boolean fastMap() {
        return fastMap;
    }

    /** {@inheritDoc} */
    @Override public boolean topologyLocked() {
        return topLocked;
    }

    /** {@inheritDoc} */
    @Override public boolean clientRequest() {
        return clientReq;
    }

    /** {@inheritDoc} */
    @Override public boolean returnValue() {
        return retval;
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /** {@inheritDoc} */
    @Override public boolean hasPrimary() {
        return hasPrimary;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheEntryPredicate[] filter() {
        return filter;
    }

    /** {@inheritDoc} */
    @Override public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (expiryPlc != null && expiryPlcBytes == null)
            expiryPlcBytes = CU.marshal(cctx, new IgniteExternalizableExpiryPolicy(expiryPlc));

        prepareMarshalCacheObjects(keys, cctx);

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

        if (op == TRANSFORM) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (entryProcessorsBytes == null)
                entryProcessorsBytes = marshalCollection(entryProcessors, cctx);

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);
        }
        else
            prepareMarshalCacheObjects(vals, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (expiryPlcBytes != null && expiryPlc == null)
            expiryPlc = U.unmarshal(ctx, expiryPlcBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        if (filter != null) {
            for (CacheEntryPredicate p : filter) {
                if (p != null)
                    p.finishUnmarshal(cctx, ldr);
            }
        }

        if (op == TRANSFORM) {
            if (entryProcessors == null)
                entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
        }
        else
            finishUnmarshalCacheObjects(vals, cctx, ldr);

        if (partIds != null && !partIds.isEmpty()) {
            assert partIds.size() == keys.size();

            for (int i = 0; i < keys.size(); i++)
                keys.get(i).partition(partIds.get(i));
        }
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partIds != null && !partIds.isEmpty() ? partIds.get(0) : -1;
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
                if (!writer.writeBoolean("keepBinary", keepBinary))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection("partIds", partIds, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeBoolean("retval", retval))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeBoolean("skipStore", skipStore))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeBoolean("topLocked", topLocked))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeMessage("updateVer", updateVer))
                    return false;

                writer.incrementState();

            case 26:
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
                keepBinary = reader.readBoolean("keepBinary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 17:
                partIds = reader.readCollection("partIds", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                retval = reader.readBoolean("retval");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                skipStore = reader.readBoolean("skipStore");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 22:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                topLocked = reader.readBoolean("topLocked");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                updateVer = reader.readMessage("updateVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                vals = reader.readCollection("vals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicFullUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public void cleanup(boolean clearKeys) {
        vals = null;
        entryProcessors = null;
        entryProcessorsBytes = null;
        invokeArgs = null;
        invokeArgsBytes = null;

        if (clearKeys)
            keys = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 27;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicFullUpdateRequest.class, this, "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
