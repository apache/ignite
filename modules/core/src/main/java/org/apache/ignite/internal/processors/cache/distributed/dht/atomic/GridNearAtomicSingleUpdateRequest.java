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
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicSingleUpdateRequest extends GridCacheMessage
    implements GridNearAtomicUpdateRequestInterface, GridCacheDeployable {
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

    /** Key to update. */
    @GridToStringInclude
    private KeyCacheObject key;

    /** Value to update. */
    private CacheObject val;

    /** Entry processor. */
    @GridDirectTransient
    private EntryProcessor<Object, Object, Object> entryProc;

    /** Entry processor bytes. */
    private byte[] entryProcBytes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Conflict version. */
    private GridCacheVersion conflictVer;

    /** Conflict TTL. */
    private long conflictTtl = CU.TTL_NOT_CHANGED;

    /** Conflict expire time. */
    private long conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;

    /** Return value flag. */
    private boolean retval;

    /** Expiry policy. */
    @GridDirectTransient
    private ExpiryPolicy expiryPlc;

    /** Expiry policy bytes. */
    private byte[] expiryPlcBytes;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Skip write-through to a persistent storage. */
    private boolean skipStore;

    /** */
    private boolean clientReq;

    /** Keep binary flag. */
    private boolean keepBinary;

    /** */
    @GridDirectTransient
    private GridNearAtomicUpdateResponseInterface res;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicSingleUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param key Key.
     * @param val Value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
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
     */
    @SuppressWarnings("unchecked")
    public GridNearAtomicSingleUpdateRequest(
        KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
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
        boolean addDepInfo
    ) {
        assert futVer != null;

        this.key = key;

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

        EntryProcessor<Object, Object, Object> entryProc = null;

        if (op == TRANSFORM) {
            assert val instanceof EntryProcessor : val;

            entryProc = (EntryProcessor<Object, Object, Object>)val;
        }

        assert val != null || op == DELETE;

        if (entryProc != null)
            this.entryProc = entryProc;
        else if (val != null) {
            assert val instanceof CacheObject : val;

            this.val = (CacheObject)val;
        }

        this.conflictVer = conflictVer;

        if (conflictTtl >= 0)
            this.conflictTtl = conflictTtl;

        if (conflictExpireTime >= 0)
            this.conflictExpireTime = conflictExpireTime;
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
    @Override public boolean fastMap() {
        return fastMap;
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
    @Override public boolean topologyLocked() {
        return topLocked;
    }

    /** {@inheritDoc} */
    @Override public boolean clientRequest() {
        return clientReq;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /** {@inheritDoc} */
    @Override public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** {@inheritDoc} */
    @Override public boolean returnValue() {
        return retval;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheEntryPredicate[] filter() {
        return filter;
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
    @Override public List<KeyCacheObject> keys() {
        return Collections.singletonList(key);
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return Collections.singletonList(op == TRANSFORM ? entryProc : val);
    }

    /** {@inheritDoc} */
    @Override public GridCacheOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public CacheObject value(int idx) {
        assert idx == 0;
        assert op == UPDATE : op;

        return val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert idx == 0;
        assert op == TRANSFORM : op;

        return entryProc;
    }

    /** {@inheritDoc} */
    @Override public CacheObject writeValue(int idx) {
        assert idx == 0;

        return val;
    }

    /** {@inheritDoc} */
    @Override @Nullable public List<GridCacheVersion> conflictVersions() {
        return conflictVer == null ? null : Collections.singletonList(conflictVer);
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheVersion conflictVersion(int idx) {
        assert idx == 0;

        return conflictVer;
    }

    /** {@inheritDoc} */
    @Override public long conflictTtl(int idx) {
        assert idx == 0;

        return conflictTtl;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        assert idx == 0;

        return conflictExpireTime;
    }

    /** {@inheritDoc} */
    @Override public boolean hasPrimary() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean onResponse(GridNearAtomicUpdateResponseInterface res) {
        if (this.res == null) {
            this.res = res;

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridNearAtomicUpdateResponseInterface response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObject(key, cctx);

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

        if (expiryPlc != null && expiryPlcBytes == null)
            expiryPlcBytes = CU.marshal(cctx, new IgniteExternalizableExpiryPolicy(expiryPlc));

        if (op == TRANSFORM) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (entryProcBytes == null)
                entryProcBytes = marshal(entryProc, cctx);

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);
        }
        else
            prepareMarshalCacheObject(val, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObject(key, cctx, ldr);

        if (op == TRANSFORM) {
            if (entryProc == null)
                entryProc = unmarshal(entryProcBytes, ctx, ldr);

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
        }
        else
            finishUnmarshalCacheObject(val, cctx, ldr);

        if (filter != null) {
            for (CacheEntryPredicate p : filter) {
                if (p != null)
                    p.finishUnmarshal(cctx, ldr);
            }
        }

        if (expiryPlcBytes != null && expiryPlc == null)
            expiryPlc = ctx.marshaller().unmarshal(expiryPlcBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
                if (!writer.writeBoolean("clientReq", clientReq))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("conflictExpireTime", conflictExpireTime))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("conflictTtl", conflictTtl))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("conflictVer", conflictVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByteArray("entryProcBytes", entryProcBytes))
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
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeBoolean("keepBinary", keepBinary))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage("key", key))
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
                if (!writer.writeMessage("val", val))
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
                conflictExpireTime = reader.readLong("conflictExpireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                conflictTtl = reader.readLong("conflictTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                conflictVer = reader.readMessage("conflictVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                entryProcBytes = reader.readByteArray("entryProcBytes");

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
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageCollectionItemType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                keepBinary = reader.readBoolean("keepBinary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                key = reader.readMessage("key");

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
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicSingleUpdateRequest.class);
    }

    @Override public void cleanup(boolean clearKeys) {
        val = null;
        entryProc = null;
        entryProcBytes = null;
        invokeArgs = null;
        invokeArgsBytes = null;

        if (clearKeys)
            key = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -23;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicSingleUpdateRequest.class, this, "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
