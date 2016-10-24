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
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearAtomicAbstractUpdateRequest extends GridCacheMessage implements GridCacheDeployable,
    GridNearAtomicUpdateRequestInt {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Target node ID. */
    @GridDirectTransient
    protected UUID nodeId;

    /** Future version. */
    protected GridCacheVersion futVer;

    /** Fast map flag. */
    protected boolean fastMap;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    protected GridCacheVersion updateVer;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    protected boolean topLocked;

    /** Write synchronization mode. */
    protected CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    protected GridCacheOperation op;

    /** Flag indicating whether request contains primary keys. */
    protected boolean hasPrimary;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name hash. */
    protected int taskNameHash;

    /** Skip write-through to a persistent storage. */
    protected boolean skipStore;

    /** */
    protected boolean clientReq;

    /** Keep binary flag. */
    protected boolean keepBinary;

    /** Return value flag. */
    protected boolean retval;

    /** Expiry policy. */
    @GridDirectTransient
    protected ExpiryPolicy expiryPlc;

    /** Expiry policy bytes. */
    protected byte[] expiryPlcBytes;

    /** Filter. */
    protected CacheEntryPredicate[] filter;

    /** */
    @GridDirectTransient
    private GridNearAtomicUpdateResponse res;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicAbstractUpdateRequest() {
        // NoOp
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
     * @param filter Optional filter for atomic check.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip write-through to a persistent storage.
     * @param keepBinary Keep binary flag.
     * @param clientReq Client node request flag.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearAtomicAbstractUpdateRequest(
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
        @Nullable CacheEntryPredicate[] filter,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        boolean clientReq,
        boolean addDepInfo
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
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.clientReq = clientReq;
        this.addDepInfo = addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Mapped node ID.
     */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    @Override public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Subject ID.
     */
    @Override public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    @Override public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Future version.
     */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    @Override public boolean fastMap() {
        return fastMap;
    }

    /**
     * @return Update version for fast-map request.
     */
    @Override public GridCacheVersion updateVersion() {
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
    @Override public boolean topologyLocked() {
        return topLocked;
    }

    /**
     * @return {@code True} if request sent from client node.
     */
    @Override public boolean clientRequest() {
        return clientReq;
    }

    /**
     * @return Cache write synchronization mode.
     */
    @Override public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Expiry policy.
     */
    @Override public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /**
     * @return Return value flag.
     */
    @Override public boolean returnValue() {
        return retval;
    }

    /**
     * @return Filter.
     */
    @Override @Nullable public CacheEntryPredicate[] filter() {
        return filter;
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /**
     * @return Keep binary flag.
     */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @return Update operation.
     */
    @Override public GridCacheOperation operation() {
        return op;
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Override @Nullable public Object[] invokeArguments() {
        return null;
    }

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    @Override public boolean hasPrimary() {
        return hasPrimary;
    }

    /**
     * @param res Response.
     * @return {@code True} if current response was {@code null}.
     */
    @Override public boolean onResponse(GridNearAtomicUpdateResponse res) {
        if (this.res == null) {
            this.res = res;

            return true;
        }

        return false;
    }

    /**
     * @return Response.
     */
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
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

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
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

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
                if (!writer.writeByteArray("expiryPlcBytes", expiryPlcBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeBoolean("fastMap", fastMap))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeObjectArray("filter", filter, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeBoolean("hasPrimary", hasPrimary))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("keepBinary", keepBinary))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("retval", retval))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBoolean("skipStore", skipStore))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeBoolean("topLocked", topLocked))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMessage("updateVer", updateVer))
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
                expiryPlcBytes = reader.readByteArray("expiryPlcBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                fastMap = reader.readBoolean("fastMap");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                filter = reader.readObjectArray("filter", MessageCollectionItemType.MSG, CacheEntryPredicate.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                hasPrimary = reader.readBoolean("hasPrimary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                keepBinary = reader.readBoolean("keepBinary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 11:
                retval = reader.readBoolean("retval");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                skipStore = reader.readBoolean("skipStore");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 15:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                topLocked = reader.readBoolean("topLocked");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                updateVer = reader.readMessage("updateVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicAbstractUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 19;
    }
}
