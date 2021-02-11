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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Near cache lock request to primary node. 'Near' means 'Initiating node' here, not 'Near Cache'.
 */
public class GridNearLockRequest extends GridDistributedLockRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int NEED_RETURN_VALUE_FLAG_MASK = 0x01;

    /** */
    private static final int FIRST_CLIENT_REQ_FLAG_MASK = 0x02;

    /** */
    private static final int SYNC_COMMIT_FLAG_MASK = 0x04;

    /** */
    private static final int NEAR_CACHE_FLAG_MASK = 0x08;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Mini future ID. */
    private int miniId;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    /** Array of mapped DHT versions for this entry. */
    @GridToStringInclude
    private GridCacheVersion[] dhtVers;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** TTL for create operation. */
    private long createTtl;

    /** TTL for read operation. */
    private long accessTtl;

    /** */
    private byte flags;

    /** Transaction label. */
    private String txLbl;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearLockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @param futId Future ID.
     * @param lockVer Cache version.
     * @param isInTx {@code True} if implicit transaction lock.
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param retVal Return value flag.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param keyCnt Number of keys.
     * @param txSize Expected transaction size.
     * @param syncCommit Synchronous commit flag.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param createTtl TTL for create operation.
     * @param accessTtl TTL for read operation.
     * @param skipStore Skip store flag.
     * @param firstClientReq {@code True} if first lock request for lock operation sent from client node.
     * @param addDepInfo Deployment info flag.
     * @param txLbl Transaction label.
     */
    public GridNearLockRequest(
        int cacheId,
        @NotNull AffinityTopologyVersion topVer,
        UUID nodeId,
        long threadId,
        IgniteUuid futId,
        GridCacheVersion lockVer,
        boolean isInTx,
        boolean isRead,
        boolean retVal,
        TransactionIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int keyCnt,
        int txSize,
        boolean syncCommit,
        @Nullable UUID subjId,
        int taskNameHash,
        long createTtl,
        long accessTtl,
        boolean skipStore,
        boolean keepBinary,
        boolean firstClientReq,
        boolean nearCache,
        boolean addDepInfo,
        @Nullable String txLbl
    ) {
        super(
            cacheId,
            nodeId,
            lockVer,
            threadId,
            futId,
            lockVer,
            isInTx,
            isRead,
            isolation,
            isInvalidate,
            timeout,
            keyCnt,
            txSize,
            skipStore,
            keepBinary,
            addDepInfo);

        assert topVer.compareTo(AffinityTopologyVersion.ZERO) > 0;

        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;

        this.txLbl = txLbl;

        dhtVers = new GridCacheVersion[keyCnt];

        setFlag(syncCommit, SYNC_COMMIT_FLAG_MASK);
        setFlag(firstClientReq, FIRST_CLIENT_REQ_FLAG_MASK);
        setFlag(retVal, NEED_RETURN_VALUE_FLAG_MASK);
        setFlag(nearCache, NEAR_CACHE_FLAG_MASK);
    }

    /**
     * @return {@code True} if near cache enabled on originating node.
     */
    public boolean nearCache() {
        return isFlag(NEAR_CACHE_FLAG_MASK);
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return {@code True} if first lock request for lock operation sent from client node.
     */
    public boolean firstClientRequest() {
        return isFlag(FIRST_CLIENT_REQ_FLAG_MASK);
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.q
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Sync commit flag.
     */
    public boolean syncCommit() {
        return isFlag(SYNC_COMMIT_FLAG_MASK);
    }

    /**
     * @return Filter.
     */
    public CacheEntryPredicate[] filter() {
        return filter;
    }

    /**
     * @param filter Filter.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void filter(CacheEntryPredicate[] filter, GridCacheContext ctx)
        throws IgniteCheckedException {
        this.filter = filter;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future Id.
     */
    public void miniId(int miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Need return value flag.
     */
    public boolean needReturnValue() {
        return isFlag(NEED_RETURN_VALUE_FLAG_MASK);
    }

    /**
     * Adds a key.
     *
     * @param key Key.
     * @param retVal Flag indicating whether value should be returned.
     * @param dhtVer DHT version.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addKeyBytes(
        KeyCacheObject key,
        boolean retVal,
        @Nullable GridCacheVersion dhtVer,
        GridCacheContext ctx
    ) throws IgniteCheckedException {
        dhtVers[idx] = dhtVer;

        // Delegate to super.
        addKeyBytes(key, retVal, ctx);
    }

    /**
     * @param idx Index of the key.
     * @return DHT version for key at given index.
     */
    public GridCacheVersion dhtVersion(int idx) {
        return dhtVers[idx];
    }

    /**
     * @return New TTL to set after entry is created, -1 to leave unchanged.
     */
    public long createTtl() {
        return createTtl;
    }

    /**
     * @return TTL for read operation.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /**
     * @return Transaction label.
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (filter != null) {
            GridCacheContext cctx = ctx.cacheContext(cacheId);

            for (CacheEntryPredicate p : filter) {
                if (p != null)
                    p.prepareMarshal(cctx);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (filter != null) {
            GridCacheContext cctx = ctx.cacheContext(cacheId);

            for (CacheEntryPredicate p : filter) {
                if (p != null)
                    p.finishUnmarshal(cctx, ldr);
            }
        }
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
            case 21:
                if (!writer.writeLong("accessTtl", accessTtl))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeLong("createTtl", createTtl))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeObjectArray("dhtVers", dhtVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeObjectArray("filter", filter, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 29:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 30:
                if (!writer.writeString("txLbl", txLbl))
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
            case 21:
                accessTtl = reader.readLong("accessTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                createTtl = reader.readLong("createTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                dhtVers = reader.readObjectArray("dhtVers", MessageCollectionItemType.MSG, GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                filter = reader.readObjectArray("filter", MessageCollectionItemType.MSG, CacheEntryPredicate.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 28:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 29:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 30:
                txLbl = reader.readString("txLbl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearLockRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 51;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 31;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockRequest.class, this, "filter", Arrays.toString(filter),
            "super", super.toString());
    }
}
