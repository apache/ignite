package org.apache.ignite.internal.processors.cache.distributed.near;

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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class GridNearLockRequestV2 extends GridDistributedLockRequest implements GridNearLockRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Mini future ID. */
    private int miniId;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    private byte flags;

    /** Array of mapped DHT versions for this entry. */
    @GridToStringInclude
    private GridCacheVersion[] dhtVers;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** TTL for read operation. */
    private long accessTtl;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearLockRequestV2() {
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
     * @param implicitTx Flag to indicate that transaction is implicit.
     * @param implicitSingleTx Implicit-transaction-with-one-key flag.
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
     * @param accessTtl TTL for read operation.
     * @param skipStore Skip store flag.
     * @param firstClientReq {@code True} if first lock request for lock operation sent from client node.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearLockRequestV2(
        int cacheId,
        @NotNull AffinityTopologyVersion topVer,
        UUID nodeId,
        long threadId,
        IgniteUuid futId,
        GridCacheVersion lockVer,
        boolean isInTx,
        boolean implicitTx,
        boolean implicitSingleTx,
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
        long accessTtl,
        boolean skipStore,
        boolean keepBinary,
        boolean firstClientReq,
        boolean addDepInfo

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
        if (implicitTx)
            flags |= 1;
        if (implicitSingleTx)
            flags |= (1 << 1);
        if (syncCommit)
            flags |= (1 << 2);
        if (retVal)
            flags |= (1 << 3);
        if (firstClientReq)
            flags |= (1 << 4);

        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.accessTtl = accessTtl;

        dhtVers = new GridCacheVersion[keyCnt];
    }

    /**
     * @return {@code True} if first lock request for lock operation sent from client node.
     */
    public boolean firstClientRequest() {
        return (flags & (1 << 4)) != 0;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
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
     * @return Implicit transaction flag.
     */
    public boolean implicitTx() {
        return (flags & 1) != 0;
    }

    /**
     * @return Implicit-transaction-with-one-key flag.
     */
    public boolean implicitSingleTx() {
        return (flags & (1 << 1)) != 0;
    }

    /**
     * @return Sync commit flag.
     */
    public boolean syncCommit() {
        return (flags & (1 << 2)) != 0;
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

    public IgniteUuid oldVersionMiniId() {
        return new IgniteUuid(GridNearLockFuture.DUMMY_UUID, miniId);
    }

    /**
     * @param hasTransforms {@code True} if originating transaction has transform entries.
     */
    public void hasTransforms(boolean hasTransforms) {
        if (hasTransforms)
            flags |= (1 << 5);
        else
            flags &= ~(1 << 5);
    }

    /**
     * @return {@code True} if originating transaction has transform entries.
     */
    public boolean hasTransforms() {
        return (flags & (1 << 5)) != 0;
    }

    /**
     * @return Need return value flag.
     */
    public boolean needReturnValue() {
        return (flags & (1 << 3)) != 0;
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
     * @return TTL for read operation.
     */
    public long accessTtl() {
        return accessTtl;
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
            case 20:
                if (!writer.writeLong("accessTtl", accessTtl))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeObjectArray("dhtVers", dhtVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeObjectArray("filter", filter, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeMessage("topVer", topVer))
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
            case 20:
                accessTtl = reader.readLong("accessTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                dhtVers = reader.readObjectArray("dhtVers", MessageCollectionItemType.MSG, GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                filter = reader.readObjectArray("filter", MessageCollectionItemType.MSG, CacheEntryPredicate.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearLockRequestV1.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 125;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 28;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockRequestV2.class, this, "filter", Arrays.toString(filter),
            "super", super.toString());
    }
}