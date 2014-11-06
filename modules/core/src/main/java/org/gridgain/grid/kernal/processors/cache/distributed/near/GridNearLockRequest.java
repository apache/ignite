/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near cache lock request.
 */
public class GridNearLockRequest<K, V> extends GridDistributedLockRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private long topVer;

    /** Mini future ID. */
    private GridUuid miniId;

    /** Filter. */
    private byte[][] filterBytes;

    /** Filter. */
    @GridDirectTransient
    private GridPredicate<GridCacheEntry<K, V>>[] filter;

    /** Synchronous commit flag. */
    private boolean syncCommit;

    /** Synchronous rollback flag. */
    private boolean syncRollback;

    /** Implicit flag. */
    private boolean implicitTx;

    /** Implicit transaction with one key flag. */
    private boolean implicitSingleTx;

    /** One phase commit flag. */
    private boolean onePhaseCommit;

    /** Array of mapped DHT versions for this entry. */
    @GridToStringInclude
    private GridCacheVersion[] dhtVers;

    /** Subject ID. */
    @GridDirectVersion(1)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(2)
    private int taskNameHash;

    /** Has transforms flag. */
    @GridDirectVersion(3)
    private boolean hasTransforms;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearLockRequest() {
        // No-op.
    }

    /**
     * @param topVer Topology version.
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @param futId Future ID.
     * @param lockVer Cache version.
     * @param isInTx {@code True} if implicit transaction lock.
     * @param implicitTx Flag to indicate that transaction is implicit.
     * @param implicitSingleTx Implicit-transaction-with-one-key flag.
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param keyCnt Number of keys.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock If partition is locked.
     */
    public GridNearLockRequest(
        long topVer,
        UUID nodeId,
        long threadId,
        GridUuid futId,
        GridCacheVersion lockVer,
        boolean isInTx,
        boolean implicitTx,
        boolean implicitSingleTx,
        boolean isRead,
        GridCacheTxIsolation isolation,
        boolean isInvalidate,
        long timeout,
        boolean syncCommit,
        boolean syncRollback,
        int keyCnt,
        int txSize,
        @Nullable GridCacheTxKey grpLockKey,
        boolean partLock,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(nodeId, lockVer, threadId, futId, lockVer, isInTx, isRead, isolation, isInvalidate, timeout, keyCnt,
            txSize, grpLockKey, partLock);

        assert topVer > 0;

        this.topVer = topVer;
        this.implicitTx = implicitTx;
        this.implicitSingleTx = implicitSingleTx;
        this.syncCommit = syncCommit;
        this.syncRollback = syncRollback;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        dhtVers = new GridCacheVersion[keyCnt];
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
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
     * @return Implicit transaction flag.
     */
    public boolean implicitTx() {
        return implicitTx;
    }

    /**
     * @return Implicit-transaction-with-one-key flag.
     */
    public boolean implicitSingleTx() {
        return implicitSingleTx;
    }

    /**
     * @return One phase commit flag.
     */
    public boolean onePhaseCommit() {
        return onePhaseCommit;
    }

    /**
     * @param onePhaseCommit One phase commit flag.
     */
    public void onePhaseCommit(boolean onePhaseCommit) {
        this.onePhaseCommit = onePhaseCommit;
    }

    /**
     * @return Filter.
     */
    public GridPredicate<GridCacheEntry<K, V>>[] filter() {
        return filter;
    }

    /**
     * @param filter Filter.
     * @param ctx Context.
     * @throws GridException If failed.
     */
    public void filter(GridPredicate<GridCacheEntry<K, V>>[] filter, GridCacheContext<K, V> ctx)
        throws GridException {
        this.filter = filter;
    }

    /**
     * @return Synchronous commit flag.
     */
    public boolean syncCommit() {
        return syncCommit;
    }

    /**
     * @return Synchronous rollback flag.
     */
    public boolean syncRollback() {
        return syncRollback;
    }

    /**
     * @return Mini future ID.
     */
    public GridUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future Id.
     */
    public void miniId(GridUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @param hasTransforms {@code True} if originating transaction has transform entries.
     */
    public void hasTransforms(boolean hasTransforms) {
        this.hasTransforms = hasTransforms;
    }

    /**
     * @return {@code True} if originating transaction has transform entries.
     */
    public boolean hasTransforms() {
        return hasTransforms;
    }

    /**
     * Adds a key.
     *
     * @param key Key.
     * @param retVal Flag indicating whether value should be returned.
     * @param keyBytes Key bytes.
     * @param dhtVer DHT version.
     * @param writeEntry Write entry if implicit transaction mapped on one node.
     * @param drVer DR version.
     * @param ctx Context.
     * @throws GridException If failed.
     */
    public void addKeyBytes(
        K key,
        byte[] keyBytes,
        boolean retVal,
        @Nullable GridCacheVersion dhtVer,
        @Nullable GridCacheTxEntry<K, V> writeEntry,
        @Nullable GridCacheVersion drVer,
        GridCacheContext<K, V> ctx
    ) throws GridException {
        dhtVers[idx] = dhtVer;

        // Delegate to super.
        addKeyBytes(key, keyBytes, writeEntry, retVal, null, drVer, ctx);
    }

    /**
     * @param idx Index of the key.
     * @return DHT version for key at given index.
     */
    public GridCacheVersion dhtVersion(int idx) {
        return dhtVers[idx];
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (filterBytes == null)
            filterBytes = marshalFilter(filter, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (filter == null && filterBytes != null)
            filter = unmarshalFilter(filterBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearLockRequest _clone = new GridNearLockRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearLockRequest _clone = (GridNearLockRequest)_msg;

        _clone.topVer = topVer;
        _clone.miniId = miniId;
        _clone.filterBytes = filterBytes;
        _clone.filter = filter;
        _clone.syncCommit = syncCommit;
        _clone.syncRollback = syncRollback;
        _clone.implicitTx = implicitTx;
        _clone.implicitSingleTx = implicitSingleTx;
        _clone.onePhaseCommit = onePhaseCommit;
        _clone.dhtVers = dhtVers;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
        _clone.hasTransforms = hasTransforms;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 23:
                if (dhtVers != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(dhtVers.length))
                            return false;

                        commState.it = arrayIterator(dhtVers);
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putCacheVersion((GridCacheVersion)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 24:
                if (filterBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(filterBytes.length))
                            return false;

                        commState.it = arrayIterator(filterBytes);
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray((byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 25:
                if (!commState.putBoolean(implicitSingleTx))
                    return false;

                commState.idx++;

            case 26:
                if (!commState.putBoolean(implicitTx))
                    return false;

                commState.idx++;

            case 27:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 28:
                if (!commState.putBoolean(onePhaseCommit))
                    return false;

                commState.idx++;

            case 29:
                if (!commState.putBoolean(syncCommit))
                    return false;

                commState.idx++;

            case 30:
                if (!commState.putBoolean(syncRollback))
                    return false;

                commState.idx++;

            case 31:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 32:
                if (!commState.putUuid(subjId))
                    return false;

                commState.idx++;

            case 33:
                if (!commState.putInt(taskNameHash))
                    return false;

                commState.idx++;

            case 34:
                if (!commState.putBoolean(hasTransforms))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 23:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (dhtVers == null)
                        dhtVers = new GridCacheVersion[commState.readSize];

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        dhtVers[i] = (GridCacheVersion)_val;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 24:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (filterBytes == null)
                        filterBytes = new byte[commState.readSize][];

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        filterBytes[i] = (byte[])_val;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 25:
                if (buf.remaining() < 1)
                    return false;

                implicitSingleTx = commState.getBoolean();

                commState.idx++;

            case 26:
                if (buf.remaining() < 1)
                    return false;

                implicitTx = commState.getBoolean();

                commState.idx++;

            case 27:
                GridUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 28:
                if (buf.remaining() < 1)
                    return false;

                onePhaseCommit = commState.getBoolean();

                commState.idx++;

            case 29:
                if (buf.remaining() < 1)
                    return false;

                syncCommit = commState.getBoolean();

                commState.idx++;

            case 30:
                if (buf.remaining() < 1)
                    return false;

                syncRollback = commState.getBoolean();

                commState.idx++;

            case 31:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 32:
                UUID subjId0 = commState.getUuid();

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 33:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt();

                commState.idx++;

            case 34:
                if (buf.remaining() < 1)
                    return false;

                hasTransforms = commState.getBoolean();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 50;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockRequest.class, this, "filter", Arrays.toString(filter),
            "super", super.toString());
    }
}
