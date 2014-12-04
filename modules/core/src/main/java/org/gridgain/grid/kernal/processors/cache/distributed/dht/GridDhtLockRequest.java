/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * DHT lock request.
 */
public class GridDhtLockRequest<K, V> extends GridDistributedLockRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near keys. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> nearKeys;

    /** Near keys to lock. */
    @GridToStringExclude
    @GridDirectCollection(byte[].class)
    private List<byte[]> nearKeyBytes;

    /** Invalidate reader flags. */
    private BitSet invalidateEntries;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Owner mapped version, if any. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<K, GridCacheVersion> owned;

    /** Owner mapped version bytes. */
    private byte[] ownedBytes;

    /** Topology version. */
    private long topVer;

    /** Subject ID. */
    @GridDirectVersion(1)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(2)
    private int taskNameHash;

    /** Indexes of keys needed to be preloaded. */
    @GridDirectVersion(3)
    private BitSet preloadKeys;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtLockRequest() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param threadId Thread ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param lockVer Cache version.
     * @param topVer Topology version.
     * @param isInTx {@code True} if implicit transaction lock.
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param dhtCnt DHT count.
     * @param nearCnt Near count.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key.
     * @param partLock {@code True} if partition lock.
     */
    public GridDhtLockRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion nearXidVer,
        long threadId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion lockVer,
        long topVer,
        boolean isInTx,
        boolean isRead,
        GridCacheTxIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int dhtCnt,
        int nearCnt,
        int txSize,
        @Nullable GridCacheTxKey grpLockKey,
        boolean partLock,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(cacheId, nodeId, nearXidVer, threadId, futId, lockVer, isInTx, isRead, isolation, isInvalidate, timeout,
            dhtCnt == 0 ? nearCnt : dhtCnt, txSize, grpLockKey, partLock);

        this.topVer = topVer;

        nearKeyBytes = nearCnt == 0 ? Collections.<byte[]>emptyList() : new ArrayList<byte[]>(nearCnt);
        nearKeys = nearCnt == 0 ? Collections.<K>emptyList() : new ArrayList<K>(nearCnt);
        invalidateEntries = new BitSet(dhtCnt == 0 ? nearCnt : dhtCnt);

        assert miniId != null;

        this.miniId = miniId;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nodeId();
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
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Near keys.
     */
    public List<byte[]> nearKeyBytes() {
        return nearKeyBytes == null ? Collections.<byte[]>emptyList() : nearKeyBytes;
    }

    /**
     * Adds a Near key.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param ctx Context.
     * @throws GridException If failed.
     */
    public void addNearKey(K key, byte[] keyBytes, GridCacheSharedContext<K, V> ctx) throws GridException {
        if (ctx.deploymentEnabled())
            prepareObject(key, ctx);

        nearKeys.add(key);

        if (keyBytes != null)
            nearKeyBytes.add(keyBytes);
    }

    /**
     * @return Near keys.
     */
    public List<K> nearKeys() {
        return nearKeys == null ? Collections.<K>emptyList() : nearKeys;
    }

    /**
     * Adds a DHT key.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param writeEntry Write entry.
     * @param drVer DR version.
     * @param invalidateEntry Flag indicating whether node should attempt to invalidate reader.
     * @param ctx Context.
     * @throws GridException If failed.
     */
    public void addDhtKey(
        K key,
        byte[] keyBytes,
        GridCacheTxEntry<K, V> writeEntry,
        @Nullable GridCacheVersion drVer,
        boolean invalidateEntry,
        GridCacheContext<K, V> ctx
    ) throws GridException {
        invalidateEntries.set(idx, invalidateEntry);

        addKeyBytes(key, keyBytes, writeEntry, false, null, drVer, ctx);
    }

    /**
     * Marks last added key for preloading.
     */
    public void markLastKeyForPreload() {
        assert idx > 0;

        if (preloadKeys == null)
            preloadKeys = new BitSet();

        preloadKeys.set(idx - 1, true);
    }

    /**
     * @return {@code True} if need to preload key with given index.
     */
    public boolean needPreloadKey(int idx) {
        return preloadKeys != null && preloadKeys.get(idx);
    }

    /**
     * Sets owner and its mapped version.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param ownerMapped Owner mapped version.
     */
    public void owned(K key, byte[] keyBytes, GridCacheVersion ownerMapped) {
        if (owned == null)
            owned = new GridLeanMap<>(3);

        owned.put(key, ownerMapped);
    }

    /**
     * @param key Key.
     * @return Owner and its mapped versions.
     */
    @Nullable public GridCacheVersion owned(K key) {
        return owned == null ? null : owned.get(key);
    }

    /**
     * @param idx Entry index to check.
     * @return {@code True} if near entry should be invalidated.
     */
    public boolean invalidateNearEntry(int idx) {
        return invalidateEntries.get(idx);
    }

    /**
     * @return Mini ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        assert F.isEmpty(nearKeys) || !F.isEmpty(nearKeyBytes);

        if (owned != null)
            ownedBytes = CU.marshal(ctx, owned);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (nearKeys == null && nearKeyBytes != null)
            nearKeys = unmarshalCollection(nearKeyBytes, ctx, ldr);

        if (ownedBytes != null)
            owned = ctx.marshaller().unmarshal(ownedBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtLockRequest _clone = new GridDhtLockRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtLockRequest _clone = (GridDhtLockRequest)_msg;

        _clone.nearKeys = nearKeys;
        _clone.nearKeyBytes = nearKeyBytes;
        _clone.invalidateEntries = invalidateEntries;
        _clone.miniId = miniId;
        _clone.owned = owned;
        _clone.ownedBytes = ownedBytes;
        _clone.topVer = topVer;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
        _clone.preloadKeys = preloadKeys;
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
            case 24:
                if (!commState.putBitSet(invalidateEntries))
                    return false;

                commState.idx++;

            case 25:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 26:
                if (nearKeyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(nearKeyBytes.size()))
                            return false;

                        commState.it = nearKeyBytes.iterator();
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

            case 27:
                if (!commState.putByteArray(ownedBytes))
                    return false;

                commState.idx++;

            case 28:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 29:
                if (!commState.putUuid(subjId))
                    return false;

                commState.idx++;

            case 30:
                if (!commState.putInt(taskNameHash))
                    return false;

                commState.idx++;

            case 31:
                if (!commState.putBitSet(preloadKeys))
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
            case 24:
                BitSet invalidateEntries0 = commState.getBitSet();

                if (invalidateEntries0 == BIT_SET_NOT_READ)
                    return false;

                invalidateEntries = invalidateEntries0;

                commState.idx++;

            case 25:
                IgniteUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 26:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (nearKeyBytes == null)
                        nearKeyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        nearKeyBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 27:
                byte[] ownedBytes0 = commState.getByteArray();

                if (ownedBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                ownedBytes = ownedBytes0;

                commState.idx++;

            case 28:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 29:
                UUID subjId0 = commState.getUuid();

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 30:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt();

                commState.idx++;

            case 31:
                BitSet preloadKeys0 = commState.getBitSet();

                if (preloadKeys0 == BIT_SET_NOT_READ)
                    return false;

                preloadKeys = preloadKeys0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 29;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockRequest.class, this, "nearKeyBytesSize", nearKeyBytes.size(),
            "super", super.toString());
    }
}
