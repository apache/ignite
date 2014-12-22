/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * DHT prepare request.
 */
public class GridDhtTxPrepareRequest<K, V> extends GridDistributedTxPrepareRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Max order. */
    private UUID nearNodeId;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Topology version. */
    private long topVer;

    /** Invalidate near entries flags. */
    private BitSet invalidateNearEntries;

    /** Near writes. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<GridCacheTxEntry<K, V>> nearWrites;

    /** Serialized near writes. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> nearWritesBytes;

    /** Owned versions by key. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<GridCacheTxKey<K>, GridCacheVersion> owned;

    /** Owned versions bytes. */
    private byte[] ownedBytes;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** {@code True} if this is last prepare request for node. */
    private boolean last;

    /** Subject ID. */
    @GridDirectVersion(1)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(2)
    private int taskNameHash;

    @GridDirectVersion(3)
    /** Preload keys. */
    private BitSet preloadKeys;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxPrepareRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param topVer Topology version.
     * @param tx Transaction.
     * @param dhtWrites DHT writes.
     * @param nearWrites Near writes.
     * @param grpLockKey Group lock key if preparing group-lock transaction.
     * @param partLock {@code True} if group-lock transaction locks partition.
     * @param txNodes Transaction nodes mapping.
     * @param nearXidVer Near transaction ID.
     * @param last {@code True} if this is last prepare request for node.
     */
    public GridDhtTxPrepareRequest(
        IgniteUuid futId,
        IgniteUuid miniId,
        long topVer,
        GridDhtTxLocalAdapter<K, V> tx,
        Collection<GridCacheTxEntry<K, V>> dhtWrites,
        Collection<GridCacheTxEntry<K, V>> nearWrites,
        GridCacheTxKey grpLockKey,
        boolean partLock,
        Map<UUID, Collection<UUID>> txNodes,
        GridCacheVersion nearXidVer,
        boolean last,
        UUID subjId,
        int taskNameHash) {
        super(tx, null, dhtWrites, grpLockKey, partLock, txNodes);

        assert futId != null;
        assert miniId != null;

        this.topVer = topVer;
        this.futId = futId;
        this.nearWrites = nearWrites;
        this.miniId = miniId;
        this.nearXidVer = nearXidVer;
        this.last = last;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        invalidateNearEntries = new BitSet(dhtWrites == null ? 0 : dhtWrites.size());

        nearNodeId = tx.nearNodeId();
    }

    /**
     * @return {@code True} if this is last prepare request for node.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return Subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Near writes.
     */
    public Collection<GridCacheTxEntry<K, V>> nearWrites() {
        return nearWrites == null ? Collections.<GridCacheTxEntry<K, V>>emptyList() : nearWrites;
    }

    /**
     * @param idx Entry index to set invalidation flag.
     * @param invalidate Invalidation flag value.
     */
    public void invalidateNearEntry(int idx, boolean invalidate) {
        invalidateNearEntries.set(idx, invalidate);
    }

    /**
     * @param idx Index to get invalidation flag value.
     * @return Invalidation flag value.
     */
    public boolean invalidateNearEntry(int idx) {
        return invalidateNearEntries.get(idx);
    }

    /**
     * Marks last added key for preloading.
     */
    public void markKeyForPreload(int idx) {
        if (preloadKeys == null)
            preloadKeys = new BitSet();

        preloadKeys.set(idx, true);
    }

    /**
     * Checks whether entry info should be sent to primary node from backup.
     *
     * @param idx Index.
     * @return {@code True} if value should be sent, {@code false} otherwise.
     */
    public boolean needPreloadKey(int idx) {
        return preloadKeys != null && preloadKeys.get(idx);
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * Sets owner and its mapped version.
     *
     * @param key Key.
     * @param ownerMapped Owner mapped version.
     */
    public void owned(GridCacheTxKey<K> key, GridCacheVersion ownerMapped) {
        if (owned == null)
            owned = new GridLeanMap<>(3);

        owned.put(key, ownerMapped);
    }

    /**
     * @return Owned versions map.
     */
    public Map<GridCacheTxKey<K>, GridCacheVersion> owned() {
        return owned;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (ownedBytes == null && owned != null) {
            ownedBytes = CU.marshal(ctx, owned);

            if (ctx.deploymentEnabled()) {
                for (GridCacheTxKey<K> k : owned.keySet())
                    prepareObject(k, ctx);
            }
        }

        if (nearWrites != null) {
            marshalTx(nearWrites, ctx);

            nearWritesBytes = new ArrayList<>(nearWrites.size());

            for (GridCacheTxEntry<K, V> e : nearWrites)
                nearWritesBytes.add(ctx.marshaller().marshal(e));
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedBytes != null && owned == null)
            owned = ctx.marshaller().unmarshal(ownedBytes, ldr);

        if (nearWritesBytes != null) {
            nearWrites = new ArrayList<>(nearWritesBytes.size());

            for (byte[] arr : nearWritesBytes)
                nearWrites.add(ctx.marshaller().<GridCacheTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(nearWrites, true, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareRequest.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtTxPrepareRequest _clone = new GridDhtTxPrepareRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtTxPrepareRequest _clone = (GridDhtTxPrepareRequest)_msg;

        _clone.nearNodeId = nearNodeId;
        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.topVer = topVer;
        _clone.invalidateNearEntries = invalidateNearEntries;
        _clone.nearWrites = nearWrites;
        _clone.nearWritesBytes = nearWritesBytes;
        _clone.owned = owned;
        _clone.ownedBytes = ownedBytes;
        _clone.nearXidVer = nearXidVer;
        _clone.last = last;
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
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 21:
                if (!commState.putGridUuid("futId", futId))
                    return false;

                commState.idx++;

            case 22:
                if (!commState.putBitSet("invalidateNearEntries", invalidateNearEntries))
                    return false;

                commState.idx++;

            case 23:
                if (!commState.putBoolean("last", last))
                    return false;

                commState.idx++;

            case 24:
                if (!commState.putGridUuid("miniId", miniId))
                    return false;

                commState.idx++;

            case 25:
                if (!commState.putUuid("nearNodeId", nearNodeId))
                    return false;

                commState.idx++;

            case 26:
                if (nearWritesBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, nearWritesBytes.size()))
                            return false;

                        commState.it = nearWritesBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray(null, (byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 27:
                if (!commState.putCacheVersion("nearXidVer", nearXidVer))
                    return false;

                commState.idx++;

            case 28:
                if (!commState.putByteArray("ownedBytes", ownedBytes))
                    return false;

                commState.idx++;

            case 29:
                if (!commState.putLong("topVer", topVer))
                    return false;

                commState.idx++;

            case 30:
                if (!commState.putUuid("subjId", subjId))
                    return false;

                commState.idx++;

            case 31:
                if (!commState.putInt("taskNameHash", taskNameHash))
                    return false;

                commState.idx++;

            case 32:
                if (!commState.putBitSet("preloadKeys", preloadKeys))
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
            case 21:
                IgniteUuid futId0 = commState.getGridUuid("futId");

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 22:
                BitSet invalidateNearEntries0 = commState.getBitSet("invalidateNearEntries");

                if (invalidateNearEntries0 == BIT_SET_NOT_READ)
                    return false;

                invalidateNearEntries = invalidateNearEntries0;

                commState.idx++;

            case 23:
                if (buf.remaining() < 1)
                    return false;

                last = commState.getBoolean("last");

                commState.idx++;

            case 24:
                IgniteUuid miniId0 = commState.getGridUuid("miniId");

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 25:
                UUID nearNodeId0 = commState.getUuid("nearNodeId");

                if (nearNodeId0 == UUID_NOT_READ)
                    return false;

                nearNodeId = nearNodeId0;

                commState.idx++;

            case 26:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (nearWritesBytes == null)
                        nearWritesBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray(null);

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        nearWritesBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 27:
                GridCacheVersion nearXidVer0 = commState.getCacheVersion("nearXidVer");

                if (nearXidVer0 == CACHE_VER_NOT_READ)
                    return false;

                nearXidVer = nearXidVer0;

                commState.idx++;

            case 28:
                byte[] ownedBytes0 = commState.getByteArray("ownedBytes");

                if (ownedBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                ownedBytes = ownedBytes0;

                commState.idx++;

            case 29:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong("topVer");

                commState.idx++;

            case 30:
                UUID subjId0 = commState.getUuid("subjId");

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 31:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt("taskNameHash");

                commState.idx++;

            case 32:
                BitSet preloadKeys0 = commState.getBitSet("preloadKeys");

                if (preloadKeys0 == BIT_SET_NOT_READ)
                    return false;

                preloadKeys = preloadKeys0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 33;
    }
}
