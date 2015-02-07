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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
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
    private Collection<IgniteTxEntry<K, V>> nearWrites;

    /** Serialized near writes. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> nearWritesBytes;

    /** Owned versions by key. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<IgniteTxKey<K>, GridCacheVersion> owned;

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
        Collection<IgniteTxEntry<K, V>> dhtWrites,
        Collection<IgniteTxEntry<K, V>> nearWrites,
        IgniteTxKey grpLockKey,
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
    public Collection<IgniteTxEntry<K, V>> nearWrites() {
        return nearWrites == null ? Collections.<IgniteTxEntry<K, V>>emptyList() : nearWrites;
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
    public void owned(IgniteTxKey<K> key, GridCacheVersion ownerMapped) {
        if (owned == null)
            owned = new GridLeanMap<>(3);

        owned.put(key, ownerMapped);
    }

    /**
     * @return Owned versions map.
     */
    public Map<IgniteTxKey<K>, GridCacheVersion> owned() {
        return owned;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (ownedBytes == null && owned != null) {
            ownedBytes = CU.marshal(ctx, owned);

            if (ctx.deploymentEnabled()) {
                for (IgniteTxKey<K> k : owned.keySet())
                    prepareObject(k, ctx);
            }
        }

        if (nearWrites != null) {
            marshalTx(nearWrites, ctx);

            nearWritesBytes = new ArrayList<>(nearWrites.size());

            for (IgniteTxEntry<K, V> e : nearWrites)
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
                nearWrites.add(ctx.marshaller().<IgniteTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(nearWrites, true, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareRequest.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridDhtTxPrepareRequest _clone = new GridDhtTxPrepareRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
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
            case 22:
                if (!commState.putGridUuid("futId", futId))
                    return false;

                commState.idx++;

            case 23:
                if (!commState.putBitSet("invalidateNearEntries", invalidateNearEntries))
                    return false;

                commState.idx++;

            case 24:
                if (!commState.putBoolean("last", last))
                    return false;

                commState.idx++;

            case 25:
                if (!commState.putGridUuid("miniId", miniId))
                    return false;

                commState.idx++;

            case 26:
                if (!commState.putUuid("nearNodeId", nearNodeId))
                    return false;

                commState.idx++;

            case 27:
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

            case 28:
                if (!commState.putCacheVersion("nearXidVer", nearXidVer))
                    return false;

                commState.idx++;

            case 29:
                if (!commState.putByteArray("ownedBytes", ownedBytes))
                    return false;

                commState.idx++;

            case 30:
                if (!commState.putLong("topVer", topVer))
                    return false;

                commState.idx++;

            case 31:
                if (!commState.putUuid("subjId", subjId))
                    return false;

                commState.idx++;

            case 32:
                if (!commState.putInt("taskNameHash", taskNameHash))
                    return false;

                commState.idx++;

            case 33:
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
            case 22:
                futId = commState.getGridUuid("futId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 23:
                invalidateNearEntries = commState.getBitSet("invalidateNearEntries");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 24:
                last = commState.getBoolean("last");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 25:
                miniId = commState.getGridUuid("miniId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 26:
                nearNodeId = commState.getUuid("nearNodeId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 27:
                if (commState.readSize == -1) {
                    int _val = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                    commState.readSize = _val;
                }

                if (commState.readSize >= 0) {
                    if (nearWritesBytes == null)
                        nearWritesBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray(null);

                        if (!commState.lastRead())
                            return false;

                        nearWritesBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 28:
                nearXidVer = commState.getCacheVersion("nearXidVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 29:
                ownedBytes = commState.getByteArray("ownedBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 30:
                topVer = commState.getLong("topVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 31:
                subjId = commState.getUuid("subjId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 32:
                taskNameHash = commState.getInt("taskNameHash");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 33:
                preloadKeys = commState.getBitSet("preloadKeys");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 34;
    }
}
