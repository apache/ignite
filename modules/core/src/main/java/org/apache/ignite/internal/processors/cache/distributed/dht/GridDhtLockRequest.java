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
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
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

    /** TTL for read operation. */
    private long accessTtl;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtLockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
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
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param accessTtl TTL for read operation.
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
        IgniteTxIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int dhtCnt,
        int nearCnt,
        int txSize,
        @Nullable IgniteTxKey grpLockKey,
        boolean partLock,
        @Nullable UUID subjId,
        int taskNameHash,
        long accessTtl
    ) {
        super(cacheId,
            nodeId,
            nearXidVer,
            threadId,
            futId,
            lockVer,
            isInTx,
            isRead,
            isolation,
            isInvalidate,
            timeout,
            dhtCnt == 0 ? nearCnt : dhtCnt,
            txSize,
            grpLockKey,
            partLock);

        this.topVer = topVer;

        nearKeyBytes = nearCnt == 0 ? Collections.<byte[]>emptyList() : new ArrayList<byte[]>(nearCnt);
        nearKeys = nearCnt == 0 ? Collections.<K>emptyList() : new ArrayList<K>(nearCnt);
        invalidateEntries = new BitSet(dhtCnt == 0 ? nearCnt : dhtCnt);

        assert miniId != null;

        this.miniId = miniId;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.accessTtl = accessTtl;
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
     * @throws IgniteCheckedException If failed.
     */
    public void addNearKey(K key, byte[] keyBytes, GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
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
     * @param invalidateEntry Flag indicating whether node should attempt to invalidate reader.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addDhtKey(
        K key,
        byte[] keyBytes,
        boolean invalidateEntry,
        GridCacheContext<K, V> ctx
    ) throws IgniteCheckedException {
        invalidateEntries.set(idx, invalidateEntry);

        addKeyBytes(key, keyBytes, false, null, ctx);
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
     * @param idx Key index.
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

    /**
     * @return TTL for read operation.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert F.isEmpty(nearKeys) || !F.isEmpty(nearKeyBytes);

        if (owned != null)
            ownedBytes = CU.marshal(ctx, owned);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
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
        _clone.accessTtl = accessTtl;
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

            case 32:
                if (!commState.putLong(accessTtl))
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

            case 32:
                if (buf.remaining() < 8)
                    return false;

                accessTtl = commState.getLong();

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
