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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Lock request message.
 */
public class GridDistributedLockRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Sender node ID. */
    private UUID nodeId;

    /** Near transaction version. */
    private GridCacheVersion nearXidVer;

    /** Thread ID. */
    private long threadId;

    /** Future ID. */
    private IgniteUuid futId;

    /** Max wait timeout. */
    private long timeout;

    /** Indicates whether lock is obtained within a scope of transaction. */
    private boolean isInTx;

    /** Invalidate flag for transactions. */
    private boolean isInvalidate;

    /** Indicates whether implicit lock so for read or write operation. */
    private boolean isRead;

    /** Transaction isolation. */
    private IgniteTxIsolation isolation;

    /** Key bytes for keys to lock. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> keyBytes;

    /** Keys. */
    @GridDirectTransient
    private List<K> keys;

    /** Write entries. */
    @GridToStringInclude
    @GridDirectTransient
    private List<IgniteTxEntry<K, V>> writeEntries;

    /** Serialized write entries. */
    private byte[] writeEntriesBytes;

    /** Array indicating whether value should be returned for a key. */
    @GridToStringInclude
    private boolean[] retVals;

    /** Key-bytes index. */
    @GridDirectTransient
    protected int idx;

    /** Key count. */
    private int txSize;

    /** Group lock key if this is a group-lock transaction. */
    @GridDirectTransient
    private IgniteTxKey grpLockKey;

    /** Group lock key bytes. */
    private byte[] grpLockKeyBytes;

    /** Partition lock flag. Only if group-lock transaction. */
    private boolean partLock;

    /** DR versions. */
    @GridToStringInclude
    private GridCacheVersion[] drVersByIdx;

    /**
     * Empty constructor.
     */
    public GridDistributedLockRequest() {
        /* No-op. */
    }

    /**
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param threadId Thread ID.
     * @param futId Future ID.
     * @param lockVer Cache version.
     * @param isInTx {@code True} if implicit transaction lock.
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param keyCnt Number of keys.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock {@code True} if this is a group-lock transaction request and whole partition is
     *      locked.
     */
    public GridDistributedLockRequest(
        int cacheId,
        UUID nodeId,
        @Nullable GridCacheVersion nearXidVer,
        long threadId,
        IgniteUuid futId,
        GridCacheVersion lockVer,
        boolean isInTx,
        boolean isRead,
        IgniteTxIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int keyCnt,
        int txSize,
        @Nullable IgniteTxKey grpLockKey,
        boolean partLock
    ) {
        super(lockVer, keyCnt);

        assert keyCnt > 0;
        assert futId != null;
        assert !isInTx || isolation != null;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.nearXidVer = nearXidVer;
        this.threadId = threadId;
        this.futId = futId;
        this.isInTx = isInTx;
        this.isRead = isRead;
        this.isolation = isolation;
        this.isInvalidate = isInvalidate;
        this.timeout = timeout;
        this.txSize = txSize;
        this.grpLockKey = grpLockKey;
        this.partLock = partLock;

        retVals = new boolean[keyCnt];
    }

    /**
     *
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     *
     * @return Owner node thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return {@code True} if implicit transaction lock.
     */
    public boolean inTx() {
        return isInTx;
    }

    /**
     * @return Invalidate flag.
     */
    public boolean isInvalidate() {
        return isInvalidate;
    }

    /**
     * @return {@code True} if lock is implicit and for a read operation.
     */
    public boolean txRead() {
        return isRead;
    }

    /**
     * @param idx Key index.
     * @return Flag indicating whether a value should be returned.
     */
    public boolean returnValue(int idx) {
        return retVals[idx];
    }

    /**
     * @return Return flags.
     */
    public boolean[] returnFlags() {
        return retVals;
    }

    /**
     * @return Transaction isolation or <tt>null</tt> if not in transaction.
     */
    public IgniteTxIsolation isolation() {
        return isolation;
    }

    /**
     *
     * @return Key to lock.
     */
    public List<byte[]> keyBytes() {
        return keyBytes;
    }

    /**
     * @return Write entries list.
     */
    public List<IgniteTxEntry<K, V>> writeEntries() {
        return writeEntries;
    }

    /**
     * @return Tx size.
     */
    public int txSize() {
        return txSize;
    }

    /**
     * Adds a key.
     *
     * @param key Key.
     * @param retVal Flag indicating whether value should be returned.
     * @param keyBytes Key bytes.
     * @param writeEntry Write entry.
     * @param cands Candidates.
     * @param drVer DR version.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addKeyBytes(
        K key,
        @Nullable byte[] keyBytes,
        @Nullable IgniteTxEntry<K, V> writeEntry,
        boolean retVal,
        @Nullable Collection<GridCacheMvccCandidate<K>> cands,
        @Nullable GridCacheVersion drVer,
        GridCacheContext<K, V> ctx
    ) throws IgniteCheckedException {
        if (ctx.deploymentEnabled())
            prepareObject(key, ctx.shared());

        if (keyBytes != null) {
            if (this.keyBytes == null)
                this.keyBytes = new ArrayList<>(keysCount());

            this.keyBytes.add(keyBytes);
        }

        if (keys == null)
            keys = new ArrayList<>(keysCount());

        keys.add(key);

        candidatesByIndex(idx, cands);
        drVersionByIndex(idx, drVer);

        retVals[idx] = retVal;

        if (writeEntry != null) {
            if (writeEntries == null) {
                assert idx == 0 : "Cannot start adding write entries in the middle of lock message [idx=" + idx +
                    ", writeEntry=" + writeEntry + ']';

                writeEntries = new ArrayList<>(keysCount());
            }

            writeEntries.add(writeEntry);
        }

        idx++;
    }

    /**
     * @return Unmarshalled keys.
     */
    public List<K> keys() {
        return keys;
    }

    /**
     * @return {@code True} if lock request for group-lock transaction.
     */
    public boolean groupLock() {
        return grpLockKey != null;
    }

    /**
     * @return Group lock key.
     */
    @Nullable public IgniteTxKey groupLockKey() {
        return grpLockKey;
    }

    /**
     * @return {@code True} if partition is locked in group-lock transaction.
     */
    public boolean partitionLock() {
        return partLock;
    }

    /**
     * @return Max lock wait time.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param idx Key index.
     * @param drVer DR version.
     */
    @SuppressWarnings({"unchecked"})
    public void drVersionByIndex(int idx, GridCacheVersion drVer) {
        assert idx < keysCount();

        // If nothing to add.
        if (drVer == null)
            return;

        if (drVersByIdx == null)
            drVersByIdx = new GridCacheVersion[keysCount()];

        drVersByIdx[idx] = drVer;
    }

    /**
     * @param idx Key index.
     * @return DR versions for given key.
     */
    public GridCacheVersion drVersionByIndex(int idx) {
        return drVersByIdx == null ? null : drVersByIdx[idx];
    }

    /**
     * @return All DR versions.
     */
    public GridCacheVersion[] drVersions() {
        return drVersByIdx;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (grpLockKey != null && grpLockKeyBytes == null) {
            if (ctx.deploymentEnabled())
                prepareObject(grpLockKey, ctx);

            grpLockKeyBytes = CU.marshal(ctx, grpLockKey);
        }

        if (writeEntries != null) {
            marshalTx(writeEntries, ctx);

            writeEntriesBytes = ctx.marshaller().marshal(writeEntries);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null)
            keys = unmarshalCollection(keyBytes, ctx, ldr);

        if (grpLockKey == null && grpLockKeyBytes != null)
            grpLockKey = ctx.marshaller().unmarshal(grpLockKeyBytes, ldr);

        if (writeEntriesBytes != null) {
            writeEntries = ctx.marshaller().unmarshal(writeEntriesBytes, ldr);

            unmarshalTx(writeEntries, false, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "OverriddenMethodCallDuringObjectConstruction",
        "CloneDoesntCallSuperClone"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDistributedLockRequest _clone = new GridDistributedLockRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDistributedLockRequest _clone = (GridDistributedLockRequest)_msg;

        _clone.nodeId = nodeId;
        _clone.nearXidVer = nearXidVer;
        _clone.threadId = threadId;
        _clone.futId = futId;
        _clone.timeout = timeout;
        _clone.isInTx = isInTx;
        _clone.isInvalidate = isInvalidate;
        _clone.isRead = isRead;
        _clone.isolation = isolation;
        _clone.keyBytes = keyBytes;
        _clone.keys = keys;
        _clone.writeEntries = writeEntries;
        _clone.writeEntriesBytes = writeEntriesBytes;
        _clone.retVals = retVals;
        _clone.idx = idx;
        _clone.txSize = txSize;
        _clone.grpLockKey = grpLockKey;
        _clone.grpLockKeyBytes = grpLockKeyBytes;
        _clone.partLock = partLock;
        _clone.drVersByIdx = drVersByIdx;
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
            case 8:
                if (drVersByIdx != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(drVersByIdx.length))
                            return false;

                        commState.it = arrayIterator(drVersByIdx);
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

            case 9:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putByteArray(grpLockKeyBytes))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putBoolean(isInTx))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putBoolean(isInvalidate))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putBoolean(isRead))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putEnum(isolation))
                    return false;

                commState.idx++;

            case 15:
                if (keyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(keyBytes.size()))
                            return false;

                        commState.it = keyBytes.iterator();
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

            case 16:
                if (!commState.putCacheVersion(nearXidVer))
                    return false;

                commState.idx++;

            case 17:
                if (!commState.putUuid(nodeId))
                    return false;

                commState.idx++;

            case 18:
                if (!commState.putBoolean(partLock))
                    return false;

                commState.idx++;

            case 19:
                if (!commState.putBooleanArray(retVals))
                    return false;

                commState.idx++;

            case 20:
                if (!commState.putLong(threadId))
                    return false;

                commState.idx++;

            case 21:
                if (!commState.putLong(timeout))
                    return false;

                commState.idx++;

            case 22:
                if (!commState.putInt(txSize))
                    return false;

                commState.idx++;

            case 23:
                if (!commState.putByteArray(writeEntriesBytes))
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
            case 8:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (drVersByIdx == null)
                        drVersByIdx = new GridCacheVersion[commState.readSize];

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        drVersByIdx[i] = (GridCacheVersion)_val;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 9:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 10:
                byte[] grpLockKeyBytes0 = commState.getByteArray();

                if (grpLockKeyBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                grpLockKeyBytes = grpLockKeyBytes0;

                commState.idx++;

            case 11:
                if (buf.remaining() < 1)
                    return false;

                isInTx = commState.getBoolean();

                commState.idx++;

            case 12:
                if (buf.remaining() < 1)
                    return false;

                isInvalidate = commState.getBoolean();

                commState.idx++;

            case 13:
                if (buf.remaining() < 1)
                    return false;

                isRead = commState.getBoolean();

                commState.idx++;

            case 14:
                if (buf.remaining() < 1)
                    return false;

                byte isolation0 = commState.getByte();

                isolation = IgniteTxIsolation.fromOrdinal(isolation0);

                commState.idx++;

            case 15:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (keyBytes == null)
                        keyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        keyBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 16:
                GridCacheVersion nearXidVer0 = commState.getCacheVersion();

                if (nearXidVer0 == CACHE_VER_NOT_READ)
                    return false;

                nearXidVer = nearXidVer0;

                commState.idx++;

            case 17:
                UUID nodeId0 = commState.getUuid();

                if (nodeId0 == UUID_NOT_READ)
                    return false;

                nodeId = nodeId0;

                commState.idx++;

            case 18:
                if (buf.remaining() < 1)
                    return false;

                partLock = commState.getBoolean();

                commState.idx++;

            case 19:
                boolean[] retVals0 = commState.getBooleanArray();

                if (retVals0 == BOOLEAN_ARR_NOT_READ)
                    return false;

                retVals = retVals0;

                commState.idx++;

            case 20:
                if (buf.remaining() < 8)
                    return false;

                threadId = commState.getLong();

                commState.idx++;

            case 21:
                if (buf.remaining() < 8)
                    return false;

                timeout = commState.getLong();

                commState.idx++;

            case 22:
                if (buf.remaining() < 4)
                    return false;

                txSize = commState.getInt();

                commState.idx++;

            case 23:
                byte[] writeEntriesBytes0 = commState.getByteArray();

                if (writeEntriesBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                writeEntriesBytes = writeEntriesBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 22;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedLockRequest.class, this, "keysCnt", retVals.length,
            "super", super.toString());
    }
}
