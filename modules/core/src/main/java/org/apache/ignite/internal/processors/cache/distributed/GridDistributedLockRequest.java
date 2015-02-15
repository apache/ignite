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
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.transactions.*;
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeArrayField("drVersByIdx", drVersByIdx, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeField("futId", futId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeField("grpLockKeyBytes", grpLockKeyBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeField("isInTx", isInTx, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeField("isInvalidate", isInvalidate, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeField("isRead", isRead, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeField("isolation", isolation != null ? (byte)isolation.ordinal() : -1, MessageFieldType.BYTE))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollectionField("keyBytes", keyBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeField("nearXidVer", nearXidVer, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeField("nodeId", nodeId, MessageFieldType.UUID))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeField("partLock", partLock, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeField("retVals", retVals, MessageFieldType.BOOLEAN_ARR))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeField("threadId", threadId, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeField("timeout", timeout, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeField("txSize", txSize, MessageFieldType.INT))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeField("writeEntriesBytes", writeEntriesBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 8:
                drVersByIdx = reader.readArrayField("drVersByIdx", MessageFieldType.MSG, GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                futId = reader.readField("futId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                grpLockKeyBytes = reader.readField("grpLockKeyBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                isInTx = reader.readField("isInTx", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                isInvalidate = reader.readField("isInvalidate", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                isRead = reader.readField("isRead", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                byte isolationOrd;

                isolationOrd = reader.readField("isolation", MessageFieldType.BYTE);

                if (!reader.isLastRead())
                    return false;

                isolation = IgniteTxIsolation.fromOrdinal(isolationOrd);

                readState++;

            case 15:
                keyBytes = reader.readCollectionField("keyBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 16:
                nearXidVer = reader.readField("nearXidVer", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 17:
                nodeId = reader.readField("nodeId", MessageFieldType.UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 18:
                partLock = reader.readField("partLock", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 19:
                retVals = reader.readField("retVals", MessageFieldType.BOOLEAN_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 20:
                threadId = reader.readField("threadId", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 21:
                timeout = reader.readField("timeout", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 22:
                txSize = reader.readField("txSize", MessageFieldType.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 23:
                writeEntriesBytes = reader.readField("writeEntriesBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 21;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedLockRequest.class, this, "keysCnt", retVals.length,
            "super", super.toString());
    }
}
