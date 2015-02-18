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
    private TransactionIsolation isolation;

    /** Key bytes for keys to lock. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> keyBytes;

    /** Keys. */
    @GridDirectTransient
    private List<K> keys;

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
        TransactionIsolation isolation,
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
    public TransactionIsolation isolation() {
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
     * @param cands Candidates.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addKeyBytes(
        K key,
        @Nullable byte[] keyBytes,
        boolean retVal,
        @Nullable Collection<GridCacheMvccCandidate<K>> cands,
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

        retVals[idx] = retVal;

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

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (grpLockKey != null && grpLockKeyBytes == null) {
            if (ctx.deploymentEnabled())
                prepareObject(grpLockKey, ctx);

            grpLockKeyBytes = CU.marshal(ctx, grpLockKey);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null)
            keys = unmarshalCollection(keyBytes, ctx, ldr);

        if (grpLockKey == null && grpLockKeyBytes != null)
            grpLockKey = ctx.marshaller().unmarshal(grpLockKeyBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(new MessageHeader(directType(), (byte)22)))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeByteArray("grpLockKeyBytes", grpLockKeyBytes))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean("isInTx", isInTx))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("isInvalidate", isInvalidate))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBoolean("isRead", isRead))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeByte("isolation", isolation != null ? (byte)isolation.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection("keyBytes", keyBytes, Type.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("nearXidVer", nearXidVer))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeBoolean("partLock", partLock))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeBooleanArray("retVals", retVals))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeInt("txSize", txSize))
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
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                grpLockKeyBytes = reader.readByteArray("grpLockKeyBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                isInTx = reader.readBoolean("isInTx");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                isInvalidate = reader.readBoolean("isInvalidate");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                isRead = reader.readBoolean("isRead");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                byte isolationOrd;

                isolationOrd = reader.readByte("isolation");

                if (!reader.isLastRead())
                    return false;

                isolation = TransactionIsolation.fromOrdinal(isolationOrd);

                readState++;

            case 14:
                keyBytes = reader.readCollection("keyBytes", Type.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 15:
                nearXidVer = reader.readMessage("nearXidVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 16:
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 17:
                partLock = reader.readBoolean("partLock");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 18:
                retVals = reader.readBooleanArray("retVals");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 19:
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 20:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 21:
                txSize = reader.readInt("txSize");

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
