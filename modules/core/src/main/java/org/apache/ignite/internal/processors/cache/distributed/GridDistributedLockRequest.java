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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.KEEP_BINARY_FLAG_MASK;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.SKIP_STORE_FLAG_MASK;

/**
 * Lock request message.
 */
public class GridDistributedLockRequest extends GridDistributedBaseMessage {
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
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** Partition IDs of keys to lock. */
    @GridDirectCollection(int.class)
    protected List<Integer> partIds;

    /** Array indicating whether value should be returned for a key. */
    @GridToStringInclude
    private boolean[] retVals;

    /** Key-bytes index. */
    @GridDirectTransient
    protected int idx;

    /** Key count. */
    private int txSize;

    /**
     * Additional flags.
     * GridCacheUtils.SKIP_STORE_FLAG_MASK - for skipStore flag value.
     */
    private byte flags;

    /**
     * Empty constructor.
     */
    public GridDistributedLockRequest() {
        /* No-op. */
    }

    /**
     * @param cacheId Cache ID.
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
     * @param skipStore Skip store flag.
     * @param addDepInfo Deployment info flag.
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
        boolean skipStore,
        boolean keepBinary,
        boolean addDepInfo
    ) {
        super(lockVer, keyCnt, addDepInfo);

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

        retVals = new boolean[keyCnt];

        skipStore(skipStore);
        keepBinary(keepBinary);
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
     * Sets skip store flag value.
     *
     * @param skipStore Skip store flag.
     */
    private void skipStore(boolean skipStore) {
        flags = skipStore ? (byte)(flags | SKIP_STORE_FLAG_MASK) : (byte)(flags & ~SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return Skip store flag.
     */
    public boolean skipStore() {
        return (flags & SKIP_STORE_FLAG_MASK) == 1;
    }

    /**
     * @param keepBinary Keep binary flag.
     */
    public void keepBinary(boolean keepBinary) {
        flags = keepBinary ? (byte)(flags | KEEP_BINARY_FLAG_MASK) : (byte)(flags & ~KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @return Keep binary.
     */
    public boolean keepBinary() {
        return (flags & KEEP_BINARY_FLAG_MASK) != 0;
    }

    /**
     * @return Transaction isolation or <tt>null</tt> if not in transaction.
     */
    public TransactionIsolation isolation() {
        return isolation;
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
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addKeyBytes(
        KeyCacheObject key,
        boolean retVal,
        GridCacheContext ctx
    ) throws IgniteCheckedException {
        if (keys == null) {
            keys = new ArrayList<>(keysCount());
            partIds = new ArrayList<>(keysCount());
        }

        keys.add(key);

        partIds.add(key.partition());

        retVals[idx] = retVal;

        idx++;
    }

    /**
     * @return Unmarshalled keys.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partIds != null && !partIds.isEmpty() ? partIds.get(0) : -1;
    }

    /**
     * @return Max lock wait time.
     */
    public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.txLockMessageLogger();
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        if (partIds != null && !partIds.isEmpty()) {
            assert partIds.size() == keys.size();

            for (int i = 0; i < keys.size(); i++)
                keys.get(i).partition(partIds.get(i));
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
            case 7:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("isInTx", isInTx))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean("isInvalidate", isInvalidate))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("isRead", isRead))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeByte("isolation", isolation != null ? (byte)isolation.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage("nearXidVer", nearXidVer))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeBooleanArray("retVals", retVals))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeInt("txSize", txSize))
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
            case 7:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                isInTx = reader.readBoolean("isInTx");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                isInvalidate = reader.readBoolean("isInvalidate");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                isRead = reader.readBoolean("isRead");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                byte isolationOrd;

                isolationOrd = reader.readByte("isolation");

                if (!reader.isLastRead())
                    return false;

                isolation = TransactionIsolation.fromOrdinal(isolationOrd);

                reader.incrementState();

            case 13:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                nearXidVer = reader.readMessage("nearXidVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                retVals = reader.readBooleanArray("retVals");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                txSize = reader.readInt("txSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDistributedLockRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 21;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedLockRequest.class, this, "keysCnt", retVals.length,
            "super", super.toString());
    }
}
