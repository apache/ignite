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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
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
    private IgniteUuid miniId;

    /** Filter. */
    private byte[][] filterBytes;

    /** Filter. */
    @GridDirectTransient
    private IgnitePredicate<Cache.Entry<K, V>>[] filter;

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
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Has transforms flag. */
    private boolean hasTransforms;

    /** Sync commit flag. */
    private boolean syncCommit;

    /** TTL for read operation. */
    private long accessTtl;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearLockRequest() {
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
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param keyCnt Number of keys.
     * @param txSize Expected transaction size.
     * @param syncCommit Synchronous commit flag.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param partLock If partition is locked.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param accessTtl TTL for read operation.
     */
    public GridNearLockRequest(
        int cacheId,
        long topVer,
        UUID nodeId,
        long threadId,
        IgniteUuid futId,
        GridCacheVersion lockVer,
        boolean isInTx,
        boolean implicitTx,
        boolean implicitSingleTx,
        boolean isRead,
        IgniteTxIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int keyCnt,
        int txSize,
        boolean syncCommit,
        @Nullable IgniteTxKey grpLockKey,
        boolean partLock,
        @Nullable UUID subjId,
        int taskNameHash,
        long accessTtl
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
            grpLockKey,
            partLock);

        assert topVer > 0;

        this.topVer = topVer;
        this.implicitTx = implicitTx;
        this.implicitSingleTx = implicitSingleTx;
        this.syncCommit = syncCommit;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.accessTtl = accessTtl;

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
     * @return Task name hash.q
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
     * @return Sync commit flag.
     */
    public boolean syncCommit() {
        return syncCommit;
    }

    /**
     * @return Filter.
     */
    public IgnitePredicate<Cache.Entry<K, V>>[] filter() {
        return filter;
    }

    /**
     * @param filter Filter.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void filter(IgnitePredicate<Cache.Entry<K, V>>[] filter, GridCacheContext<K, V> ctx)
        throws IgniteCheckedException {
        this.filter = filter;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future Id.
     */
    public void miniId(IgniteUuid miniId) {
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
     * @throws IgniteCheckedException If failed.
     */
    public void addKeyBytes(
        K key,
        byte[] keyBytes,
        boolean retVal,
        @Nullable GridCacheVersion dhtVer,
        @Nullable IgniteTxEntry<K, V> writeEntry,
        @Nullable GridCacheVersion drVer,
        GridCacheContext<K, V> ctx
    ) throws IgniteCheckedException {
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

    /** {@inheritDoc} */
    @Override protected boolean transferExpiryPolicy() {
        return true;
    }

    /**
     * @return TTL for read operation.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (filterBytes == null)
            filterBytes = marshalFilter(filter, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (filter == null && filterBytes != null)
            filter = unmarshalFilter(filterBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridNearLockRequest _clone = (GridNearLockRequest)_msg;

        _clone.topVer = topVer;
        _clone.miniId = miniId;
        _clone.filterBytes = filterBytes;
        _clone.filter = filter;
        _clone.implicitTx = implicitTx;
        _clone.implicitSingleTx = implicitSingleTx;
        _clone.onePhaseCommit = onePhaseCommit;
        _clone.dhtVers = dhtVers;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
        _clone.hasTransforms = hasTransforms;
        _clone.syncCommit = syncCommit;
        _clone.accessTtl = accessTtl;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriteState state) {
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!super.writeTo(buf, state))
            return false;

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 24:
                if (!writer.writeLong("accessTtl", accessTtl))
                    return false;

                state.increment();

            case 25:
                if (!writer.writeObjectArray("dhtVers", dhtVers, Type.MSG))
                    return false;

                state.increment();

            case 26:
                if (!writer.writeObjectArray("filterBytes", filterBytes, Type.BYTE_ARR))
                    return false;

                state.increment();

            case 27:
                if (!writer.writeBoolean("hasTransforms", hasTransforms))
                    return false;

                state.increment();

            case 28:
                if (!writer.writeBoolean("implicitSingleTx", implicitSingleTx))
                    return false;

                state.increment();

            case 29:
                if (!writer.writeBoolean("implicitTx", implicitTx))
                    return false;

                state.increment();

            case 30:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                state.increment();

            case 31:
                if (!writer.writeBoolean("onePhaseCommit", onePhaseCommit))
                    return false;

                state.increment();

            case 32:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                state.increment();

            case 33:
                if (!writer.writeBoolean("syncCommit", syncCommit))
                    return false;

                state.increment();

            case 34:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                state.increment();

            case 35:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 24:
                accessTtl = reader.readLong("accessTtl");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 25:
                dhtVers = reader.readObjectArray("dhtVers", Type.MSG, GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 26:
                filterBytes = reader.readObjectArray("filterBytes", Type.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 27:
                hasTransforms = reader.readBoolean("hasTransforms");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 28:
                implicitSingleTx = reader.readBoolean("implicitSingleTx");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 29:
                implicitTx = reader.readBoolean("implicitTx");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 30:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 31:
                onePhaseCommit = reader.readBoolean("onePhaseCommit");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 32:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 33:
                syncCommit = reader.readBoolean("syncCommit");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 34:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 35:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 51;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockRequest.class, this, "filter", Arrays.toString(filter),
            "super", super.toString());
    }
}
