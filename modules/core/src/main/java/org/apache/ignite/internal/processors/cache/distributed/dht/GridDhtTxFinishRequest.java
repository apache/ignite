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
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near transaction finish request.
 */
public class GridDhtTxFinishRequest<K, V> extends GridDistributedTxFinishRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Transaction isolation. */
    private IgniteTxIsolation isolation;

    /** Near writes. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxEntry<K, V>> nearWrites;

    /** Serialized near writes. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> nearWritesBytes;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** System invalidation flag. */
    private boolean sysInvalidate;

    /** Topology version. */
    private long topVer;

    /** Pending versions with order less than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> pendingVers;

    /** One phase commit flag for fast-commit path. */
    private boolean onePhaseCommit;

    /** One phase commit write version. */
    private GridCacheVersion writeVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** TTLs for optimistic transaction. */
    private GridLongList ttls;

    /** Near cache TTLs for optimistic transaction. */
    private GridLongList nearTtls;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxFinishRequest() {
        // No-op.
    }

    /**
     * @param nearNodeId Near node ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param topVer Topology version.
     * @param xidVer Transaction ID.
     * @param threadId Thread ID.
     * @param commitVer Commit version.
     * @param isolation Transaction isolation.
     * @param commit Commit flag.
     * @param invalidate Invalidate flag.
     * @param sys System flag.
     * @param sysInvalidate System invalidation flag.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pendingVers Pending versions.
     * @param txSize Expected transaction size.
     * @param writes Write entries.
     * @param nearWrites Near cache writes.
     * @param recoverWrites Recovery write entries.
     * @param onePhaseCommit One phase commit flag.
     * @param grpLockKey Group lock key.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     */
    public GridDhtTxFinishRequest(
        UUID nearNodeId,
        IgniteUuid futId,
        IgniteUuid miniId,
        long topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        long threadId,
        IgniteTxIsolation isolation,
        boolean commit,
        boolean invalidate,
        boolean sys,
        boolean sysInvalidate,
        boolean syncCommit,
        boolean syncRollback,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers,
        int txSize,
        Collection<IgniteTxEntry<K, V>> writes,
        Collection<IgniteTxEntry<K, V>> nearWrites,
        Collection<IgniteTxEntry<K, V>> recoverWrites,
        boolean onePhaseCommit,
        @Nullable IgniteTxKey grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(xidVer, futId, commitVer, threadId, commit, invalidate, sys, syncCommit, syncRollback, baseVer,
            committedVers, rolledbackVers, txSize, writes, recoverWrites, grpLockKey);

        assert miniId != null;
        assert nearNodeId != null;
        assert isolation != null;

        this.pendingVers = pendingVers;
        this.topVer = topVer;
        this.nearNodeId = nearNodeId;
        this.isolation = isolation;
        this.nearWrites = nearWrites;
        this.miniId = miniId;
        this.sysInvalidate = sysInvalidate;
        this.onePhaseCommit = onePhaseCommit;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Near writes.
     */
    public Collection<IgniteTxEntry<K, V>> nearWrites() {
        return nearWrites == null ? Collections.<IgniteTxEntry<K, V>>emptyList() : nearWrites;
    }

    /**
     * @return Mini ID.
     */
    public IgniteUuid miniId() {
        return miniId;
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
     * @return Transaction isolation.
     */
    public IgniteTxIsolation isolation() {
        return isolation;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return System invalidate flag.
     */
    public boolean isSystemInvalidate() {
        return sysInvalidate;
    }

    /**
     * @return One phase commit flag.
     */
    public boolean onePhaseCommit() {
        return onePhaseCommit;
    }

    /**
     * @return Write version for one-phase commit transactions.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @param writeVer Write version for one-phase commit transactions.
     */
    public void writeVersion(GridCacheVersion writeVer) {
        this.writeVer = writeVer;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * Gets versions of not acquired locks with version less then one of transaction being committed.
     *
     * @return Versions of locks for entries participating in transaction that have not been acquired yet
     *      have version less then one of transaction being committed.
     */
    public Collection<GridCacheVersion> pendingVersions() {
        return pendingVers == null ? Collections.<GridCacheVersion>emptyList() : pendingVers;
    }

    /**
     * @param idx Entry index.
     * @param ttl TTL.
     */
    public void ttl(int idx, long ttl) {
        if (ttl != -1L) {
            if (ttls == null) {
                ttls = new GridLongList();

                for (int i = 0; i < idx - 1; i++)
                    ttls.add(-1L);
            }
        }

        if (ttls != null)
            ttls.add(ttl);
    }

    /**
     * @return TTLs for optimistic transaction.
     */
    public GridLongList ttls() {
        return ttls;
    }

    /**
     * @param idx Entry index.
     * @param ttl TTL.
     */
    public void nearTtl(int idx, long ttl) {
        if (ttl != -1L) {
            if (nearTtls == null) {
                nearTtls = new GridLongList();

                for (int i = 0; i < idx - 1; i++)
                    nearTtls.add(-1L);
            }
        }

        if (nearTtls != null)
            nearTtls.add(ttl);
    }

    /**
     * @return TTLs for optimistic transaction.
     */
    public GridLongList nearTtls() {
        return nearTtls;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

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

        if (nearWritesBytes != null) {
            nearWrites = new ArrayList<>(nearWritesBytes.size());

            for (byte[] arr : nearWritesBytes)
                nearWrites.add(ctx.marshaller().<IgniteTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(nearWrites, true, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxFinishRequest.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtTxFinishRequest _clone = (GridDhtTxFinishRequest)_msg;

        _clone.nearNodeId = nearNodeId;
        _clone.isolation = isolation;
        _clone.nearWrites = nearWrites;
        _clone.nearWritesBytes = nearWritesBytes;
        _clone.miniId = miniId;
        _clone.sysInvalidate = sysInvalidate;
        _clone.topVer = topVer;
        _clone.pendingVers = pendingVers;
        _clone.onePhaseCommit = onePhaseCommit;
        _clone.writeVer = writeVer != null ? (GridCacheVersion)writeVer.clone() : null;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
        _clone.ttls = ttls != null ? (GridLongList)ttls.clone() : null;
        _clone.nearTtls = nearTtls != null ? (GridLongList)nearTtls.clone() : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        MessageWriteState state = MessageWriteState.get();
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 21:
                if (!writer.writeEnum("isolation", isolation))
                    return false;

                state.increment();

            case 22:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                state.increment();

            case 23:
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                state.increment();

            case 24:
                if (!writer.writeMessage("nearTtls", nearTtls))
                    return false;

                state.increment();

            case 25:
                if (!writer.writeCollection("nearWritesBytes", nearWritesBytes, byte[].class))
                    return false;

                state.increment();

            case 26:
                if (!writer.writeBoolean("onePhaseCommit", onePhaseCommit))
                    return false;

                state.increment();

            case 27:
                if (!writer.writeCollection("pendingVers", pendingVers, GridCacheVersion.class))
                    return false;

                state.increment();

            case 28:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                state.increment();

            case 29:
                if (!writer.writeBoolean("sysInvalidate", sysInvalidate))
                    return false;

                state.increment();

            case 30:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                state.increment();

            case 31:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                state.increment();

            case 32:
                if (!writer.writeMessage("ttls", ttls))
                    return false;

                state.increment();

            case 33:
                if (!writer.writeMessage("writeVer", writeVer))
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
            case 21:
                isolation = reader.readEnum("isolation", IgniteTxIsolation.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 22:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 23:
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 24:
                nearTtls = reader.readMessage("nearTtls");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 25:
                nearWritesBytes = reader.readCollection("nearWritesBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 26:
                onePhaseCommit = reader.readBoolean("onePhaseCommit");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 27:
                pendingVers = reader.readCollection("pendingVers", GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 28:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 29:
                sysInvalidate = reader.readBoolean("sysInvalidate");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 30:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 31:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 32:
                ttls = reader.readMessage("ttls");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 33:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 32;
    }
}
