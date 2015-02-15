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

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near transaction prepare request.
 */
public class GridNearTxPrepareRequest<K, V> extends GridDistributedTxPrepareRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Near mapping flag. */
    private boolean near;

    /** Topology version. */
    private long topVer;

    /** {@code True} if this last prepare request for node. */
    private boolean last;

    /** IDs of backup nodes receiving last prepare request during this prepare. */
    @GridDirectCollection(UUID.class)
    private Collection<UUID> lastBackups;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTxPrepareRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param tx Transaction.
     * @param reads Read entries.
     * @param writes Write entries.
     * @param grpLockKey Group lock key if preparing group-lock transaction.
     * @param partLock {@code True} if preparing group-lock transaction with partition lock.
     * @param near {@code True} if mapping is for near caches.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this last prepare request for node.
     * @param lastBackups IDs of backup nodes receiving last prepare request during this prepare.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     */
    public GridNearTxPrepareRequest(
        IgniteUuid futId,
        long topVer,
        IgniteInternalTx<K, V> tx,
        Collection<IgniteTxEntry<K, V>> reads,
        Collection<IgniteTxEntry<K, V>> writes,
        IgniteTxKey grpLockKey,
        boolean partLock,
        boolean near,
        Map<UUID, Collection<UUID>> txNodes,
        boolean last,
        Collection<UUID> lastBackups,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(tx, reads, writes, grpLockKey, partLock, txNodes);

        assert futId != null;

        this.futId = futId;
        this.topVer = topVer;
        this.near = near;
        this.last = last;
        this.lastBackups = lastBackups;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return IDs of backup nodes receiving last prepare request during this prepare.
     */
    public Collection<UUID> lastBackups() {
        return lastBackups;
    }

    /**
     * @return {@code True} if this last prepare request for node.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return {@code True} if mapping is for near-enabled caches.
     */
    public boolean near() {
        return near;
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
     * @param miniId Mini future ID.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
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
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     *
     */
    public void cloneEntries() {
        reads(cloneEntries(reads()));
        writes(cloneEntries(writes()));
    }

    /**
     * Clones entries so that tx entries with initialized near entries are not passed to DHT transaction.
     * Used only when local part of prepare is invoked.
     *
     * @param c Collection of entries to clone.
     * @return Cloned collection.
     */
    private Collection<IgniteTxEntry<K, V>> cloneEntries(Collection<IgniteTxEntry<K, V>> c) {
        if (F.isEmpty(c))
            return c;

        Collection<IgniteTxEntry<K, V>> cp = new ArrayList<>(c.size());

        for (IgniteTxEntry<K, V> e : c) {
            GridCacheContext<K, V> cacheCtx = e.context();

            // Clone only if it is a near cache.
            if (cacheCtx.isNear())
                cp.add(e.cleanCopy(cacheCtx.nearTx().dht().context()));
            else
                cp.add(e);
        }

        return cp;
    }

    /** {@inheritDoc} */
    @Override protected boolean transferExpiryPolicy() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridNearTxPrepareRequest _clone = (GridNearTxPrepareRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.near = near;
        _clone.topVer = topVer;
        _clone.last = last;
        _clone.lastBackups = lastBackups;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 22:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeBoolean("last", last))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection("lastBackups", lastBackups, Type.UUID))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeBoolean("near", near))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 29:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

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
            case 22:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 23:
                last = reader.readBoolean("last");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 24:
                lastBackups = reader.readCollection("lastBackups", Type.UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 25:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 26:
                near = reader.readBoolean("near");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 27:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 28:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 29:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 55;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxPrepareRequest.class, this, super.toString());
    }
}
