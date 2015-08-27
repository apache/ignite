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
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.tostring.*;
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
public class GridNearTxPrepareRequest extends GridDistributedTxPrepareRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Near mapping flag. */
    private boolean near;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** {@code True} if this last prepare request for node. */
    private boolean last;

    /** IDs of backup nodes receiving last prepare request during this prepare. */
    @GridDirectCollection(UUID.class)
    @GridToStringInclude
    private Collection<UUID> lastBackups;

    /** Need return value flag. */
    private boolean retVal;

    /** Implicit single flag. */
    private boolean implicitSingle;

    /** Explicit lock flag. Set to true if at leat one entry was explicitly locked. */
    private boolean explicitLock;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** {@code True} if first optimistic tx prepare request sent from client node. */
    private boolean firstClientReq;

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
     * @param near {@code True} if mapping is for near caches.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this last prepare request for node.
     * @param lastBackups IDs of backup nodes receiving last prepare request during this prepare.
     * @param onePhaseCommit One phase commit flag.
     * @param retVal Return value flag.
     * @param implicitSingle Implicit single flag.
     * @param explicitLock Explicit lock flag.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param firstClientReq {@code True} if first optimistic tx prepare request sent from client node.
     */
    public GridNearTxPrepareRequest(
        IgniteUuid futId,
        AffinityTopologyVersion topVer,
        IgniteInternalTx tx,
        Collection<IgniteTxEntry> reads,
        Collection<IgniteTxEntry> writes,
        boolean near,
        Map<UUID, Collection<UUID>> txNodes,
        boolean last,
        Collection<UUID> lastBackups,
        boolean onePhaseCommit,
        boolean retVal,
        boolean implicitSingle,
        boolean explicitLock,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean firstClientReq
    ) {
        super(tx, reads, writes, txNodes, onePhaseCommit);

        assert futId != null;
        assert !firstClientReq || tx.optimistic() : tx;

        this.futId = futId;
        this.topVer = topVer;
        this.near = near;
        this.last = last;
        this.lastBackups = lastBackups;
        this.retVal = retVal;
        this.implicitSingle = implicitSingle;
        this.explicitLock = explicitLock;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.firstClientReq = firstClientReq;
    }

    /**
     * @return {@code True} if first optimistic tx prepare request sent from client node.
     */
    public boolean firstClientRequest() {
        return firstClientReq;
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
     * @return Whether return value is requested.
     */
    public boolean returnValue() {
        return retVal;
    }

    /**
     * @return Implicit single flag.
     */
    public boolean implicitSingle() {
        return implicitSingle;
    }

    /**
     * @return Explicit lock flag.
     */
    public boolean explicitLock() {
        return explicitLock;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
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
    private Collection<IgniteTxEntry> cloneEntries(Collection<IgniteTxEntry> c) {
        if (F.isEmpty(c))
            return c;

        Collection<IgniteTxEntry> cp = new ArrayList<>(c.size());

        for (IgniteTxEntry e : c) {
            GridCacheContext cacheCtx = e.context();

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
            case 22:
                if (!writer.writeBoolean("explicitLock", explicitLock))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeBoolean("firstClientReq", firstClientReq))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeBoolean("implicitSingle", implicitSingle))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeBoolean("last", last))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeCollection("lastBackups", lastBackups, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 29:
                if (!writer.writeBoolean("near", near))
                    return false;

                writer.incrementState();

            case 30:
                if (!writer.writeBoolean("retVal", retVal))
                    return false;

                writer.incrementState();

            case 31:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 32:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 33:
                if (!writer.writeMessage("topVer", topVer))
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
            case 22:
                explicitLock = reader.readBoolean("explicitLock");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                firstClientReq = reader.readBoolean("firstClientReq");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                implicitSingle = reader.readBoolean("implicitSingle");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                last = reader.readBoolean("last");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                lastBackups = reader.readCollection("lastBackups", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 28:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 29:
                near = reader.readBoolean("near");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 30:
                retVal = reader.readBoolean("retVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 31:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 32:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 33:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxPrepareRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 55;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 34;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxPrepareRequest.class, this, super.toString());
    }
}
