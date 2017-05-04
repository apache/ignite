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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Near transaction finish request.
 */
public class GridDhtTxFinishRequest extends GridDistributedTxFinishRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int WAIT_REMOTE_TX_FLAG_MASK = 0x01;

    /** */
    public static final int NEED_RETURN_VALUE_FLAG_MASK = 0x02;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Transaction isolation. */
    private TransactionIsolation isolation;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** System invalidation flag. */
    private boolean sysInvalidate;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Pending versions with order less than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> pendingVers;

    /** Check committed flag. */
    private boolean checkCommitted;

    /** Partition update counter. */
    @GridToStringInclude
    @GridDirectCollection(Long.class)
    private GridLongList partUpdateCnt;

    /** One phase commit write version. */
    private GridCacheVersion writeVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** */
    private byte flags;

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
     * @param plc IO policy.
     * @param sysInvalidate System invalidation flag.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pendingVers Pending versions.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param addDepInfo Deployment info flag.
     */
    public GridDhtTxFinishRequest(
        UUID nearNodeId,
        IgniteUuid futId,
        IgniteUuid miniId,
        @NotNull AffinityTopologyVersion topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        long threadId,
        TransactionIsolation isolation,
        boolean commit,
        boolean invalidate,
        boolean sys,
        byte plc,
        boolean sysInvalidate,
        boolean syncCommit,
        boolean syncRollback,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        boolean retVal,
        boolean waitRemoteTxs
    ) {
        super(
            xidVer,
            futId,
            commitVer,
            threadId,
            commit,
            invalidate,
            sys,
            plc,
            syncCommit,
            syncRollback,
            baseVer,
            committedVers,
            rolledbackVers,
            txSize,
            addDepInfo);

        assert miniId != null;
        assert nearNodeId != null;
        assert isolation != null;

        this.pendingVers = pendingVers;
        this.topVer = topVer;
        this.nearNodeId = nearNodeId;
        this.isolation = isolation;
        this.miniId = miniId;
        this.sysInvalidate = sysInvalidate;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        needReturnValue(retVal);
        waitRemoteTransactions(waitRemoteTxs);
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
     * @param plc IO policy.
     * @param sysInvalidate System invalidation flag.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pendingVers Pending versions.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param updateIdxs Partition update idxs.
     * @param addDepInfo Deployment info flag.
     */
    public GridDhtTxFinishRequest(
        UUID nearNodeId,
        IgniteUuid futId,
        IgniteUuid miniId,
        @NotNull AffinityTopologyVersion topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        long threadId,
        TransactionIsolation isolation,
        boolean commit,
        boolean invalidate,
        boolean sys,
        byte plc,
        boolean sysInvalidate,
        boolean syncCommit,
        boolean syncRollback,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        Collection<Long> updateIdxs,
        boolean retVal,
        boolean waitRemoteTxs
    ) {
        this(nearNodeId, futId, miniId, topVer, xidVer, commitVer, threadId, isolation, commit, invalidate, sys, plc,
            sysInvalidate, syncCommit, syncRollback, baseVer, committedVers, rolledbackVers, pendingVers, txSize,
            subjId, taskNameHash, addDepInfo, retVal, waitRemoteTxs);

        if (updateIdxs != null && !updateIdxs.isEmpty()) {
            partUpdateCnt = new GridLongList(updateIdxs.size());

            for (Long idx : updateIdxs)
                partUpdateCnt.add(idx);
        }
    }

    /**
     * @return Partition update counters.
     */
    public GridLongList partUpdateCounters(){
        return partUpdateCnt;
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
    public TransactionIsolation isolation() {
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
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Check committed flag.
     */
    public boolean checkCommitted() {
        return checkCommitted;
    }

    /**
     * @param checkCommitted Check committed flag.
     */
    public void checkCommitted(boolean checkCommitted) {
        this.checkCommitted = checkCommitted;
    }

    /**
     * @return {@code True}
     */
    public boolean waitRemoteTransactions() {
        return (flags & WAIT_REMOTE_TX_FLAG_MASK) != 0;
    }

    /**
     * @param waitRemoteTxs Wait remote transactions flag.
     */
    public void waitRemoteTransactions(boolean waitRemoteTxs) {
        if (waitRemoteTxs)
            flags = (byte)(flags | WAIT_REMOTE_TX_FLAG_MASK);
        else
            flags &= ~WAIT_REMOTE_TX_FLAG_MASK;
    }

    /**
     * @return Flag indicating whether transaction needs return value.
     */
    public boolean needReturnValue() {
        return (flags & NEED_RETURN_VALUE_FLAG_MASK) != 0;
    }

    /**
     * @param retVal Need return value.
     */
    public void needReturnValue(boolean retVal) {
        if (retVal)
            flags = (byte)(flags | NEED_RETURN_VALUE_FLAG_MASK);
        else
            flags &= ~NEED_RETURN_VALUE_FLAG_MASK;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxFinishRequest.class, this, super.toString());
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
            case 18:
                if (!writer.writeBoolean("checkCommitted", checkCommitted))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeByte("isolation", isolation != null ? (byte)isolation.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMessage("partUpdateCnt", partUpdateCnt))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection("pendingVers", pendingVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeBoolean("sysInvalidate", sysInvalidate))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 29:
                if (!writer.writeMessage("writeVer", writeVer))
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
            case 18:
                checkCommitted = reader.readBoolean("checkCommitted");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                byte isolationOrd;

                isolationOrd = reader.readByte("isolation");

                if (!reader.isLastRead())
                    return false;

                isolation = TransactionIsolation.fromOrdinal(isolationOrd);

                reader.incrementState();

            case 21:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                partUpdateCnt = reader.readMessage("partUpdateCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                pendingVers = reader.readCollection("pendingVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                sysInvalidate = reader.readBoolean("sysInvalidate");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 28:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 29:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxFinishRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 32;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 30;
    }
}
