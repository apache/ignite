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
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishRequest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /** Near node ID. */
    private UUID nearNodeId;

    /** Transaction isolation. */
    private TransactionIsolation isolation;

    /** Mini future ID. */
    private int miniId;

    /** Pending versions with order less than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> pendingVers;

    /** Partition update counter. */
    @GridToStringInclude
    @GridDirectCollection(Long.class)
    private GridLongList partUpdateCnt;

    /** One phase commit write version. */
    private GridCacheVersion writeVer;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** */
    @GridDirectCollection(PartitionUpdateCountersMessage.class)
    private Collection<PartitionUpdateCountersMessage> updCntrs;

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
     * @param syncMode Write synchronization mode.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pendingVers Pending versions.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param addDepInfo Deployment info flag.
     * @param retVal Need return value
     * @param waitRemoteTxs Wait remote transactions flag
     * @param mvccSnapshot Mvcc snapshot.
     * @param updCntrs Update counters for mvcc Tx.
     */
    public GridDhtTxFinishRequest(
        UUID nearNodeId,
        IgniteUuid futId,
        int miniId,
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
        CacheWriteSynchronizationMode syncMode,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        boolean retVal,
        boolean waitRemoteTxs,
        MvccSnapshot mvccSnapshot,
        Collection<PartitionUpdateCountersMessage> updCntrs
    ) {
        super(
            xidVer,
            futId,
            topVer,
            commitVer,
            threadId,
            commit,
            invalidate,
            sys,
            plc,
            syncMode,
            baseVer,
            committedVers,
            rolledbackVers,
            subjId,
            taskNameHash,
            txSize,
            addDepInfo);

        assert miniId != 0;
        assert nearNodeId != null;
        assert isolation != null;

        this.pendingVers = pendingVers;
        this.nearNodeId = nearNodeId;
        this.isolation = isolation;
        this.miniId = miniId;
        this.mvccSnapshot = mvccSnapshot;
        this.updCntrs = updCntrs;

        needReturnValue(retVal);
        waitRemoteTransactions(waitRemoteTxs);
        systemInvalidate(sysInvalidate);
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
     * @param syncMode Write synchronization mode.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pendingVers Pending versions.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param updateIdxs Partition update idxs.
     * @param addDepInfo Deployment info flag.
     * @param retVal Need return value
     * @param waitRemoteTxs Wait remote transactions flag
     * @param mvccSnapshot Mvcc snapshot.
     * @param updCntrs Update counters for mvcc Tx.
     */
    public GridDhtTxFinishRequest(
        UUID nearNodeId,
        IgniteUuid futId,
        int miniId,
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
        CacheWriteSynchronizationMode syncMode,
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
        boolean waitRemoteTxs,
        MvccSnapshot mvccSnapshot,
        Collection<PartitionUpdateCountersMessage> updCntrs
    ) {
        this(nearNodeId,
            futId,
            miniId,
            topVer,
            xidVer,
            commitVer,
            threadId,
            isolation,
            commit,
            invalidate,
            sys,
            plc,
            sysInvalidate,
            syncMode,
            baseVer,
            committedVers,
            rolledbackVers,
            pendingVers,
            txSize,
            subjId,
            taskNameHash,
            addDepInfo,
            retVal,
            waitRemoteTxs,
            mvccSnapshot,
            updCntrs);
    }

    /**
     * @return Counter.
     */
    public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Partition update counters.
     */
    public GridLongList partUpdateCounters() {
        return partUpdateCnt;
    }

    /**
     * @return Mini ID.
     */
    public int miniId() {
        return miniId;
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
        return isFlag(SYS_INVALIDATE_FLAG_MASK);
    }

    /**
     * @param sysInvalidate System invalidation flag.
     */
    private void systemInvalidate(boolean sysInvalidate) {
        setFlag(sysInvalidate, SYS_INVALIDATE_FLAG_MASK);
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
     * @return Check committed flag.
     */
    public boolean checkCommitted() {
        return isFlag(CHECK_COMMITTED_FLAG_MASK);
    }

    /**
     * @param checkCommitted Check committed flag.
     */
    public void checkCommitted(boolean checkCommitted) {
        setFlag(checkCommitted, CHECK_COMMITTED_FLAG_MASK);
    }

    /**
     * @return {@code True}
     */
    public boolean waitRemoteTransactions() {
        return isFlag(WAIT_REMOTE_TX_FLAG_MASK);
    }

    /**
     * @param waitRemoteTxs Wait remote transactions flag.
     */
    private void waitRemoteTransactions(boolean waitRemoteTxs) {
        setFlag(waitRemoteTxs, WAIT_REMOTE_TX_FLAG_MASK);
    }

    /**
     * @return Flag indicating whether transaction needs return value.
     */
    public boolean needReturnValue() {
        return isFlag(NEED_RETURN_VALUE_FLAG_MASK);
    }

    /**
     * @param retVal Need return value.
     */
    public void needReturnValue(boolean retVal) {
        setFlag(retVal, NEED_RETURN_VALUE_FLAG_MASK);
    }

    /**
     * @return Partition counters update deferred until transaction commit.
     */
    public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs;
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
                if (!writer.writeByte("isolation", isolation != null ? (byte)isolation.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeMessage("partUpdateCnt", partUpdateCnt))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeCollection("pendingVers", pendingVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeCollection("updCntrs", updCntrs, MessageCollectionItemType.MSG))
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
            case 22:
                byte isolationOrd;

                isolationOrd = reader.readByte("isolation");

                if (!reader.isLastRead())
                    return false;

                isolation = TransactionIsolation.fromOrdinal(isolationOrd);

                reader.incrementState();

            case 23:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                partUpdateCnt = reader.readMessage("partUpdateCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                pendingVers = reader.readCollection("pendingVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 28:
                updCntrs = reader.readCollection("updCntrs", MessageCollectionItemType.MSG);

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
    @Override public short directType() {
        return 32;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 30;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return U.safeAbs(version().hashCode());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxFinishRequest.class, this, super.toString());
    }
}
