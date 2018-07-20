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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Near transaction prepare request to primary node. 'Near' means 'Initiating node' here, not 'Near Cache'.
 */
public class GridNearTxPrepareRequest extends GridDistributedTxPrepareRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int NEAR_FLAG_MASK = 0x01;

    /** */
    private static final int FIRST_CLIENT_REQ_FLAG_MASK = 0x02;

    /** */
    private static final int IMPLICIT_SINGLE_FLAG_MASK = 0x04;

    /** */
    private static final int EXPLICIT_LOCK_FLAG_MASK = 0x08;

    /** */
    private static final int ALLOW_WAIT_TOP_FUT_FLAG_MASK = 0x10;

    /** */
    private static final int REQUEST_MVCC_CNTR_FLAG_MASK = 0x20;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private int miniId;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** */
    @GridToStringExclude
    private byte flags;

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
     * @param timeout Transaction timeout.
     * @param reads Read entries.
     * @param writes Write entries.
     * @param near {@code True} if mapping is for near caches.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this last prepare request for node.
     * @param onePhaseCommit One phase commit flag.
     * @param retVal Return value flag.
     * @param implicitSingle Implicit single flag.
     * @param explicitLock Explicit lock flag.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param firstClientReq {@code True} if first optimistic tx prepare request sent from client node.
     * @param allowWaitTopFut {@code True} if it is safe for first client request to wait for topology future.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearTxPrepareRequest(
        IgniteUuid futId,
        AffinityTopologyVersion topVer,
        GridNearTxLocal tx,
        long timeout,
        Collection<IgniteTxEntry> reads,
        Collection<IgniteTxEntry> writes,
        boolean near,
        Map<UUID, Collection<UUID>> txNodes,
        boolean last,
        boolean onePhaseCommit,
        boolean retVal,
        boolean implicitSingle,
        boolean explicitLock,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean firstClientReq,
        boolean allowWaitTopFut,
        boolean addDepInfo
    ) {
        super(tx,
            timeout,
            reads,
            writes,
            txNodes,
            retVal,
            last,
            onePhaseCommit,
            addDepInfo);

        assert futId != null;
        assert !firstClientReq || tx.optimistic() : tx;

        this.futId = futId;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        setFlag(near, NEAR_FLAG_MASK);
        setFlag(implicitSingle, IMPLICIT_SINGLE_FLAG_MASK);
        setFlag(explicitLock, EXPLICIT_LOCK_FLAG_MASK);
        setFlag(firstClientReq, FIRST_CLIENT_REQ_FLAG_MASK);
        setFlag(allowWaitTopFut, ALLOW_WAIT_TOP_FUT_FLAG_MASK);
    }

    /**
     * @return {@code True} if need request MVCC counter on primary node on prepare step.
     */
    public boolean requestMvccCounter() {
        return isFlag(REQUEST_MVCC_CNTR_FLAG_MASK);
    }

    /**
     * @param val {@code True} if need request MVCC counter on primary node on prepare step.
     */
    public void requestMvccCounter(boolean val) {
        setFlag(val, REQUEST_MVCC_CNTR_FLAG_MASK);
    }

    /**
     * @return {@code True} if it is safe for first client request to wait for topology future
     *      completion.
     */
    public boolean allowWaitTopologyFuture() {
        return isFlag(ALLOW_WAIT_TOP_FUT_FLAG_MASK);
    }

    /**
     * @return {@code True} if first optimistic tx prepare request sent from client node.
     */
    public final boolean firstClientRequest() {
        return isFlag(FIRST_CLIENT_REQ_FLAG_MASK);
    }

    /**
     * @return {@code True} if mapping is for near-enabled caches.
     */
    public final boolean near() {
        return isFlag(NEAR_FLAG_MASK);
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
    public int miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future ID.
     */
    public void miniId(int miniId) {
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
     * @return Implicit single flag.
     */
    public final boolean implicitSingle() {
        return isFlag(IMPLICIT_SINGLE_FLAG_MASK);
    }

    /**
     * @return Explicit lock flag.
     */
    public final boolean explicitLock() {
        return isFlag(EXPLICIT_LOCK_FLAG_MASK);
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

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
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
            case 20:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 25:
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
            case 20:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxPrepareRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 55;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 26;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return U.safeAbs(version().hashCode());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (near())
            flags.append("[near]");
        if (firstClientRequest())
            flags.append("[firstClientReq]");
        if (implicitSingle())
            flags.append("[implicitSingle]");
        if (explicitLock())
            flags.append("[explicitLock]");

        return S.toString(GridNearTxPrepareRequest.class, this,
            "flags", flags.toString(),
            "super", super.toString());
    }
}
