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
import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Transaction completion message.
 */
public class GridDistributedTxFinishRequest extends GridDistributedBaseMessage implements IgniteTxStateAware {
    /** */
    protected static final int WAIT_REMOTE_TX_FLAG_MASK = 0x01;

    /** */
    protected static final int CHECK_COMMITTED_FLAG_MASK = 0x02;

    /** */
    protected static final int NEED_RETURN_VALUE_FLAG_MASK = 0x04;

    /** */
    protected static final int SYS_INVALIDATE_FLAG_MASK = 0x08;

    /** */
    protected static final int EXPLICIT_LOCK_FLAG_MASK = 0x10;

    /** */
    protected static final int STORE_ENABLED_FLAG_MASK = 0x20;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Future ID. */
    private IgniteUuid futId;

    /** Thread ID. */
    private long threadId;

    /** Commit version. */
    private GridCacheVersion commitVer;

    /** Invalidate flag. */
    private boolean invalidate;

    /** Commit flag. */
    private boolean commit;

    /** Min version used as base for completed versions. */
    private GridCacheVersion baseVer;

    /** Expected txSize. */
    private int txSize;

    /** System transaction flag. */
    private boolean sys;

    /** IO policy. */
    private byte plc;

    /** Task name hash. */
    private int taskNameHash;

    /** */
    private byte flags;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Transient TX state. */
    @GridDirectTransient
    private IgniteTxState txState;

    /**
     * Empty constructor.
     */
    public GridDistributedTxFinishRequest() {
        /* No-op. */
    }

    /**
     * @param xidVer Transaction ID.
     * @param futId future ID.
     * @param threadId Thread ID.
     * @param commitVer Commit version.
     * @param commit Commit flag.
     * @param invalidate Invalidate flag.
     * @param sys System transaction flag.
     * @param plc IO policy.
     * @param syncMode Write synchronization mode.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param txSize Expected transaction size.
     * @param addDepInfo Deployment info flag.
     */
    public GridDistributedTxFinishRequest(
        GridCacheVersion xidVer,
        IgniteUuid futId,
        @NotNull AffinityTopologyVersion topVer,
        @Nullable GridCacheVersion commitVer,
        long threadId,
        boolean commit,
        boolean invalidate,
        boolean sys,
        byte plc,
        CacheWriteSynchronizationMode syncMode,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        int taskNameHash,
        int txSize,
        boolean addDepInfo
    ) {
        super(xidVer, 0, addDepInfo);

        assert xidVer != null;
        assert syncMode != null;

        this.futId = futId;
        this.topVer = topVer;
        this.commitVer = commitVer;
        this.threadId = threadId;
        this.commit = commit;
        this.invalidate = invalidate;
        this.sys = sys;
        this.plc = plc;
        this.syncMode = syncMode;
        this.baseVer = baseVer;
        this.taskNameHash = taskNameHash;
        this.txSize = txSize;

        completedVersions(committedVers, rolledbackVers);
    }

    /**
     * @return Transaction write synchronization mode (can be null is message sent from old nodes).
     */
    public final CacheWriteSynchronizationMode syncMode() {
        return syncMode;
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    protected final void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    protected final boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return Task name hash.
     */
    public final int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Topology version.
     */
    @Override public final AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return System transaction flag.
     */
    public boolean system() {
        return sys;
    }

    /**
     * @return IO policy.
     */
    public byte policy() {
        return plc;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Commit version.
     */
    public GridCacheVersion commitVersion() {
        return commitVer;
    }

    /**
     * @return Commit flag.
     */
    public boolean commit() {
        return commit;
    }

    /**
     *
     * @return Invalidate flag.
     */
    public boolean isInvalidate() {
        return invalidate;
    }

    /**
     * @return Base version.
     */
    public GridCacheVersion baseVersion() {
        return baseVer;
    }

    /**
     * @return Expected tx size.
     */
    public int txSize() {
        return txSize;
    }

    /**
     *
     * @return {@code True} if reply is required.
     */
    public boolean replyRequired() {
        assert syncMode != null;

        return syncMode == FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public void txState(IgniteTxState txState) {
        this.txState = txState;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txFinishMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeMessage(baseVer))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean(commit))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage(commitVer))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeByte(flags))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeIgniteUuid(futId))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeBoolean(invalidate))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeByte(plc))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeByte(syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeBoolean(sys))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeInt(taskNameHash))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeLong(threadId))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeAffinityTopologyVersion(topVer))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeInt(txSize))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 8:
                baseVer = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                commit = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                commitVer = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                flags = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                futId = reader.readIgniteUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                invalidate = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                plc = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                byte syncModeOrd;

                syncModeOrd = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 16:
                sys = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                taskNameHash = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                threadId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                topVer = reader.readAffinityTopologyVersion();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                txSize = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 23;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishRequest.class, this,
            "super", super.toString());
    }
}
