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

import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.lang.IgniteUuid;
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
    @Order(7)
    public AffinityTopologyVersion topVer;

    /** Future ID. */
    @Order(8)
    public IgniteUuid futId;

    /** Thread ID. */
    @Order(9)
    public long threadId;

    /** Commit version. */
    @Order(10)
    public GridCacheVersion commitVer;

    /** Invalidate flag. */
    @Order(11)
    public boolean invalidate;

    /** Commit flag. */
    @Order(12)
    public boolean commit;

    /** Min version used as base for completed versions. */
    @Order(13)
    public GridCacheVersion baseVer;

    /** IO policy. */
    @Order(14)
    public byte plc;

    /** Task name hash. */
    @Order(15)
    public int taskNameHash;

    /** */
    @Order(16)
    public byte flags;

    /** Write synchronization mode. */
    @Order(17)
    public CacheWriteSynchronizationMode syncMode;

    /** Transient TX state. */
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
     * @param plc IO policy.
     * @param syncMode Write synchronization mode.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
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
        byte plc,
        CacheWriteSynchronizationMode syncMode,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        int taskNameHash,
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
        this.plc = plc;
        this.syncMode = syncMode;
        this.baseVer = baseVer;
        this.taskNameHash = taskNameHash;

        completedVersions(committedVers, rolledbackVers);
    }

    /**
     * @return Transaction write synchronization mode.
     */
    @Nullable public final CacheWriteSynchronizationMode syncMode() {
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
     * @return Topology version.
     */
    @Override public final AffinityTopologyVersion topologyVersion() {
        return topVer;
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
    @Override public short directType() {
        return 23;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishRequest.class, this,
            "super", super.toString());
    }
}
