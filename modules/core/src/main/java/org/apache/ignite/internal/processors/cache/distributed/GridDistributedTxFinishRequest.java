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
    @Order(value = 7, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Future ID. */
    @Order(value = 8, method = "futureId")
    private IgniteUuid futId;

    /** Thread ID. */
    @Order(9)
    private long threadId;

    /** Commit version. */
    @Order(value = 10, method = "commitVersion")
    private GridCacheVersion commitVer;

    /** Invalidate flag. */
    @Order(value = 11, method = "isInvalidate")
    private boolean invalidate;

    /** Commit flag. */
    @Order(12)
    private boolean commit;

    /** Min version used as base for completed versions. */
    @Order(value = 13, method = "baseVersion")
    private GridCacheVersion baseVer;

    /** IO policy. */
    @Order(value = 14, method = "policy")
    private byte plc;

    /** Task name hash. */
    @Order(15)
    private int taskNameHash;

    /** */
    @Order(16)
    private byte flags;

    /** Write synchronization mode wrapper message. */
    @Order(value = 17)
    private CacheWriteSynchronizationMode syncMode;

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
     * @return Transaction write synchronization mode (can be null is message sent from old nodes).
     */
    @Nullable public final CacheWriteSynchronizationMode syncMode() {
        return syncMode;
    }

    /**
     * @param syncMode Transaction write synchronization mode wrapper message.
     */
    public void syncMode(CacheWriteSynchronizationMode syncMode) {
        this.syncMode = syncMode;
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
     * @param taskNameHash Task name hash.
     */
    public void taskNameHash(int taskNameHash) {
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return Topology version.
     */
    @Override public final AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return IO policy.
     */
    public byte policy() {
        return plc;
    }

    /**
     * @param plc IO policy.
     */
    public void policy(byte plc) {
        this.plc = plc;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @param threadId Thread ID.
     */
    public void threadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * @return Commit version.
     */
    public GridCacheVersion commitVersion() {
        return commitVer;
    }

    /**
     * @param commitVer Commit version.
     */
    public void commitVersion(GridCacheVersion commitVer) {
        this.commitVer = commitVer;
    }

    /**
     * @return Commit flag.
     */
    public boolean commit() {
        return commit;
    }

    /**
     * @param commit Commit flag.
     */
    public void commit(boolean commit) {
        this.commit = commit;
    }

    /**
     *
     * @return Invalidate flag.
     */
    public boolean isInvalidate() {
        return invalidate;
    }

    /**
     * @param invalidate Invalidate flag.
     */
    public void isInvalidate(boolean invalidate) {
        this.invalidate = invalidate;
    }

    /**
     * @return Base version.
     */
    public GridCacheVersion baseVersion() {
        return baseVer;
    }

    /**
     * @param baseVer Base version.
     */
    public void baseVersion(GridCacheVersion baseVer) {
        this.baseVer = baseVer;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
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
