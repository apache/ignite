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

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Near cache lock request to primary node. 'Near' means 'Initiating node' here, not 'Near Cache'.
 */
public class GridNearLockRequest extends GridDistributedLockRequest {
    /** */
    private static final int NEED_RETURN_VALUE_FLAG_MASK = 0x01;

    /** */
    private static final int FIRST_CLIENT_REQ_FLAG_MASK = 0x02;

    /** */
    private static final int SYNC_COMMIT_FLAG_MASK = 0x04;

    /** */
    private static final int NEAR_CACHE_FLAG_MASK = 0x08;

    /** Topology version. */
    @Order(value = 20, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Mini future ID. */
    @Order(21)
    private int miniId;

    /** Array of mapped DHT versions for this entry. */
    @Order(value = 22, method = "dhtVersions")
    @GridToStringInclude
    private GridCacheVersion[] dhtVers;

    /** Task name hash. */
    @Order(23)
    private int taskNameHash;

    /** TTL for create operation. */
    @Order(24)
    private long createTtl;

    /** TTL for read operation. */
    @Order(25)
    private long accessTtl;

    /** */
    @Order(value = 26, method = "nearFlags")
    private byte flags;

    /** Transaction label. */
    @Order(value = 27, method = "txLabel")
    private String txLbl;

    /**
     * Empty constructor.
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
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param retVal Return value flag.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param keyCnt Number of keys.
     * @param txSize Expected transaction size.
     * @param syncCommit Synchronous commit flag.
     * @param taskNameHash Task name hash code.
     * @param createTtl TTL for create operation.
     * @param accessTtl TTL for read operation.
     * @param skipStore Skip store flag.
     * @param firstClientReq {@code True} if first lock request for lock operation sent from client node.
     * @param addDepInfo Deployment info flag.
     * @param txLbl Transaction label.
     */
    public GridNearLockRequest(
        int cacheId,
        @NotNull AffinityTopologyVersion topVer,
        UUID nodeId,
        long threadId,
        IgniteUuid futId,
        GridCacheVersion lockVer,
        boolean isInTx,
        boolean isRead,
        boolean retVal,
        TransactionIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int keyCnt,
        int txSize,
        boolean syncCommit,
        int taskNameHash,
        long createTtl,
        long accessTtl,
        boolean skipStore,
        boolean keepBinary,
        boolean firstClientReq,
        boolean nearCache,
        boolean addDepInfo,
        @Nullable String txLbl
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
            skipStore,
            keepBinary,
            addDepInfo);

        assert topVer.compareTo(AffinityTopologyVersion.ZERO) > 0;

        this.topVer = topVer;
        this.taskNameHash = taskNameHash;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;

        this.txLbl = txLbl;

        dhtVers = new GridCacheVersion[keyCnt];

        setFlag(syncCommit, SYNC_COMMIT_FLAG_MASK);
        setFlag(firstClientReq, FIRST_CLIENT_REQ_FLAG_MASK);
        setFlag(retVal, NEED_RETURN_VALUE_FLAG_MASK);
        setFlag(nearCache, NEAR_CACHE_FLAG_MASK);
    }

    /**
     * @return {@code True} if near cache enabled on originating node.
     */
    public boolean nearCache() {
        return isFlag(NEAR_CACHE_FLAG_MASK);
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

    /**
     * @return {@code True} if first lock request for lock operation sent from client node.
     */
    public boolean firstClientRequest() {
        return isFlag(FIRST_CLIENT_REQ_FLAG_MASK);
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @param taskNameHash Task name hash.
     */
    public void taskNameHash(int taskNameHash) {
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return Sync commit flag.
     */
    public boolean syncCommit() {
        return isFlag(SYNC_COMMIT_FLAG_MASK);
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future Id.
     */
    public void miniId(int miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Need return value flag.
     */
    public boolean needReturnValue() {
        return isFlag(NEED_RETURN_VALUE_FLAG_MASK);
    }

    /**
     * Adds a key.
     *
     * @param key Key.
     * @param retVal Flag indicating whether value should be returned.
     * @param dhtVer DHT version.
     */
    public void addKeyBytes(KeyCacheObject key, boolean retVal, @Nullable GridCacheVersion dhtVer) {
        dhtVers[idx] = dhtVer;

        // Delegate to super.
        addKeyBytes(key, retVal);
    }

    /**
     * @return Array of mapped DHT versions for this entry.
     */
    public GridCacheVersion[] dhtVersions() {
        return dhtVers;
    }

    /**
     * @param dhtVers Array of mapped DHT versions for this entry.
     */
    public void dhtVersions(GridCacheVersion[] dhtVers) {
        this.dhtVers = dhtVers;
    }

    /**
     * @param idx Index of the key.
     * @return DHT version for key at given index.
     */
    public GridCacheVersion dhtVersion(int idx) {
        return dhtVers[idx];
    }

    /**
     * @return New TTL to set after entry is created, -1 to leave unchanged.
     */
    public long createTtl() {
        return createTtl;
    }

    /**
     * @param createTtl New TTL to set after entry is created, -1 to leave unchanged.
     */
    public void createTtl(long createTtl) {
        this.createTtl = createTtl;
    }

    /**
     * @return TTL for read operation.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /**
     * @param accessTtl TTL for read operation.
     */
    public void accessTtl(long accessTtl) {
        this.accessTtl = accessTtl;
    }

    /**
     * @return Flags.
     */
    public byte nearFlags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void nearFlags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Transaction label.
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /**
     * @param txLbl Transaction label.
     */
    public void txLabel(String txLbl) {
        this.txLbl = txLbl;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 51;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockRequest.class, this, "super", super.toString());
    }
}
