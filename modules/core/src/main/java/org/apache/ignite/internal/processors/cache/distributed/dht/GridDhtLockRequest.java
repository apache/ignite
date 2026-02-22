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

import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * DHT lock request.
 */
public class GridDhtLockRequest extends GridDistributedLockRequest {
    /** Invalidate reader flags. */
    @Order(20)
    BitSet invalidateEntries;

    /** Mini future ID. */
    @Order(21)
    IgniteUuid miniId;

    /** Owner mapped version, if any. */
    @GridToStringInclude
    private Map<KeyCacheObject, GridCacheVersion> owned;

    /** Array of keys from {@link #owned}. Used during marshalling and unmarshalling. */
    @Order(22)
    @GridToStringExclude
    KeyCacheObject[] ownedKeys;

    /** Array of values from {@link #owned}. Used during marshalling and unmarshalling. */
    @Order(23)
    @GridToStringExclude
    GridCacheVersion[] ownedValues;

    /** Topology version. */
    @Order(24)
    AffinityTopologyVersion topVer;

    /** Task name hash. */
    @Order(25)
    int taskNameHash;

    /** Indexes of keys needed to be preloaded. */
    @Order(26)
    BitSet preloadKeys;

    /** TTL for read operation. */
    @Order(27)
    long accessTtl;

    /** Transaction label. */
    @Order(28)
    String txLbl;

    /**
     * Empty constructor.
     */
    public GridDhtLockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param threadId Thread ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param lockVer Cache version.
     * @param topVer Topology version.
     * @param isInTx {@code True} if implicit transaction lock.
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param dhtCnt DHT count.
     * @param txSize Expected transaction size.
     * @param taskNameHash Task name hash code.
     * @param accessTtl TTL for read operation.
     * @param skipStore Skip store flag.
     * @param storeUsed Cache store used flag.
     * @param keepBinary Keep binary flag.
     * @param addDepInfo Deployment info flag.
     * @param txLbl Transaction label.
     */
    public GridDhtLockRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion nearXidVer,
        long threadId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion lockVer,
        @NotNull AffinityTopologyVersion topVer,
        boolean isInTx,
        boolean isRead,
        TransactionIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int dhtCnt,
        int txSize,
        int taskNameHash,
        long accessTtl,
        boolean skipStore,
        boolean skipReadThrough,
        boolean storeUsed,
        boolean keepBinary,
        boolean addDepInfo,
        String txLbl
    ) {
        super(cacheId,
            nodeId,
            nearXidVer,
            threadId,
            futId,
            lockVer,
            isInTx,
            isRead,
            isolation,
            isInvalidate,
            timeout,
            dhtCnt,
            txSize,
            skipStore,
            skipReadThrough,
            keepBinary,
            addDepInfo);

        this.topVer = topVer;

        storeUsed(storeUsed);

        invalidateEntries = new BitSet(dhtCnt);

        assert miniId != null;

        this.miniId = miniId;
        this.taskNameHash = taskNameHash;
        this.accessTtl = accessTtl;

        this.txLbl = txLbl;
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
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Adds a DHT key.
     *
     * @param key Key.
     * @param invalidateEntry Flag indicating whether node should attempt to invalidate reader.
     */
    public void addDhtKey(KeyCacheObject key, boolean invalidateEntry) {
        invalidateEntries.set(idx, invalidateEntry);

        addKeyBytes(key, false);
    }

    /**
     * Marks last added key for preloading.
     */
    public void markLastKeyForPreload() {
        assert idx > 0;

        if (preloadKeys == null)
            preloadKeys = new BitSet();

        preloadKeys.set(idx - 1, true);
    }

    /**
     * @param idx Key index.
     * @return {@code True} if need to preload key with given index.
     */
    public boolean needPreloadKey(int idx) {
        return preloadKeys != null && preloadKeys.get(idx);
    }

    /**
     * Sets owner and its mapped version.
     *
     * @param key Key.
     * @param ownerMapped Owner mapped version.
     */
    public void owned(KeyCacheObject key, GridCacheVersion ownerMapped) {
        if (owned == null)
            owned = new GridLeanMap<>(3);

        owned.put(key, ownerMapped);
    }

    /**
     * @param idx Entry index to check.
     * @return {@code True} if near entry should be invalidated.
     */
    public boolean invalidateNearEntry(int idx) {
        return invalidateEntries.get(idx);
    }

    /**
     * @return Mini ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return TTL for read operation.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /**
     * @return Transaction label.
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (owned != null && ownedKeys == null) {
            ownedKeys = new KeyCacheObject[owned.size()];
            ownedValues = new GridCacheVersion[ownedKeys.length];

            int i = 0;

            for (Map.Entry<KeyCacheObject, GridCacheVersion> entry : owned.entrySet()) {
                ownedKeys[i] = entry.getKey();
                ownedValues[i] = entry.getValue();
                i++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedKeys != null) {
            owned = new GridLeanMap<>(ownedKeys.length);

            for (int i = 0; i < ownedKeys.length; i++) {
                ownedKeys[i].finishUnmarshal(ctx.cacheContext(cacheId).cacheObjectContext(), ldr);
                owned.put(ownedKeys[i], ownedValues[i]);
            }

            ownedKeys = null;
            ownedValues = null;
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 30;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockRequest.class, this, "super", super.toString());
    }
}
