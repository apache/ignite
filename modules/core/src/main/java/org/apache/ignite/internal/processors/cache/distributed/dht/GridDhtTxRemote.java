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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteSingleStateImpl;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteStateImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridDhtTxRemote extends GridDistributedTxRemoteAdapter {
    /** Near node ID. */
    private final UUID nearNodeId;

    /** Near transaction ID. */
    private final GridCacheVersion nearXidVer;

    /** Store write through flag. */
    private final boolean storeWriteThrough;

    /**
     * This constructor is meant for optimistic transactions.
     *
     * @param ctx Cache context.
     * @param nearNodeId Near node ID.
     * @param nodeId Node ID.
     * @param topVer Topology version.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param nearXidVer Near transaction ID.
     * @param txNodes Transaction nodes mapping.
     * @param storeWriteThrough Cache store write through flag.
     * @param txLbl Transaction label.
     */
    public GridDhtTxRemote(
        GridCacheSharedContext ctx,
        UUID nearNodeId,
        UUID nodeId,
        AffinityTopologyVersion topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        GridCacheVersion nearXidVer,
        Map<UUID, Collection<UUID>> txNodes,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean single,
        boolean storeWriteThrough,
        @Nullable String txLbl) {
        super(
            ctx,
            nodeId,
            xidVer,
            commitVer,
            sys,
            plc,
            concurrency,
            isolation,
            invalidate,
            timeout,
            txSize,
            subjId,
            taskNameHash,
            txLbl
        );

        assert nearNodeId != null;

        this.nearNodeId = nearNodeId;
        this.nearXidVer = nearXidVer;
        this.txNodes = txNodes;
        this.storeWriteThrough = storeWriteThrough;

        txState = single ? new IgniteTxRemoteSingleStateImpl() :
            new IgniteTxRemoteStateImpl(
                Collections.emptyMap(),
                new ConcurrentLinkedHashMap<>(U.capacity(txSize), 0.75f, 1));

        assert topVer != null && topVer.topologyVersion() > 0 : topVer;

        topologyVersion(topVer);
    }

    /**
     * This constructor is meant for pessimistic transactions.
     *
     * @param ctx Cache context.
     * @param nearNodeId Near node ID.
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param topVer Topology version.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param storeWriteThrough Cache store write through flag.
     * @param txLbl Transaction label.
     */
    public GridDhtTxRemote(
        GridCacheSharedContext ctx,
        UUID nearNodeId,
        UUID nodeId,
        GridCacheVersion nearXidVer,
        AffinityTopologyVersion topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean storeWriteThrough,
        @Nullable String txLbl) {
        super(
            ctx,
            nodeId,
            xidVer,
            commitVer,
            sys,
            plc,
            concurrency,
            isolation,
            invalidate,
            timeout,
            txSize,
            subjId,
            taskNameHash,
            txLbl
        );

        assert nearNodeId != null;

        this.nearXidVer = nearXidVer;
        this.nearNodeId = nearNodeId;
        this.storeWriteThrough = storeWriteThrough;

        txState = new IgniteTxRemoteStateImpl(
            Collections.emptyMap(),
            new ConcurrentLinkedHashMap<>(U.capacity(txSize), 0.75f, 1));

        assert topVer != null && topVer.topologyVersion() > 0 : topVer;

        topologyVersion(topVer);
    }

    /** {@inheritDoc} */
    @Override public boolean remote() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean dht() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storeWriteThrough() {
        return storeWriteThrough;
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nearNodeId();
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        Collection<UUID> res = new ArrayList<>(2);

        res.add(nearNodeId);
        res.add(nodeId);

        return res;
    }

    /** {@inheritDoc} */
    @Override public UUID otherNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override protected boolean updateNearCache(GridCacheContext cacheCtx, KeyCacheObject key, AffinityTopologyVersion topVer) {
        if (!cacheCtx.isDht() || !isNearEnabled(cacheCtx) || cctx.localNodeId().equals(nearNodeId))
            return false;

        if (cacheCtx.config().getBackups() == 0)
            return true;

        // Check if we are on the backup node.
        return !cacheCtx.affinity().backupsByKey(key, topVer).contains(cctx.localNode());
    }

    /** {@inheritDoc} */
    @Override public void addInvalidPartition(int cacheId, int part) {
        super.addInvalidPartition(cacheId, part);

        txState.invalidPartition(cacheId, part, xidVersion());
    }

    /**
     * @param entry Write entry.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void addWrite(IgniteTxEntry entry, ClassLoader ldr) throws IgniteCheckedException {
        entry.unmarshal(cctx, false, ldr);

        GridCacheContext cacheCtx = entry.context();

        GridDhtCacheEntry cached = cacheCtx.dht().entryExx(entry.key(), topologyVersion());

        checkInternal(entry.txKey());

        // Initialize cache entry.
        entry.cached(cached);

        txState.addWriteEntry(entry.txKey(), entry);

        addExplicit(entry);
    }

    /**
     * @param cacheCtx Cache context.
     * @param op Write operation.
     * @param key Key to add to write set.
     * @param val Value.
     * @param entryProcessors Entry processors.
     * @param ttl TTL.
     * @param skipStore Skip store flag.
     */
    public void addWrite(GridCacheContext cacheCtx,
        GridCacheOperation op,
        IgniteTxKey key,
        @Nullable CacheObject val,
        @Nullable Collection<T2<EntryProcessor<Object, Object, Object>, Object[]>> entryProcessors,
        long ttl,
        boolean skipStore,
        boolean keepBinary) {
        checkInternal(key);

        if (isSystemInvalidate())
            return;

        GridDhtCacheEntry cached = cacheCtx.dht().entryExx(key.key(), topologyVersion());

        IgniteTxEntry txEntry = new IgniteTxEntry(cacheCtx,
            this,
            op,
            val,
            ttl,
            -1L,
            cached,
            null,
            skipStore,
            keepBinary);

        txEntry.entryProcessors(entryProcessors);

        txState.addWriteEntry(key, txEntry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDhtTxRemote.class, this, "super", super.toString());
    }
}
