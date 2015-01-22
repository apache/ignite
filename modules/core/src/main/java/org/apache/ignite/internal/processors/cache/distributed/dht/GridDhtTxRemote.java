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

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridDhtTxRemote<K, V> extends GridDistributedTxRemoteAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Remote future ID. */
    private IgniteUuid rmtFutId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Transaction nodes mapping (primary node -> related backup nodes). */
    private Map<UUID, Collection<UUID>> txNodes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxRemote() {
        // No-op.
    }

    /**
     * This constructor is meant for optimistic transactions.
     *
     * @param nearNodeId Near node ID.
     * @param rmtFutId Remote future ID.
     * @param nodeId Node ID.
     * @param rmtThreadId Remote thread ID.
     * @param topVer Topology version.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param ctx Cache context.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @param nearXidVer Near transaction ID.
     * @param txNodes Transaction nodes mapping.
     */
    public GridDhtTxRemote(
        GridCacheSharedContext<K, V> ctx,
        UUID nearNodeId,
        IgniteUuid rmtFutId,
        UUID nodeId,
        long rmtThreadId,
        long topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        @Nullable IgniteTxKey grpLockKey,
        GridCacheVersion nearXidVer,
        Map<UUID, Collection<UUID>> txNodes,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, sys, concurrency, isolation, invalidate, timeout, txSize,
            grpLockKey, subjId, taskNameHash);

        assert nearNodeId != null;
        assert rmtFutId != null;

        this.nearNodeId = nearNodeId;
        this.rmtFutId = rmtFutId;
        this.nearXidVer = nearXidVer;
        this.txNodes = txNodes;

        readMap = Collections.emptyMap();

        writeMap = new ConcurrentLinkedHashMap<>(txSize, 1.0f);

        topologyVersion(topVer);
    }

    /**
     * This constructor is meant for pessimistic transactions.
     *
     * @param nearNodeId Near node ID.
     * @param rmtFutId Remote future ID.
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param rmtThreadId Remote thread ID.
     * @param topVer Topology version.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param ctx Cache context.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if transaction is group-lock.
     */
    public GridDhtTxRemote(
        GridCacheSharedContext<K, V> ctx,
        UUID nearNodeId,
        IgniteUuid rmtFutId,
        UUID nodeId,
        GridCacheVersion nearXidVer,
        long rmtThreadId,
        long topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        @Nullable IgniteTxKey grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, sys, concurrency, isolation, invalidate, timeout, txSize,
            grpLockKey, subjId, taskNameHash);

        assert nearNodeId != null;
        assert rmtFutId != null;

        this.nearXidVer = nearXidVer;
        this.nearNodeId = nearNodeId;
        this.rmtFutId = rmtFutId;

        readMap = Collections.emptyMap();
        writeMap = new ConcurrentLinkedHashMap<>(txSize, 1.0f);

        topologyVersion(topVer);
    }

    /** {@inheritDoc} */
    @Override public boolean dht() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nearNodeId();
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        return Arrays.asList(nearNodeId, nodeId);
    }

    /** {@inheritDoc} */
    @Override public UUID otherNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean enforceSerializable() {
        return false; // Serializable will be enforced on primary mode.
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /**
     * @return Near node ID.
     */
    UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return Remote future ID.
     */
    IgniteUuid remoteFutureId() {
        return rmtFutId;
    }

    /** {@inheritDoc} */
    @Override protected boolean updateNearCache(GridCacheContext<K, V> cacheCtx, K key, long topVer) {
        if (!cacheCtx.isDht() || !isNearEnabled(cacheCtx) || cctx.localNodeId().equals(nearNodeId))
            return false;

        if (cacheCtx.config().getBackups() == 0)
            return true;

        // Check if we are on the backup node.
        return !cacheCtx.affinity().backups(key, topVer).contains(cctx.localNode());
    }

    /** {@inheritDoc} */
    @Override public void addInvalidPartition(GridCacheContext<K, V> cacheCtx, int part) {
        super.addInvalidPartition(cacheCtx, part);

        for (Iterator<IgniteTxEntry<K, V>> it = writeMap.values().iterator(); it.hasNext();) {
            IgniteTxEntry<K, V> e = it.next();

            GridCacheEntryEx<K, V> cached = e.cached();

            if (cached != null) {
                if (cached.partition() == part)
                    it.remove();
            }
            else if (cacheCtx.affinity().partition(e.key()) == part)
                it.remove();
        }
    }

    /**
     * @param entry Write entry.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void addWrite(IgniteTxEntry<K, V> entry, ClassLoader ldr) throws IgniteCheckedException {
        entry.unmarshal(cctx, false, ldr);

        GridCacheContext<K, V> cacheCtx = entry.context();

        try {
            GridDhtCacheEntry<K, V> cached = cacheCtx.dht().entryExx(entry.key(), topologyVersion());

            checkInternal(entry.txKey());

            // Initialize cache entry.
            entry.cached(cached, entry.keyBytes());

            writeMap.put(entry.txKey(), entry);

            addExplicit(entry);
        }
        catch (GridDhtInvalidPartitionException e) {
            addInvalidPartition(cacheCtx, e.partition());
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param op Write operation.
     * @param key Key to add to write set.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param drVer Data center replication version.
     * @param entryProcessors Entry processors.
     * @param ttl TTL.
     */
    public void addWrite(GridCacheContext<K, V> cacheCtx,
        GridCacheOperation op,
        IgniteTxKey<K> key,
        byte[] keyBytes,
        @Nullable V val,
        @Nullable byte[] valBytes,
        @Nullable Collection<T2<EntryProcessor<K, V, ?>, Object[]>> entryProcessors,
        @Nullable GridCacheVersion drVer,
        long ttl) {
        checkInternal(key);

        if (isSystemInvalidate())
            return;

        GridDhtCacheEntry<K, V> cached = cacheCtx.dht().entryExx(key.key(), topologyVersion());

        IgniteTxEntry<K, V> txEntry = new IgniteTxEntry<>(cacheCtx,
            this,
            op,
            val,
            ttl,
            -1L,
            cached,
            drVer);

        txEntry.keyBytes(keyBytes);
        txEntry.valueBytes(valBytes);
        txEntry.entryProcessors(entryProcessors);

        writeMap.put(key, txEntry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDhtTxRemote.class, this, "super", super.toString());
    }
}
