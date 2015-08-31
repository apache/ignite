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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridNearTxRemote extends GridDistributedTxRemoteAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Evicted keys. */
    private Collection<IgniteTxKey> evicted = new LinkedList<>();

    /** Near node ID. */
    private UUID nearNodeId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Owned versions. */
    private Map<IgniteTxKey, GridCacheVersion> owned;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTxRemote() {
        // No-op.
    }

    /**
     * This constructor is meant for optimistic transactions.
     *
     * @param ldr Class loader.
     * @param nodeId Node ID.
     * @param nearNodeId Near node ID.
     * @param rmtThreadId Remote thread ID.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param writeEntries Write entries.
     * @param ctx Cache registry.
     * @param txSize Expected transaction size.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public GridNearTxRemote(
        GridCacheSharedContext ctx,
        ClassLoader ldr,
        UUID nodeId,
        UUID nearNodeId,
        long rmtThreadId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean invalidate,
        long timeout,
        Collection<IgniteTxEntry> writeEntries,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) throws IgniteCheckedException {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, sys, plc, concurrency, isolation, invalidate, timeout,
            txSize, subjId, taskNameHash);

        assert nearNodeId != null;

        this.nearNodeId = nearNodeId;

        readMap = Collections.emptyMap();

        writeMap = new LinkedHashMap<>(
            writeEntries != null ? Math.max(txSize, writeEntries.size()) : txSize, 1.0f);

        if (writeEntries != null) {
            for (IgniteTxEntry entry : writeEntries) {
                entry.unmarshal(ctx, true, ldr);

                addEntry(entry);
            }
        }
    }

    /**
     * This constructor is meant for pessimistic transactions.
     *
     * @param nodeId Node ID.
     * @param nearNodeId Near node ID.
     * @param nearXidVer Near transaction ID.
     * @param rmtThreadId Remote thread ID.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param ctx Cache registry.
     * @param txSize Expected transaction size.
     */
    public GridNearTxRemote(
        GridCacheSharedContext ctx,
        UUID nodeId,
        UUID nearNodeId,
        GridCacheVersion nearXidVer,
        long rmtThreadId,
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
        int taskNameHash
    ) {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, sys, plc, concurrency, isolation, invalidate, timeout,
            txSize, subjId, taskNameHash);

        assert nearNodeId != null;

        this.nearXidVer = nearXidVer;
        this.nearNodeId = nearNodeId;

        readMap = new LinkedHashMap<>(1, 1.0f);
        writeMap = new LinkedHashMap<>(txSize, 1.0f);
    }

    /** {@inheritDoc} */
    @Override public boolean near() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean enforceSerializable() {
        return false; // Serializable will be enforced on primary mode.
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion ownedVersion(IgniteTxKey key) {
        return owned == null ? null : owned.get(key);
    }

    /**
     * @return Near transaction ID.
     */
    @Override public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * Adds owned versions to map.
     *
     * @param vers Map of owned versions.
     */
    public void ownedVersions(Map<IgniteTxKey, GridCacheVersion> vers) {
        if (F.isEmpty(vers))
            return;

        if (owned == null)
            owned = new GridLeanMap<>(vers.size());

        owned.putAll(vers);
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        return Arrays.asList(nodeId, nearNodeId);
    }

    /**
     * @return Evicted keys.
     */
    public Collection<IgniteTxKey> evicted() {
        return evicted;
    }

    /**
     * Adds evicted key bytes to evicted collection.
     *
     * @param key Evicted key.
     */
    public void addEvicted(IgniteTxKey key) {
        evicted.add(key);
    }

    /**
     * Adds entries to started near remote tx.
     *
     * @param ldr Class loader.
     * @param entries Entries to add.
     * @throws IgniteCheckedException If failed.
     */
    public void addEntries(ClassLoader ldr, Iterable<IgniteTxEntry> entries) throws IgniteCheckedException {
        for (IgniteTxEntry entry : entries) {
            entry.unmarshal(cctx, true, ldr);

            addEntry(entry);
        }
    }

    /**
     * @param entry Entry to enlist.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if entry was enlisted.
     */
    private boolean addEntry(IgniteTxEntry entry) throws IgniteCheckedException {
        checkInternal(entry.txKey());

        GridCacheContext cacheCtx = entry.context();

        if (!cacheCtx.isNear())
            cacheCtx = cacheCtx.dht().near().context();

        GridNearCacheEntry cached = cacheCtx.near().peekExx(entry.key());

        if (cached == null) {
            evicted.add(entry.txKey());

            return false;
        }
        else {
            cached.unswap();

            try {
                CacheObject val = cached.peek(true, false, false, null);

                if (val == null && cached.evictInternal(false, xidVer, null)) {
                    evicted.add(entry.txKey());

                    return false;
                }
                else {
                    // Initialize cache entry.
                    entry.cached(cached);

                    writeMap.put(entry.txKey(), entry);

                    addExplicit(entry);

                    return true;
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                evicted.add(entry.txKey());

                if (log.isDebugEnabled())
                    log.debug("Got removed entry when adding to remote transaction (will ignore): " + cached);

                return false;
            }
        }
    }

    /**
     * @param key Key to add to read set.
     * @param val Value.
     * @param drVer Data center replication version.
     * @param skipStore Skip store flag.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if entry has been enlisted.
     */
    public boolean addEntry(
        GridCacheContext cacheCtx,
        IgniteTxKey key,
        GridCacheOperation op,
        CacheObject val,
        @Nullable GridCacheVersion drVer,
        boolean skipStore
    ) throws IgniteCheckedException {
        checkInternal(key);

        GridNearCacheEntry cached = cacheCtx.near().peekExx(key.key());

        try {
            if (cached == null) {
                evicted.add(key);

                return false;
            }
            else {
                cached.unswap();

                CacheObject peek = cached.peek(true, false, false, null);

                if (peek == null && cached.evictInternal(false, xidVer, null)) {
                    cached.context().cache().removeIfObsolete(key.key());

                    evicted.add(key);

                    return false;
                }
                else {
                    IgniteTxEntry txEntry = new IgniteTxEntry(cacheCtx,
                        this,
                        op,
                        val,
                        -1L,
                        -1L,
                        cached,
                        drVer,
                        skipStore);

                    writeMap.put(key, txEntry);

                    return true;
                }
            }
        }
        catch (GridCacheEntryRemovedException ignore) {
            evicted.add(key);

            if (log.isDebugEnabled())
                log.debug("Got removed entry when adding reads to remote transaction (will ignore): " + cached);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridNearTxRemote.class, this, "super", super.toString());
    }
}