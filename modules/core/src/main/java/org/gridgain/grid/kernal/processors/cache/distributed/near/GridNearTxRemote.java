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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCachePeekMode.*;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridNearTxRemote<K, V> extends GridDistributedTxRemoteAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Evicted keys. */
    private Collection<IgniteTxKey<K>> evicted = new LinkedList<>();

    /** Near node ID. */
    private UUID nearNodeId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Owned versions. */
    private Map<IgniteTxKey<K>, GridCacheVersion> owned;

    /** Group lock flag. */
    private boolean grpLock;

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
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public GridNearTxRemote(
        GridCacheSharedContext<K, V> ctx,
        ClassLoader ldr,
        UUID nodeId,
        UUID nearNodeId,
        long rmtThreadId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation,
        boolean invalidate,
        long timeout,
        Collection<IgniteTxEntry<K, V>> writeEntries,
        int txSize,
        @Nullable IgniteTxKey grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) throws IgniteCheckedException {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, sys, concurrency, isolation, invalidate, timeout, txSize,
            grpLockKey, subjId, taskNameHash);

        assert nearNodeId != null;

        this.nearNodeId = nearNodeId;

        readMap = Collections.emptyMap();

        writeMap = new LinkedHashMap<>(
            writeEntries != null ? Math.max(txSize, writeEntries.size()) : txSize, 1.0f);

        if (writeEntries != null)
            for (IgniteTxEntry<K, V> entry : writeEntries) {
                entry.unmarshal(ctx, true, ldr);

                addEntry(entry);
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
     * @param grpLockKey Collection of group lock keys if this is a group-lock transaction.
     */
    public GridNearTxRemote(
        GridCacheSharedContext<K, V> ctx,
        UUID nodeId,
        UUID nearNodeId,
        GridCacheVersion nearXidVer,
        long rmtThreadId,
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
    @Override public GridCacheVersion ownedVersion(IgniteTxKey<K> key) {
        return owned == null ? null : owned.get(key);
    }

    /**
     * Marks near local transaction as group lock. Note that near remote transaction may be
     * marked as group lock even if it does not contain any locked key.
     */
    public void markGroupLock() {
        grpLock = true;
    }

    /** {@inheritDoc} */
    @Override public boolean groupLock() {
        return grpLock || super.groupLock();
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
    public void ownedVersions(Map<IgniteTxKey<K>, GridCacheVersion> vers) {
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
    public Collection<IgniteTxKey<K>> evicted() {
        return evicted;
    }

    /**
     * Adds evicted key bytes to evicted collection.
     *
     * @param key Evicted key.
     */
    public void addEvicted(IgniteTxKey<K> key) {
        evicted.add(key);
    }

    /**
     * Adds entries to started near remote tx.
     *
     * @param ldr Class loader.
     * @param entries Entries to add.
     * @throws IgniteCheckedException If failed.
     */
    public void addEntries(ClassLoader ldr, Iterable<IgniteTxEntry<K, V>> entries) throws IgniteCheckedException {
        for (IgniteTxEntry<K, V> entry : entries) {
            entry.unmarshal(cctx, true, ldr);

            addEntry(entry);
        }
    }

    /**
     * @param entry Entry to enlist.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if entry was enlisted.
     */
    private boolean addEntry(IgniteTxEntry<K, V> entry) throws IgniteCheckedException {
        checkInternal(entry.txKey());

        GridCacheContext<K, V> cacheCtx = entry.context();

        if (!cacheCtx.isNear())
            cacheCtx = cacheCtx.dht().near().context();

        GridNearCacheEntry<K, V> cached = cacheCtx.near().peekExx(entry.key());

        if (cached == null) {
            evicted.add(entry.txKey());

            return false;
        }
        else {
            cached.unswap();

            try {
                if (cached.peek(GLOBAL, CU.<K, V>empty()) == null && cached.evictInternal(false, xidVer, null)) {
                    evicted.add(entry.txKey());

                    return false;
                }
                else {
                    // Initialize cache entry.
                    entry.cached(cached, entry.keyBytes());

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
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param drVer Data center replication version.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if entry has been enlisted.
     */
    public boolean addEntry(
        GridCacheContext<K, V> cacheCtx,
        IgniteTxKey<K> key,
        byte[] keyBytes,
        GridCacheOperation op,
        V val,
        byte[] valBytes,
        @Nullable GridCacheVersion drVer
    ) throws IgniteCheckedException {
        checkInternal(key);

        GridNearCacheEntry<K, V> cached = cacheCtx.near().peekExx(key.key());

        try {
            if (cached == null) {
                evicted.add(key);

                return false;
            }
            else {
                cached.unswap();

                if (cached.peek(GLOBAL, CU.<K, V>empty()) == null && cached.evictInternal(false, xidVer, null)) {
                    cached.context().cache().removeIfObsolete(key.key());

                    evicted.add(key);

                    return false;
                }
                else {
                    IgniteTxEntry<K, V> txEntry = new IgniteTxEntry<>(cacheCtx,
                        this,
                        op,
                        val,
                        -1L,
                        -1L,
                        cached,
                        drVer);

                    txEntry.keyBytes(keyBytes);
                    txEntry.valueBytes(valBytes);

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
