/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridNearTxRemote<K, V> extends GridDistributedTxRemoteAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Evicted keys. */
    private Collection<K> evicted = new LinkedList<>();

    /** Near node ID. */
    private UUID nearNodeId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Evicted keys. */
    private Collection<byte[]> evictedBytes = new LinkedList<>();

    /** Owned versions. */
    private Map<K, GridCacheVersion> owned;

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
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param writeEntries Write entries.
     * @param ctx Cache registry.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @throws GridException If unmarshalling failed.
     */
    public GridNearTxRemote(
        ClassLoader ldr,
        UUID nodeId,
        UUID nearNodeId,
        long rmtThreadId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        boolean invalidate,
        long timeout,
        Collection<GridCacheTxEntry<K, V>> writeEntries,
        GridCacheContext<K, V> ctx,
        int txSize,
        @Nullable Object grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) throws GridException {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, concurrency, isolation, invalidate, timeout, txSize,
            grpLockKey, subjId, taskNameHash);

        assert nearNodeId != null;

        this.nearNodeId = nearNodeId;

        readMap = Collections.emptyMap();

        writeMap = new LinkedHashMap<>(
            writeEntries != null ? Math.max(txSize, writeEntries.size()) : txSize, 1.0f);

        if (writeEntries != null)
            for (GridCacheTxEntry<K, V> entry : writeEntries) {
                entry.unmarshal(ctx, ldr);

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
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param drVer DR version.
     * @param ctx Cache registry.
     * @param txSize Ecpected transaction size.
     * @param grpLockKey Collection of group lock keys if this is a group-lock transaction.
     * @throws GridException If failed.
     */
    public GridNearTxRemote(
        UUID nodeId,
        UUID nearNodeId,
        GridCacheVersion nearXidVer,
        long rmtThreadId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        boolean invalidate,
        long timeout,
        K key,
        byte[] keyBytes,
        V val,
        byte[] valBytes,
        @Nullable GridCacheVersion drVer,
        GridCacheContext<K, V> ctx,
        int txSize,
        @Nullable Object grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) throws GridException {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, concurrency, isolation, invalidate, timeout, txSize,
            grpLockKey, subjId, taskNameHash);

        assert nearNodeId != null;

        this.nearXidVer = nearXidVer;
        this.nearNodeId = nearNodeId;

        readMap = new LinkedHashMap<>(1, 1.0f);
        writeMap = new LinkedHashMap<>(txSize, 1.0f);

        addEntry(key, keyBytes, val, valBytes, drVer);
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
    @Override public GridCacheVersion ownedVersion(K key) {
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
    public void ownedVersions(Map<K, GridCacheVersion> vers) {
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
    public Collection<K> evicted() {
        return evicted;
    }

    /**
     * @return Evicted bytes.
     */
    public Collection<byte[]> evictedBytes() {
        return evictedBytes;
    }

    /**
     * Adds evicted key bytes to evicted collection.
     *
     * @param key Evicted key.
     * @param bytes Bytes of evicted key.
     */
    public void addEvicted(K key, byte[] bytes) {
        evicted.add(key);

        if (bytes != null)
            evictedBytes.add(bytes);
    }

    /**
     * @return {@code True} if transaction contains a valid list of evicted bytes.
     */
    public boolean hasEvictedBytes() {
        // Sizes of byte list and key list can differ if node crashed and transaction moved
        // from local to remote. This check is for safety, as nothing bad will happen if
        // some near keys will remain in remote node readers list.
        return !evictedBytes.isEmpty() && evictedBytes.size() == evicted.size();
    }

    /**
     * Adds entries to started near remote tx.
     *
     * @param ldr Class loader.
     * @param entries Entries to add.
     * @throws GridException If failed.
     */
    public void addEntries(ClassLoader ldr, Iterable<GridCacheTxEntry<K, V>> entries) throws GridException {
        for (GridCacheTxEntry<K, V> entry : entries) {
            entry.unmarshal(cctx, ldr);

            addEntry(entry);
        }
    }

    /**
     * @param entry Entry to enlist.
     * @throws GridException If failed.
     * @return {@code True} if entry was enlisted.
     */
    private boolean addEntry(GridCacheTxEntry<K, V> entry) throws GridException {
        checkInternal(entry.key());

        GridNearCacheEntry<K, V> cached = cctx.near().peekExx(entry.key());

        if (cached == null) {
            evicted.add(entry.key());

            if (entry.keyBytes() != null)
                evictedBytes.add(entry.keyBytes());

            return false;
        }
        else {
            cached.unswap();

            try {
                if (cached.peek(GLOBAL, CU.<K, V>empty()) == null && cached.evictInternal(false, xidVer, null)) {
                    evicted.add(entry.key());

                    if (entry.keyBytes() != null)
                        evictedBytes.add(entry.keyBytes());

                    return false;
                }
                else {
                    // Initialize cache entry.
                    entry.cached(cached, entry.keyBytes());

                    writeMap.put(entry.key(), entry);

                    addExplicit(entry);

                    return true;
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                evicted.add(entry.key());

                if (entry.keyBytes() != null)
                    evictedBytes.add(entry.keyBytes());

                if (log.isDebugEnabled())
                    log.debug("Got removed entry when adding to remote transaction (will ignore): " + cached);

                return false;
            }
        }
    }

    /**
     * @param key Key to add to write set.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param drVer Data center replication version.
     */
    void addWrite(K key, byte[] keyBytes, @Nullable V val, @Nullable byte[] valBytes,
        @Nullable GridCacheVersion drVer) {
        checkInternal(key);

        GridNearCacheEntry<K, V> cached = cctx.near().entryExx(key, topologyVersion());

        GridCacheTxEntry<K, V> txEntry = new GridCacheTxEntry<>(cctx, this, NOOP, val, 0L, -1L, cached, drVer);

        txEntry.keyBytes(keyBytes);
        txEntry.valueBytes(valBytes);

        writeMap.put(key, txEntry);
    }

    /**
     * @param key Key to add to read set.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param drVer Data center replication version.
     * @throws GridException If failed.
     * @return {@code True} if entry has been enlisted.
     */
    private boolean addEntry(K key, byte[] keyBytes, V val, byte[] valBytes, @Nullable GridCacheVersion drVer)
        throws GridException {
        checkInternal(key);

        GridNearCacheEntry<K, V> cached = cctx.near().peekExx(key);

        try {
            if (cached == null) {
                evicted.add(key);

                if (keyBytes != null)
                    evictedBytes.add(keyBytes);

                return false;
            }
            else {
                cached.unswap();

                if (cached.peek(GLOBAL, CU.<K, V>empty()) == null && cached.evictInternal(false, xidVer, null)) {
                    cached.context().cache().removeIfObsolete(key);

                    evicted.add(key);

                    if (keyBytes != null)
                        evictedBytes.add(keyBytes);

                    return false;
                }
                else {
                    GridCacheTxEntry<K, V> txEntry = new GridCacheTxEntry<>(cctx, this, NOOP, val, 0L, -1L, cached,
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

            if (keyBytes != null)
                evictedBytes.add(keyBytes);

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
