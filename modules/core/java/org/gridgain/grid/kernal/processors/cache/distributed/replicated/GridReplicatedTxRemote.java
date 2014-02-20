// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;

/**
 * Transaction created by system implicitly on remote nodes.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedTxRemote<K, V> extends GridDistributedTxRemoteAdapter<K, V> {
    /** Transaction nodes map. */
    private Map<UUID, Collection<UUID>> txNodes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridReplicatedTxRemote() {
        // No-op.
    }

    /**
     * This constructor is meant for optimistic transactions.
     *
     * @param ldr Class loader.
     * @param nodeId Node ID.
     * @param rmtThreadId Remote thread ID.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param readEntries Read entries.
     * @param writeEntries Write entries.
     * @param ctx Cache registry.
     * @param txSize Expected transaction size.
     * @param txNodes Transaction nodes map with originating node ID as single key in map.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     * @throws GridException If unmarshalling failed.
     */
    public GridReplicatedTxRemote(
        ClassLoader ldr,
        UUID nodeId,
        long rmtThreadId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        boolean invalidate,
        long timeout,
        Collection<GridCacheTxEntry<K, V>> readEntries,
        Collection<GridCacheTxEntry<K, V>> writeEntries,
        GridCacheContext<K, V> ctx,
        int txSize,
        Map<UUID, Collection<UUID>> txNodes,
        @Nullable Object grpLockKey
    ) throws GridException {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, concurrency, isolation, invalidate, timeout, txSize,
            grpLockKey);

        this.txNodes = txNodes;

        readMap = new LinkedHashMap<>(
            readEntries != null ? readEntries.size() : 0, 1.0f);

        if (readEntries != null) {
            for (GridCacheTxEntry<K, V> entry : readEntries) {
                entry.unmarshal(ctx, ldr);

                // Initialize cache entry.
                entry.cached(ctx.cache().entryEx(entry.key()), entry.keyBytes());

                checkInternal(entry.key());

                readMap.put(entry.key(), entry);
            }
        }

        writeMap = new LinkedHashMap<>(
            writeEntries != null ? Math.max(writeEntries.size(), txSize) : txSize, 1.0f);

        if (writeEntries != null) {
            for (GridCacheTxEntry<K, V> entry : writeEntries) {
                entry.unmarshal(ctx, ldr);

                // Initialize cache entry.
                entry.cached(ctx.cache().entryEx(entry.key()), entry.keyBytes());

                checkInternal(entry.key());

                writeMap.put(entry.key(), entry);

                addExplicit(entry);
            }
        }
    }

    /**
     * This constructor is meant for pessimistic transactions.
     *
     * @param nodeId Node ID.
     * @param rmtThreadId Remote thread ID.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param ctx Cache registry.
     * @param txSize Expected transaction size.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     */
    public GridReplicatedTxRemote(
        UUID nodeId,
        long rmtThreadId,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        boolean invalidate,
        long timeout,
        GridCacheContext<K, V> ctx,
        int txSize,
        @Nullable Object grpLockKey
    ) {
        super(ctx, nodeId, rmtThreadId, xidVer, commitVer, concurrency, isolation, invalidate, timeout, txSize,
            grpLockKey);

        readMap = new LinkedHashMap<>(1, 1.0f);
        writeMap = new LinkedHashMap<>(txSize, 1.0f);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheVersion nearXidVersion() {
        return xidVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /** {@inheritDoc} */
    @Override public boolean replicated() {
        return true;
    }

    /**
     * Prepare phase.
     *
     * @throws GridException If prepare failed.
     */
    @Override public void prepare() throws GridException {
        // If another thread is doing prepare or rollback.
        if (!state(PREPARING)) {
            if (log.isDebugEnabled())
                log.debug("Invalid transaction state for prepare: " + this);

            return;
        }

        try {
            cctx.tm().prepareTx(this);

            state(PREPARED);
        }
        catch (GridException e) {
            setRollbackOnly();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridReplicatedTxRemote.class, this, "super", super.toString());
    }
}
