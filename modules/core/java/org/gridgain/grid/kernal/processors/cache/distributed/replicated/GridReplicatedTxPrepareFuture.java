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
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;

/**
 * Replicated cache transaction future.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridReplicatedTxPrepareFuture<K, V> extends GridFutureAdapter<GridCacheTxEx<K, V>>
    implements GridCacheFuture<GridCacheTxEx<K, V>> {
    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Future ID. */
    private GridUuid futId = GridUuid.randomUuid();

    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<K, V> cctx;

    /** Cache transaction. */
    @GridToStringExclude // Need to exclude due to circular dependencies.
    private GridCacheTxLocalEx<K, V> tx;

    /** Participating nodes. */
    private Collection<? extends GridNode> nodes;

    /** Participating node IDs. */
    private Collection<UUID> nodeIds;

    /** Error. */
    @GridToStringExclude
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Map of results. */
    @GridToStringExclude
    private ConcurrentMap<UUID, GridDistributedTxPrepareResponse<K, V>> results;

    /** Latch to count replies. */
    private AtomicInteger replyCnt;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Logger. */
    @GridToStringExclude
    private GridLogger log;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridReplicatedTxPrepareFuture() {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param tx Cache transaction.
     * @param nodes Nodes to expect replies from.
     */
    public GridReplicatedTxPrepareFuture(
        GridCacheContext<K, V> cctx,
        GridCacheTxLocalEx<K, V> tx,
        Collection<GridNode> nodes) {
        super(cctx.kernalContext());

        assert tx != null;
        assert nodes != null;

        this.cctx = cctx;
        this.tx = tx;
        this.nodes = nodes;

        nodeIds = new HashSet<>(nodes.size(), 1.0f);

        for (GridNode n : nodes)
            nodeIds.add(n.id());

        results = new ConcurrentHashMap8<>(nodes.size(), 1.0f);

        replyCnt = new AtomicInteger(nodes.size());

        log = U.logger(ctx, logRef, GridReplicatedTxPrepareFuture.class);
    }

    /** {@inheritDoc} */
    @Override public GridUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /**
     * @return Participating nodes.
     */
    @Override public Collection<? extends GridNode> nodes() {
        return nodes;
    }

    /**
     * Gets replicated tx mapping. Map will contain singleton map with remote nodes as values.
     *
     * @return Nodes mapping.
     */
    public Map<UUID, Collection<UUID>> nodesMapping() {
        return Collections.singletonMap(cctx.localNodeId(), nodeIds);
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @return Lock version.
     */
    public GridCacheTxLocalEx<K, V> tx() {
        return tx;
    }

    /**
     * @return Remaining replies.
     */
    public long remainingReplies() {
        return replyCnt.get();
    }


    /**
     * @param nodeId ID of removed node.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (GridNode n : nodes) {
            if (n.id().equals(nodeId)) {
                onResult(nodeId, new GridDistributedTxPrepareResponse<K, V>(tx.xidVersion(),
                    new GridTopologyException("Valid exception to signal node departure: " + n)));

                return true;
            }
        }

        return false;
    }

    /**
     * Completes this future.
     */
    public void complete() {
        replyCnt.set(0);

        onComplete();
    }

    /**
     * @param e Error.
     */
    public void onError(Throwable e) {
        if (err.compareAndSet(null, e)) {
            boolean marked = tx.setRollbackOnly();

            // Simulate all replies.
            replyCnt.set(0);

            if (e instanceof GridCacheTxRollbackException) {
                if (marked) {
                    try {
                        tx.rollback();
                    }
                    catch (GridException ex) {
                        U.error(log, "Failed to automatically rollback transaction: " + tx, ex);
                    }
                }
            }

            onComplete();
        }
    }

    /**
     * @param nodeId Sender node.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridDistributedTxPrepareResponse<K, V> res) {
        // Skip if canceled and ignore duplicate responses.
        if (!isCancelled() && results.putIfAbsent(nodeId, res) == null) {
            if (res.error() != null) {
                // Node departure is a valid result.
                if (res.error() instanceof GridTopologyException) {
                    /* No-op. */
                    if (log.isDebugEnabled()) {
                        log.debug("Ignoring departed node from future: " + this);
                    }

                    if (replyCnt.decrementAndGet() == 0) {
                        onAllReplies();
                    }
                }
                // In case of error, unlock only once.
                else {
                    if (log.isDebugEnabled()) {
                        log.debug("Received failed result response to commit request: " + res);
                    }

                    onError(res.error());
                }

                return;
            }

            if (err.get() == null) {
                for (GridCacheTxEntry<K, V> txEntry : tx.writeEntries()) {
                    while (true) {
                        try {
                            GridDistributedCacheEntry<K, V> entry = (GridDistributedCacheEntry<K,V>)txEntry.cached();

                            assert entry != null;

                            // Sync up remote candidates.
                            entry.addRemoteCandidates(
                                res.candidatesForKey(txEntry.key()),
                                res.version(),
                                res.committedVersions(),
                                res.rolledbackVersions());

                            break;
                        }
                        // Possible if entry cached within transaction is obsolete.
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled()) {
                                log.debug("Got removed entry in future onResult method (will retry): " + txEntry);
                            }

                            txEntry.cached(cctx.cache().entryEx(txEntry.key()), txEntry.keyBytes());
                        }
                    }
                }

                if (replyCnt.decrementAndGet() == 0) {
                    onAllReplies();
                }
            }
        }
    }

    /**
     * Callback for whenever all replies are received.
     */
    public void onAllReplies() {
        tx.state(PREPARED);

        // Ready all locks.
        if (!isDone()) {
            Collection<GridCacheTxEntry<K, V>> checkEntries = tx.groupLock() ?
                Collections.singletonList(tx.groupLockEntry()) :
                tx.writeEntries();

            for (GridCacheTxEntry<K, V> txEntry : checkEntries) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = (GridDistributedCacheEntry<K,V>)txEntry.cached();

                    try {
                        GridCacheMvccCandidate<K> c = entry.readyLock(tx.xidVersion());

                        if (log.isDebugEnabled())
                            log.debug("Current lock owner for entry [owner=" + c + ", entry=" + entry + ']');

                        break; // While.
                    }
                    // Possible if entry cached within transaction is obsolete.
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in future onAllReplies method (will retry): " + txEntry);

                        txEntry.cached(cctx.cache().entryEx(txEntry.key()), txEntry.keyBytes());
                    }
                }
            }
        }

        onComplete();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to cancel transaction: " + tx);
        }

        // Attempt rollback.
        if (onCancelled()) {
            // Clean up.
            cctx.mvcc().removeFuture(this);

            try {
                tx.rollback();
            }
            catch (GridException ex) {
                U.error(log, "Failed to rollback the transaction: " + tx, ex);
            }

            if (log.isDebugEnabled()) {
                log.debug("Transaction was cancelled and rolled back: " + tx);
            }

            return true;
        }

        return false;
    }

    /**
     * Completeness callback.
     */
    private void onComplete() {
        if (onDone(tx, err.get())) {
            // Clean up.
            cctx.mvcc().removeFuture(this);
        }
    }

    /**
     * Checks for errors.
     *
     * @throws GridException If check failed.
     */
    private void checkError() throws GridException {
        if (err.get() != null) {
            throw U.cast(err.get());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridReplicatedTxPrepareFuture.class, this,  "err",
            err == null ? "" : err.toString());
    }
}
