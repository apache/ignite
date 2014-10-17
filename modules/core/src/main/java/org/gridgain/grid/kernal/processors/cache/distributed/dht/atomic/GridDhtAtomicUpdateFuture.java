/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache.FORCE_TRANSFORM_BACKUP_SINCE;

/**
 * DHT atomic cache backup update future.
 */
public class GridDhtAtomicUpdateFuture<K, V> extends GridFutureAdapter<Void>
    implements GridCacheAtomicFuture<K, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static GridLogger log;

    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Write version. */
    private GridCacheVersion writeVer;

    /** Force transform backup flag. */
    private boolean forceTransformBackups;

    /** Completion callback. */
    @GridToStringExclude
    private CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb;

    /** Mappings. */
    @GridToStringInclude
    private ConcurrentMap<UUID, GridDhtAtomicUpdateRequest<K, V>> mappings = new ConcurrentHashMap8<>();

    /** Entries with readers. */
    private Map<K, GridDhtCacheEntry<K, V>> nearReadersEntries;

    /** Update request. */
    private GridNearAtomicUpdateRequest<K, V> updateReq;

    /** Update response. */
    private GridNearAtomicUpdateResponse<K, V> updateRes;

    /** Future keys. */
    private Collection<K> keys;

    /** Future map time. */
    private volatile long mapTime;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicUpdateFuture() {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param completionCb Callback to invoke when future is completed.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    public GridDhtAtomicUpdateFuture(
        GridCacheContext<K, V> cctx,
        CI2<GridNearAtomicUpdateRequest<K, V>, GridNearAtomicUpdateResponse<K, V>> completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicUpdateRequest<K, V> updateReq,
        GridNearAtomicUpdateResponse<K, V> updateRes
    ) {
        super(cctx.kernalContext());

        this.cctx = cctx;
        this.writeVer = writeVer;

        futVer = cctx.versions().next(updateReq.topologyVersion());
        this.updateReq = updateReq;
        this.completionCb = completionCb;
        this.updateRes = updateRes;

        forceTransformBackups = updateReq.forceTransformBackups();

        log = U.logger(ctx, logRef, GridDhtAtomicUpdateFuture.class);

        keys = new ArrayList<>(updateReq.keys().size());
    }

    /** {@inheritDoc} */
    @Override public GridUuid futureId() {
        return futVer.asGridUuid();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends GridNode> nodes() {
        return F.view(F.viewReadOnly(mappings.keySet(), U.id2Node(cctx.kernalContext())), F.notNull());
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Processing node leave event [fut=" + this + ", nodeId=" + nodeId + ']');

        GridDhtAtomicUpdateRequest<K, V> req = mappings.get(nodeId);

        if (req != null) {
            updateRes.addFailedKeys(req.keys(), new GridTopologyException("Failed to write keys on backup (node left" +
                " grid before response is received): " + nodeId));

            // Remove only after added keys to failed set.
            mappings.remove(nodeId);

            checkComplete();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void checkTimeout(long timeout) {
        long mapTime0 = mapTime;

        if (mapTime0 > 0 && U.currentTimeMillis() > mapTime0 + timeout) {
            GridException ex = new GridCacheAtomicUpdateTimeoutException("Cache update timeout out " +
                "(consider increasing networkTimeout configuration property).");

            updateRes.addFailedKeys(keys, ex);

            onDone(ex);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean waitForPartitionExchange() {
        // Wait dht update futures in PRIMARY mode.
        return cctx.config().getAtomicWriteOrderMode() == GridCacheAtomicWriteOrderMode.PRIMARY;
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        return updateReq.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends K> keys() {
        return keys;
    }

    /**
     * @param entry Entry to map.
     * @param val Value to write.
     * @param valBytes Value bytes.
     * @param drTtl DR TTL (optional).
     * @param drExpireTime DR expire time (optional).
     * @param drVer DR version (optional).
     * @param ttl Time to live.
     */
    public void addWriteEntry(GridDhtCacheEntry<K, V> entry, @Nullable V val, @Nullable byte[] valBytes,
        GridClosure<V, V> transformC, long drTtl, long drExpireTime, @Nullable GridCacheVersion drVer, long ttl) {
        long topVer = updateReq.topologyVersion();

        Collection<GridNode> dhtNodes = cctx.dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        GridCacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        keys.add(entry.key());

        for (GridNode node : dhtNodes) {
            UUID nodeId = node.id();

            if (!nodeId.equals(ctx.localNodeId())) {
                boolean supportsForceTransformBackup = node.version().compareTo(FORCE_TRANSFORM_BACKUP_SINCE) >= 0;

                GridDhtAtomicUpdateRequest<K, V> updateReq = mappings.get(nodeId);

                if (updateReq == null) {
                    updateReq = new GridDhtAtomicUpdateRequest<>(nodeId, futVer, writeVer, syncMode, topVer, ttl,
                        forceTransformBackups && supportsForceTransformBackup, this.updateReq.subjectId(),
                        this.updateReq.taskNameHash());

                    mappings.put(nodeId, updateReq);
                }

                updateReq.addWriteValue(entry.key(), entry.keyBytes(), val, valBytes, transformC, drTtl,
                    drExpireTime, drVer);
            }
        }
    }

    /**
     * @param readers Entry readers.
     * @param entry Entry.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ttl Time to live.
     */
    public void addNearWriteEntries(Iterable<UUID> readers, GridDhtCacheEntry<K, V> entry, @Nullable V val,
        @Nullable byte[] valBytes, GridClosure<V, V> transformC, long ttl) {
        GridCacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        keys.add(entry.key());

        long topVer = updateReq.topologyVersion();

        for (UUID nodeId : readers) {
            GridDhtAtomicUpdateRequest<K, V> updateReq = mappings.get(nodeId);

            if (updateReq == null) {
                GridNode node = ctx.discovery().node(nodeId);

                // Node left the grid.
                if (node == null)
                    continue;

                boolean supportsForceTransformBackup = node.version().compareTo(FORCE_TRANSFORM_BACKUP_SINCE) >= 0;

                updateReq = new GridDhtAtomicUpdateRequest<>(nodeId, futVer, writeVer, syncMode, topVer, ttl,
                    forceTransformBackups && supportsForceTransformBackup, this.updateReq.subjectId(),
                    this.updateReq.taskNameHash());

                mappings.put(nodeId, updateReq);
            }

            if (nearReadersEntries == null)
                nearReadersEntries = new HashMap<>();

            nearReadersEntries.put(entry.key(), entry);

            updateReq.addNearWriteValue(entry.key(), entry.keyBytes(), val, valBytes, transformC);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeAtomicFuture(version());

            if (updateReq.writeSynchronizationMode() == FULL_SYNC)
                completionCb.apply(updateReq, updateRes);

            return true;
        }

        return false;
    }

    /**
     * Sends requests to remote nodes.
     */
    public void map() {
        mapTime = U.currentTimeMillis();

        if (!mappings.isEmpty()) {
            for (GridDhtAtomicUpdateRequest<K, V> req : mappings.values()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending DHT atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

                    cctx.io().send(req.nodeId(), req);
                }
                catch (GridTopologyException ignored) {
                    U.warn(log, "Failed to send update request to backup node because it left grid: " +
                        req.nodeId());

                    mappings.remove(req.nodeId());
                }
                catch (GridException e) {
                    U.error(log, "Failed to send update request to backup node (did node leave the grid?): "
                        + req.nodeId(), e);

                    mappings.remove(req.nodeId());
                }
            }
        }

        checkComplete();

        // Send response right away if no ACKs from backup is required.
        // Backups will send ACKs anyway, future will be completed after all backups have replied.
        if (updateReq.writeSynchronizationMode() != FULL_SYNC)
            completionCb.apply(updateReq, updateRes);
    }

    /**
     * Callback for backup update response.
     *
     * @param nodeId Backup node ID.
     * @param updateRes Update response.
     */
    public void onResult(UUID nodeId, GridDhtAtomicUpdateResponse<K, V> updateRes) {
        if (log.isDebugEnabled())
            log.debug("Received DHT atomic update future result [nodeId=" + nodeId + ", updateRes=" + updateRes + ']');

        if (updateRes.error() != null)
            this.updateRes.addFailedKeys(updateRes.failedKeys(), updateRes.error());

        if (!F.isEmpty(updateRes.nearEvicted())) {
            for (K key : updateRes.nearEvicted()) {
                GridDhtCacheEntry<K, V> entry = nearReadersEntries.get(key);

                try {
                    entry.removeReader(nodeId, updateRes.messageId());
                }
                catch (GridCacheEntryRemovedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Entry with evicted reader was removed [entry=" + entry + ", err=" + e + ']');
                }
            }
        }

        mappings.remove(nodeId);

        checkComplete();
    }

    /**
     * Deferred update response.
     *
     * @param nodeId Backup node ID.
     */
    public void onResult(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Received deferred DHT atomic update future result [nodeId=" + nodeId + ']');

        mappings.remove(nodeId);

        checkComplete();
    }

    /**
     * Checks if all required responses are received.
     */
    private void checkComplete() {
        // Always wait for replies from all backups.
        if (mappings.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Completing DHT atomic update future: " + this);

            onDone();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateFuture.class, this);
    }
}
