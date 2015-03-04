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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * DHT atomic cache backup update future.
 */
public class GridDhtAtomicUpdateFuture<K, V> extends GridFutureAdapter<Void>
    implements GridCacheAtomicFuture<K, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

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
    @Override public IgniteUuid futureId() {
        return futVer.asGridUuid();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends ClusterNode> nodes() {
        return F.view(F.viewReadOnly(mappings.keySet(), U.id2Node(cctx.kernalContext())), F.notNull());
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Processing node leave event [fut=" + this + ", nodeId=" + nodeId + ']');

        GridDhtAtomicUpdateRequest<K, V> req = mappings.get(nodeId);

        if (req != null) {
            updateRes.addFailedKeys(req.keys(), new ClusterTopologyCheckedException("Failed to write keys on backup (node left" +
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
            IgniteCheckedException ex = new CacheAtomicUpdateTimeoutCheckedException("Cache update timeout out " +
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
        return cctx.config().getAtomicWriteOrderMode() == CacheAtomicWriteOrderMode.PRIMARY;
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
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param drExpireTime DR expire time (optional).
     * @param drVer DR version (optional).
     */
    public void addWriteEntry(GridDhtCacheEntry<K, V> entry,
        @Nullable V val,
        @Nullable byte[] valBytes,
        EntryProcessor<K, V, ?> entryProcessor,
        long ttl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer) {
        long topVer = updateReq.topologyVersion();

        Collection<ClusterNode> dhtNodes = cctx.dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        keys.add(entry.key());

        for (ClusterNode node : dhtNodes) {
            UUID nodeId = node.id();

            if (!nodeId.equals(ctx.localNodeId())) {
                GridDhtAtomicUpdateRequest<K, V> updateReq = mappings.get(nodeId);

                if (updateReq == null) {
                    updateReq = new GridDhtAtomicUpdateRequest<>(
                        cctx.cacheId(),
                        nodeId,
                        futVer,
                        writeVer,
                        syncMode,
                        topVer,
                        forceTransformBackups,
                        this.updateReq.subjectId(),
                        this.updateReq.taskNameHash(),
                        forceTransformBackups ? this.updateReq.invokeArguments() : null);

                    mappings.put(nodeId, updateReq);
                }

                updateReq.addWriteValue(entry.key(),
                    entry.keyBytes(),
                    val,
                    valBytes,
                    entryProcessor,
                    ttl,
                    drExpireTime,
                    drVer);
            }
        }
    }

    /**
     * @param readers Entry readers.
     * @param entry Entry.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param entryProcessor Entry processor..
     * @param ttl TTL for near cache update (optional).
     * @param expireTime Expire time for near cache update (optional).
     */
    public void addNearWriteEntries(Iterable<UUID> readers,
        GridDhtCacheEntry<K, V> entry,
        @Nullable V val,
        @Nullable byte[] valBytes,
        EntryProcessor<K, V, ?> entryProcessor,
        long ttl,
        long expireTime) {
        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        keys.add(entry.key());

        long topVer = updateReq.topologyVersion();

        for (UUID nodeId : readers) {
            GridDhtAtomicUpdateRequest<K, V> updateReq = mappings.get(nodeId);

            if (updateReq == null) {
                ClusterNode node = ctx.discovery().node(nodeId);

                // Node left the grid.
                if (node == null)
                    continue;

                updateReq = new GridDhtAtomicUpdateRequest<>(
                    cctx.cacheId(),
                    nodeId,
                    futVer,
                    writeVer,
                    syncMode,
                    topVer,
                    forceTransformBackups,
                    this.updateReq.subjectId(),
                    this.updateReq.taskNameHash(),
                    forceTransformBackups ? this.updateReq.invokeArguments() : null);

                mappings.put(nodeId, updateReq);
            }

            if (nearReadersEntries == null)
                nearReadersEntries = new HashMap<>();

            nearReadersEntries.put(entry.key(), entry);

            updateReq.addNearWriteValue(entry.key(),
                entry.keyBytes(),
                val,
                valBytes,
                entryProcessor,
                ttl,
                expireTime);
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

                    cctx.io().send(req.nodeId(), req, cctx.ioPolicy());
                }
                catch (ClusterTopologyCheckedException ignored) {
                    U.warn(log, "Failed to send update request to backup node because it left grid: " +
                        req.nodeId());

                    mappings.remove(req.nodeId());
                }
                catch (IgniteCheckedException e) {
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
