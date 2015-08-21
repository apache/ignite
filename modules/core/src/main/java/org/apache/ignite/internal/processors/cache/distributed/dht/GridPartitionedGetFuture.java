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
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Colocated get future.
 */
public class GridPartitionedGetFuture<K, V> extends GridCompoundIdentityFuture<Map<K, V>>
    implements GridCacheFuture<Map<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default max remap count value. */
    public static final int DFLT_MAX_REMAP_CNT = 3;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Maximum number of attempts to remap key to the same primary node. */
    private static final int MAX_REMAP_CNT = IgniteSystemProperties.getInteger(IGNITE_NEAR_GET_MAX_REMAPS,
        DFLT_MAX_REMAP_CNT);

    /** Context. */
    private final GridCacheContext<K, V> cctx;

    /** Keys. */
    private Collection<KeyCacheObject> keys;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Reload flag. */
    private boolean reload;

    /** Read-through flag. */
    private boolean readThrough;

    /** Force primary flag. */
    private boolean forcePrimary;

    /** Future ID. */
    private IgniteUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Trackable flag. */
    private volatile boolean trackable;

    /** Remap count. */
    private AtomicInteger remapCnt = new AtomicInteger();

    /** Subject ID. */
    private UUID subjId;

    /** Task name. */
    private String taskName;

    /** Whether to deserialize portable objects. */
    private boolean deserializePortable;

    /** Expiry policy. */
    private IgniteCacheExpiryPolicy expiryPlc;

    /** Skip values flag. */
    private boolean skipVals;

    /** Flag indicating whether future can be remapped on a newer topology version. */
    private final boolean canRemap;

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param topVer Topology version.
     * @param readThrough Read through flag.
     * @param reload Reload flag.
     * @param forcePrimary If {@code true} then will force network trip to primary node even
     *          if called on backup node.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     */
    public GridPartitionedGetFuture(
        GridCacheContext<K, V> cctx,
        Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer,
        boolean readThrough,
        boolean reload,
        boolean forcePrimary,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean canRemap
    ) {
        super(cctx.kernalContext(), CU.<K, V>mapsReducer(keys.size()));

        assert !F.isEmpty(keys);

        this.cctx = cctx;
        this.keys = keys;
        this.topVer = topVer;
        this.readThrough = readThrough;
        this.reload = reload;
        this.forcePrimary = forcePrimary;
        this.subjId = subjId;
        this.deserializePortable = deserializePortable;
        this.taskName = taskName;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;
        this.canRemap = canRemap;

        futId = IgniteUuid.randomUuid();

        ver = cctx.versions().next();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridPartitionedGetFuture.class);
    }

    /**
     * Initializes future.
     */
    public void init() {
        AffinityTopologyVersion topVer = this.topVer.topologyVersion() > 0 ? this.topVer :
            canRemap ? cctx.affinity().affinityTopologyVersion() : cctx.shared().exchange().readyAffinityVersion();

        map(keys, Collections.<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>>emptyMap(), topVer);

        markInitialized();
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // Should not flip trackable flag from true to false since get future can be remapped.
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<? extends ClusterNode> nodes() {
        return
            F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<Map<K, V>>, ClusterNode>() {
                @Nullable @Override public ClusterNode apply(IgniteInternalFuture<Map<K, V>> f) {
                    if (isMini(f))
                        return ((MiniFuture)f).node();

                    return cctx.discovery().localNode();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    found = true;

                    f.onNodeLeft(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));
                }
            }

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearGetResponse res) {
        for (IgniteInternalFuture<Map<K, V>> fut : futures()) {
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.futureId().equals(res.miniId())) {
                    assert f.node().id().equals(nodeId);

                    f.onResult(res);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Map<K, V> res, Throwable err) {
        if (super.onDone(res, err)) {
            // Don't forget to clean up.
            if (trackable)
                cctx.mvcc().removeFuture(this);

            cache().sendTtlUpdateRequest(expiryPlc);

            return true;
        }

        return false;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<Map<K, V>> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * @param keys Keys.
     * @param mapped Mappings to check for duplicates.
     * @param topVer Topology version on which keys should be mapped.
     */
    private void map(
        Collection<KeyCacheObject> keys,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        AffinityTopologyVersion topVer
    ) {
        if (CU.affinityNodes(cctx, topVer).isEmpty()) {
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                "(all partition nodes left the grid)."));

            return;
        }

        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings =
            U.newHashMap(CU.affinityNodes(cctx, topVer).size());

        final int keysSize = keys.size();

        Map<K, V> locVals = U.newHashMap(keysSize);

        boolean hasRmtNodes = false;

        // Assign keys to primary nodes.
        for (KeyCacheObject key : keys)
            hasRmtNodes |= map(key, mappings, locVals, topVer, mapped);

        if (isDone())
            return;

        if (!locVals.isEmpty())
            add(new GridFinishedFuture<>(locVals));

        if (hasRmtNodes) {
            trackable = true;

            cctx.mvcc().addFuture(this);
        }

        // Create mini futures.
        for (Map.Entry<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> entry : mappings.entrySet()) {
            final ClusterNode n = entry.getKey();

            final LinkedHashMap<KeyCacheObject, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                final GridDhtFuture<Collection<GridCacheEntryInfo>> fut =
                    cache().getDhtAsync(n.id(),
                        -1,
                        mappedKeys,
                        readThrough,
                        reload,
                        topVer,
                        subjId,
                        taskName == null ? 0 : taskName.hashCode(),
                        expiryPlc,
                        skipVals);

                final Collection<Integer> invalidParts = fut.invalidPartitions();

                if (!F.isEmpty(invalidParts)) {
                    Collection<KeyCacheObject> remapKeys = new ArrayList<>(keysSize);

                    for (KeyCacheObject key : keys) {
                        if (key != null && invalidParts.contains(cctx.affinity().partition(key)))
                            remapKeys.add(key);
                    }

                    AffinityTopologyVersion updTopVer = cctx.discovery().topologyVersionEx();

                    assert updTopVer.compareTo(topVer) > 0 : "Got invalid partitions for local node but topology version did " +
                        "not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                        ", invalidParts=" + invalidParts + ']';

                    // Remap recursively.
                    map(remapKeys, mappings, updTopVer);
                }

                // Add new future.
                add(fut.chain(new C1<IgniteInternalFuture<Collection<GridCacheEntryInfo>>, Map<K, V>>() {
                    @Override public Map<K, V> apply(IgniteInternalFuture<Collection<GridCacheEntryInfo>> fut) {
                        try {
                            return createResultMap(fut.get());
                        }
                        catch (Exception e) {
                            U.error(log, "Failed to get values from dht cache [fut=" + fut + "]", e);

                            onDone(e);

                            return Collections.emptyMap();
                        }
                    }
                }));
            }
            else {
                MiniFuture fut = new MiniFuture(n, mappedKeys, topVer);

                GridCacheMessage req = new GridNearGetRequest(
                    cctx.cacheId(),
                    futId,
                    fut.futureId(),
                    ver,
                    mappedKeys,
                    readThrough,
                    reload,
                    topVer,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc != null ? expiryPlc.forAccess() : -1L,
                    skipVals);

                add(fut); // Append new future.

                try {
                    cctx.io().send(n, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        fut.onNodeLeft((ClusterTopologyCheckedException)e);
                    else
                        fut.onResult(e);
                }
            }
        }
    }

    /**
     * @param mappings Mappings.
     * @param key Key to map.
     * @param locVals Local values.
     * @param topVer Topology version.
     * @param mapped Previously mapped.
     * @return {@code True} if has remote nodes.
     */
    @SuppressWarnings("ConstantConditions")
    private boolean map(
        KeyCacheObject key,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings,
        Map<K, V> locVals,
        AffinityTopologyVersion topVer,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped
    ) {
        GridDhtCacheAdapter<K, V> colocated = cache();

        boolean remote = false;

        // Allow to get cached value from the local node.
        boolean allowLocRead = !forcePrimary || cctx.affinity().primary(cctx.localNode(), key, topVer);

        while (true) {
            GridCacheEntryEx entry = null;

            try {
                if (!reload && allowLocRead) {
                    try {
                        entry = colocated.context().isSwapOrOffheapEnabled() ? colocated.entryEx(key) :
                            colocated.peekEx(key);

                        // If our DHT cache do has value, then we peek it.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked();

                            CacheObject v = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /**update-metrics*/false,
                                /*event*/!skipVals,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                expiryPlc);

                            colocated.context().evicts().touch(entry, topVer);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null) {
                                if (isNew && entry.markObsoleteIfEmpty(ver))
                                    colocated.removeIfObsolete(key);
                            }
                            else {
                                cctx.addResult(locVals, key, v, skipVals, false, deserializePortable, true);

                                return false;
                            }
                        }
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        // No-op.
                    }
                }

                ClusterNode node = affinityNode(key, topVer);

                if (node == null) {
                    onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                        "(all partition nodes left the grid)."));

                    return false;
                }

                remote = !node.isLocal();

                LinkedHashMap<KeyCacheObject, Boolean> keys = mapped.get(node);

                if (keys != null && keys.containsKey(key)) {
                    if (remapCnt.incrementAndGet() > MAX_REMAP_CNT) {
                        onDone(new ClusterTopologyCheckedException("Failed to remap key to a new node after " +
                            MAX_REMAP_CNT + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                            U.toShortString(node) + ", mappings=" + mapped + ']'));

                        return false;
                    }
                }

                LinkedHashMap<KeyCacheObject, Boolean> old = mappings.get(node);

                if (old == null)
                    mappings.put(node, old = new LinkedHashMap<>(3, 1f));

                old.put(key, false);

                break;
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op, will retry.
            }
            catch (GridCacheFilterFailedException e) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for entry: " + e);

                colocated.context().evicts().touch(entry, topVer);

                break;
            }
        }

        return remote;
    }

    /**
     * @return Near cache.
     */
    private GridDhtCacheAdapter<K, V> cache() {
        return cctx.dht();
    }

    /**
     * Finds affinity node to send get request to.
     *
     * @param key Key to get.
     * @param topVer Topology version.
     * @return Affinity node from which the key will be requested.
     */
    private ClusterNode affinityNode(KeyCacheObject key, AffinityTopologyVersion topVer) {
        if (!canRemap) {
            List<ClusterNode> nodes = cctx.affinity().nodes(key, topVer);

            for (ClusterNode node : nodes) {
                if (cctx.discovery().alive(node))
                    return node;
            }

            return null;
        }
        else
            return cctx.affinity().primary(key, topVer);
    }

    /**
     * @param infos Entry infos.
     * @return Result map.
     */
    private Map<K, V> createResultMap(Collection<GridCacheEntryInfo> infos) {
        int keysSize = infos.size();

        if (keysSize != 0) {
            Map<K, V> map = new GridLeanMap<>(keysSize);

            for (GridCacheEntryInfo info : infos)
                cctx.addResult(map, info.key(), info.value(), skipVals, false, deserializePortable, false);

            return map;
        }

        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionedGetFuture.class, this, super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Map<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Node ID. */
        private final ClusterNode node;

        /** Keys. */
        @GridToStringInclude
        private final LinkedHashMap<KeyCacheObject, Boolean> keys;

        /** Topology version on which this future was mapped. */
        private final AffinityTopologyVersion topVer;

        /** {@code True} if remapped after node left. */
        private boolean remapped;

        /**
         * @param node Node.
         * @param keys Keys.
         * @param topVer Topology version.
         */
        MiniFuture(ClusterNode node, LinkedHashMap<KeyCacheObject, Boolean> keys, AffinityTopologyVersion topVer) {
            this.node = node;
            this.keys = keys;
            this.topVer = topVer;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @return Keys.
         */
        public Collection<KeyCacheObject> keys() {
            return keys.keySet();
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Failure exception.
         */
        @SuppressWarnings("UnusedParameters")
        synchronized void onNodeLeft(ClusterTopologyCheckedException e) {
            if (remapped)
                return;

            remapped = true;

            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            // Try getting from existing nodes.
            if (!canRemap) {
                map(keys.keySet(), F.t(node, keys), topVer);

                onDone(Collections.<K, V>emptyMap());
            }
            else {
                final AffinityTopologyVersion updTopVer =
                    new AffinityTopologyVersion(Math.max(topVer.topologyVersion() + 1, cctx.discovery().topologyVersion()));

                final GridFutureRemapTimeoutObject timeout = new GridFutureRemapTimeoutObject(this,
                    cctx.kernalContext().config().getNetworkTimeout(),
                    updTopVer,
                    e);

                cctx.affinity().affinityReadyFuture(updTopVer).listen(
                    new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                            if (timeout.finish()) {
                                cctx.kernalContext().timeout().removeTimeoutObject(timeout);

                                try {
                                    fut.get();

                                    // Remap.
                                    map(keys.keySet(), F.t(node, keys), updTopVer);

                                    onDone(Collections.<K, V>emptyMap());
                                }
                                catch (IgniteCheckedException e) {
                                    GridPartitionedGetFuture.this.onDone(e);
                                }
                            }
                        }
                    }
                );

                cctx.kernalContext().timeout().addTimeoutObject(timeout);
            }
        }

        /**
         * @param res Result callback.
         */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        void onResult(final GridNearGetResponse res) {
            final Collection<Integer> invalidParts = res.invalidPartitions();

            // If error happened on remote node, fail the whole future.
            if (res.error() != null) {
                onDone(res.error());

                return;
            }

            // Remap invalid partitions.
            if (!F.isEmpty(invalidParts)) {
                AffinityTopologyVersion rmtTopVer = res.topologyVersion();

                assert !rmtTopVer.equals(AffinityTopologyVersion.ZERO);

                if (rmtTopVer.compareTo(topVer) <= 0) {
                    // Fail the whole get future.
                    onDone(new IgniteCheckedException("Failed to process invalid partitions response (remote node reported " +
                        "invalid partitions but remote topology version does not differ from local) " +
                        "[topVer=" + topVer + ", rmtTopVer=" + rmtTopVer + ", invalidParts=" + invalidParts +
                        ", nodeId=" + node.id() + ']'));

                    return;
                }

                if (log.isDebugEnabled())
                    log.debug("Remapping mini get future [invalidParts=" + invalidParts + ", fut=" + this + ']');

                // Need to wait for next topology version to remap.
                IgniteInternalFuture<Long> topFut = cctx.discovery().topologyFuture(rmtTopVer.topologyVersion());

                topFut.listen(new CIX1<IgniteInternalFuture<Long>>() {
                    @SuppressWarnings("unchecked")
                    @Override public void applyx(IgniteInternalFuture<Long> fut) throws IgniteCheckedException {
                        AffinityTopologyVersion topVer = new AffinityTopologyVersion(fut.get());

                        // This will append new futures to compound list.
                        map(F.view(keys.keySet(),  new P1<KeyCacheObject>() {
                            @Override public boolean apply(KeyCacheObject key) {
                                return invalidParts.contains(cctx.affinity().partition(key));
                            }
                        }), F.t(node, keys), topVer);

                        onDone(createResultMap(res.entries()));
                    }
                });
            }
            else {
                try {
                    onDone(createResultMap(res.entries()));
                }
                catch (Exception e) {
                    onDone(e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}
