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
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
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

    /** Maximum number of attempts to remap key to the same primary node. */
    private static final int MAX_REMAP_CNT = IgniteSystemProperties.getInteger(IGNITE_NEAR_GET_MAX_REMAPS,
        DFLT_MAX_REMAP_CNT);

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Keys. */
    private Collection<? extends K> keys;

    /** Topology version. */
    private long topVer;

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

    /** Filters. */
    private IgnitePredicate<Cache.Entry<K, V>>[] filters;

    /** Logger. */
    private IgniteLogger log;

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

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridPartitionedGetFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param topVer Topology version.
     * @param readThrough Read through flag.
     * @param reload Reload flag.
     * @param forcePrimary If {@code true} then will force network trip to primary node even
     *          if called on backup node.
     * @param filters Filters.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     */
    public GridPartitionedGetFuture(
        GridCacheContext<K, V> cctx,
        Collection<? extends K> keys,
        long topVer,
        boolean readThrough,
        boolean reload,
        boolean forcePrimary,
        @Nullable IgnitePredicate<Cache.Entry<K, V>>[] filters,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiryPlc
    ) {
        super(cctx.kernalContext(), CU.<K, V>mapsReducer(keys.size()));

        assert !F.isEmpty(keys);

        this.cctx = cctx;
        this.keys = keys;
        this.topVer = topVer;
        this.readThrough = readThrough;
        this.reload = reload;
        this.forcePrimary = forcePrimary;
        this.filters = filters;
        this.subjId = subjId;
        this.deserializePortable = deserializePortable;
        this.taskName = taskName;
        this.expiryPlc = expiryPlc;

        futId = IgniteUuid.randomUuid();

        ver = cctx.versions().next();

        log = U.logger(ctx, logRef, GridPartitionedGetFuture.class);
    }

    /**
     * Initializes future.
     */
    public void init() {
        long topVer = this.topVer > 0 ? this.topVer : cctx.affinity().affinityTopologyVersion();

        map(keys, Collections.<ClusterNode, LinkedHashMap<K, Boolean>>emptyMap(), topVer);

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

    /**
     * @return Keys.
     */
    Collection<? extends K> keys() {
        return keys;
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
        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));

                    return true;
                }
            }

        return false;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearGetResponse<K, V> res) {
        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.futureId().equals(res.miniId())) {
                    assert f.node().id().equals(nodeId);

                    f.onResult(res);
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
    private void map(Collection<? extends K> keys, Map<ClusterNode, LinkedHashMap<K, Boolean>> mapped, long topVer) {
        if (CU.affinityNodes(cctx, topVer).isEmpty()) {
            onDone(new ClusterTopologyCheckedException("Failed to map keys for cache (all partition nodes left the grid)."));

            return;
        }

        Map<ClusterNode, LinkedHashMap<K, Boolean>> mappings = U.newHashMap(CU.affinityNodes(cctx, topVer).size());

        final int keysSize = keys.size();

        Map<K, V> locVals = U.newHashMap(keysSize);

        boolean hasRmtNodes = false;

        // Assign keys to primary nodes.
        for (K key : keys) {
            if (key == null) {
                NullPointerException err = new NullPointerException("Null key");

                onDone(err);

                throw err;
            }

            hasRmtNodes |= map(key, mappings, locVals, topVer, mapped);
        }

        if (isDone())
            return;

        if (!locVals.isEmpty())
            add(new GridFinishedFuture<>(cctx.kernalContext(), locVals));

        if (hasRmtNodes) {
            trackable = true;

            cctx.mvcc().addFuture(this);
        }

        // Create mini futures.
        for (Map.Entry<ClusterNode, LinkedHashMap<K, Boolean>> entry : mappings.entrySet()) {
            final ClusterNode n = entry.getKey();

            final LinkedHashMap<K, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                final GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
                    cache().getDhtAsync(n.id(),
                        -1,
                        mappedKeys,
                        readThrough,
                        reload,
                        topVer,
                        subjId,
                        taskName == null ? 0 : taskName.hashCode(),
                        deserializePortable,
                        filters,
                        expiryPlc);

                final Collection<Integer> invalidParts = fut.invalidPartitions();

                if (!F.isEmpty(invalidParts)) {
                    Collection<K> remapKeys = new ArrayList<>(keysSize);

                    for (K key : keys) {
                        if (key != null && invalidParts.contains(cctx.affinity().partition(key)))
                            remapKeys.add(key);
                    }

                    long updTopVer = ctx.discovery().topologyVersion();

                    assert updTopVer > topVer : "Got invalid partitions for local node but topology version did " +
                        "not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                        ", invalidParts=" + invalidParts + ']';

                    // Remap recursively.
                    map(remapKeys, mappings, updTopVer);
                }

                // Add new future.
                add(fut.chain(new C1<IgniteInternalFuture<Collection<GridCacheEntryInfo<K, V>>>, Map<K, V>>() {
                    @Override public Map<K, V> apply(IgniteInternalFuture<Collection<GridCacheEntryInfo<K, V>>> fut) {
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

                GridCacheMessage<K, V> req = new GridNearGetRequest<>(
                    cctx.cacheId(),
                    futId,
                    fut.futureId(),
                    ver,
                    mappedKeys,
                    readThrough,
                    reload,
                    topVer,
                    filters,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc != null ? expiryPlc.forAccess() : -1L);

                add(fut); // Append new future.

                try {
                    cctx.io().send(n, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        fut.onResult((ClusterTopologyCheckedException)e);
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
    private boolean map(K key, Map<ClusterNode, LinkedHashMap<K, Boolean>> mappings, Map<K, V> locVals,
        long topVer, Map<ClusterNode, LinkedHashMap<K, Boolean>> mapped) {
        GridDhtCacheAdapter<K, V> colocated = cache();

        boolean remote = false;

        // Allow to get cached value from the local node.
        boolean allowLocRead = !forcePrimary || cctx.affinity().primary(cctx.localNode(), key, topVer);

        while (true) {
            GridCacheEntryEx<K, V> entry = null;

            try {
                if (!reload && allowLocRead) {
                    try {
                        entry = colocated.context().isSwapOrOffheapEnabled() ? colocated.entryEx(key) :
                            colocated.peekEx(key);

                        // If our DHT cache do has value, then we peek it.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked();

                            V v = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /**update-metrics*/false,
                                /*event*/true,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                filters,
                                expiryPlc);

                            colocated.context().evicts().touch(entry, topVer);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null) {
                                if (isNew && entry.markObsoleteIfEmpty(ver))
                                    colocated.removeIfObsolete(key);
                            }
                            else {
                                if (cctx.portableEnabled())
                                    v = (V)cctx.unwrapPortableIfNeeded(v, !deserializePortable);

                                locVals.put(key, v);

                                return false;
                            }
                        }
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        // No-op.
                    }
                }

                ClusterNode node = cctx.affinity().primary(key, topVer);

                remote = !node.isLocal();

                LinkedHashMap<K, Boolean> keys = mapped.get(node);

                if (keys != null && keys.containsKey(key)) {
                    if (remapCnt.incrementAndGet() > MAX_REMAP_CNT) {
                        onDone(new ClusterTopologyCheckedException("Failed to remap key to a new node after " + MAX_REMAP_CNT
                            + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                            U.toShortString(node) + ", mappings=" + mapped + ']'));

                        return false;
                    }
                }

                LinkedHashMap<K, Boolean> old = mappings.get(node);

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
     * @param infos Entry infos.
     * @return Result map.
     */
    private Map<K, V> createResultMap(Collection<GridCacheEntryInfo<K, V>> infos) {
        int keysSize = infos.size();

        try {
            if (keysSize != 0) {
                Map<K, V> map = new GridLeanMap<>(keysSize);

                for (GridCacheEntryInfo<K, V> info : infos) {
                    info.unmarshalValue(cctx, cctx.deploy().globalLoader());

                    K key = info.key();
                    V val = info.value();

                    if (cctx.portableEnabled()) {
                        key = (K)cctx.unwrapPortableIfNeeded(key, !deserializePortable);
                        val = (V)cctx.unwrapPortableIfNeeded(val, !deserializePortable);
                    }

                    map.put(key, val);
                }

                return map;
            }
        }
        catch (IgniteCheckedException e) {
            // Fail.
            onDone(e);
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
        private ClusterNode node;

        /** Keys. */
        @GridToStringInclude
        private LinkedHashMap<K, Boolean> keys;

        /** Topology version on which this future was mapped. */
        private long topVer;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param node Node.
         * @param keys Keys.
         * @param topVer Topology version.
         */
        MiniFuture(ClusterNode node, LinkedHashMap<K, Boolean> keys, long topVer) {
            super(cctx.kernalContext());

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
        public Collection<K> keys() {
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
        void onResult(ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            long updTopVer = ctx.discovery().topologyVersion();

            assert updTopVer > topVer : "Got topology exception but topology version did " +
                "not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                ", nodeId=" + node.id() + ']';

            // Remap.
            map(keys.keySet(), F.t(node, keys), updTopVer);

            onDone(Collections.<K, V>emptyMap());
        }

        /**
         * @param res Result callback.
         */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        void onResult(final GridNearGetResponse<K, V> res) {
            final Collection<Integer> invalidParts = res.invalidPartitions();

            // If error happened on remote node, fail the whole future.
            if (res.error() != null) {
                onDone(res.error());

                return;
            }

            // Remap invalid partitions.
            if (!F.isEmpty(invalidParts)) {
                long rmtTopVer = res.topologyVersion();

                assert rmtTopVer != 0;

                if (rmtTopVer <= topVer) {
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
                IgniteInternalFuture<Long> topFut = ctx.discovery().topologyFuture(rmtTopVer);

                topFut.listenAsync(new CIX1<IgniteInternalFuture<Long>>() {
                    @SuppressWarnings("unchecked")
                    @Override public void applyx(IgniteInternalFuture<Long> fut) throws IgniteCheckedException {
                        long topVer = fut.get();

                        // This will append new futures to compound list.
                        map(F.view(keys.keySet(),  new P1<K>() {
                            @Override public boolean apply(K key) {
                                return invalidParts.contains(cctx.affinity().partition(key));
                            }
                        }), F.t(node, keys), topVer);

                        onDone(createResultMap(res.entries()));
                    }
                });
            }
            else
                onDone(createResultMap(res.entries()));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}
