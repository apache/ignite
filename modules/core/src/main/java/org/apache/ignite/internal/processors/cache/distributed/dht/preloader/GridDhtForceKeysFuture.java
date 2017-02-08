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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;

/**
 * Force keys request future.
 */
public final class GridDhtForceKeysFuture<K, V> extends GridCompoundFuture<Object, Collection<K>>
    implements GridDhtFuture<Collection<K>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Wait for 1 second for topology to change. */
    private static final long REMAP_PAUSE = 1000;

    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Topology. */
    private GridDhtPartitionTopology top;

    /** Keys to request. */
    private Collection<KeyCacheObject> keys;

    /** Keys for which local node is no longer primary. */
    private Collection<Integer> invalidParts = new GridLeanSet<>();

    /** Topology change counter. */
    private AtomicInteger topCntr = new AtomicInteger(1);

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Future ID. */
    private IgniteUuid futId = IgniteUuid.randomUuid();

    /** Preloader. */
    private GridDhtPreloader preloader;

    /** Trackable flag. */
    private boolean trackable;

    /**
     * @param cctx Cache context.
     * @param topVer Topology version.
     * @param keys Keys.
     * @param preloader Preloader.
     */
    public GridDhtForceKeysFuture(
        GridCacheContext<K, V> cctx,
        AffinityTopologyVersion topVer,
        Collection<KeyCacheObject> keys,
        GridDhtPreloader preloader
    ) {
        assert topVer.topologyVersion() != 0 : topVer;
        assert !F.isEmpty(keys) : keys;

        this.cctx = cctx;
        this.keys = keys;
        this.topVer = topVer;
        this.preloader = preloader;

        top = cctx.dht().topology();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtForceKeysFuture.class);
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Collection<K> res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            if (trackable)
                preloader.remoteFuture(this);

            return true;
        }

        return false;
    }

    /**
     * @param evt Discovery event.
     */
    @SuppressWarnings( {"unchecked"})
    public void onDiscoveryEvent(DiscoveryEvent evt) {
        topCntr.incrementAndGet();

        int type = evt.type();

        for (IgniteInternalFuture<?> f : futures()) {
            if (isMini(f)) {
                MiniFuture mini = (MiniFuture)f;

                mini.onDiscoveryEvent();

                if (type == EVT_NODE_LEFT || type == EVT_NODE_FAILED) {
                    if (mini.node().id().equals(evt.eventNode().id())) {
                        mini.onResult();

                        break;
                    }
                }
            }
        }
    }

    /**
     * @param nodeId Node left callback.
     * @param res Response.
     */
    @SuppressWarnings( {"unchecked"})
    public void onResult(UUID nodeId, GridDhtForceKeysResponse res) {
        for (IgniteInternalFuture<Object> f : futures())
            if (isMini(f)) {
                MiniFuture mini = (MiniFuture)f;

                if (mini.miniId().equals(res.miniId())) {
                    mini.onResult(res);

                    return;
                }
            }

        if (log.isDebugEnabled())
            log.debug("Failed to find mini future for response [cacheName=" + cctx.name() + ", res=" + res + ']');
    }

    /**
     * Initializes this future.
     */
    public void init() {
        assert cctx.preloader().startFuture().isDone();

        map(keys, Collections.<ClusterNode>emptyList());

        markInitialized();
    }

    /**
     * @param keys Keys.
     * @param exc Exclude nodes.
     * @return {@code True} if some mapping was added.
     */
    private boolean map(Iterable<KeyCacheObject> keys, Collection<ClusterNode> exc) {
        Map<ClusterNode, Set<KeyCacheObject>> mappings = null;

        for (KeyCacheObject key : keys)
            mappings = map(key, mappings, exc);

        if (isDone())
            return false;

        boolean ret = false;

        if (mappings != null) {
            ClusterNode loc = cctx.localNode();

            int curTopVer = topCntr.get();

            if (!preloader.addFuture(this)) {
                assert isDone() : this;

                return false;
            }

            trackable = true;

            // Create mini futures.
            for (Map.Entry<ClusterNode, Set<KeyCacheObject>> mapped : mappings.entrySet()) {
                ClusterNode n = mapped.getKey();
                Set<KeyCacheObject> mappedKeys = mapped.getValue();

                int cnt = F.size(mappedKeys);

                if (cnt > 0) {
                    ret = true;

                    MiniFuture fut = new MiniFuture(n, mappedKeys, curTopVer, exc);

                    GridDhtForceKeysRequest req = new GridDhtForceKeysRequest(
                        cctx.cacheId(),
                        futId,
                        fut.miniId(),
                        mappedKeys,
                        topVer,
                        cctx.deploymentEnabled());

                    try {
                        add(fut); // Append new future.

                        assert !n.id().equals(loc.id());

                        if (log.isDebugEnabled())
                            log.debug("Sending force key request [cacheName=" + cctx.name() + "node=" + n.id() +
                                ", req=" + req + ']');

                        cctx.io().send(n, req, cctx.ioPolicy());
                    }
                    catch (IgniteCheckedException e) {
                        // Fail the whole thing.
                        if (e instanceof ClusterTopologyCheckedException)
                            fut.onResult();
                        else if (!cctx.kernalContext().isStopping())
                            fut.onResult(e);
                    }
                }
            }
        }

        return ret;
    }

    /**
     * @param key Key.
     * @param exc Exclude nodes.
     * @param mappings Mappings.
     * @return Mappings.
     */
    private Map<ClusterNode, Set<KeyCacheObject>> map(KeyCacheObject key,
        @Nullable Map<ClusterNode, Set<KeyCacheObject>> mappings,
        Collection<ClusterNode> exc)
    {
        ClusterNode loc = cctx.localNode();

        GridCacheEntryEx e = cctx.dht().peekEx(key);

        try {
            if (e != null && !e.isNewLocked()) {
                if (log.isDebugEnabled()) {
                    int part = cctx.affinity().partition(key);

                    log.debug("Will not rebalance key (entry is not new) [cacheName=" + cctx.name() +
                        ", key=" + key + ", part=" + part + ", locId=" + cctx.nodeId() + ']');
                }

                // Key has been rebalanced or retrieved already.
                return mappings;
            }
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Received removed DHT entry for force keys request [entry=" + e +
                    ", locId=" + cctx.nodeId() + ']');
        }

        int part = cctx.affinity().partition(key);

        List<ClusterNode> owners = F.isEmpty(exc) ? top.owners(part, topVer) :
            new ArrayList<>(F.view(top.owners(part, topVer), F.notIn(exc)));

        if (owners.isEmpty() || (owners.contains(loc) && cctx.rebalanceEnabled())) {
            if (log.isDebugEnabled())
                log.debug("Will not rebalance key (local node is owner) [key=" + key + ", part=" + part +
                    "topVer=" + topVer + ", locId=" + cctx.nodeId() + ']');

            // Key is already rebalanced.
            return mappings;
        }

        // Create partition.
        GridDhtLocalPartition locPart = top.localPartition(part, topVer, false);

        if (log.isDebugEnabled())
            log.debug("Mapping local partition [loc=" + cctx.localNodeId() + ", topVer" + topVer +
                ", part=" + locPart + ", owners=" + owners + ", allOwners=" + U.toShortString(top.owners(part)) + ']');

        if (locPart == null)
            invalidParts.add(part);
        // If rebalance is disabled, then local partition is always MOVING.
        else if (locPart.state() == MOVING) {
            Collections.sort(owners, CU.nodeComparator(false));

            // Load from youngest owner.
            ClusterNode pick = F.first(owners);

            assert pick != null;

            if (!cctx.rebalanceEnabled() && loc.id().equals(pick.id()))
                pick = F.first(F.view(owners, F.remoteNodes(loc.id())));

            if (pick == null) {
                if (log.isDebugEnabled())
                    log.debug("Will not rebalance key (no nodes to request from with rebalancing disabled) [key=" +
                        key + ", part=" + part + ", locId=" + cctx.nodeId() + ']');

                return mappings;
            }

            if (mappings == null)
                mappings = U.newHashMap(keys.size());

            Collection<KeyCacheObject> mappedKeys = F.addIfAbsent(mappings, pick, F.<KeyCacheObject>newSet());

            assert mappedKeys != null;

            mappedKeys.add(key);

            if (log.isDebugEnabled())
                log.debug("Will rebalance key from node [cacheName=" + cctx.namex() + ", key=" + key + ", part=" +
                    part + ", node=" + pick.id() + ", locId=" + cctx.nodeId() + ']');
        }
        else if (locPart.state() != OWNING)
            invalidParts.add(part);
        else {
            if (log.isDebugEnabled())
                log.debug("Will not rebalance key (local partition is not MOVING) [cacheName=" + cctx.name() +
                    ", key=" + key + ", part=" + locPart + ", locId=" + cctx.nodeId() + ']');
        }

        return mappings;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                return f.toString();
            }
        });

        return S.toString(GridDhtForceKeysFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Mini-future ID. */
        private IgniteUuid miniId = IgniteUuid.randomUuid();

        /** Node. */
        private ClusterNode node;

        /** Requested keys. */
        private Collection<KeyCacheObject> keys;

        /** Topology version for this mini-future. */
        private int curTopVer;

        /** Pause latch for remapping missed keys. */
        private CountDownLatch pauseLatch = new CountDownLatch(1);

        /** Excludes. */
        private Collection<ClusterNode> exc;

        /**
         * @param node Node.
         * @param keys Keys.
         * @param curTopVer Topology version for this mini-future.
         * @param exc Exclude node list.
         */
        MiniFuture(ClusterNode node, Collection<KeyCacheObject> keys, int curTopVer, Collection<ClusterNode> exc) {
            assert node != null;
            assert curTopVer > 0;
            assert exc != null;

            this.node = node;
            this.keys = keys;
            this.curTopVer = curTopVer;
            this.exc = exc;
        }

        /**
         * @return Mini-future ID.
         */
        IgniteUuid miniId() {
            return miniId;
        }

        /**
         * @return Node ID.
         */
        ClusterNode node() {
            return node;
        }

        /**
         *
         */
        void onDiscoveryEvent() {
            pauseLatch.countDown();
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
         */
        void onResult() {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            // Remap.
            map(keys, /*exclude*/F.asList(node));

            onDone(true);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridDhtForceKeysResponse res) {
            if (res.error() != null) {
                onDone(res.error());

                return;
            }

            Collection<KeyCacheObject> missedKeys = res.missedKeys();

            boolean remapMissed = false;

            if (!F.isEmpty(missedKeys)) {
                if (curTopVer != topCntr.get() || pauseLatch.getCount() == 0)
                    map(missedKeys, Collections.<ClusterNode>emptyList());
                else
                    remapMissed = true;
            }

            // If rebalancing is disabled, we need to check other backups.
            if (!cctx.rebalanceEnabled()) {
                Collection<KeyCacheObject> retryKeys = F.view(
                    keys,
                    F0.notIn(missedKeys),
                    F0.notIn(F.viewReadOnly(res.forcedInfos(), CU.<KeyCacheObject, V>info2Key())));

                if (!retryKeys.isEmpty())
                    map(retryKeys, F.concat(false, node, exc));
            }

            boolean rec = cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED);

            boolean replicate = cctx.isDrEnabled();

            for (GridCacheEntryInfo info : res.forcedInfos()) {
                int p = cctx.affinity().partition(info.key());

                GridDhtLocalPartition locPart = top.localPartition(p, AffinityTopologyVersion.NONE, false);

                if (locPart != null && locPart.state() == MOVING && locPart.reserve()) {
                    GridCacheEntryEx entry = cctx.dht().entryEx(info.key());

                    try {
                        if (entry.initialValue(
                            info.value(),
                            info.version(),
                            info.ttl(),
                            info.expireTime(),
                            true,
                            topVer,
                            replicate ? DR_PRELOAD : DR_NONE,
                            false
                        )) {
                            if (rec && !entry.isInternal())
                                cctx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(),
                                    (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, info.value(), true, null,
                                    false, null, null, null, false);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        onDone(e);

                        return;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Trying to rebalance removed entry (will ignore) [cacheName=" +
                                cctx.namex() + ", entry=" + entry + ']');
                    }
                    finally {
                        locPart.release();
                    }
                }
            }

            if (remapMissed && pause())
                map(missedKeys, Collections.<ClusterNode>emptyList());

            // Finish mini future.
            onDone(true);
        }

        /**
         * Pause to avoid crazy resending in case of topology changes.
         *
         * @return {@code True} if was not interrupted.
         */
        private boolean pause() {
            try {
                U.await(pauseLatch, REMAP_PAUSE, MILLISECONDS);

                return true;
            }
            catch (IgniteInterruptedCheckedException e) {
                // Fail.
                onDone(e);

                return false;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, super.toString());
        }
    }
}
