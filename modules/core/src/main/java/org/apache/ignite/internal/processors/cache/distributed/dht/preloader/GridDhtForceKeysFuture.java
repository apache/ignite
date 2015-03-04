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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;

/**
 * Force keys request future.
 */
public final class GridDhtForceKeysFuture<K, V> extends GridCompoundFuture<Object, Collection<K>>
    implements GridDhtFuture<Collection<K>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Wait for 1 second for topology to change. */
    private static final long REMAP_PAUSE = 1000;

    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Topology. */
    private GridDhtPartitionTopology<K, V> top;

    /** Logger. */
    private IgniteLogger log;

    /** Keys to request. */
    private Collection<? extends K> keys;

    /** Keys for which local node is no longer primary. */
    private Collection<Integer> invalidParts = new GridLeanSet<>();

    /** Topology change counter. */
    private AtomicInteger topCntr = new AtomicInteger(1);

    /** Topology version. */
    private long topVer;

    /** Future ID. */
    private IgniteUuid futId = IgniteUuid.randomUuid();

    /** Preloader. */
    private GridDhtPreloader<K, V> preloader;

    /** Trackable flag. */
    private boolean trackable;

    /**
     * @param cctx Cache context.
     * @param topVer Topology version.
     * @param keys Keys.
     * @param preloader Preloader.
     */
    public GridDhtForceKeysFuture(GridCacheContext<K, V> cctx, long topVer, Collection<? extends K> keys,
        GridDhtPreloader<K, V> preloader) {
        super(cctx.kernalContext());

        assert topVer != 0 : topVer;
        assert !F.isEmpty(keys) : keys;

        this.cctx = cctx;
        this.keys = keys;
        this.topVer = topVer;
        this.preloader = preloader;

        top = cctx.dht().topology();

        log = U.logger(ctx, logRef, GridDhtForceKeysFuture.class);

        syncNotify(true);
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtForceKeysFuture() {
        // No-op.
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
                        mini.onResult(new ClusterTopologyCheckedException("Node left grid (will retry): " +
                            evt.eventNode().id()));

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
    public void onResult(UUID nodeId, GridDhtForceKeysResponse<K, V> res) {
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
    private boolean map(Iterable<? extends K> keys, Collection<ClusterNode> exc) {
        Map<ClusterNode, Set<K>> mappings = new HashMap<>();

        ClusterNode loc = cctx.localNode();

        int curTopVer = topCntr.get();

        for (K key : keys)
            map(key, mappings, exc);

        if (isDone())
            return false;

        boolean ret = false;

        if (!mappings.isEmpty()) {
            preloader.addFuture(this);

            trackable = true;

            // Create mini futures.
            for (Map.Entry<ClusterNode, Set<K>> mapped : mappings.entrySet()) {
                ClusterNode n = mapped.getKey();
                Set<K> mappedKeys = mapped.getValue();

                int cnt = F.size(mappedKeys);

                if (cnt > 0) {
                    ret = true;

                    MiniFuture fut = new MiniFuture(n, mappedKeys, curTopVer, exc);

                    GridDhtForceKeysRequest<K, V> req = new GridDhtForceKeysRequest<>(
                        cctx.cacheId(),
                        futId,
                        fut.miniId(),
                        mappedKeys,
                        topVer);

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
                            fut.onResult((ClusterTopologyCheckedException)e);
                        else
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
     */
    private void map(K key, Map<ClusterNode, Set<K>> mappings, Collection<ClusterNode> exc) {
        ClusterNode loc = cctx.localNode();

        int part = cctx.affinity().partition(key);

        GridCacheEntryEx<K, V> e = cctx.dht().peekEx(key);

        try {
            if (e != null && !e.isNewLocked()) {
                if (log.isDebugEnabled())
                    log.debug("Will not preload key (entry is not new) [cacheName=" + cctx.name() +
                        ", key=" + key + ", part=" + part + ", locId=" + cctx.nodeId() + ']');

                // Key has been preloaded or retrieved already.
                return;
            }
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Received removed DHT entry for force keys request [entry=" + e +
                    ", locId=" + cctx.nodeId() + ']');
        }

        List<ClusterNode> owners = F.isEmpty(exc) ? top.owners(part, topVer) :
            new ArrayList<>(F.view(top.owners(part, topVer), F.notIn(exc)));

        if (owners.isEmpty() || (owners.contains(loc) && cctx.preloadEnabled())) {
            if (log.isDebugEnabled())
                log.debug("Will not preload key (local node is owner) [key=" + key + ", part=" + part +
                    "topVer=" + topVer + ", locId=" + cctx.nodeId() + ']');

            // Key is already preloaded.
            return;
        }

        // Create partition.
        GridDhtLocalPartition<K, V> locPart = top.localPartition(part, topVer, false);

        if (log.isDebugEnabled())
            log.debug("Mapping local partition [loc=" + cctx.localNodeId() + ", topVer" + topVer +
                ", part=" + locPart + ", owners=" + owners + ", allOwners=" + U.toShortString(top.owners(part)) + ']');

        if (locPart == null)
            invalidParts.add(part);
        // If preloader is disabled, then local partition is always MOVING.
        else if (locPart.state() == MOVING) {
            Collections.sort(owners, CU.nodeComparator(false));

            // Load from youngest owner.
            ClusterNode pick = F.first(owners);

            assert pick != null;

            if (!cctx.preloadEnabled() && loc.id().equals(pick.id()))
                pick = F.first(F.view(owners, F.remoteNodes(loc.id())));

            if (pick == null) {
                if (log.isDebugEnabled())
                    log.debug("Will not preload key (no nodes to request from with preloading disabled) [key=" +
                        key + ", part=" + part + ", locId=" + cctx.nodeId() + ']');

                return;
            }

            Collection<K> mappedKeys = F.addIfAbsent(mappings, pick, F.<K>newSet());

            assert mappedKeys != null;

            mappedKeys.add(key);

            if (log.isDebugEnabled())
                log.debug("Will preload key from node [cacheName=" + cctx.namex() + ", key=" + key + ", part=" +
                    part + ", node=" + pick.id() + ", locId=" + cctx.nodeId() + ']');
        }
        else if (locPart.state() != OWNING)
            invalidParts.add(part);
        else {
            if (log.isDebugEnabled())
                log.debug("Will not preload key (local partition is not MOVING) [cacheName=" + cctx.name() +
                    ", key=" + key + ", part=" + locPart + ", locId=" + cctx.nodeId() + ']');
        }
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
        private Collection<K> keys;

        /** Topology version for this mini-future. */
        private int curTopVer;

        /** Pause latch for remapping missed keys. */
        private CountDownLatch pauseLatch = new CountDownLatch(1);

        /** Excludes. */
        private Collection<ClusterNode> exc;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param node Node.
         * @param keys Keys.
         * @param curTopVer Topology version for this mini-future.
         * @param exc Exclude node list.
         */
        MiniFuture(ClusterNode node, Collection<K> keys, int curTopVer, Collection<ClusterNode> exc) {
            super(cctx.kernalContext());

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
         * @param e Node failure.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            // Remap.
            map(keys, /*exclude*/F.asList(node));

            onDone(true);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridDhtForceKeysResponse<K, V> res) {
            Collection<K> missedKeys = res.missedKeys();

            boolean remapMissed = false;

            if (!F.isEmpty(missedKeys)) {
                if (curTopVer != topCntr.get() || pauseLatch.getCount() == 0)
                    map(missedKeys, Collections.<ClusterNode>emptyList());
                else
                    remapMissed = true;
            }

            // If preloading is disabled, we need to check other backups.
            if (!cctx.preloadEnabled()) {
                Collection<K> retryKeys = F.view(
                    keys,
                    F0.notIn(missedKeys),
                    F0.notIn(F.viewReadOnly(res.forcedInfos(), CU.<K, V>info2Key())));

                if (!retryKeys.isEmpty())
                    map(retryKeys, F.concat(false, node, exc));
            }

            boolean rec = cctx.events().isRecordable(EVT_CACHE_PRELOAD_OBJECT_LOADED);

            boolean replicate = cctx.isDrEnabled();

            for (GridCacheEntryInfo<K, V> info : res.forcedInfos()) {
                int p = cctx.affinity().partition(info.key());

                GridDhtLocalPartition<K, V> locPart = top.localPartition(p, -1, false);

                if (locPart != null && locPart.state() == MOVING && locPart.reserve()) {
                    GridCacheEntryEx<K, V> entry = cctx.dht().entryEx(info.key());

                    try {
                        if (entry.initialValue(
                            info.value(),
                            info.valueBytes(),
                            info.version(),
                            info.ttl(),
                            info.expireTime(),
                            true,
                            topVer,
                            replicate ? DR_PRELOAD : DR_NONE
                        )) {
                            if (rec && !entry.isInternal())
                                cctx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(),
                                    (IgniteUuid)null, null, EVT_CACHE_PRELOAD_OBJECT_LOADED, info.value(), true, null,
                                    false, null, null, null);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        onDone(e);

                        return;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Trying to preload removed entry (will ignore) [cacheName=" +
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
