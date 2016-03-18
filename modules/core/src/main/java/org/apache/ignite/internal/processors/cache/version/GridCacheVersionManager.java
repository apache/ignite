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

package org.apache.ignite.internal.processors.cache.version;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

/**
 * Makes sure that cache lock order values come in proper sequence.
 * <p>
 * NOTE: this class should not make use of any cache specific structures,
 * like, for example GridCacheContext, as it may be reused between different
 * caches.
 */
public class GridCacheVersionManager extends GridCacheSharedManagerAdapter {
    /** Timestamp used as base time for cache topology version (January 1, 2014). */
    public static final long TOP_VER_BASE_TIME = 1388520000000L;

    /**
     * Current order. Initialize to current time to make sure that
     * local version increments even after restarts.
     */
    private final AtomicLong order = new AtomicLong(U.currentTimeMillis());

    /** Current order for store operations. */
    private final AtomicLong loadOrder = new AtomicLong(0);

    /** Last version. */
    private volatile CacheVersion last;

    /** Data center ID. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private byte dataCenterId;

    /** */
    private long gridStartTime;

    /** */
    private CacheVersion ISOLATED_STREAMER_VER;

    /** */
    private boolean delayAffAssign = true;

    /** */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt.type() == EVT_NODE_METRICS_UPDATED;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            ClusterNode node = cctx.discovery().node(discoEvt.node().id());

            if (node != null && !node.id().equals(cctx.localNodeId()))
                onReceived(discoEvt.eventNode().id(), node.metrics().getLastDataVersion());
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        last = new GridCacheVersion(0, 0, order.get(), 0, dataCenterId);

        cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_METRICS_UPDATED);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0(boolean reconnect) throws IgniteCheckedException {
        for (ClusterNode n : cctx.discovery().remoteNodes())
            onReceived(n.id(), n.metrics().getLastDataVersion());
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        cctx.gridEvents().removeLocalEventListener(discoLsnr, EVT_NODE_METRICS_UPDATED);
    }

    /**
     * Sets data center ID.
     *
     * @param dataCenterId Data center ID.
     */
    public void dataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;

        last = new GridCacheVersion(0, 0, order.get(), 0, dataCenterId);
    }

    /**
     * @param nodeId Node ID.
     * @param ver Remote version.
     */
    public void onReceived(UUID nodeId, CacheVersion ver) {
        onReceived(nodeId, ver.order());
    }

    /**
     * TODO: GG-10885.
     *
     * @param nodeId Node ID.
     * @param ver Remote version.
     */
    public void onReceived(UUID nodeId, long ver) {
        if (ver > 0)
            while (true) {
                long order = this.order.get();

                // If another version is larger, we update.
                if (ver > order) {
                    if (!this.order.compareAndSet(order, ver))
                        // Try again.
                        continue;
                    else if (log.isDebugEnabled())
                        log.debug("Updated version from node [nodeId=" + nodeId + ", ver=" + ver + ']');
                }
                else if (log.isDebugEnabled()) {
                    log.debug("Did not update version from node (version has lower order) [nodeId=" + nodeId +
                        ", ver=" + ver + ", curOrder=" + this.order + ']');
                }

                break;
            }
    }

    /**
     * @param nodeId Node ID.
     * @param ver Received version.
     * @return Next version.
     */
    public CacheVersion onReceivedAndNext(UUID nodeId, CacheVersion ver) {
        onReceived(nodeId, ver);

        return next(ver);
    }

    /**
     * @param ptr Offheap address.
     * @param verEx If {@code true} reads {@link GridCacheVersionEx} instance.
     * @return Version.
     */
    public CacheVersion readVersion(long ptr, boolean verEx) {
        CacheVersion ver = createVersionFromRaw(GridUnsafe.getInt(ptr),
            GridUnsafe.getInt(ptr + 4),
            GridUnsafe.getLong(ptr + 8),
            GridUnsafe.getLong(ptr + 16));

        if (verEx) {
            ptr += 24;

            ver = createVersionExFromRaw(GridUnsafe.getInt(ptr),
                GridUnsafe.getInt(ptr + 4),
                GridUnsafe.getLong(ptr + 8),
                GridUnsafe.getLong(ptr + 16),
                ver);
        }

        return ver;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param verEx If {@code true} reads {@link GridCacheVersionEx} instance.
     * @return Version.
     */
    public CacheVersion readVersion(byte[] arr, long off, boolean verEx) {
        int topVer = GridUnsafe.getInt(arr, off);

        off += 4;

        int nodeOrderDrId = GridUnsafe.getInt(arr, off);

        off += 4;

        long globalTime = GridUnsafe.getLong(arr, off);

        off += 8;

        long order = GridUnsafe.getLong(arr, off);

        off += 8;

        CacheVersion ver = createVersionFromRaw(topVer, nodeOrderDrId, globalTime, order);

        if (verEx) {
            topVer = GridUnsafe.getInt(arr, off);

            off += 4;

            nodeOrderDrId = GridUnsafe.getInt(arr, off);

            off += 4;

            globalTime = GridUnsafe.getLong(arr, off);

            off += 8;

            order = GridUnsafe.getLong(arr, off);

            ver = createVersionExFromRaw(topVer, nodeOrderDrId, globalTime, order, ver);
        }

        return ver;
    }

    /**
     * @param ver Version.
     * @param conflictVer Conflict version.
     * @return
     */
    public CacheVersion versionEx(CacheVersion ver, CacheVersion conflictVer) {
        return createVersionEx(ver, conflictVer);
    }

    private CacheVersion createVersionEx(CacheVersion ver, CacheVersion conflictVer) {
        if (delayAffAssign)
            return new CacheVersionImplEx((CacheVersionImpl)ver, (CacheVersionImpl)conflictVer);
        else {
            return new GridCacheVersionEx(ver.topologyVersion(),
                ver.globalTime(),
                ver.order(),
                ver.nodeOrder(),
                ver.dataCenterId(),
                (GridCacheVersion)conflictVer);
        }
    }

    private CacheVersion createVersionExFromRaw(int topVer,
        int nodeOrderDrId,
        long globalTime,
        long order,
        CacheVersion conflictVer) {
        if (delayAffAssign) {
            return new CacheVersionImplEx(topVer,
                nodeOrderDrId,
                globalTime,
                order,
                (CacheVersionImpl)conflictVer);
        }
        else {
            return new GridCacheVersionEx(topVer,
                nodeOrderDrId,
                globalTime,
                order,
                (GridCacheVersion)conflictVer);
        }
    }

    private CacheVersion createVersionFromRaw(int topVer, int nodeOrderDrId, long globalTime, long order) {
        if (delayAffAssign)
            return new CacheVersionImpl(topVer, nodeOrderDrId, globalTime, order);
        else
            return new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);
    }

    /**
     * Version for entries loaded with isolated streamer, should be less than any version generated
     * for entries update.
     *
     * @return Version for entries loaded with isolated streamer.
     */
    public CacheVersion isolatedStreamerVersion() {
        if (ISOLATED_STREAMER_VER == null) {
            long topVer = 1;

            if (gridStartTime == 0)
                gridStartTime = cctx.kernalContext().discovery().gridStartTime();

            topVer += (gridStartTime - TOP_VER_BASE_TIME) / 1000;

            ISOLATED_STREAMER_VER = delayAffAssign ? new CacheVersionImpl((int)topVer, 0, 0, 0, 1, dataCenterId) :
                new GridCacheVersion((int)topVer, 0, 0, 1, dataCenterId);
        }

        return ISOLATED_STREAMER_VER;
    }

    /**
     * @return Next version based on current topology.
     */
    public CacheVersion next() {
        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        return next(topVer.topologyVersion(), topVer.minorTopologyVersion(), true, false, dataCenterId);
    }

    /**
     * @param dataCenterId Data center id.
     * @return Next version based on current topology with given data center id.
     */
    public CacheVersion next(byte dataCenterId) {
        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        return next(topVer.topologyVersion(), topVer.minorTopologyVersion(), true, false, dataCenterId);
    }

    /**
     * Gets next version based on given topology version. Given value should be
     * real topology version calculated as number of grid topology changes and
     * obtained from discovery manager.
     *
     * @param topVer Topology version for which new version should be obtained.
     * @return Next version based on given topology version.
     */
    public CacheVersion next(AffinityTopologyVersion topVer) {
        return next(topVer.topologyVersion(), topVer.minorTopologyVersion(), true, false, dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public CacheVersion nextForLoad() {
        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        return next(topVer.topologyVersion(), topVer.minorTopologyVersion(), true, true, dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public CacheVersion nextForLoad(AffinityTopologyVersion topVer) {
        return next(topVer.topologyVersion(), topVer.minorTopologyVersion(), true, true, dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public CacheVersion nextForLoad(CacheVersion ver) {
        long topVer = ver.topologyVersion();
        int minorTopVer = delayAffAssign ? ((CacheVersionImpl)ver).minorTopologyVersion() : 0;

        return next(topVer, minorTopVer, false, true, dataCenterId);
    }

    /**
     * Gets next version based on given cache version.
     *
     * @param ver Cache version for which new version should be obtained.
     * @return Next version based on given cache version.
     */
    public CacheVersion next(CacheVersion ver) {
        long topVer = ver.topologyVersion();
        int minorTopVer = delayAffAssign ? ((CacheVersionImpl)ver).minorTopologyVersion() : 0;

        return next(topVer, minorTopVer, false, false, dataCenterId);
    }

    /**
     * The version is generated by taking last order plus one and random {@link UUID}.
     * Such algorithm ensures that lock IDs constantly grow in value and older
     * lock IDs are smaller than new ones. Therefore, older lock IDs appear
     * in the pending set before newer ones, hence preventing starvation.
     *
     * @param topVer Topology version for which new version should be obtained.
     * @param addTime If {@code true} then adds to the given topology version number of seconds
     *        from the start time of the first grid node.
     * @param dataCenterId Data center id.
     * @return New lock order.
     */
    private CacheVersion next(long topVer, int minorTopVer, boolean addTime, boolean forLoad, byte dataCenterId) {
        if (topVer == -1L) {
            AffinityTopologyVersion topVer0 = cctx.kernalContext().discovery().topologyVersionEx();

            topVer = topVer0.topologyVersion();
            minorTopVer = topVer0.minorTopologyVersion();
        }

        long globalTime = cctx.kernalContext().clockSync().adjustedTime(topVer);

        if (addTime) {
            if (gridStartTime == 0)
                gridStartTime = cctx.kernalContext().discovery().gridStartTime();

            topVer += (gridStartTime - TOP_VER_BASE_TIME) / 1000;
        }

        int locNodeOrder = (int)cctx.localNode().order();

        long ord = forLoad ? loadOrder.incrementAndGet() : order.incrementAndGet();

        CacheVersion next;

        if (delayAffAssign) {
            next = new CacheVersionImpl(
                (int)topVer,
                minorTopVer,
                globalTime,
                ord,
                locNodeOrder,
                dataCenterId);
        }
        else {
            next = new GridCacheVersion(
                (int)topVer,
                globalTime,
                ord,
                locNodeOrder,
                dataCenterId);
        }

        last = next;

        return next;
    }

    /**
     * Gets last generated version without generating a new one.
     *
     * @return Last generated version.
     */
    public CacheVersion last() {
        return last;
    }
}