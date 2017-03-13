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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
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
    private volatile GridCacheVersion last;

    /** Data center ID. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private byte dataCenterId;

    /** */
    private long gridStartTime;

    /** */
    private GridCacheVersion ISOLATED_STREAMER_VER;

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
    public void onReceived(UUID nodeId, GridCacheVersion ver) {
        onReceived(nodeId, ver.order());
    }

    /**
     * @param nodeId Node ID.
     * @param ver Remote version.
     */
    public void onReceived(UUID nodeId, long ver) {
        if (ver > 0) {
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
    }

    /**
     * @param rcvOrder Received order.
     */
    public void onExchange(long rcvOrder) {
        long order;

        while (true) {
            order = this.order.get();

            if (rcvOrder > order) {
                if (this.order.compareAndSet(order, rcvOrder))
                    break;
            }
            else
                break;
        }
    }

    /**
     * @param nodeId Node ID.
     * @param ver Received version.
     * @return Next version.
     */
    public GridCacheVersion onReceivedAndNext(UUID nodeId, GridCacheVersion ver) {
        onReceived(nodeId, ver);

        return next(ver);
    }

    /**
     * Version for entries loaded with isolated streamer, should be less than any version generated
     * for entries update.
     *
     * @return Version for entries loaded with isolated streamer.
     */
    public GridCacheVersion isolatedStreamerVersion() {
        if (ISOLATED_STREAMER_VER == null) {
            long topVer = 1;

            if (gridStartTime == 0)
                gridStartTime = cctx.kernalContext().discovery().gridStartTime();

            topVer += (gridStartTime - TOP_VER_BASE_TIME) / 1000;

            ISOLATED_STREAMER_VER = new GridCacheVersion((int)topVer, 0, 0, 1, dataCenterId);
        }

        return ISOLATED_STREAMER_VER;
    }

    /**
     * @return Next version based on current topology.
     */
    public GridCacheVersion next() {
        return next(cctx.kernalContext().discovery().topologyVersion(), true, false, dataCenterId);
    }

    /**
     * @param dataCenterId Data center id.
     * @return Next version based on current topology with given data center id.
     */
    public GridCacheVersion next(byte dataCenterId) {
        return next(cctx.kernalContext().discovery().topologyVersion(), true, false, dataCenterId);
    }

    /**
     * Gets next version based on given topology version. Given value should be
     * real topology version calculated as number of grid topology changes and
     * obtained from discovery manager.
     *
     * @param topVer Topology version for which new version should be obtained.
     * @return Next version based on given topology version.
     */
    public GridCacheVersion next(AffinityTopologyVersion topVer) {
        return next(topVer.topologyVersion(), true, false, dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad() {
        return next(cctx.kernalContext().discovery().topologyVersion(), true, true, dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad(AffinityTopologyVersion topVer) {
        return next(topVer.topologyVersion(), true, true, dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad(GridCacheVersion ver) {
        return next(ver.topologyVersion(), false, true, dataCenterId);
    }

    /**
     * Gets next version based on given cache version.
     *
     * @param ver Cache version for which new version should be obtained.
     * @return Next version based on given cache version.
     */
    public GridCacheVersion next(GridCacheVersion ver) {
        return next(ver.topologyVersion(), false, false, dataCenterId);
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
    private GridCacheVersion next(long topVer, boolean addTime, boolean forLoad, byte dataCenterId) {
        if (topVer == -1)
            topVer = cctx.kernalContext().discovery().topologyVersion();

        long globalTime = cctx.kernalContext().clockSync().adjustedTime(topVer);

        if (addTime) {
            if (gridStartTime == 0)
                gridStartTime = cctx.kernalContext().discovery().gridStartTime();

            topVer += (gridStartTime - TOP_VER_BASE_TIME) / 1000;
        }

        int locNodeOrder = (int)cctx.localNode().order();

        long ord = forLoad ? loadOrder.incrementAndGet() : order.incrementAndGet();

        GridCacheVersion next = new GridCacheVersion(
            (int)topVer,
            globalTime,
            ord,
            locNodeOrder,
            dataCenterId);

        last = next;

        return next;
    }

    /**
     * Gets last generated version without generating a new one.
     *
     * @return Last generated version.
     */
    public GridCacheVersion last() {
        return last;
    }
}
