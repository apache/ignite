/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Makes sure that cache lock order values come in proper sequence.
 * <p>
 * NOTE: this class should not make use of any cache specific structures,
 * like, for example GridCacheContext, as it may be reused between different
 * caches.
 */
public class GridCacheVersionManager<K, V> extends GridCacheManagerAdapter<K, V> {
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

    /** Serializable transaction flag. */
    private boolean txSerEnabled;

    /** Data cetner ID. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private byte dataCenterId;

    /** */
    private long gridStartTime;

    /** */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(GridEvent evt) {
            assert evt.type() == EVT_NODE_METRICS_UPDATED;

            GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

            GridNode node = cctx.discovery().node(discoEvt.node().id());

            if (node != null && !node.id().equals(cctx.nodeId()))
                onReceived(discoEvt.eventNode().id(), node.metrics().getLastDataVersion());
        }
    };

    /**
     * @return Pre-generated UUID.
     */
    private GridUuid uuid() {
        return GridUuid.randomUuid();
    }

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        txSerEnabled = cctx.config().isTxSerializableEnabled();

        dataCenterId = cctx.kernalContext().config().getDataCenterId();

        last = new GridCacheVersion(0, 0, order.get(), 0, dataCenterId);

        cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_METRICS_UPDATED);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws GridException {
        for (GridNode n : cctx.discovery().remoteNodes())
            onReceived(n.id(), n.metrics().getLastDataVersion());
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        cctx.gridEvents().removeLocalEventListener(discoLsnr, EVT_NODE_METRICS_UPDATED);
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
    public GridCacheVersion onReceivedAndNext(UUID nodeId, GridCacheVersion ver) {
        onReceived(nodeId, ver);

        return next(ver);
    }

    /**
     * @return Next version based on current topology.
     */
    public GridCacheVersion next() {
        return next(cctx.kernalContext().discovery().topologyVersion(), true, false);
    }

    /**
     * Gets next version based on given topology version. Given value should be
     * real topology version calculated as number of grid topology changes and
     * obtained from discovery manager.
     *
     * @param topVer Topology version for which new version should be obtained.
     * @return Next version based on given topology version.
     */
    public GridCacheVersion next(long topVer) {
        return next(topVer, true, false);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad() {
        return next(cctx.kernalContext().discovery().topologyVersion(), true, true);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad(long topVer) {
        return next(topVer, true, true);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad(GridCacheVersion ver) {
        return next(ver.topologyVersion(), false, true);
    }

    /**
     * Gets next version based on given cache version.
     *
     * @param ver Cache version for which new version should be obtained.
     * @return Next version based on given cache version.
     */
    public GridCacheVersion next(GridCacheVersion ver) {
        return next(ver.topologyVersion(), false, false);
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
     * @return New lock order.
     */
    private GridCacheVersion next(long topVer, boolean addTime, boolean forLoad) {
        if (topVer == -1)
            topVer = cctx.kernalContext().discovery().topologyVersion();

        if (addTime) {
            if (gridStartTime == 0)
                gridStartTime = cctx.kernalContext().discovery().gridStartTime();

            topVer += (gridStartTime - TOP_VER_BASE_TIME) / 1000;
        }

        long globalTime = cctx.config().getAtomicWriteOrderMode() == GridCacheAtomicWriteOrderMode.CLOCK ?
            cctx.kernalContext().clockSync().adjustedTime(topVer) : 0;

        int locNodeOrder = (int)cctx.localNode().order();

        if (txSerEnabled) {
            synchronized (this) {
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
        }
        else {
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
    }

    /**
     * Gets last generated version without generating a new one.
     *
     * @return Last generated version.
     */
    public GridCacheVersion last() {
        if (txSerEnabled) {
            synchronized (this) {
                return last;
            }
        }
        else
            return last;
    }
}
