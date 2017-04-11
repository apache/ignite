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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 * Thread pool for supplying partitions to demanding nodes.
 */
class GridDhtPartitionSupplier {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private GridDhtPartitionTopology top;

    /** */
    private final boolean depEnabled;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo> preloadPred;

    /** Supply context map. T2: nodeId, idx, topVer. */
    private final Map<T3<UUID, Integer, AffinityTopologyVersion>, SupplyContext> scMap = new HashMap<>();

    /**
     * @param cctx Cache context.
     */
    GridDhtPartitionSupplier(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        log = cctx.logger(getClass());

        top = cctx.dht().topology();

        depEnabled = cctx.gridDeploy().enabled();
    }

    /**
     *
     */
    void stop() {
        synchronized (scMap) {
            Iterator<T3<UUID, Integer, AffinityTopologyVersion>> it = scMap.keySet().iterator();

            while (it.hasNext()) {
                T3<UUID, Integer, AffinityTopologyVersion> t = it.next();

                clearContext(scMap.get(t), log);

                it.remove();
            }
        }
    }

    /**
     * Clear context.
     *
     * @param sc Supply context.
     * @param log Logger.
     */
    private static void clearContext(
        final SupplyContext sc,
        final IgniteLogger log) {
        if (sc != null) {
            final Iterator it = sc.entryIt;

            if (it != null && it instanceof GridCloseableIterator && !((GridCloseableIterator)it).isClosed()) {
                try {
                    ((GridCloseableIterator)it).close();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Iterator close failed.", e);
                }
            }

            final GridDhtLocalPartition loc = sc.loc;

            if (loc != null) {
                assert loc.reservations() > 0;

                loc.release();
            }
        }
    }

    /**
     * Handles new topology.
     *
     * @param topVer Topology version.
     */
    @SuppressWarnings("ConstantConditions")
    public void onTopologyChanged(AffinityTopologyVersion topVer) {
        synchronized (scMap) {
            Iterator<T3<UUID, Integer, AffinityTopologyVersion>> it = scMap.keySet().iterator();

            while (it.hasNext()) {
                T3<UUID, Integer, AffinityTopologyVersion> t = it.next();

                if (topVer.compareTo(t.get3()) > 0) { // Clear all obsolete contexts.
                    clearContext(scMap.get(t), log);

                    it.remove();

                    if (log.isDebugEnabled())
                        log.debug("Supply context removed [node=" + t.get1() + "]");
                }
            }
        }
    }

    /**
     * Sets preload predicate for supply pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * @param d Demand message.
     * @param idx Index.
     * @param id Node uuid.
     */
    @SuppressWarnings("unchecked")
    public void handleDemandMessage(int idx, UUID id, GridDhtPartitionDemandMessage d) {
        assert d != null;
        assert id != null;

        AffinityTopologyVersion cutTop = cctx.affinity().affinityTopologyVersion();
        AffinityTopologyVersion demTop = d.topologyVersion();

        T3<UUID, Integer, AffinityTopologyVersion> scId = new T3<>(id, idx, demTop);

        if (d.updateSequence() == -1) { //Demand node requested context cleanup.
            synchronized (scMap) {
                clearContext(scMap.remove(scId), log);

                return;
            }
        }

        if (cutTop.compareTo(demTop) > 0) {
            if (log.isDebugEnabled())
                log.debug("Demand request cancelled [current=" + cutTop + ", demanded=" + demTop +
                    ", from=" + id + ", idx=" + idx + "]");

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Demand request accepted [current=" + cutTop + ", demanded=" + demTop +
                ", from=" + id + ", idx=" + idx + "]");

        GridDhtPartitionSupplyMessage s = new GridDhtPartitionSupplyMessage(
            d.updateSequence(), cctx.cacheId(), d.topologyVersion(), cctx.deploymentEnabled());

        ClusterNode node = cctx.discovery().node(id);

        if (node == null)
            return; // Context will be cleaned at topology change.

        try {
            SupplyContext sctx;

            synchronized (scMap) {
                sctx = scMap.remove(scId);

                assert sctx == null || d.updateSequence() == sctx.updateSeq;
            }

            // Initial demand request should contain partitions list.
            if (sctx == null && d.partitions() == null)
                return;

            assert !(sctx != null && d.partitions() != null);

            long bCnt = 0;

            SupplyContextPhase phase = SupplyContextPhase.NEW;

            boolean newReq = true;

            long maxBatchesCnt = cctx.config().getRebalanceBatchesPrefetchCount();

            if (sctx != null) {
                phase = sctx.phase;

                maxBatchesCnt = 1;
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Starting supplying rebalancing [cache=" + cctx.name() +
                        ", fromNode=" + node.id() + ", partitionsCount=" + d.partitions().size() +
                        ", topology=" + d.topologyVersion() + ", updateSeq=" + d.updateSequence() +
                        ", idx=" + idx + "]");
            }

            Iterator<Integer> partIt = sctx != null ? sctx.partIt : d.partitions().iterator();

            while ((sctx != null && newReq) || partIt.hasNext()) {
                int part = sctx != null && newReq ? sctx.part : partIt.next();

                newReq = false;

                GridDhtLocalPartition loc;

                if (sctx != null && sctx.loc != null) {
                    loc = sctx.loc;

                    assert loc.reservations() > 0;
                }
                else {
                    loc = top.localPartition(part, d.topologyVersion(), false);

                    if (loc == null || loc.state() != OWNING || !loc.reserve()) {
                        // Reply with partition of "-1" to let sender know that
                        // this node is no longer an owner.
                        s.missed(part);

                        if (log.isDebugEnabled())
                            log.debug("Requested partition is not owned by local node [part=" + part +
                                ", demander=" + id + ']');

                        continue;
                    }
                }

                try {
                    boolean partMissing = false;

                    if (phase == SupplyContextPhase.NEW)
                        phase = SupplyContextPhase.OFFHEAP;

                    if (phase == SupplyContextPhase.OFFHEAP) {
                        IgniteRebalanceIterator iter;

                        if (sctx == null || sctx.entryIt == null) {
                            iter = cctx.offheap().rebalanceIterator(part, d.topologyVersion(), d.partitionCounter(part));

                            if (!iter.historical())
                                s.clean(part);
                        }
                        else
                            iter = (IgniteRebalanceIterator)sctx.entryIt;

                        while (iter.hasNext()) {
                            if (!cctx.affinity().partitionBelongs(node, part, d.topologyVersion())) {
                                // Demander no longer needs this partition,
                                // so we send '-1' partition and move on.
                                s.missed(part);

                                if (log.isDebugEnabled())
                                    log.debug("Demanding node does not need requested partition " +
                                        "[part=" + part + ", nodeId=" + id + ']');

                                partMissing = true;

                                if (sctx != null) {
                                    sctx = new SupplyContext(
                                        phase,
                                        partIt,
                                        null,
                                        part,
                                        loc,
                                        d.updateSequence());
                                }

                                break;
                            }

                            if (s.messageSize() >= cctx.config().getRebalanceBatchSize()) {
                                if (++bCnt >= maxBatchesCnt) {
                                    saveSupplyContext(scId,
                                        phase,
                                        partIt,
                                        part,
                                        iter,
                                        loc,
                                        d.topologyVersion(),
                                        d.updateSequence());

                                    loc = null;

                                    reply(node, d, s, scId);

                                    return;
                                }
                                else {
                                    if (!reply(node, d, s, scId))
                                        return;

                                    s = new GridDhtPartitionSupplyMessage(d.updateSequence(),
                                        cctx.cacheId(),
                                        d.topologyVersion(),
                                        cctx.deploymentEnabled());
                                }
                            }

                            CacheDataRow row = iter.next();

                            GridCacheEntryInfo info = new GridCacheEntryInfo();

                            info.key(row.key());
                            info.expireTime(row.expireTime());
                            info.version(row.version());
                            info.value(row.value());

                            if (preloadPred == null || preloadPred.apply(info))
                                s.addEntry0(part, info, cctx);
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Rebalance predicate evaluated to false (will not send " +
                                        "cache entry): " + info);

                                continue;
                            }

                            // Need to manually prepare cache message.
// TODO GG-11141.
//                                if (depEnabled && !prepared) {
//                                    ClassLoader ldr = swapEntry.keyClassLoaderId() != null ?
//                                        cctx.deploy().getClassLoader(swapEntry.keyClassLoaderId()) :
//                                        swapEntry.valueClassLoaderId() != null ?
//                                            cctx.deploy().getClassLoader(swapEntry.valueClassLoaderId()) :
//                                            null;
//
//                                    if (ldr == null)
//                                        continue;
//
//                                    if (ldr instanceof GridDeploymentInfo) {
//                                        s.prepare((GridDeploymentInfo)ldr);
//
//                                        prepared = true;
//                                    }
//                                }
                        }

                        if (partMissing)
                            continue;
                    }

                    // Mark as last supply message.
                    s.last(part);

                    phase = SupplyContextPhase.NEW;

                    sctx = null;
                }
                finally {
                    if (loc != null)
                        loc.release();
                }
            }

            reply(node, d, s, scId);

            if (log.isDebugEnabled())
                log.debug("Finished supplying rebalancing [cache=" + cctx.name() +
                    ", fromNode=" + node.id() +
                    ", topology=" + d.topologyVersion() + ", updateSeq=" + d.updateSequence() +
                    ", idx=" + idx + "]");
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send partition supply message to node: " + id, e);
        }
        catch (IgniteSpiException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send message to node (current node is stopping?) [node=" + node.id() +
                    ", msg=" + e.getMessage() + ']');
        }
    }

    /**
     * @param n Node.
     * @param d DemandMessage
     * @param s Supply message.
     * @return {@code True} if message was sent, {@code false} if recipient left grid.
     * @throws IgniteCheckedException If failed.
     */
    private boolean reply(ClusterNode n,
        GridDhtPartitionDemandMessage d,
        GridDhtPartitionSupplyMessage s,
        T3<UUID, Integer, AffinityTopologyVersion> scId)
        throws IgniteCheckedException {

        try {
            if (log.isDebugEnabled())
                log.debug("Replying to partition demand [node=" + n.id() + ", demand=" + d + ", supply=" + s + ']');

            cctx.io().sendOrderedMessage(n, d.topic(), s, cctx.ioPolicy(), d.timeout());

            // Throttle preloading.
            if (cctx.config().getRebalanceThrottle() > 0)
                U.sleep(cctx.config().getRebalanceThrottle());

            return true;
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to send partition supply message because node left grid: " + n.id());

            synchronized (scMap) {
                clearContext(scMap.remove(scId), log);
            }

            return false;
        }
    }

    /**
     * @param t Tuple.
     * @param phase Phase.
     * @param partIt Partition it.
     * @param part Partition.
     * @param entryIt Entry it.
     */
    private void saveSupplyContext(
        T3<UUID, Integer, AffinityTopologyVersion> t,
        SupplyContextPhase phase,
        Iterator<Integer> partIt,
        int part,
        Iterator<?> entryIt,
        GridDhtLocalPartition loc,
        AffinityTopologyVersion topVer,
        long updateSeq) {
        synchronized (scMap) {
            if (cctx.affinity().affinityTopologyVersion().equals(topVer)) {
                assert scMap.get(t) == null;

                scMap.put(t,
                    new SupplyContext(phase,
                        partIt,
                        entryIt,
                        part,
                        loc,
                        updateSeq));
            }
            else if (loc != null) {
                assert loc.reservations() > 0;

                loc.release();
            }
        }
    }

    /**
     * Supply context phase.
     */
    private enum SupplyContextPhase {
        /** */
        NEW,
        /** */
        OFFHEAP
    }

    /**
     * Supply context.
     */
    private static class SupplyContext {
        /** Phase. */
        private final SupplyContextPhase phase;

        /** Partition iterator. */
        @GridToStringExclude
        private final Iterator<Integer> partIt;

        /** Entry iterator. */
        @GridToStringExclude
        private final Iterator<?> entryIt;

        /** Partition. */
        private final int part;

        /** Local partition. */
        private final GridDhtLocalPartition loc;

        /** Update seq. */
        private final long updateSeq;

        /**
         * @param phase Phase.
         * @param partIt Partition iterator.
         * @param loc Partition.
         * @param updateSeq Update sequence.
         * @param entryIt Entry iterator.
         * @param part Partition.
         */
        public SupplyContext(SupplyContextPhase phase,
            Iterator<Integer> partIt,
            Iterator<?> entryIt,
            int part,
            GridDhtLocalPartition loc,
            long updateSeq) {
            this.phase = phase;
            this.partIt = partIt;
            this.entryIt = entryIt;
            this.part = part;
            this.loc = loc;
            this.updateSeq = updateSeq;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(SupplyContext.class, this);
        }
    }

    /**
     * Dumps debug information.
     */
    public void dumpDebugInfo() {
        synchronized (scMap) {
            if (!scMap.isEmpty()) {
                U.warn(log, "Rebalancing supplier reserved following partitions:");

                for (SupplyContext sc : scMap.values()) {
                    if (sc.loc != null)
                        U.warn(log, ">>> " + sc.loc);
                }
            }
        }
    }
}
