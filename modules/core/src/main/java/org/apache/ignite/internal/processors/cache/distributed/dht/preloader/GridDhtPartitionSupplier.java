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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_MISSED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_SUPPLIED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Class for supplying partitions to demanding nodes.
 */
class GridDhtPartitionSupplier {
    /** */
    private final CacheGroupContext grp;

    /** */
    private final IgniteLogger log;

    /** */
    private GridDhtPartitionTopology top;

    /** Supply context map. T3: nodeId, topicId, topVer. */
    private final Map<T3<UUID, Integer, AffinityTopologyVersion>, SupplyContext> scMap = new HashMap<>();

    /**
     * Override for rebalance throttle.
     * @deprecated Use {@link IgniteConfiguration#getRebalanceThrottle()} instead.
     */
    @Deprecated
    private long rebalanceThrottleOverride =
        IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_REBALANCE_THROTTLE_OVERRIDE, 0);

    /**
     * @param grp Cache group.
     */
    GridDhtPartitionSupplier(CacheGroupContext grp) {
        assert grp != null;

        this.grp = grp;

        log = grp.shared().logger(getClass());

        top = grp.topology();

        if (rebalanceThrottleOverride > 0)
            LT.info(log, "Using rebalance throttle override: " + rebalanceThrottleOverride);
    }

    /**
     * Clears all supply contexts in case of node stopping.
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
     * Clears supply context.
     *
     * @param sc Supply context.
     * @param log Logger.
     */
    private static void clearContext(SupplyContext sc, IgniteLogger log) {
        if (sc != null) {
            final IgniteRebalanceIterator it = sc.iterator;

            if (it != null && !it.isClosed()) {
                try {
                    it.close();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Iterator close failed.", e);
                }
            }
        }
    }

    /**
     * Handle topology change and clear supply context map of outdated contexts.
     */
    void onTopologyChanged() {
        synchronized (scMap) {
            Iterator<T3<UUID, Integer, AffinityTopologyVersion>> it = scMap.keySet().iterator();

            Collection<UUID> aliveNodes = grp.shared().discovery().aliveServerNodes().stream()
                .map(ClusterNode::id)
                .collect(Collectors.toList());

            while (it.hasNext()) {
                T3<UUID, Integer, AffinityTopologyVersion> t = it.next();

                if (!aliveNodes.contains(t.get1())) { // Clear all obsolete contexts.
                    clearContext(scMap.get(t), log);

                    it.remove();

                    if (log.isDebugEnabled())
                        log.debug("Supply context removed [grp=" + grp.cacheOrGroupName() + ", demander=" + t.get1() + "]");
                }
            }
        }
    }

    /**
     * For each demand message method lookups (or creates new) supply context and starts to iterate entries across requested partitions.
     * Each entry in iterator is placed to prepared supply message.
     *
     * If supply message size in bytes becomes greater than {@link IgniteConfiguration#getRebalanceBatchSize()}
     * method sends this message to demand node and saves partial state of iterated entries to supply context,
     * then restores the context again after new demand message with the same context id is arrived.
     *
     * @param topicId Id of the topic is used for the supply-demand communication.
     * @param nodeId Id of the node which sent the demand message.
     * @param demandMsg Demand message.
     */
    public void handleDemandMessage(int topicId, UUID nodeId, GridDhtPartitionDemandMessage demandMsg) {
        assert demandMsg != null;
        assert nodeId != null;

        T3<UUID, Integer, AffinityTopologyVersion> contextId = new T3<>(nodeId, topicId, demandMsg.topologyVersion());

        if (demandMsg.rebalanceId() < 0) { // Demand node requested context cleanup.
            synchronized (scMap) {
                SupplyContext sctx = scMap.get(contextId);

                if (sctx != null && sctx.rebalanceId == -demandMsg.rebalanceId()) {
                    clearContext(scMap.remove(contextId), log);

                    if (log.isDebugEnabled())
                        log.debug("Supply context cleaned [" + supplyRoutineInfo(topicId, nodeId, demandMsg)
                            + ", supplyContext=" + sctx + "]");
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Stale supply context cleanup message [" + supplyRoutineInfo(topicId, nodeId, demandMsg)
                            + ", supplyContext=" + sctx + "]");
                }

                return;
            }
        }

        ClusterNode demanderNode = grp.shared().discovery().node(nodeId);

        if (demanderNode == null) {
            if (log.isDebugEnabled())
                log.debug("Demand message rejected (demander left cluster) ["
                    + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]");

            return;
        }

        IgniteRebalanceIterator iter = null;

        SupplyContext sctx = null;

        try {
            synchronized (scMap) {
                sctx = scMap.remove(contextId);

                if (sctx != null && demandMsg.rebalanceId() < sctx.rebalanceId) {
                    // Stale message, return context back and return.
                    scMap.put(contextId, sctx);

                    if (log.isDebugEnabled())
                        log.debug("Stale demand message [" + supplyRoutineInfo(topicId, nodeId, demandMsg) +
                            ", actualContext=" + sctx + "]");

                    return;
                }
            }

            // Demand request should not contain empty partitions if no supply context is associated with it.
            if (sctx == null && (demandMsg.partitions() == null || demandMsg.partitions().isEmpty())) {
                if (log.isDebugEnabled())
                    log.debug("Empty demand message (no context and partitions) ["
                        + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]");

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Demand message accepted ["
                    + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]");

            assert !(sctx != null && !demandMsg.partitions().isEmpty());

            long maxBatchesCnt = /* Each thread should gain prefetched batches. */
                grp.preloader().batchesPrefetchCount() * grp.shared().gridConfig().getRebalanceThreadPoolSize();

            if (sctx == null) {
                if (log.isDebugEnabled())
                    log.debug("Starting supplying rebalancing [" + supplyRoutineInfo(topicId, nodeId, demandMsg) +
                        ", fullPartitions=" + S.compact(demandMsg.partitions().fullSet()) +
                        ", histPartitions=" + S.compact(demandMsg.partitions().historicalSet()) + "]");
            }
            else
                maxBatchesCnt = 1;

            GridDhtPartitionSupplyMessage supplyMsg = new GridDhtPartitionSupplyMessage(
                demandMsg.rebalanceId(),
                grp.groupId(),
                demandMsg.topologyVersion(),
                grp.deploymentEnabled()
            );

            Set<Integer> remainingParts;

            if (sctx == null || sctx.iterator == null) {
                iter = grp.offheap().rebalanceIterator(demandMsg.partitions(), demandMsg.topologyVersion());

                remainingParts = new HashSet<>(demandMsg.partitions().fullSet());

                CachePartitionPartialCountersMap histMap = demandMsg.partitions().historicalMap();

                for (int i = 0; i < histMap.size(); i++) {
                    int p = histMap.partitionAt(i);

                    remainingParts.add(p);
                }

                for (Integer part : demandMsg.partitions().fullSet()) {
                    if (iter.isPartitionMissing(part))
                        continue;

                    GridDhtLocalPartition loc = top.localPartition(part, demandMsg.topologyVersion(), false);

                    assert loc != null && loc.state() == GridDhtPartitionState.OWNING
                        : "Partition should be in OWNING state: " + loc;

                    supplyMsg.addEstimatedKeysCount(grp.offheap().totalPartitionEntriesCount(part));
                }

                for (int i = 0; i < histMap.size(); i++) {
                    int p = histMap.partitionAt(i);

                    if (iter.isPartitionMissing(p))
                        continue;

                    supplyMsg.addEstimatedKeysCount(histMap.updateCounterAt(i) - histMap.initialUpdateCounterAt(i));
                }
            }
            else {
                iter = sctx.iterator;

                remainingParts = sctx.remainingParts;
            }

            final int msgMaxSize = grp.preloader().batchSize();

            long batchesCnt = 0;

            CacheDataRow prevRow = null;

            while (iter.hasNext()) {
                CacheDataRow row = iter.peek();

                // Prevent mvcc entry history splitting into separate batches.
                boolean canFlushHistory = !grp.mvccEnabled() ||
                    prevRow != null && ((grp.sharedGroup() && row.cacheId() != prevRow.cacheId()) ||
                        !row.key().equals(prevRow.key()));

                if (canFlushHistory && supplyMsg.messageSize() >= msgMaxSize) {
                    if (++batchesCnt >= maxBatchesCnt) {
                        saveSupplyContext(contextId,
                            iter,
                            remainingParts,
                            demandMsg.rebalanceId()
                        );

                        reply(topicId, demanderNode, demandMsg, supplyMsg, contextId);

                        return;
                    }
                    else {
                        if (!reply(topicId, demanderNode, demandMsg, supplyMsg, contextId))
                            return;

                        supplyMsg = new GridDhtPartitionSupplyMessage(demandMsg.rebalanceId(),
                            grp.groupId(),
                            demandMsg.topologyVersion(),
                            grp.deploymentEnabled());
                    }
                }

                row = iter.next();

                prevRow = row;

                int part = row.partition();

                GridDhtLocalPartition loc = top.localPartition(part, demandMsg.topologyVersion(), false);

                assert (loc != null && loc.state() == OWNING && loc.reservations() > 0) || iter.isPartitionMissing(part)
                    : "Partition should be in OWNING state and has at least 1 reservation " + loc;

                if (iter.isPartitionMissing(part) && remainingParts.contains(part)) {
                    supplyMsg.missed(part);

                    remainingParts.remove(part);

                    if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_MISSED))
                        grp.addRebalanceMissEvent(part);

                    if (log.isDebugEnabled())
                        log.debug("Requested partition is marked as missing ["
                            + supplyRoutineInfo(topicId, nodeId, demandMsg) + ", p=" + part + "]");

                    continue;
                }

                if (!remainingParts.contains(part))
                    continue;

                GridCacheEntryInfo info = extractEntryInfo(row);

                if (info == null)
                    continue;

                supplyMsg.addEntry0(part, iter.historical(part), info, grp.shared(), grp.cacheObjectContext());

                if (iter.isPartitionDone(part)) {
                    supplyMsg.last(part, loc.updateCounter());

                    remainingParts.remove(part);

                    if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_SUPPLIED))
                        grp.addRebalanceSupplyEvent(part);
                }
            }

            Iterator<Integer> remainingIter = remainingParts.iterator();

            while (remainingIter.hasNext()) {
                int p = remainingIter.next();

                if (iter.isPartitionDone(p)) {
                    GridDhtLocalPartition loc = top.localPartition(p, demandMsg.topologyVersion(), false);

                    assert loc != null
                        : "Supply partition is gone: grp=" + grp.cacheOrGroupName() + ", p=" + p;

                    supplyMsg.last(p, loc.updateCounter());

                    remainingIter.remove();

                    if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_SUPPLIED))
                        grp.addRebalanceSupplyEvent(p);
                }
                else if (iter.isPartitionMissing(p)) {
                    supplyMsg.missed(p);

                    remainingIter.remove();

                    if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_MISSED))
                        grp.addRebalanceMissEvent(p);
                }
            }

            assert remainingParts.isEmpty()
                : "Partitions after rebalance should be either done or missing: " + remainingParts;

            if (sctx != null)
                clearContext(sctx, log);
            else
                iter.close();

            reply(topicId, demanderNode, demandMsg, supplyMsg, contextId);

            if (log.isInfoEnabled())
                log.info("Finished supplying rebalancing [" + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]");
        }
        catch (Throwable t) {
            if (iter != null && !iter.isClosed()) {
                try {
                    iter.close();
                }
                catch (IgniteCheckedException e) {
                    t.addSuppressed(e);
                }
            }

            if (grp.shared().kernalContext().isStopping())
                return;

            // Sending supply messages with error requires new protocol.
            boolean sendErrMsg = demanderNode.version().compareTo(GridDhtPartitionSupplyMessageV2.AVAILABLE_SINCE) >= 0;

            if (t instanceof IgniteSpiException) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send message to node (current node is stopping?) ["
                        + supplyRoutineInfo(topicId, nodeId, demandMsg) + ", msg=" + t.getMessage() + ']');

                sendErrMsg = false;
            }
            else
                U.error(log, "Failed to continue supplying ["
                    + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]", t);

            try {
                if (sctx != null)
                    clearContext(sctx, log);
                else if (iter != null)
                    iter.close();
            }
            catch (Throwable t1) {
                U.error(log, "Failed to cleanup supplying context ["
                    + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]", t1);
            }

            if (!sendErrMsg)
                return;

            try {
                GridDhtPartitionSupplyMessageV2 errMsg = new GridDhtPartitionSupplyMessageV2(
                    demandMsg.rebalanceId(),
                    grp.groupId(),
                    demandMsg.topologyVersion(),
                    grp.deploymentEnabled(),
                    t
                );

                reply(topicId, demanderNode, demandMsg, errMsg, contextId);
            }
            catch (Throwable t1) {
                U.error(log, "Failed to send supply error message ["
                    + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]", t1);
            }

            grp.shared().kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR,
                new IgniteCheckedException("Failed to continue supplying ["
                    + supplyRoutineInfo(topicId, nodeId, demandMsg) + "]", t)
            ));
        }
    }

    /**
     * Extracts entry info from row.
     *
     * @param row Cache data row.
     * @return Entry info.
     */
    private GridCacheEntryInfo extractEntryInfo(CacheDataRow row) {
        GridCacheEntryInfo info = grp.mvccEnabled() ?
            new GridCacheMvccEntryInfo() : new GridCacheEntryInfo();

        info.key(row.key());
        info.cacheId(row.cacheId());

        if (grp.mvccEnabled()) {
            assert row.mvccCoordinatorVersion() != MvccUtils.MVCC_CRD_COUNTER_NA;

            // Rows from rebalance iterator have actual states already.
            if (row.mvccTxState() != TxState.COMMITTED)
                return null;

            ((MvccVersionAware)info).mvccVersion(row);
            ((GridCacheMvccEntryInfo)info).mvccTxState(TxState.COMMITTED);

            if (row.newMvccCoordinatorVersion() != MvccUtils.MVCC_CRD_COUNTER_NA &&
                row.newMvccTxState() == TxState.COMMITTED) {
                ((MvccUpdateVersionAware)info).newMvccVersion(row);
                ((GridCacheMvccEntryInfo)info).newMvccTxState(TxState.COMMITTED);
            }
        }

        info.value(row.value());
        info.version(row.version());
        info.expireTime(row.expireTime());

        return info;
    }

    /**
     * Sends supply message to demand node.
     *
     * @param demander Recipient of supply message.
     * @param demandMsg Demand message.
     * @param supplyMsg Supply message.
     * @param contextId Supply context id.
     * @return {@code True} if message was sent, {@code false} if recipient left grid.
     * @throws IgniteCheckedException If failed.
     */
    private boolean reply(
        int topicId,
        ClusterNode demander,
        GridDhtPartitionDemandMessage demandMsg,
        GridDhtPartitionSupplyMessage supplyMsg,
        T3<UUID, Integer, AffinityTopologyVersion> contextId
    ) throws IgniteCheckedException {
        try {
            if (log.isDebugEnabled())
                log.debug("Send next supply message [" + supplyRoutineInfo(topicId, demander.id(), demandMsg) + "]");

            grp.shared().io().sendOrderedMessage(demander, demandMsg.topic(), supplyMsg, grp.ioPolicy(), demandMsg.timeout());

            // Throttle preloading.
            if (rebalanceThrottleOverride > 0)
                U.sleep(rebalanceThrottleOverride);
            else if (grp.preloader().throttle() > 0)
                U.sleep(grp.preloader().throttle());

            return true;
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to send supply message (demander left): [" + supplyRoutineInfo(topicId, demander.id(), demandMsg) + "]");

            synchronized (scMap) {
                clearContext(scMap.remove(contextId), log);
            }

            return false;
        }
    }

    /**
     * String representation of supply routine.
     *
     * @param topicId Topic id.
     * @param demander Demander.
     * @param demandMsg Demand message.
     */
    private String supplyRoutineInfo(int topicId, UUID demander, GridDhtPartitionDemandMessage demandMsg) {
        return "grp=" + grp.cacheOrGroupName() + ", demander=" + demander + ", topVer=" + demandMsg.topologyVersion() + ", topic=" + topicId;
    }

    /**
     * Saves supply context with given parameters to {@code scMap}.
     *
     * @param contextId Supply context id.
     * @param entryIt Entries rebalance iterator.
     * @param remainingParts Set of partitions that weren't sent yet.
     * @param rebalanceId Rebalance id.
     */
    private void saveSupplyContext(
        T3<UUID, Integer, AffinityTopologyVersion> contextId,
        IgniteRebalanceIterator entryIt,
        Set<Integer> remainingParts,
        long rebalanceId
    ) {
        synchronized (scMap) {
            assert scMap.get(contextId) == null;

            scMap.put(contextId, new SupplyContext(entryIt, remainingParts, rebalanceId));
        }
    }

    /**
     * Supply context.
     */
    private static class SupplyContext {
        /** Entries iterator. */
        @GridToStringExclude
        private final IgniteRebalanceIterator iterator;

        /** Set of partitions which weren't sent yet. */
        private final Set<Integer> remainingParts;

        /** Rebalance id. */
        private final long rebalanceId;

        /**
         * Constructor.
         *
         * @param iterator Entries rebalance iterator.
         * @param remainingParts Set of partitions which weren't sent yet.
         * @param rebalanceId Rebalance id.
         */
        SupplyContext(IgniteRebalanceIterator iterator, Set<Integer> remainingParts, long rebalanceId) {
            this.iterator = iterator;
            this.remainingParts = remainingParts;
            this.rebalanceId = rebalanceId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SupplyContext.class, this);
        }
    }
}
