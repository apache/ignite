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

package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Affinity cached function.
 */
public class GridAffinityAssignmentCache {
    /** Cleanup history size. */
    private final int MAX_HIST_SIZE = getInteger(IGNITE_AFFINITY_HISTORY_SIZE, 500);

    /** Cache name. */
    private final String cacheName;

    /** */
    private final int cacheId;

    /** Number of backups. */
    private final int backups;

    /** Affinity function. */
    private final AffinityFunction aff;

    /** */
    private final IgnitePredicate<ClusterNode> nodeFilter;

    /** Partitions count. */
    private final int partsCnt;

    /** Affinity calculation results cache: topology version => partition => nodes. */
    private final ConcurrentNavigableMap<AffinityTopologyVersion, HistoryAffinityAssignment> affCache;

    /** */
    private List<List<ClusterNode>> idealAssignment;

    /** Cache item corresponding to the head topology version. */
    private final AtomicReference<GridAffinityAssignment> head;

    /** Ready futures. */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts = new ConcurrentHashMap8<>();

    /** Log. */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final boolean locCache;

    /** Node stop flag. */
    private volatile IgniteCheckedException stopErr;

    /** History size ignoring client events changes. */
    private final AtomicInteger histSize = new AtomicInteger();

    /** Full history size. */
    private final AtomicInteger fullHistSize = new AtomicInteger();

    /** */
    private final Object similarAffKey;

    /**
     * Constructs affinity cached calculations.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param aff Affinity function.
     * @param nodeFilter Node filter.
     * @param backups Number of backups.
     * @param locCache Local cache flag.
     */
    @SuppressWarnings("unchecked")
    public GridAffinityAssignmentCache(GridKernalContext ctx,
        String cacheName,
        AffinityFunction aff,
        IgnitePredicate<ClusterNode> nodeFilter,
        int backups,
        boolean locCache)
    {
        assert ctx != null;
        assert aff != null;
        assert nodeFilter != null;

        this.ctx = ctx;
        this.aff = aff;
        this.nodeFilter = nodeFilter;
        this.cacheName = cacheName;
        this.backups = backups;
        this.locCache = locCache;

        cacheId = CU.cacheId(cacheName);

        log = ctx.log(GridAffinityAssignmentCache.class);

        partsCnt = aff.partitions();
        affCache = new ConcurrentSkipListMap<>();
        head = new AtomicReference<>(new GridAffinityAssignment(AffinityTopologyVersion.NONE));

        similarAffKey = ctx.affinity().similaryAffinityKey(aff, nodeFilter, backups, partsCnt);

        assert similarAffKey != null;
    }

    /**
     * @return Key to find caches with similar affinity.
     */
    public Object similarAffinityKey() {
        return similarAffKey;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * Initializes affinity with given topology version and assignment.
     *
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment for topology version.
     */
    public void initialize(AffinityTopologyVersion topVer, List<List<ClusterNode>> affAssignment) {
        assert topVer.compareTo(lastVersion()) >= 0 : "[topVer = " + topVer + ", last=" + lastVersion() + ']';
        assert idealAssignment != null;

        GridAffinityAssignment assignment = new GridAffinityAssignment(topVer, affAssignment, idealAssignment);

        affCache.put(topVer, new HistoryAffinityAssignment(assignment));
        head.set(assignment);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (initialized affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        onHistoryAdded(assignment);
    }

    /**
     * @param assignment Assignment.
     */
    public void idealAssignment(List<List<ClusterNode>> assignment) {
        this.idealAssignment = assignment;
    }

    /**
     * @return Assignment.
     */
    @Nullable public List<List<ClusterNode>> idealAssignment() {
        return idealAssignment;
    }

    /**
     * @return {@code True} if affinity function has {@link AffinityCentralizedFunction} annotation.
     */
    public boolean centralizedAffinityFunction() {
        return U.hasAnnotation(aff, AffinityCentralizedFunction.class);
    }

    /**
     * Kernal stop callback.
     *
     * @param err Error.
     */
    public void cancelFutures(IgniteCheckedException err) {
        stopErr = err;

        for (AffinityReadyFuture fut : readyFuts.values())
            fut.onDone(err);
    }

    /**
     *
     */
    public void onReconnected() {
        idealAssignment = null;

        affCache.clear();

        head.set(new GridAffinityAssignment(AffinityTopologyVersion.NONE));

        stopErr = null;
    }

    /**
     * Calculates affinity cache for given topology version.
     *
     * @param topVer Topology version to calculate affinity cache for.
     * @param discoEvt Discovery event that caused this topology version change.
     * @return Affinity assignments.
     */
    @SuppressWarnings("IfMayBeConditional")
    public List<List<ClusterNode>> calculate(AffinityTopologyVersion topVer, DiscoveryEvent discoEvt) {
        if (log.isDebugEnabled())
            log.debug("Calculating affinity [topVer=" + topVer + ", locNodeId=" + ctx.localNodeId() +
                ", discoEvt=" + discoEvt + ']');

        List<List<ClusterNode>> prevAssignment = idealAssignment;

        // Resolve nodes snapshot for specified topology version.
        List<ClusterNode> sorted;

        if (!locCache) {
            sorted = new ArrayList<>(ctx.discovery().cacheAffinityNodes(cacheId(), topVer));

            Collections.sort(sorted, GridNodeOrderComparator.INSTANCE);
        }
        else
            sorted = Collections.singletonList(ctx.discovery().localNode());

        List<List<ClusterNode>> assignment;

        if (prevAssignment != null && discoEvt != null) {
            boolean affNode = CU.affinityNode(discoEvt.eventNode(), nodeFilter);

            if (!affNode)
                assignment = prevAssignment;
            else
                assignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted, prevAssignment,
                    discoEvt, topVer, backups));
        }
        else
            assignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted, prevAssignment, discoEvt,
                topVer, backups));

        assert assignment != null;

        idealAssignment = assignment;

        if (locCache)
            initialize(topVer, assignment);

        return assignment;
    }

    /**
     * Copies previous affinity assignment when discovery event does not cause affinity assignment changes
     * (e.g. client node joins on leaves).
     *
     * @param evt Event.
     * @param topVer Topology version.
     */
    public void clientEventTopologyChange(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        assert topVer.compareTo(lastVersion()) >= 0 : "[topVer = " + topVer + ", last=" + lastVersion() + ']';

        GridAffinityAssignment aff = head.get();

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT  || aff.primaryPartitions(evt.eventNode().id()).isEmpty() : evt;
        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT  || aff.backupPartitions(evt.eventNode().id()).isEmpty() : evt;

        GridAffinityAssignment assignmentCpy = new GridAffinityAssignment(topVer, aff);

        affCache.put(topVer, new HistoryAffinityAssignment(assignmentCpy));
        head.set(assignmentCpy);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (use previous affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        onHistoryAdded(assignmentCpy);
    }

    /**
     * @return Last calculated affinity version.
     */
    public AffinityTopologyVersion lastVersion() {
        return head.get().topologyVersion();
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> assignments(AffinityTopologyVersion topVer) {
        AffinityAssignment aff = cachedAffinity(topVer);

        return aff.assignment();
    }

    /**
     * Gets future that will be completed after topology with version {@code topVer} is calculated.
     *
     * @param topVer Topology version to await for.
     * @return Future that will be completed after affinity for topology version {@code topVer} is calculated.
     */
    @Nullable public IgniteInternalFuture<AffinityTopologyVersion> readyFuture(AffinityTopologyVersion topVer) {
        GridAffinityAssignment aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Returning finished future for readyFuture [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            return null;
        }

        GridFutureAdapter<AffinityTopologyVersion> fut = F.addIfAbsent(readyFuts, topVer,
            new AffinityReadyFuture(topVer));

        aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Completing topology ready future right away [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            fut.onDone(topVer);
        }
        else if (stopErr != null)
            fut.onDone(stopErr);

        return fut;
    }

    /**
     * @return Partition count.
     */
    public int partitions() {
        return partsCnt;
    }

    /**
     * Gets affinity nodes for specified partition.
     *
     * @param part Partition.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(int part, AffinityTopologyVersion topVer) {
        // Resolve cached affinity nodes.
        return cachedAffinity(topVer).get(part);
    }

    /**
     * Get primary partitions for specified node ID.
     *
     * @param nodeId Node ID to get primary partitions for.
     * @param topVer Topology version.
     * @return Primary partitions for specified node ID.
     */
    public Set<Integer> primaryPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).primaryPartitions(nodeId);
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @param topVer Topology version.
     * @return Backup partitions for specified node ID.
     */
    public Set<Integer> backupPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).backupPartitions(nodeId);
    }

    /**
     * Dumps debug information.
     */
    public void dumpDebugInfo() {
        if (!readyFuts.isEmpty()) {
            U.warn(log, "Pending affinity ready futures [cache=" + cacheName + ", lastVer=" + lastVersion() + "]:");

            for (AffinityReadyFuture fut : readyFuts.values())
                U.warn(log, ">>> " + fut);
        }
    }

    /**
     * Get cached affinity for specified topology version.
     *
     * @param topVer Topology version.
     * @return Cached affinity.
     */
    public AffinityAssignment cachedAffinity(AffinityTopologyVersion topVer) {
        if (topVer.equals(AffinityTopologyVersion.NONE))
            topVer = lastVersion();
        else
            awaitTopologyVersion(topVer);

        assert topVer.topologyVersion() >= 0 : topVer;

        AffinityAssignment cache = head.get();

        if (!cache.topologyVersion().equals(topVer)) {
            cache = affCache.get(topVer);

            if (cache == null) {
                throw new IllegalStateException("Getting affinity for topology version earlier than affinity is " +
                    "calculated [locNode=" + ctx.discovery().localNode() +
                    ", cache=" + cacheName +
                    ", topVer=" + topVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ']');
            }
        }

        assert cache.topologyVersion().equals(topVer) : "Invalid cached affinity: " + cache;

        return cache;
    }

    /**
     * @param part Partition.
     * @param startVer Start version.
     * @param endVer End version.
     * @return {@code True} if primary changed or required affinity version not found in history.
     */
    public boolean primaryChanged(int part, AffinityTopologyVersion startVer, AffinityTopologyVersion endVer) {
        AffinityAssignment aff = affCache.get(startVer);

        if (aff == null)
            return false;

        List<ClusterNode> nodes = aff.get(part);

        if (nodes.isEmpty())
            return true;

        ClusterNode primary = nodes.get(0);

        for (AffinityAssignment assignment : affCache.tailMap(startVer, false).values()) {
            List<ClusterNode> nodes0 = assignment.assignment().get(part);

            if (nodes0.isEmpty())
                return true;

            if (!nodes0.get(0).equals(primary))
                return true;

            if (assignment.topologyVersion().equals(endVer))
                return false;
        }

        return true;
    }

    /**
     * @param aff Affinity cache.
     */
    public void init(GridAffinityAssignmentCache aff) {
        assert aff.lastVersion().compareTo(lastVersion()) >= 0;
        assert aff.idealAssignment() != null;

        idealAssignment(aff.idealAssignment());

        initialize(aff.lastVersion(), aff.assignments(aff.lastVersion()));
    }

    /**
     * @param topVer Topology version to wait.
     */
    private void awaitTopologyVersion(AffinityTopologyVersion topVer) {
        GridAffinityAssignment aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0)
            return;

        try {
            if (log.isDebugEnabled())
                log.debug("Will wait for topology version [locNodeId=" + ctx.localNodeId() +
                ", topVer=" + topVer + ']');

            IgniteInternalFuture<AffinityTopologyVersion> fut = readyFuture(topVer);

            if (fut != null)
                fut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for affinity ready future for topology version: " + topVer,
                e);
        }
    }

    /**
     * @param aff Added affinity assignment.
     */
    private void onHistoryAdded(GridAffinityAssignment aff) {
        int fullSize = fullHistSize.incrementAndGet();

        int size;

        if (aff.clientEventChange())
            size = histSize.get();
        else
            size = histSize.incrementAndGet();

        int rmvCnt = size - MAX_HIST_SIZE;

        if (rmvCnt <= 0) {
            if (fullSize > MAX_HIST_SIZE * 2)
                rmvCnt = MAX_HIST_SIZE;
        }

        if (rmvCnt > 0) {
            Iterator<HistoryAffinityAssignment> it = affCache.values().iterator();

            while (it.hasNext() && rmvCnt > 0) {
                AffinityAssignment aff0 = it.next();

                it.remove();

                rmvCnt--;

                if (!aff0.clientEventChange())
                    histSize.decrementAndGet();

                fullHistSize.decrementAndGet();
            }
        }
    }


    /**
     * Affinity ready future. Will remove itself from ready futures map.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<AffinityTopologyVersion> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private AffinityTopologyVersion reqTopVer;

        /**
         *
         * @param reqTopVer Required topology version.
         */
        private AffinityReadyFuture(AffinityTopologyVersion reqTopVer) {
            this.reqTopVer = reqTopVer;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(AffinityTopologyVersion res, @Nullable Throwable err) {
            assert res != null || err != null;

            boolean done = super.onDone(res, err);

            if (done)
                readyFuts.remove(reqTopVer, this);

            return done;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityReadyFuture.class, this);
        }
    }
}
