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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.getFloat;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Affinity cached function.
 */
public class GridAffinityAssignmentCache {
    /** Cleanup history size. */
    private final int MAX_HIST_SIZE = getInteger(IGNITE_AFFINITY_HISTORY_SIZE, 500);

    /** Partition distribution. */
    private final float partDistribution = getFloat(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, 50f);

    /** Group name if specified or cache name. */
    private final String cacheOrGrpName;

    /** Group ID. */
    private final int grpId;

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

    /** */
    private BaselineTopology baselineTopology;

    /** */
    private List<List<ClusterNode>> baselineAssignment;

    /** Cache item corresponding to the head topology version. */
    private final AtomicReference<GridAffinityAssignment> head;

    /** Ready futures. */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts = new ConcurrentSkipListMap<>();

    /** Log. */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final boolean locCache;

    /** */
    private final boolean persistentCache;

    /** Node stop flag. */
    private volatile IgniteCheckedException stopErr;

    /** Full history size. */
    private final AtomicInteger fullHistSize = new AtomicInteger();

    /** */
    private final Object similarAffKey;

    /**
     * Constructs affinity cached calculations.
     *
     * @param ctx Kernal context.
     * @param cacheOrGrpName Cache or cache group name.
     * @param grpId Group ID.
     * @param aff Affinity function.
     * @param nodeFilter Node filter.
     * @param backups Number of backups.
     * @param locCache Local cache flag.
     */
    @SuppressWarnings("unchecked")
    public GridAffinityAssignmentCache(GridKernalContext ctx,
        String cacheOrGrpName,
        int grpId,
        AffinityFunction aff,
        IgnitePredicate<ClusterNode> nodeFilter,
        int backups,
        boolean locCache,
        boolean persistentCache)
    {
        assert ctx != null;
        assert aff != null;
        assert nodeFilter != null;
        assert grpId != 0;

        this.ctx = ctx;
        this.aff = aff;
        this.nodeFilter = nodeFilter;
        this.cacheOrGrpName = cacheOrGrpName;
        this.grpId = grpId;
        this.backups = backups;
        this.locCache = locCache;
        this.persistentCache = persistentCache;

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
     * @return Group name if it is specified, otherwise cache name.
     */
    public String cacheOrGroupName() {
        return cacheOrGrpName;
    }

    /**
     * @return Cache group ID.
     */
    public int groupId() {
        return grpId;
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

        HistoryAffinityAssignment hAff = affCache.put(topVer, new HistoryAffinityAssignment(assignment));

        head.set(assignment);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (initialized affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        // In case if value was replaced there is no sense to clean the history.
        if (hAff == null)
            onHistoryAdded();

        if (log.isTraceEnabled()) {
            log.trace("New affinity assignment [grp=" + cacheOrGrpName
                + ", topVer=" + topVer
                + ", aff=" + fold(affAssignment) + "]");
        }
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

        fullHistSize.set(0);

        head.set(new GridAffinityAssignment(AffinityTopologyVersion.NONE));

        stopErr = null;
    }

    /**
     * Calculates affinity cache for given topology version.
     *
     * @param topVer Topology version to calculate affinity cache for.
     * @param events Discovery events that caused this topology version change.
     * @param discoCache Discovery cache.
     * @return Affinity assignments.
     */
    @SuppressWarnings("IfMayBeConditional")
    public List<List<ClusterNode>> calculate(
        AffinityTopologyVersion topVer,
        @Nullable ExchangeDiscoveryEvents events,
        @Nullable DiscoCache discoCache
    ) {
        if (log.isDebugEnabled())
            log.debug("Calculating affinity [topVer=" + topVer + ", locNodeId=" + ctx.localNodeId() +
                ", discoEvts=" + events + ']');

        List<List<ClusterNode>> prevAssignment = idealAssignment;

        // Resolve nodes snapshot for specified topology version.
        List<ClusterNode> sorted;

        if (!locCache) {
            sorted = new ArrayList<>(discoCache.cacheGroupAffinityNodes(groupId()));

            Collections.sort(sorted, NodeOrderComparator.getInstance());
        }
        else
            sorted = Collections.singletonList(ctx.discovery().localNode());

        boolean hasBaseline = false;
        boolean changedBaseline = false;

        if (discoCache != null) {
            hasBaseline = discoCache.state().baselineTopology() != null && persistentCache;

            changedBaseline = !hasBaseline ? baselineTopology != null :
                !discoCache.state().baselineTopology().equals(baselineTopology);
        }

        List<List<ClusterNode>> assignment;

        if (prevAssignment != null && events != null) {
            /* Skip affinity calculation only when all nodes triggered exchange
               don't belong to affinity for current group (client node or filtered by nodeFilter). */
            boolean skipCalculation = true;

            for (DiscoveryEvent event : events.events()) {
                boolean affinityNode = CU.affinityNode(event.eventNode(), nodeFilter);

                if (affinityNode || event.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                    skipCalculation = false;

                    break;
                }
            }

            if (skipCalculation)
                assignment = prevAssignment;
            else if (hasBaseline && !changedBaseline) {
                if (baselineAssignment == null)
                    baselineAssignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(
                        discoCache.state().baselineTopology().createBaselineView(sorted, nodeFilter),
                        prevAssignment, events.lastEvent(), topVer, backups));

                assignment = currentBaselineAssignment(topVer);
            }
            else if (hasBaseline && changedBaseline) {
                baselineAssignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(
                    discoCache.state().baselineTopology().createBaselineView(sorted, nodeFilter),
                    prevAssignment, events.lastEvent(), topVer, backups));

                assignment = currentBaselineAssignment(topVer);
            }
            else {
                assignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted, prevAssignment,
                    events.lastEvent(), topVer, backups));
            }
        }
        else {
            DiscoveryEvent event = null;

            if (events != null)
                event = events.lastEvent();

            if (hasBaseline) {
                baselineAssignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(
                    discoCache.state().baselineTopology().createBaselineView(sorted, nodeFilter),
                    prevAssignment, event, topVer, backups));

                assignment = currentBaselineAssignment(topVer);
            }
            else {
                assignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted, prevAssignment,
                    event, topVer, backups));
            }
        }

        assert assignment != null;

        idealAssignment = assignment;

        if (ctx.cache().cacheMode(cacheOrGrpName) == PARTITIONED && !ctx.clientNode())
            printDistributionIfThresholdExceeded(assignment, sorted.size());

        if (hasBaseline) {
            baselineTopology = discoCache.state().baselineTopology();
            assert baselineAssignment != null;
        }
        else {
            baselineTopology = null;
            baselineAssignment = null;
        }

        if (locCache)
            initialize(topVer, assignment);

        return assignment;
    }

    /**
     * @param topVer Topology version.
     * @return Baseline assignment with filtered out offline nodes.
     */
    private List<List<ClusterNode>> currentBaselineAssignment(AffinityTopologyVersion topVer) {
        Map<Object, ClusterNode> alives = new HashMap<>();

        for (ClusterNode node : ctx.discovery().nodes(topVer)) {
            if (!node.isClient() && !node.isDaemon())
                alives.put(node.consistentId(), node);
        }

        List<List<ClusterNode>> result = new ArrayList<>(baselineAssignment.size());

        for (int p = 0; p < baselineAssignment.size(); p++) {
            List<ClusterNode> baselineMapping = baselineAssignment.get(p);
            List<ClusterNode> currentMapping = null;

            for (ClusterNode node : baselineMapping) {
                ClusterNode aliveNode = alives.get(node.consistentId());

                if (aliveNode != null) {
                    if (currentMapping == null)
                        currentMapping = new ArrayList<>();

                    currentMapping.add(aliveNode);
                }
            }

            result.add(p, currentMapping != null ? currentMapping : Collections.<ClusterNode>emptyList());
        }

        return result;
    }

    /**
     * Calculates and logs partitions distribution if threshold of uneven distribution {@link #partDistribution} is exceeded.
     *
     * @param assignments Assignments to calculate partitions distribution.
     * @param nodes Affinity nodes number.
     * @see IgniteSystemProperties#IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD
     */
    private void printDistributionIfThresholdExceeded(List<List<ClusterNode>> assignments, int nodes) {
        int locPrimaryCnt = 0;
        int locBackupCnt = 0;

        for (List<ClusterNode> assignment : assignments) {
            for (int i = 0; i < assignment.size(); i++) {
                ClusterNode node = assignment.get(i);

                if (node.isLocal()) {
                    if (i == 0)
                        locPrimaryCnt++;
                    else
                        locBackupCnt++;
                }
            }
        }

        float expCnt = (float)partsCnt / nodes;

        float deltaPrimary = Math.abs(1 - (float)locPrimaryCnt / expCnt) * 100;
        float deltaBackup = Math.abs(1 - (float)locBackupCnt / (expCnt * backups)) * 100;

        if (deltaPrimary > partDistribution || deltaBackup > partDistribution) {
            log.info(String.format("Local node affinity assignment distribution is not ideal " +
                    "[cache=%s, expectedPrimary=%.2f, actualPrimary=%d, " +
                    "expectedBackups=%.2f, actualBackups=%d, warningThreshold=%.2f%%]",
                cacheOrGrpName, expCnt, locPrimaryCnt,
                expCnt * backups, locBackupCnt, partDistribution));
        }
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

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT || aff.primaryPartitions(evt.eventNode().id()).isEmpty() : evt;

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT || aff.backupPartitions(evt.eventNode().id()).isEmpty() : evt;

        GridAffinityAssignment assignmentCpy = new GridAffinityAssignment(topVer, aff);

        HistoryAffinityAssignment hAff = affCache.put(topVer, new HistoryAffinityAssignment(assignmentCpy));

        head.set(assignmentCpy);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (use previous affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        // In case if value was replaced there is no sense to clean the history.
        if (hAff == null)
            onHistoryAdded();
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
     * @param topVer Topology version.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> readyAssignments(AffinityTopologyVersion topVer) {
        AffinityAssignment aff = readyAffinity(topVer);

        assert aff != null : "No ready affinity [grp=" + cacheOrGrpName + ", ver=" + topVer + ']';

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

            fut.onDone(aff.topologyVersion());
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
     *
     * @return {@code True} if there are pending futures.
     */
    public boolean dumpDebugInfo() {
        if (!readyFuts.isEmpty()) {
            U.warn(log, "First 3 pending affinity ready futures [grp=" + cacheOrGrpName +
                ", total=" + readyFuts.size() +
                ", lastVer=" + lastVersion() + "]:");

            int cnt = 0;

            for (AffinityReadyFuture fut : readyFuts.values()) {
                U.warn(log, ">>> " + fut);

                if (++cnt == 3)
                    break;
            }

            return true;
        }

        return false;
    }

    /**
     * @param topVer Topology version.
     * @return Assignment.
     */
    public AffinityAssignment readyAffinity(AffinityTopologyVersion topVer) {
        AffinityAssignment cache = head.get();

        if (!cache.topologyVersion().equals(topVer)) {
            cache = affCache.get(topVer);

            if (cache == null) {
                throw new IllegalStateException("Affinity for topology version is " +
                    "not initialized [locNode=" + ctx.discovery().localNode().id() +
                    ", grp=" + cacheOrGrpName +
                    ", topVer=" + topVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ']');
            }
        }

        return cache;
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
                    ", grp=" + cacheOrGrpName +
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

            if (fut != null) {
                Thread curTh = Thread.currentThread();

                String threadName = curTh.getName();

                try {
                    curTh.setName(threadName + " (waiting " + topVer + ")");

                    fut.get();
                }
                finally {
                    curTh.setName(threadName);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for affinity ready future for topology version: " + topVer,
                e);
        }
    }

    /**
     * Cleaning the affinity history.
     */
    private void onHistoryAdded() {
        if (fullHistSize.incrementAndGet() > MAX_HIST_SIZE) {
            Iterator<HistoryAffinityAssignment> it = affCache.values().iterator();

            int rmvCnt = MAX_HIST_SIZE / 2;

            AffinityTopologyVersion topVerRmv = null;

            while (it.hasNext() && rmvCnt > 0) {
                AffinityAssignment aff0 = it.next();

                it.remove();

                rmvCnt--;

                fullHistSize.decrementAndGet();

                topVerRmv = aff0.topologyVersion();
            }

            topVerRmv = it.hasNext() ? it.next().topologyVersion() : topVerRmv;

            ctx.affinity().removeCachedAffinity(topVerRmv);
        }
    }

    /**
     * @return All initialized versions.
     */
    public Collection<AffinityTopologyVersion> cachedVersions() {
        return affCache.keySet();
    }

    /**
     * @param affAssignment Affinity assignment.
     * @return String representation of given {@code affAssignment}.
     */
    private static String fold(List<List<ClusterNode>> affAssignment) {
        SB sb = new SB();

        for (int p = 0; p < affAssignment.size(); p++) {
            sb.a("Part [");
            sb.a("id=" + p + ", ");

            SB partOwners = new SB();

            List<ClusterNode> affOwners = affAssignment.get(p);

            for (ClusterNode node : affOwners) {
                partOwners.a(node.consistentId());
                partOwners.a(' ');
            }

            sb.a("owners=[");
            sb.a(partOwners);
            sb.a(']');

            sb.a("] ");
        }

        return sb.toString();
    }

    /**
     * Affinity ready future. Will remove itself from ready futures map.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<AffinityTopologyVersion> {
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
