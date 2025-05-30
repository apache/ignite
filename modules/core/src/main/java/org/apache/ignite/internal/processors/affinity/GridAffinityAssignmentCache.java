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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
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
    /** @see IgniteSystemProperties#IGNITE_AFFINITY_HISTORY_SIZE */
    public static final int DFLT_AFFINITY_HISTORY_SIZE = 25;

    /** @see IgniteSystemProperties#IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD */
    public static final float DFLT_PART_DISTRIBUTION_WARN_THRESHOLD = 50f;

    /**
     * Affinity cache will shrink when total number of non-shallow (see {@link HistoryAffinityAssignmentImpl})
     * historical instances will be greater than value of this constant.
     */
    final int maxNonShallowHistSize = getInteger(IGNITE_AFFINITY_HISTORY_SIZE, DFLT_AFFINITY_HISTORY_SIZE);

    /**
     * Affinity cache will also shrink when total number of both shallow ({@link HistoryAffinityAssignmentShallowCopy})
     * and non-shallow (see {@link HistoryAffinityAssignmentImpl}) historical instances will be greater than
     * value of this constant.
     */
    final int maxTotalHistSize = maxNonShallowHistSize * 10;

    /**
     * Independent of {@link #maxNonShallowHistSize} and {@link #maxTotalHistSize}, affinity cache will always
     * keep this number of non-shallow (see {@link HistoryAffinityAssignmentImpl}) instances.
     * We need at least one real instance, otherwise we won't be able to get affinity cache for
     * {@link GridCachePartitionExchangeManager#lastAffinityChangedTopologyVersion} in case cluster has experienced
     * too many client joins / client leaves / local cache starts.
     */
    private static final int MIN_NON_SHALLOW_HIST_SIZE = 2;

    /** Partition distribution. */
    private final float partDistribution =
        getFloat(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, DFLT_PART_DISTRIBUTION_WARN_THRESHOLD);

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
    private volatile IdealAffinityAssignment idealAssignment;

    /** */
    private volatile IdealAffinityAssignment baselineAssignment;

    /** */
    private BaselineTopology baselineTopology;

    /** Cache item corresponding to the head topology version. */
    private final AtomicReference<GridAffinityAssignmentV2> head;

    /** Ready futures. */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts = new ConcurrentSkipListMap<>();

    /** Log. */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** Node stop flag. */
    private volatile IgniteCheckedException stopErr;

    /** Number of non-shallow (see {@link HistoryAffinityAssignmentImpl}) affinity cache instances. */
    private volatile int nonShallowHistSize;

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
     */
    private GridAffinityAssignmentCache(GridKernalContext ctx,
        String cacheOrGrpName,
        int grpId,
        AffinityFunction aff,
        IgnitePredicate<ClusterNode> nodeFilter,
        int backups
    ) {
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

        log = ctx.log(GridAffinityAssignmentCache.class);

        partsCnt = aff.partitions();
        affCache = new ConcurrentSkipListMap<>();
        head = new AtomicReference<>(new GridAffinityAssignmentV2(AffinityTopologyVersion.NONE));

        similarAffKey = ctx.affinity().similaryAffinityKey(aff, nodeFilter, backups, partsCnt);

        assert similarAffKey != null;
    }

    /**
     * @param ctx Kernal context.
     * @param aff Initialized affinity function.
     * @param ccfg Cache configuration.
     * @return Affinity assignment cache instance.
     */
    public static GridAffinityAssignmentCache create(GridKernalContext ctx, AffinityFunction aff, CacheConfiguration<?, ?> ccfg) {
        return new GridAffinityAssignmentCache(ctx,
            CU.cacheOrGroupName(ccfg),
            CU.cacheGroupId(ccfg),
            aff,
            ccfg.getNodeFilter(),
            ccfg.getBackups());
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

        GridAffinityAssignmentV2 assignment = new GridAffinityAssignmentV2(topVer, affAssignment, idealAssignment.assignment());

        HistoryAffinityAssignmentImpl newHistEntry = new HistoryAffinityAssignmentImpl(assignment, backups);

        HistoryAffinityAssignment existing = affCache.put(topVer, newHistEntry);

        head.set(assignment);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (initialized affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        onHistoryAdded(existing, newHistEntry);

        if (log.isTraceEnabled()) {
            log.trace("New affinity assignment [grp=" + cacheOrGrpName
                + ", topVer=" + topVer
                + ", aff=" + fold(affAssignment) + "]");
        }
    }

    /**
     * @param assignment Assignment.
     */
    public void idealAssignment(AffinityTopologyVersion topVer, List<List<ClusterNode>> assignment) {
        this.idealAssignment = IdealAffinityAssignment.create(topVer, assignment);
    }

    /**
     * @return Assignment.
     */
    @Nullable public List<List<ClusterNode>> idealAssignmentRaw() {
        return idealAssignment != null ? idealAssignment.assignment() : null;
    }

    /**
     *
     */
    @Nullable public IdealAffinityAssignment idealAssignment() {
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

        nonShallowHistSize = 0;

        head.set(new GridAffinityAssignmentV2(AffinityTopologyVersion.NONE));

        stopErr = null;
    }

    /**
     * Calculates ideal assignment for given topology version and events happened since last calculation.
     *
     * @param topVer Topology version to calculate affinity cache for.
     * @param events Discovery events that caused this topology version change.
     * @param discoCache Discovery cache.
     * @return Ideal affinity assignment.
     */
    public IdealAffinityAssignment calculate(
        AffinityTopologyVersion topVer,
        @Nullable ExchangeDiscoveryEvents events,
        @Nullable DiscoCache discoCache
    ) {
        if (log.isDebugEnabled())
            log.debug("Calculating ideal affinity [topVer=" + topVer + ", locNodeId=" + ctx.localNodeId() +
                ", discoEvts=" + events + ']');

        IdealAffinityAssignment prevAssignment = idealAssignment;

        // Already calculated.
        if (prevAssignment != null && prevAssignment.topologyVersion().equals(topVer))
            return prevAssignment;

        // Resolve nodes snapshot for specified topology version.
        List<ClusterNode> sorted = new ArrayList<>(discoCache.cacheGroupAffinityNodes(groupId()));

        sorted.sort(NodeOrderComparator.getInstance());

        boolean hasBaseline = false;
        boolean changedBaseline = false;

        BaselineTopology blt = null;

        if (discoCache != null) {
            blt = discoCache.state().baselineTopology();

            hasBaseline = blt != null;

            changedBaseline = !hasBaseline ? baselineTopology != null : !blt.equals(baselineTopology);
        }

        IdealAffinityAssignment assignment;

        if (prevAssignment != null && events != null) {
            /* Skip affinity calculation only when all nodes triggered exchange
               don't belong to affinity for current group (client node or filtered by nodeFilter). */
            boolean skipCalculation = true;

            for (DiscoveryEvent evt : events.events()) {
                boolean affNode = CU.affinityNode(evt.eventNode(), nodeFilter);

                if (affNode || evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                    skipCalculation = false;

                    break;
                }
            }

            if (hasBaseline && changedBaseline) {
                recalculateBaselineAssignment(topVer, events, prevAssignment, sorted, blt);

                assignment = IdealAffinityAssignment.create(
                    topVer,
                    sorted,
                    baselineAssignmentWithoutOfflineNodes(discoCache)
                );
            }
            else if (skipCalculation)
                assignment = prevAssignment;
            else if (hasBaseline) {
                if (baselineAssignment == null)
                    recalculateBaselineAssignment(topVer, events, prevAssignment, sorted, blt);

                assignment = IdealAffinityAssignment.create(
                    topVer,
                    sorted,
                    baselineAssignmentWithoutOfflineNodes(discoCache)
                );
            }
            else {
                List<List<ClusterNode>> calculated = aff.assignPartitions(new GridAffinityFunctionContextImpl(
                    sorted,
                    prevAssignment.assignment(),
                    events.lastEvent(),
                    topVer,
                    backups
                ));

                assignment = IdealAffinityAssignment.create(topVer, sorted, calculated);
            }
        }
        else {
            if (hasBaseline) {
                recalculateBaselineAssignment(topVer, events, prevAssignment, sorted, blt);

                assignment = IdealAffinityAssignment.createWithPreservedPrimaries(
                    topVer,
                    baselineAssignmentWithoutOfflineNodes(discoCache),
                    baselineAssignment
                );
            }
            else {
                List<List<ClusterNode>> calculated = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted,
                    prevAssignment != null ? prevAssignment.assignment() : null,
                    events != null ? events.lastEvent() : null,
                    topVer,
                    backups
                ));

                assignment = IdealAffinityAssignment.create(topVer, sorted, calculated);
            }
        }

        assert assignment != null;

        idealAssignment = assignment;

        if (ctx.cache().cacheMode(cacheOrGrpName) == PARTITIONED && !ctx.clientNode())
            printDistributionIfThresholdExceeded(assignment.assignment(), sorted.size());

        if (hasBaseline) {
            baselineTopology = blt;

            assert baselineAssignment != null;
        }
        else {
            baselineTopology = null;
            baselineAssignment = null;
        }

        return assignment;
    }

    /**
     * @param topVer Topology version.
     * @param events Evetns.
     * @param prevAssignment Previous assignment.
     * @param sorted Sorted cache group nodes.
     * @param blt Baseline topology.
     */
    private void recalculateBaselineAssignment(
        AffinityTopologyVersion topVer,
        ExchangeDiscoveryEvents events,
        IdealAffinityAssignment prevAssignment,
        List<ClusterNode> sorted,
        BaselineTopology blt
    ) {
        List<ClusterNode> baselineAffNodes = blt.createBaselineView(sorted, nodeFilter);

        List<List<ClusterNode>> calculated = aff.assignPartitions(new GridAffinityFunctionContextImpl(
            baselineAffNodes,
            prevAssignment != null ? prevAssignment.assignment() : null,
            events != null ? events.lastEvent() : null,
            topVer,
            backups
        ));

        baselineAssignment = IdealAffinityAssignment.create(topVer, baselineAffNodes, calculated);
    }

    /**
     * @param disco Discovery history.
     * @return Baseline assignment with filtered out offline nodes.
     */
    private List<List<ClusterNode>> baselineAssignmentWithoutOfflineNodes(DiscoCache disco) {
        Map<Object, ClusterNode> alives = new HashMap<>();

        for (ClusterNode node : disco.serverNodes())
            alives.put(node.consistentId(), node);

        List<List<ClusterNode>> assignment = baselineAssignment.assignment();

        List<List<ClusterNode>> result = new ArrayList<>(assignment.size());

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> baselineMapping = assignment.get(p);
            List<ClusterNode> curMapping = null;

            for (ClusterNode node : baselineMapping) {
                ClusterNode aliveNode = alives.get(node.consistentId());

                if (aliveNode != null) {
                    if (curMapping == null)
                        curMapping = new ArrayList<>();

                    curMapping.add(aliveNode);
                }
            }

            result.add(p, curMapping != null ? curMapping : Collections.<ClusterNode>emptyList());
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

        if ((deltaPrimary > partDistribution || deltaBackup > partDistribution) && log.isInfoEnabled()) {
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

        GridAffinityAssignmentV2 aff = head.get();

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT || aff.primaryPartitions(evt.eventNode().id()).isEmpty() : evt;

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT || aff.backupPartitions(evt.eventNode().id()).isEmpty() : evt;

        GridAffinityAssignmentV2 assignmentCpy = new GridAffinityAssignmentV2(topVer, aff);

        AffinityTopologyVersion prevVer = topVer.minorTopologyVersion() == 0 ?
            new AffinityTopologyVersion(topVer.topologyVersion() - 1, Integer.MAX_VALUE) :
            new AffinityTopologyVersion(topVer.topologyVersion(), topVer.minorTopologyVersion() - 1);

        Map.Entry<AffinityTopologyVersion, HistoryAffinityAssignment> prevHistEntry = affCache.floorEntry(prevVer);

        HistoryAffinityAssignment newHistEntry = (prevHistEntry == null) ?
            new HistoryAffinityAssignmentImpl(assignmentCpy, backups) :
            new HistoryAffinityAssignmentShallowCopy(prevHistEntry.getValue().origin(), topVer);

        HistoryAffinityAssignment existing = affCache.put(topVer, newHistEntry);

        head.set(assignmentCpy);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (use previous affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        onHistoryAdded(existing, newHistEntry);
    }

    /**
     * @return Last initialized affinity version.
     */
    public AffinityTopologyVersion lastVersion() {
        return head.get().topologyVersion();
    }

    /**
     * @return Last initialized affinity assignment.
     */
    public AffinityAssignment lastReadyAffinity() {
        return head.get();
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
        GridAffinityAssignmentV2 aff = head.get();

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
     * @param topVer Topology version.
     */
    public Set<Integer> partitionPrimariesDifferentToIdeal(AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).partitionPrimariesDifferentToIdeal();
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
     * @throws IllegalStateException If affinity assignment is not initialized for the given topology version.
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
                    ", maxNonShallowHistorySize=" + maxNonShallowHistSize +
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
     * @throws IllegalArgumentException in case of the specified topology version {@code topVer}
     *                                  is earlier than affinity is calculated
     *                                  or the history of assignments is already cleaned.
     */
    public AffinityAssignment cachedAffinity(AffinityTopologyVersion topVer) {
        AffinityTopologyVersion lastAffChangeTopVer =
            ctx.cache().context().exchange().lastAffinityChangedTopologyVersion(topVer);

        return cachedAffinity(topVer, lastAffChangeTopVer);
    }

    /**
     * Get cached affinity for specified topology version.
     *
     * @param topVer Topology version for which affinity assignment is requested.
     * @param lastAffChangeTopVer Topology version of last affinity assignment change.
     * @return Cached affinity.
     * @throws IllegalArgumentException in case of the specified topology version {@code topVer}
     *                                  is earlier than affinity is calculated
     *                                  or the history of assignments is already cleaned.
     */
    public AffinityAssignment cachedAffinity(
        AffinityTopologyVersion topVer,
        AffinityTopologyVersion lastAffChangeTopVer
    ) {
        if (topVer.equals(AffinityTopologyVersion.NONE))
            topVer = lastAffChangeTopVer = lastVersion();
        else {
            if (lastAffChangeTopVer.equals(AffinityTopologyVersion.NONE))
                lastAffChangeTopVer = topVer;

            awaitTopologyVersion(lastAffChangeTopVer);
        }

        assert topVer.topologyVersion() >= 0 : topVer;

        AffinityAssignment cache = head.get();

        if (!(cache.topologyVersion().compareTo(lastAffChangeTopVer) >= 0 &&
            cache.topologyVersion().compareTo(topVer) <= 0)) {

            Map.Entry<AffinityTopologyVersion, HistoryAffinityAssignment> e = affCache.ceilingEntry(lastAffChangeTopVer);

            if (e != null)
                cache = e.getValue();

            if (cache == null) {
                throw new IllegalStateException("Getting affinity for topology version earlier than affinity is " +
                    "calculated [locNode=" + ctx.discovery().localNode() +
                    ", grp=" + cacheOrGrpName +
                    ", topVer=" + topVer +
                    ", lastAffChangeTopVer=" + lastAffChangeTopVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ", maxNonShallowHistorySize=" + maxNonShallowHistSize +
                    ']');
            }

            if (cache.topologyVersion().compareTo(topVer) > 0) {
                throw new IllegalStateException("Getting affinity for too old topology version that is already " +
                    "out of history (try to increase '" + IGNITE_AFFINITY_HISTORY_SIZE + "' system property)" +
                    " [locNode=" + ctx.discovery().localNode() +
                    ", grp=" + cacheOrGrpName +
                    ", topVer=" + topVer +
                    ", lastAffChangeTopVer=" + lastAffChangeTopVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ", maxNonShallowHistorySize=" + maxNonShallowHistSize +
                    ']');
            }
        }

        assert cache.topologyVersion().compareTo(lastAffChangeTopVer) >= 0 && cache.topologyVersion().compareTo(topVer) <= 0
            : "Invalid cached affinity: [cache=" + cache + ", topVer=" + topVer + ", lastAffChangedTopVer=" + lastAffChangeTopVer + "]";

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
        assert aff.idealAssignmentRaw() != null;

        idealAssignment(aff.lastVersion(), aff.idealAssignmentRaw());

        AffinityAssignment assign = aff.cachedAffinity(aff.lastVersion());

        initialize(aff.lastVersion(), assign.assignment());
    }

    /**
     * @param topVer Topology version to wait.
     */
    private void awaitTopologyVersion(AffinityTopologyVersion topVer) {
        GridAffinityAssignmentV2 aff = head.get();

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
     *
     * @param replaced Replaced entry in case history item was already present, null otherwise.
     * @param added New history item.
     */
    private synchronized void onHistoryAdded(
        HistoryAffinityAssignment replaced,
        HistoryAffinityAssignment added
    ) {
        if (replaced == null) {
            if (added.isFullSizeInstance())
                nonShallowHistSize++;
        }
        else if (replaced.isFullSizeInstance() != added.isFullSizeInstance())
            nonShallowHistSize += added.isFullSizeInstance() ? 1 : -1;

        int totalSize = affCache.size();

        if (!shouldContinueCleanup(nonShallowHistSize, totalSize))
            return;

        AffinityTopologyVersion lastAffChangeTopVer =
            ctx.cache().context().exchange().lastAffinityChangedTopologyVersion(head.get().topologyVersion());

        HistoryAffinityAssignment aff0 = null;

        Iterator<HistoryAffinityAssignment> it = affCache.values().iterator();

        while (it.hasNext() && shouldContinueCleanup(nonShallowHistSize, totalSize)) {
            aff0 = it.next();

            if (aff0.topologyVersion().equals(lastAffChangeTopVer))
                continue; // Keep lastAffinityChangedTopologyVersion, it's required for some operations.

            if (aff0.isFullSizeInstance()) {
                if (nonShallowHistSize <= MIN_NON_SHALLOW_HIST_SIZE)
                    continue;

                nonShallowHistSize--;
            }

            totalSize--;

            it.remove();
        }

        assert aff0 != null;

        ctx.affinity().removeCachedAffinity(aff0.topologyVersion());

        assert it.hasNext() : "All elements have been removed from affinity cache during cleanup";
    }

    /**
     * Checks whether affinity cache size conditions are still unsatisfied.
     *
     * @param nonShallowSize Non shallow size.
     * @param totalSize Total size.
     * @return <code>true</code> if affinity cache cleanup is not finished yet.
     */
    private boolean shouldContinueCleanup(int nonShallowSize, int totalSize) {
        return nonShallowSize > maxNonShallowHistSize || totalSize > maxTotalHistSize;
    }

    /**
     * @return All initialized versions.
     */
    public NavigableSet<AffinityTopologyVersion> cachedVersions() {
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
