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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Information about partitions of all nodes in topology. <br> Is sent by topology coordinator: when all {@link
 * GridDhtPartitionsSingleMessage}s were received. <br> May be also compacted as part of {@link
 * CacheAffinityChangeMessage} for node left or failed case.<br>
 */
public class GridDhtPartitionsFullMessage extends GridDhtPartitionsAbstractMessage {
    /** */
    private static final byte REBALANCED_FLAG_MASK = 0x01;

    /** grpId -> FullMap */
    @Order(value = 6, method = "partitions")
    @GridToStringInclude
    private Map<Integer, GridDhtPartitionFullMap> parts;

    /** */
    @Order(value = 7, method = "duplicatedPartitionsData")
    private Map<Integer, Integer> dupPartsData;

    /** Partitions update counters. */
    @Order(value = 8, method = "partitionsCounters")
    @GridToStringInclude
    private IgniteDhtPartitionCountersMap partCntrs;

    /** Partitions history suppliers. */
    @Order(value = 9, method = "partitionHistorySuppliers")
    @GridToStringInclude
    private IgniteDhtPartitionHistorySuppliersMap partHistSuppliers;

    /** Partitions that must be cleared and re-loaded. */
    @Order(value = 10, method = "partitionsToReload")
    @GridToStringInclude
    private IgniteDhtPartitionsToReloadMap partsToReload;

    /** Partitions sizes. */
    @Order(value = 11, method = "partitionSizes")
    private Map<Integer, GridDhtPartitionsSizeMessage> partsSizes;

    /** Topology version. */
    @Order(value = 12, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Exceptions. */
    @Order(value = 13, method = "errors")
    @GridToStringInclude
    private Map<UUID, ErrorMessage> errs;

    /** */
    @Order(value = 14, method = "resultTopologyVersion")
    private AffinityTopologyVersion resTopVer;

    /** */
    @Order(value = 15, method = "joinedNodeAffinity")
    private Map<Integer, CacheGroupAffinityMessage> joinedNodeAff;

    /** */
    @Order(value = 16, method = "idealAffinityDiff")
    private Map<Integer, CacheGroupAffinityMessage> idealAffDiff;

    /** */
    @Order(value = 17, method = "rebalancedFlags")
    private byte flags;

    /** */
    @Order(value = 18, method = "lostPartitionsMap")
    @GridToStringExclude
    private Map<Integer, int[]> lostParts;

    /**
     * Empty constructor.
     */
    public GridDhtPartitionsFullMessage() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     * @param lastVer Last version.
     * @param topVer Topology version. For messages not related to exchange may be {@link AffinityTopologyVersion#NONE}.
     * @param partHistSuppliers Suppliers.
     * @param partsToReload Partitions to reload.
     */
    public GridDhtPartitionsFullMessage(@Nullable GridDhtPartitionExchangeId id,
        @Nullable GridCacheVersion lastVer,
        @NotNull AffinityTopologyVersion topVer,
        @Nullable IgniteDhtPartitionHistorySuppliersMap partHistSuppliers,
        @Nullable IgniteDhtPartitionsToReloadMap partsToReload) {
        super(id, lastVer);

        assert id == null || topVer.equals(id.topologyVersion());

        this.topVer = topVer;
        this.partHistSuppliers = partHistSuppliers;
        this.partsToReload = partsToReload;
    }

    /** {@inheritDoc} */
    @Override void copyStateTo(GridDhtPartitionsAbstractMessage msg) {
        super.copyStateTo(msg);

        GridDhtPartitionsFullMessage cp = (GridDhtPartitionsFullMessage)msg;

        if (parts != null) {
            cp.parts = new HashMap<>(parts.size());

            for (Map.Entry<Integer, GridDhtPartitionFullMap> e : parts.entrySet()) {
                GridDhtPartitionFullMap val = e.getValue();

                cp.parts.put(e.getKey(), new GridDhtPartitionFullMap(
                    val.nodeId(),
                    val.nodeOrder(),
                    val.updateSequence(),
                    val.map(),
                    false));
            }
        }
        else
            cp.parts = null;

        cp.dupPartsData = dupPartsData;
        cp.partCntrs = partCntrs;
        cp.partHistSuppliers = partHistSuppliers;
        cp.partsToReload = partsToReload;
        cp.partsSizes = partsSizes;
        cp.topVer = topVer;
        cp.errs = errs;
        cp.resTopVer = resTopVer;
        cp.joinedNodeAff = joinedNodeAff;
        cp.idealAffDiff = idealAffDiff;
        cp.flags = flags;
        cp.lostParts = lostParts;
    }

    /**
     * @return Message copy.
     */
    public GridDhtPartitionsFullMessage copy() {
        GridDhtPartitionsFullMessage cp = new GridDhtPartitionsFullMessage();

        copyStateTo(cp);

        return cp;
    }

    /**
     * @param resTopVer Result topology version.
     */
    public void resultTopologyVersion(AffinityTopologyVersion resTopVer) {
        this.resTopVer = resTopVer;
    }

    /**
     * @return Result topology version.
     */
    public AffinityTopologyVersion resultTopologyVersion() {
        return resTopVer;
    }

    /**
     * @return Caches affinity for joining nodes.
     */
    @Nullable public Map<Integer, CacheGroupAffinityMessage> joinedNodeAffinity() {
        return joinedNodeAff;
    }

    /**
     * @param joinedNodeAff Caches affinity for joining nodes.
     */
    public void joinedNodeAffinity(Map<Integer, CacheGroupAffinityMessage> joinedNodeAff) {
        this.joinedNodeAff = joinedNodeAff;
    }

    /**
     * @return Difference with ideal affinity.
     */
    @Nullable public Map<Integer, CacheGroupAffinityMessage> idealAffinityDiff() {
        return idealAffDiff;
    }

    /**
     * @param idealAffDiff Difference with ideal affinity.
     */
    public void idealAffinityDiff(Map<Integer, CacheGroupAffinityMessage> idealAffDiff) {
        this.idealAffDiff = idealAffDiff;
    }

    /**
     * @return Local partitions.
     */
    public Map<Integer, GridDhtPartitionFullMap> partitions() {
        if (parts == null)
            parts = new HashMap<>();

        return parts;
    }

    /**
     * @param parts Local partitions.
     */
    public void partitions(Map<Integer, GridDhtPartitionFullMap> parts) {
        this.parts = parts;
    }

    /**
     * @return Duplicated partitions data.
     */
    public Map<Integer, Integer> duplicatedPartitionsData() {
        return dupPartsData;
    }

    /**
     * @param dupPartsData Duplicated partitions data.
     */
    public void duplicatedPartitionsData(Map<Integer, Integer> dupPartsData) {
        this.dupPartsData = dupPartsData;
    }

    /**
     * @param grpId Cache group ID.
     * @return {@code True} if message contains full map for given cache.
     */
    public boolean containsGroup(int grpId) {
        return parts != null && parts.containsKey(grpId);
    }

    /**
     * @param grpId Cache group ID.
     * @param fullMap Full partitions map.
     * @param dupDataCache Optional ID of cache with the same partition state map.
     */
    public void addFullPartitionsMap(int grpId, GridDhtPartitionFullMap fullMap, @Nullable Integer dupDataCache) {
        assert fullMap != null;

        if (parts == null)
            parts = new HashMap<>();

        if (!parts.containsKey(grpId)) {
            parts.put(grpId, fullMap);

            if (dupDataCache != null) {
                assert compressed();
                assert parts.containsKey(dupDataCache);

                if (dupPartsData == null)
                    dupPartsData = new HashMap<>();

                dupPartsData.put(grpId, dupDataCache);
            }
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param cntrMap Partition update counters.
     */
    public void addPartitionUpdateCounters(int grpId, CachePartitionFullCountersMap cntrMap) {
        if (partCntrs == null)
            partCntrs = new IgniteDhtPartitionCountersMap();

        partCntrs.putIfAbsent(grpId, cntrMap);
    }

    /**
     * @param grpId Group id.
     * @param lostParts Lost parts.
     */
    public void addLostPartitions(int grpId, Collection<Integer> lostParts) {
        if (lostParts.isEmpty())
            return;

        if (this.lostParts == null)
            this.lostParts = new HashMap<>();

        this.lostParts.put(grpId, lostParts.stream().mapToInt(v -> v).toArray());
    }

    /**
     * @param grpId Group id.
     * @return Lost partitions for a group.
     */
    @Nullable public Set<Integer> lostPartitions(int grpId) {
        if (lostParts == null)
            return null;

        int[] parts = lostParts.get(grpId);

        if (parts == null)
            return Collections.emptySet();

        return IntStream.of(parts).boxed().collect(Collectors.toSet());
    }

    /**
     * @return Lost partitions for groups.
     */
    public Map<Integer, int[]> lostPartitionsMap() {
        return lostParts;
    }

    /**
     * @param lostParts Lost partitions for groups.
     */
    public void lostPartitionsMap(Map<Integer, int[]> lostParts) {
        this.lostParts = lostParts;
    }

    /**
     * @param grpId Cache group ID.
     * @return Partition update counters.
     */
    public CachePartitionFullCountersMap partitionUpdateCounters(int grpId) {
        return partCntrs == null ? null : partCntrs.get(grpId);
    }

    /**
     * @return Partitions update counters.
     */
    public IgniteDhtPartitionCountersMap partitionsCounters() {
        return partCntrs;
    }

    /**
     * @param partCntrs Partitions update counters.
     */
    public void partitionsCounters(IgniteDhtPartitionCountersMap partCntrs) {
        this.partCntrs = partCntrs;
    }

    /**
     * @return Partitions history suppliers.
     */
    public IgniteDhtPartitionHistorySuppliersMap partitionHistorySuppliers() {
        if (partHistSuppliers == null)
            return IgniteDhtPartitionHistorySuppliersMap.empty();

        return partHistSuppliers;
    }

    /**
     * @param partHistSuppliers Partitions history suppliers.
     */
    public void partitionHistorySuppliers(IgniteDhtPartitionHistorySuppliersMap partHistSuppliers) {
        this.partHistSuppliers = partHistSuppliers;
    }

    /**
     * @return Partitions that must be cleared and re-loaded.
     */
    public IgniteDhtPartitionsToReloadMap partitionsToReload() {
        return partsToReload;
    }

    /**
     * @param partsToReload Partitions that must be cleared and re-loaded.
     */
    public void partitionsToReload(IgniteDhtPartitionsToReloadMap partsToReload) {
        this.partsToReload = partsToReload;
    }

    /**
     *
     */
    public Collection<Integer> partsToReload(UUID nodeId, int grpId) {
        if (partsToReload == null)
            return Collections.emptySet();

        return partsToReload.get(nodeId, grpId);
    }

    /**
     * Supplies partition sizes map for all cache groups.
     *
     * @param partsSizes Partitions sizes map.
     */
    public void partitionSizes(Map<Integer, GridDhtPartitionsSizeMessage> partsSizes) {
        this.partsSizes = partsSizes;
    }

    /**
     * Returns partition sizes map for all cache groups.
     *
     * @return Partition sizes map (grpId, (partId, partSize)).
     */
    public Map<Integer, GridDhtPartitionsSizeMessage> partitionSizes() {
        if (partsSizes == null)
            return Collections.emptyMap();

        return partsSizes;
    }

    /**
     * @return Errors map.
     */
    @Nullable public Map<UUID, Throwable> errorsMap() {
        return errs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toThrowable()));
    }

    /**
     * @param errs Errors map.
     */
    public void errorsMap(Map<UUID, Exception> errs) {
        this.errs = new HashMap<>(errs.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new ErrorMessage(entry.getValue()))));
    }

    /**
     * @return Exceptions.
     */
    public Map<UUID, ErrorMessage> errors() {
        return errs;
    }

    /**
     * @param errs Exceptions.
     */
    public void errors(Map<UUID, ErrorMessage> errs) {
        this.errs = errs;
    }

    /**
     * Rebalance finished.
     */
    public boolean rebalanced() {
        return (flags & REBALANCED_FLAG_MASK) != 0;
    }

    /**
     * @param rebalanced {@code True} if data is fully rebalanced.
     */
    public void rebalanced(boolean rebalanced) {
        flags = rebalanced ? (byte)(flags | REBALANCED_FLAG_MASK) : (byte)(flags & ~REBALANCED_FLAG_MASK);
    }

    /**
     * @return Rebalanced flags.
     */
    public byte rebalancedFlags() {
        return flags;
    }

    /**
     * @param flags Rebalanced flags.
     */
    public void rebalancedFlags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (dupPartsData != null) {
            assert parts != null;

            for (Map.Entry<Integer, Integer> e : dupPartsData.entrySet()) {
                GridDhtPartitionFullMap map1 = parts.get(e.getKey());
                GridDhtPartitionFullMap map2 = parts.get(e.getValue());

                assert map1 != null : e.getKey();
                assert map2 != null : e.getValue();
                assert map1.size() == map2.size();

                for (Map.Entry<UUID, GridDhtPartitionMap> e0: map2.entrySet()) {
                    GridDhtPartitionMap partMap1 = map1.get(e0.getKey());

                    assert partMap1 != null && partMap1.map().isEmpty() : partMap1;
                    assert !partMap1.hasMovingPartitions() : partMap1;

                    GridDhtPartitionMap partMap2 = e0.getValue();

                    assert partMap2 != null;

                    for (Map.Entry<Integer, GridDhtPartitionState> stateEntry : partMap2.entrySet())
                        partMap1.put(stateEntry.getKey(), stateEntry.getValue());
                }
            }
        }

        if (parts == null)
            parts = new HashMap<>();

        if (partCntrs == null)
            partCntrs = new IgniteDhtPartitionCountersMap();

        if (partHistSuppliers == null)
            partHistSuppliers = new IgniteDhtPartitionHistorySuppliersMap();

        if (partsToReload == null)
            partsToReload = new IgniteDhtPartitionsToReloadMap();

        if (errs == null)
            errs = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 46;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsFullMessage.class, this, "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }

    /**
     * Merges (replaces with newer) partitions map from given {@code other} full message.
     *
     * @param other Other full message.
     */
    public void merge(GridDhtPartitionsFullMessage other, GridDiscoveryManager discovery) {
        assert other.exchangeId() == null && exchangeId() == null :
            "Both current and merge full message must have exchangeId == null"
                + other.exchangeId() + "," + exchangeId();

        for (Map.Entry<Integer, GridDhtPartitionFullMap> grpAndMap : other.partitions().entrySet()) {
            int grpId = grpAndMap.getKey();
            GridDhtPartitionFullMap updMap = grpAndMap.getValue();

            GridDhtPartitionFullMap currMap = partitions().get(grpId);

            if (currMap == null)
                partitions().put(grpId, updMap);
            else {
                ClusterNode curMapSentBy = discovery.node(currMap.nodeId());
                ClusterNode newMapSentBy = discovery.node(updMap.nodeId());

                if (newMapSentBy == null)
                    continue;

                if (curMapSentBy == null || newMapSentBy.order() > curMapSentBy.order() || updMap.compareTo(currMap) >= 0)
                    partitions().put(grpId, updMap);
            }
        }
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        partCntrs = null;
    }
}
