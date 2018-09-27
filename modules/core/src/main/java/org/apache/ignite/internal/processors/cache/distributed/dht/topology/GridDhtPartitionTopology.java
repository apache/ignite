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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.jetbrains.annotations.Nullable;

/**
 * DHT partition topology.
 */
@GridToStringExclude
public interface GridDhtPartitionTopology {
    /**
     * @return  Total cache partitions.
     */
    public int partitions();

    /**
     * Locks the topology, usually during mapping on locks or transactions.
     */
    public void readLock();

    /**
     * Unlocks topology locked by {@link #readLock()} method.
     */
    public void readUnlock();

    /**
     * @return {@code True} if locked by current thread.
     */
    public boolean holdsLock();
    /**
     * Updates topology version.
     *
     * @param exchFut Exchange future.
     * @param discoCache Discovery data cache.
     * @param updateSeq Update sequence.
     * @param stopping Stopping flag.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public void updateTopologyVersion(
        GridDhtTopologyFuture exchFut,
        DiscoCache discoCache,
        MvccCoordinator mvccCrd,
        long updateSeq,
        boolean stopping
    ) throws IgniteInterruptedCheckedException;

    /**
     * @return Result topology version of last finished exchange.
     */
    public AffinityTopologyVersion readyTopologyVersion();

    /**
     * @return Start topology version of last exchange.
     */
    public AffinityTopologyVersion lastTopologyChangeVersion();

    /**
     * Gets a future that will be completed when partition exchange map for this
     * particular topology version is done.
     *
     * @return Topology version ready future.
     */
    public GridDhtTopologyFuture topologyVersionFuture();

    /**
     * @return {@code True} if cache is being stopped.
     */
    public boolean stopping();

    /**
     * @return Cache group ID.
     */
    public int groupId();

    /**
     * Pre-initializes this topology.
     *
     * @param exchFut Exchange future.
     * @param affReady Affinity ready flag.
     * @param updateMoving
     * @throws IgniteCheckedException If failed.
     */
    public void beforeExchange(GridDhtPartitionsExchangeFuture exchFut,
        boolean affReady,
        boolean updateMoving)
        throws IgniteCheckedException;

    /**
     * @param affVer Affinity version.
     * @param exchFut Exchange future.
     * @return {@code True} if partitions must be refreshed.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public boolean initPartitionsWhenAffinityReady(AffinityTopologyVersion affVer, GridDhtPartitionsExchangeFuture exchFut)
        throws IgniteInterruptedCheckedException;

    /**
     * Initializes local data structures after partitions are restored from persistence.
     *
     * @param topVer Topology version.
     */
    public void afterStateRestored(AffinityTopologyVersion topVer);

    /**
     * Post-initializes this topology.
     *
     * @param exchFut Exchange future.
     * @return {@code True} if mapping was changed.
     * @throws IgniteCheckedException If failed.
     */
    public boolean afterExchange(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException;

    /**
     * @param topVer Topology version at the time of creation.
     * @param p Partition ID.
     * @param create If {@code true}, then partition will be created if it's not there.
     * @return Local partition.
     * @throws GridDhtInvalidPartitionException If partition is evicted or absent and
     *      does not belong to this node.
     */
    @Nullable public GridDhtLocalPartition localPartition(int p, AffinityTopologyVersion topVer, boolean create)
        throws GridDhtInvalidPartitionException;

    /**
     * Unconditionally creates partition during restore of persisted partition state.
     *
     * @param p Partition ID.
     * @return Partition.
     * @throws IgniteCheckedException If failed.
     */
    public GridDhtLocalPartition forceCreatePartition(int p) throws IgniteCheckedException;

    /**
     * @param topVer Topology version at the time of creation.
     * @param p Partition ID.
     * @param create If {@code true}, then partition will be created if it's not there.
     * @return Local partition.
     * @throws GridDhtInvalidPartitionException If partition is evicted or absent and
     *      does not belong to this node.
     */
    @Nullable public GridDhtLocalPartition localPartition(int p, AffinityTopologyVersion topVer, boolean create,
        boolean showRenting)
        throws GridDhtInvalidPartitionException;

    /**
     * @param parts Partitions to release (should be reserved before).
     */
    public void releasePartitions(int... parts);

    /**
     * @param part Partition number.
     * @return Local partition.
     * @throws GridDhtInvalidPartitionException If partition is evicted or absent and
     *      does not belong to this node.
     */
    @Nullable public GridDhtLocalPartition localPartition(int part)
        throws GridDhtInvalidPartitionException;

    /**
     * @return All local partitions by copying them into another list.
     */
    public List<GridDhtLocalPartition> localPartitions();

    /**
     *
     * @return All current active local partitions.
     */
    public Iterable<GridDhtLocalPartition> currentLocalPartitions();

    /**
     * @return Local IDs.
     */
    public GridDhtPartitionMap localPartitionMap();

    /**
     * @param nodeId Node ID.
     * @param part Partition.
     * @return Partition state.
     */
    public GridDhtPartitionState partitionState(UUID nodeId, int part);

    /**
     * @return Current update sequence.
     */
    public long updateSequence();

    /**
     * @param p Partition ID.
     * @param topVer Topology version.
     * @return Collection of all nodes responsible for this partition with primary node being first.
     */
    public List<ClusterNode> nodes(int p, AffinityTopologyVersion topVer);

    /**
     * @param p Partition ID.
     * @param affAssignment Assignments.
     * @param affNodes Node assigned for given partition by affinity.
     * @return Collection of all nodes responsible for this partition with primary node being first. The first N
     *      elements of this collection (with N being 1 + backups) are actual DHT affinity nodes, other nodes
     *      are current additional owners of the partition after topology change.
     */
    @Nullable public List<ClusterNode> nodes(int p, AffinityAssignment affAssignment, List<ClusterNode> affNodes);

    /**
     * @param p Partition ID.
     * @return Collection of all nodes who {@code own} this partition.
     */
    public List<ClusterNode> owners(int p);

    /**
     * @return List indexed by partition number, each list element is collection of all nodes who
     *      owns corresponding partition.
     */
    public List<List<ClusterNode>> allOwners();

    /**
     * @param p Partition ID.
     * @param topVer Topology version.
     * @return Collection of all nodes who {@code own} this partition.
     */
    public List<ClusterNode> owners(int p, AffinityTopologyVersion topVer);

    /**
     * @param p Partition ID.
     * @return Collection of all nodes who {@code are preloading} this partition.
     */
    public List<ClusterNode> moving(int p);

    /**
     * @param onlyActive If {@code true}, then only {@code active} partitions will be returned.
     * @return Node IDs mapped to partitions.
     */
    public GridDhtPartitionFullMap partitionMap(boolean onlyActive);

    /**
     * @return {@code True} If one of cache nodes has partitions in {@link GridDhtPartitionState#MOVING} state.
     */
    public boolean hasMovingPartitions();

    /**
     * @param e Entry removed from cache.
     */
    public void onRemoved(GridDhtCacheEntry e);

    /**
     * @param exchangeResVer Result topology version for exchange. Value should be greater than previously passed. Null value
     *      means full map received is not related to exchange
     * @param partMap Update partition map.
     * @param cntrMap Partition update counters.
     * @param partsToReload Set of partitions that need to be reloaded.
     * @param msgTopVer Topology version from incoming message. This value is not null only for case message is not
     *      related to exchange. Value should be not less than previous 'Topology version from exchange'.
     * @return {@code True} if local state was changed.
     */
    public boolean update(
        @Nullable AffinityTopologyVersion exchangeResVer,
        GridDhtPartitionFullMap partMap,
        @Nullable CachePartitionFullCountersMap cntrMap,
        Set<Integer> partsToReload,
        @Nullable Map<Integer, Long> partSizes,
        @Nullable AffinityTopologyVersion msgTopVer);

    /**
     * @param exchId Exchange ID.
     * @param parts Partitions.
     * @param force {@code True} to skip stale update check.
     * @return {@code True} if local state was changed.
     */
    public boolean update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts,
        boolean force);

    /**
     * Collects update counters collected during exchange. Called on coordinator.
     *
     * @param cntrMap Counters map.
     */
    public void collectUpdateCounters(CachePartitionPartialCountersMap cntrMap);

    /**
     * Applies update counters collected during exchange on coordinator. Called on coordinator.
     */
    public void applyUpdateCounters();

    /**
     * Checks if there is at least one owner for each partition in the cache topology.
     * If not, marks such a partition as LOST.
     * <p>
     * This method should be called on topology coordinator after all partition messages are received.
     *
     * @param resTopVer Exchange result version.
     * @param discoEvt Discovery event for which we detect lost partitions.
     * @return {@code True} if partitions state got updated.
     */
    public boolean detectLostPartitions(AffinityTopologyVersion resTopVer, DiscoveryEvent discoEvt);

    /**
     * Resets the state of all LOST partitions to OWNING.
     *
     * @param resTopVer Exchange result version.
     */
    public void resetLostPartitions(AffinityTopologyVersion resTopVer);

    /**
     * @return Collection of lost partitions, if any.
     */
    public Collection<Integer> lostPartitions();

    /**
     * @return Partition update counters.
     */
    public CachePartitionFullCountersMap fullUpdateCounters();

    /**
     * @return Partition update counters.
     */
    public CachePartitionPartialCountersMap localUpdateCounters(boolean skipZeros);

    /**
     * @return Partition cache sizes.
     */
    public Map<Integer, Long> partitionSizes();

    /**
     * @param part Partition to own.
     * @return {@code True} if owned.
     */
    public boolean own(GridDhtLocalPartition part);

    /**
     * Owns all moving partitions for the given topology version.
     *
     * @param topVer Topology version.
     */
    public void ownMoving(AffinityTopologyVersion topVer);

    /**
     * @param part Evicted partition.
     * @param updateSeq Update sequence increment flag.
     */
    public void onEvicted(GridDhtLocalPartition part, boolean updateSeq);

    /**
     * @param nodeId Node to get partitions for.
     * @return Partitions for node.
     */
    @Nullable public GridDhtPartitionMap partitions(UUID nodeId);

    /**
     * Prints memory stats.
     *
     * @param threshold Threshold for number of entries.
     */
    public void printMemoryStats(int threshold);

    /**
     * @return Sizes of up-to-date partition versions in topology.
     */
    Map<Integer, Long> globalPartSizes();

    /**
     * @param partSizes Sizes of up-to-date partition versions in topology.
     */
    void globalPartSizes(@Nullable Map<Integer, Long> partSizes);

    /**
     * @param topVer Topology version.
     * @return {@code True} if rebalance process finished.
     */
    public boolean rebalanceFinished(AffinityTopologyVersion topVer);

    /**
     * Calculates nodes and partitions which have non-actual state and must be rebalanced.
     * State of all current owners that aren't contained in the given {@code ownersByUpdCounters} will be reset to MOVING.
     *
     * @param ownersByUpdCounters Map (partition, set of node IDs that have most actual state about partition
     *                            (update counter is maximal) and should hold OWNING state for such partition).
     * @param haveHistory Set of partitions which have WAL history to rebalance.
     * @return Map (nodeId, set of partitions that should be rebalanced <b>fully</b> by this node).
     */
    public Map<UUID, Set<Integer>> resetOwners(Map<Integer, Set<UUID>> ownersByUpdCounters, Set<Integer> haveHistory);

    /**
     * Callback on exchange done.
     *
     * @param assignment New affinity assignment.
     * @param updateRebalanceVer {@code True} if need check rebalance state.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture fut, AffinityAssignment assignment, boolean updateRebalanceVer);

    public MvccCoordinator mvccCoordinator();
}
