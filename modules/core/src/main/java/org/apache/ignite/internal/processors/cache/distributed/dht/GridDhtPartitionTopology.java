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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 * DHT partition topology.
 */
@GridToStringExclude
public interface GridDhtPartitionTopology {
    /**
     * Locks the topology, usually during mapping on locks or transactions.
     */
    public void readLock();

    /**
     * Unlocks topology locked by {@link #readLock()} method.
     */
    public void readUnlock();

    /**
     * Updates topology version.
     *
     * @param exchId Exchange ID.
     * @param exchFut Exchange future.
     * @param updateSeq Update sequence.
     * @param stopping Stopping flag.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public void updateTopologyVersion(
        GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchFut,
        long updateSeq,
        boolean stopping
    ) throws IgniteInterruptedCheckedException;

    /**
     * Topology version.
     *
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion();

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
     * @return Cache ID.
     */
    public int cacheId();

    /**
     * Pre-initializes this topology.
     *
     * @param exchFut Exchange future.
     * @param affReady Affinity ready flag.
     * @throws IgniteCheckedException If failed.
     */
    public void beforeExchange(GridDhtPartitionsExchangeFuture exchFut, boolean affReady)
        throws IgniteCheckedException;

    /**
     * @param exchFut Exchange future.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public void initPartitions(GridDhtPartitionsExchangeFuture exchFut) throws IgniteInterruptedCheckedException;

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
     * @param parts Partitions to release (should be reserved before).
     */
    public void releasePartitions(int... parts);

    /**
     * @param key Cache key.
     * @param create If {@code true}, then partition will be created if it's not there.
     * @return Local partition.
     * @throws GridDhtInvalidPartitionException If partition is evicted or absent and
     *      does not belong to this node.
     */
    @Nullable public GridDhtLocalPartition localPartition(Object key, boolean create)
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
     * @return Collection of all nodes responsible for this partition with primary node being first.
     */
    @Nullable public List<ClusterNode> nodes(int p, AffinityAssignment affAssignment, List<ClusterNode> affNodes);

    /**
     * @param p Partition ID.
     * @return Collection of all nodes who {@code own} this partition.
     */
    public List<ClusterNode> owners(int p);

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
     * @param exchId Exchange ID.
     * @param partMap Update partition map.
     * @param cntrMap Partition update counters.
     * @return Local partition map if there were evictions or {@code null} otherwise.
     */
    public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionFullMap partMap,
        @Nullable Map<Integer, T2<Long, Long>> cntrMap);

    /**
     * @param exchId Exchange ID.
     * @param parts Partitions.
     * @return Local partition map if there were evictions or {@code null} otherwise.
     */
    @Nullable public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts);

    /**
     * @param cntrMap Counters map.
     */
    public void applyUpdateCounters(Map<Integer, T2<Long, Long>> cntrMap);

    /**
     * Checks if there is at least one owner for each partition in the cache topology.
     * If not, marks such a partition as LOST.
     * <p>
     * This method should be called on topology coordinator after all partition messages are received.
     *
     * @param discoEvt Discovery event for which we detect lost partitions.
     * @return {@code True} if partitons state got updated.
     */
    public boolean detectLostPartitions(DiscoveryEvent discoEvt);

    /**
     * Resets the state of all LOST partitions to OWNING.
     */
    public void resetLostPartitions();

    /**
     * @return Collection of lost partitions, if any.
     */
    public Collection<Integer> lostPartitions();

    /**
     *
     */
    public void checkEvictions();

    /**
     * @param skipZeros If {@code true} then filters out zero counters.
     * @return Partition update counters.
     */
    public Map<Integer, T2<Long, Long>> updateCounters(boolean skipZeros);

    /**
     * @param part Partition to own.
     * @return {@code True} if owned.
     */
    public boolean own(GridDhtLocalPartition part);

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
     * @param topVer Topology version.
     * @return {@code True} if rebalance process finished.
     */
    public boolean rebalanceFinished(AffinityTopologyVersion topVer);

    /**
     * Make nodes from provided set owners for a given partition.
     * State of all current owners that aren't contained in the set will be reset to MOVING.
     * @param p Partition ID.
     * @param updateSeq If should increment sequence when updated.
     * @param owners Set of new owners.
     */
    public void setOwners(int p, Set<UUID> owners, boolean updateSeq);
}
