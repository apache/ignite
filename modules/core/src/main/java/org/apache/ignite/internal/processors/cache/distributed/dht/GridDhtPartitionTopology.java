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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;

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
     */
    public void updateTopologyVersion(
        GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchFut,
        long updateSeq,
        boolean stopping
    );

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
     * Pre-initializes this topology.
     *
     * @param exchFut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    public void beforeExchange(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException;

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
     * @return All current local partitions.
     */
    public Collection<GridDhtLocalPartition> currentLocalPartitions();

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
    public Collection<ClusterNode> nodes(int p, AffinityTopologyVersion topVer);

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
     * @param topVer Topology version.
     * @param e Entry added to cache.
     * @return Local partition.
     */
    public GridDhtLocalPartition onAdded(AffinityTopologyVersion topVer, GridDhtCacheEntry e);

    /**
     * @param e Entry removed from cache.
     */
    public void onRemoved(GridDhtCacheEntry e);

    /**
     * @param exchId Exchange ID.
     * @param partMap Update partition map.
     * @return Local partition map if there were evictions or {@code null} otherwise.
     */
    public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId, GridDhtPartitionFullMap partMap);

    /**
     * @param exchId Exchange ID.
     * @param parts Partitions.
     * @return Local partition map if there were evictions or {@code null} otherwise.
     */
    @Nullable public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts);

    /**
     * @param part Partition to own.
     * @return {@code True} if owned.
     */
    public boolean own(GridDhtLocalPartition part);

    /**
     * @param part Evicted partition.
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
}
