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

package org.apache.ignite.mxbean;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;

/**
 * This interface defines JMX view on {@link CacheGroupContext}.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
@MXBeanDescription("MBean that provides access to cache group descriptor.")
public interface CacheGroupMetricsMXBean {
    /**
     * Gets cache group id.
     *
     * @return Cache group id.
     */
    @MXBeanDescription("Cache group id.")
    public int getGroupId();

    /**
     * Gets cache group name.
     *
     * @return Cache group name.
     */
    @MXBeanDescription("Cache group name.")
    public String getGroupName();

    /**
     * Gets list of cache names of this cache group.
     *
     * @return List of cache names.
     */
    @MXBeanDescription("List of caches.")
    public List<String> getCaches();

    /**
     * Gets count of backups configured for this cache group.
     *
     * @return Count of backups.
     */
    @MXBeanDescription("Count of backups configured for cache group.")
    public int getBackups();

    /**
     * Gets count of partitions for this cache group.
     *
     * @return Count of partitions.
     */
    @MXBeanDescription("Count of partitions for cache group.")
    public int getPartitions();

    /**
     * Calculates minimum number of partitions copies for all partitions of this cache group.
     *
     * @return Minimum number of copies.
     */
    @MXBeanDescription("Minimum number of partition copies for all partitions of this cache group.")
    public int getMinimumNumberOfPartitionCopies();

    /**
     * Calculates maximum number of partitions copies for all partitions of this cache group.
     *
     * @return Maximum number of copies.
     */
    @MXBeanDescription("Maximum number of partition copies for all partitions of this cache group.")
    public int getMaximumNumberOfPartitionCopies();

    /**
     * Gets count of partitions with state OWNING for this cache group located on this node.
     *
     * @return Partitions count.
     */
    @MXBeanDescription("Count of partitions with state OWNING for this cache group located on this node.")
    public int getLocalNodeOwningPartitionsCount();

    /**
     * Gets count of partitions with state MOVING for this cache group located on this node.
     *
     * @return Partitions count.
     */
    @MXBeanDescription("Count of partitions with state MOVING for this cache group located on this node.")
    public int getLocalNodeMovingPartitionsCount();

    /**
     * Gets count of partitions with state RENTING for this cache group located on this node.
     *
     * @return Partitions count.
     */
    @MXBeanDescription("Count of partitions with state RENTING for this cache group located on this node.")
    public int getLocalNodeRentingPartitionsCount();

    /**
     * Gets count of entries remains to evict in RENTING partitions located on this node for this cache group.
     *
     * @return Entries count.
     */
    @MXBeanDescription("Count of entries remains to evict in RENTING partitions located on this node for this cache group.")
    public long getLocalNodeRentingEntriesCount();

    /**
     * Gets count of partitions with state OWNING for this cache group in the entire cluster.
     *
     * @return Partitions count.
     */
    @MXBeanDescription("Count of partitions for this cache group in the entire cluster with state OWNING.")
    public int getClusterOwningPartitionsCount();

    /**
     * Gets count of partitions with state MOVING for this cache group in the entire cluster.
     *
     * @return Partitions count.
     */
    @MXBeanDescription("Count of partitions for this cache group in the entire cluster with state MOVING.")
    public int getClusterMovingPartitionsCount();

    /**
     * Gets allocation map of partitions with state OWNING in the cluster.
     *
     * @return Map from partition number to set of nodes, where partition is located.
     */
    @MXBeanDescription("Allocation map of partitions with state OWNING in the cluster.")
    public Map<Integer, Set<String>> getOwningPartitionsAllocationMap();

    /**
     * Gets allocation map of partitions with state MOVING in the cluster.
     *
     * @return Map from partition number to set of nodes, where partition is located
     */
    @MXBeanDescription("Allocation map of partitions with state MOVING in the cluster.")
    public Map<Integer, Set<String>> getMovingPartitionsAllocationMap();

    /**
     * Gets affinity partitions assignment map.
     *
     * @return Map from partition number to list of nodes. The first node in this list is where the PRIMARY partition is
     * assigned, other nodes in the list is where the BACKUP partitions is assigned.
     */
    @MXBeanDescription("Affinity partitions assignment map.")
    public Map<Integer, List<String>> getAffinityPartitionsAssignmentMap();

    /**
     * Cache group type.
     */
    @MXBeanDescription("Cache group type.")
    public String getType();

    /**
     * Local partition ids.
     */
    @MXBeanDescription("Local partition ids.")
    public List<Integer> getPartitionIds();

    /**
     * Cache group total allocated pages.
     */
    @MXBeanDescription("Cache group total allocated pages.")
    public long getTotalAllocatedPages();

    /**
     * Total size of memory allocated for group, in bytes.
     */
    @MXBeanDescription("Total size of memory allocated for group, in bytes.")
    public long getTotalAllocatedSize();

    /**
     * Storage space allocated for group, in bytes.
     */
    @MXBeanDescription("Storage space allocated for group, in bytes.")
    public long getStorageSize();

    /**
     * Storage space allocated for group adjusted for possible sparsity, in bytes.
     */
    @MXBeanDescription("Storage space allocated for group adjusted for possible sparsity, in bytes.")
    public long getSparseStorageSize();

    /**
     * @return Number of partitions need processed for finished indexes create or rebuilding.
     * It is calculated as the number of local partition minus the processed.
     * A value of 0 indicates that the index is built.
     */
    @MXBeanDescription("Count of partitions need processed for finished indexes create or rebuilding.")
    public long getIndexBuildCountPartitionsLeft();
}
