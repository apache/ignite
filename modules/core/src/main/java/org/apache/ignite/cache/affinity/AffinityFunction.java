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

package org.apache.ignite.cache.affinity;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Cache key affinity which maps keys to nodes. This interface is utilized for
 * both, replicated and partitioned caches. Cache affinity can be configured
 * for individual caches via {@link org.apache.ignite.configuration.CacheConfiguration#getAffinity()} method.
 * <p>
 * Whenever a key is given to cache, it is first passed to a pluggable
 * {@link AffinityKeyMapper} which may potentially map this key to an alternate
 * key which should be used for affinity. The key returned from
 * {@link AffinityKeyMapper#affinityKey(Object)} method is then passed to
 * {@link #partition(Object) partition(Object)} method to find out the partition for the key.
 * On each topology change, partition-to-node mapping is calculated using
 * {@link #assignPartitions(AffinityFunctionContext)} method, which assigns a collection
 * of nodes to each partition.
 * This collection of nodes is used for node affinity. In {@link org.apache.ignite.cache.CacheMode#REPLICATED REPLICATED}
 * cache mode the key will be cached on all returned nodes; generally, all caching nodes
 * participate in caching every key in replicated mode. In {@link org.apache.ignite.cache.CacheMode#PARTITIONED PARTITIONED}
 * mode, only primary and backup nodes are returned with primary node always in the
 * first position. So if there is {@code 1} backup node, then the returned collection will
 * have {@code 2} nodes in it - {@code primary} node in first position, and {@code backup}
 * node in second.
 * <p>
 * For more information about cache affinity and examples refer to {@link AffinityKeyMapper} and
 * {@link AffinityKeyMapped @AffinityKeyMapped} documentation.
 * @see AffinityKeyMapped
 * @see AffinityKeyMapper
 */
public interface AffinityFunction extends Serializable {
    /**
     * Resets cache affinity to its initial state. This method will be called by
     * the system any time the affinity has been sent to remote node where
     * it has to be reinitialized. If your implementation of affinity function
     * has no initialization logic, leave this method empty.
     */
    public void reset();

    /**
     * Gets total number of partitions available. All caches should always provide
     * correct partition count which should be the same on all participating nodes.
     * Note that partitions should always be numbered from {@code 0} inclusively to
     * {@code N} exclusively without any gaps.
     *
     * @return Total partition count.
     */
    public int partitions();

    /**
     * Gets partition number for a given key starting from {@code 0}. Partitioned caches
     * should make sure that keys are about evenly distributed across all partitions
     * from {@code 0} to {@link #partitions() partition count} for best performance.
     * <p>
     * Note that for fully replicated caches it is possible to segment key sets among different
     * grid node groups. In that case each node group should return a unique partition
     * number. However, unlike partitioned cache, mappings of keys to nodes in
     * replicated caches are constant and a node cannot migrate from one partition
     * to another.
     *
     * @param key Key to get partition for.
     * @return Partition number for a given key.
     */
    public int partition(Object key);

    /**
     * Gets affinity nodes for a partition. In case of replicated cache, all returned
     * nodes are updated in the same manner. In case of partitioned cache, the returned
     * list should contain only the primary and back up nodes with primary node being
     * always first.
     * <p>
     * Note that partitioned affinity must obey the following contract: given that node
     * <code>N</code> is primary for some key <code>K</code>, if any other node(s) leave
     * grid and no node joins grid, node <code>N</code> will remain primary for key <code>K</code>.
     *
     * @param affCtx Affinity function context. Will provide all required information to calculate
     *      new partition assignments.
     * @return Unmodifiable list indexed by partition number. Each element of array is a collection in which
     *      first node is a primary node and other nodes are backup nodes.
     */
    public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx);

    /**
     * Removes node from affinity. This method is called when it is safe to remove left node from
     * affinity mapping.
     *
     * @param nodeId ID of node to remove.
     */
    public void removeNode(UUID nodeId);
}