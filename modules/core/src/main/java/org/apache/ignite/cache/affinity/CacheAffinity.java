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

import org.apache.ignite.cluster.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Provides affinity information to detect which node is primary and which nodes are
 * backups for a partitioned cache. You can get an instance of this interface by calling
 * {@code Cache.affinity()} method.
 * <p>
 * Mapping of a key to a node is a three-step operation. First step will get an affinity key for given key
 * using {@link CacheAffinityKeyMapper}. If mapper is not specified, the original key will be used. Second step
 * will map affinity key to partition using {@link CacheAffinityFunction#partition(Object)} method. Third step
 * will map obtained partition to nodes for current grid topology version.
 * <p>
 * Interface provides various {@code 'mapKeysToNodes(..)'} methods which provide node affinity mapping for
 * given keys. All {@code 'mapKeysToNodes(..)'} methods are not transactional and will not enlist
 * keys into ongoing transaction.
 */
public interface CacheAffinity<K> {
    /**
     * Gets number of partitions in cache according to configured affinity function.
     *
     * @return Number of cache partitions.
     * @see CacheAffinityFunction
     * @see org.apache.ignite.configuration.CacheConfiguration#getAffinity()
     * @see org.apache.ignite.configuration.CacheConfiguration#setAffinity(CacheAffinityFunction)
     */
    public int partitions();

    /**
     * Gets partition id for the given key.
     *
     * @param key Key to get partition id for.
     * @return Partition id.
     * @see CacheAffinityFunction
     * @see org.apache.ignite.configuration.CacheConfiguration#getAffinity()
     * @see org.apache.ignite.configuration.CacheConfiguration#setAffinity(CacheAffinityFunction)
     */
    public int partition(K key);

    /**
     * Returns {@code true} if given node is the primary node for given key.
     *
     * @param n Node to check.
     * @param key Key to check.
     * @return {@code True} if local node is the primary node for given key.
     */
    public boolean isPrimary(ClusterNode n, K key);

    /**
     * Returns {@code true} if local node is one of the backup nodes for given key.
     *
     * @param n Node to check.
     * @param key Key to check.
     * @return {@code True} if local node is one of the backup nodes for given key.
     */
    public boolean isBackup(ClusterNode n, K key);

    /**
     * Returns {@code true} if local node is primary or one of the backup nodes
     * <p>
     * This method is essentially equivalent to calling
     * <i>"{@link #isPrimary(org.apache.ignite.cluster.ClusterNode, Object)} ||
     *      {@link #isBackup(org.apache.ignite.cluster.ClusterNode, Object)})"</i>,
     * however it is more efficient as it makes both checks at once.
     *
     * @param n Node to check.
     * @param key Key to check.
     * @return {@code True} if local node is primary or backup for given key.
     */
    public boolean isPrimaryOrBackup(ClusterNode n, K key);

    /**
     * Gets partition ids for which nodes of the given projection has primary
     * ownership.
     * <p>
     * Note that since {@link org.apache.ignite.cluster.ClusterNode} implements {@link org.apache.ignite.cluster.ClusterGroup},
     * to find out primary partitions for a single node just pass
     * a single node into this method.
     * <p>
     * This method may return an empty array if none of nodes in the projection
     * have nearOnly disabled.
     *
     * @param n Grid node.
     * @return Partition ids for which given projection has primary ownership.
     * @see CacheAffinityFunction
     * @see org.apache.ignite.configuration.CacheConfiguration#getAffinity()
     * @see org.apache.ignite.configuration.CacheConfiguration#setAffinity(CacheAffinityFunction)
     */
    public int[] primaryPartitions(ClusterNode n);

    /**
     * Gets partition ids for which nodes of the given projection has backup
     * ownership. Note that you can find a back up at a certain level, e.g.
     * {@code first} backup or {@code third} backup by specifying the
     * {@code 'levels} parameter. If no {@code 'level'} is specified then
     * all backup partitions are returned.
     * <p>
     * Note that since {@link org.apache.ignite.cluster.ClusterNode} implements {@link org.apache.ignite.cluster.ClusterGroup},
     * to find out backup partitions for a single node, just pass that single
     * node into this method.
     * <p>
     * This method may return an empty array if none of nodes in the projection
     * have nearOnly disabled.
     *
     * @param n Grid node.
     * @return Partition ids for which given projection has backup ownership.
     * @see CacheAffinityFunction
     * @see org.apache.ignite.configuration.CacheConfiguration#getAffinity()
     * @see org.apache.ignite.configuration.CacheConfiguration#setAffinity(CacheAffinityFunction)
     */
    public int[] backupPartitions(ClusterNode n);

    /**
     * Gets partition ids for which nodes of the given projection has ownership
     * (either primary or backup).
     * <p>
     * Note that since {@link org.apache.ignite.cluster.ClusterNode} implements {@link org.apache.ignite.cluster.ClusterGroup},
     * to find out all partitions for a single node, just pass that single
     * node into this method.
     * <p>
     * This method may return an empty array if none of nodes in the projection
     * have nearOnly disabled.
     *
     * @param n Grid node.
     * @return Partition ids for which given projection has ownership.
     * @see CacheAffinityFunction
     * @see org.apache.ignite.configuration.CacheConfiguration#getAffinity()
     * @see org.apache.ignite.configuration.CacheConfiguration#setAffinity(CacheAffinityFunction)
     */
    public int[] allPartitions(ClusterNode n);

    /**
     * Maps passed in key to a key which will be used for node affinity. The affinity
     * key may be different from actual key if some field in the actual key was
     * designated for affinity mapping via {@link CacheAffinityKeyMapped} annotation
     * or if a custom {@link CacheAffinityKeyMapper} was configured.
     *
     * @param key Key to map.
     * @return Key to be used for node-to-affinity mapping (may be the same
     *      key as passed in).
     */
    public Object affinityKey(K key);

    /**
     * This method provides ability to detect which keys are mapped to which nodes.
     * Use it to determine which nodes are storing which keys prior to sending
     * jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node mapped to all keys.</li>
     * <li>
     *      For fully replicated caches {@link CacheAffinityFunction} is
     *      used to determine which keys are mapped to which nodes.
     * </li>
     * <li>For partitioned caches, the returned map represents node-to-key affinity.</li>
     * </ul>
     *
     * @param keys Keys to map to nodes.
     * @return Map of nodes to keys or empty map if there are no alive nodes for this cache.
     */
    public Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys);

    /**
     * This method provides ability to detect to which primary node the given key
     * is mapped. Use it to determine which nodes are storing which keys prior to sending
     * jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node ID.</li>
     * <li>
     *      For fully replicated caches first node ID returned by {@link CacheAffinityFunction}
     *      is returned.
     * </li>
     * <li>For partitioned caches, primary node for the given key is returned.</li>
     * </ul>
     *
     * @param key Keys to map to a node.
     * @return Primary node for the key or {@code null} if there are no alive nodes for this cache.
     */
    @Nullable public ClusterNode mapKeyToNode(K key);

    /**
     * Gets primary and backup nodes for the key. Note that primary node is always
     * first in the returned collection.
     *
     * @param key Key to get affinity nodes for.
     * @return Collection of primary and backup nodes for the key with primary node
     *      always first.
     */
    public Collection<ClusterNode> mapKeyToPrimaryAndBackups(K key);

    /**
     * Gets primary node for the given partition.
     *
     * @param part Partition id.
     * @return Primary node for the given partition.
     * @see CacheAffinityFunction
     * @see org.apache.ignite.configuration.CacheConfiguration#getAffinity()
     * @see org.apache.ignite.configuration.CacheConfiguration#setAffinity(CacheAffinityFunction)
     */
    public ClusterNode mapPartitionToNode(int part);

    /**
     * Gets primary nodes for the given partitions.
     *
     * @param parts Partition ids.
     * @return Mapping of given partitions to their primary nodes.
     * @see CacheAffinityFunction
     * @see org.apache.ignite.configuration.CacheConfiguration#getAffinity()
     * @see org.apache.ignite.configuration.CacheConfiguration#setAffinity(CacheAffinityFunction)
     */
    public Map<Integer, ClusterNode> mapPartitionsToNodes(Collection<Integer> parts);

    /**
     * Gets primary and backup nodes for partition. Note that primary node is always
     * first in the returned collection.
     *
     * @param part Partition to get affinity nodes for.
     * @return Collection of primary and backup nodes for partition with primary node
     *      always first.
     */
    public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(int part);
}
