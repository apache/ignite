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

namespace Apache.Ignite.Core.Cache
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Provides affinity information to detect which node is primary and which nodes are
    /// backups for a partitioned cache. You can get an instance of this interface by calling
    /// <see cref="IIgnite.Affinity(string)"/> method.
    /// <para />
    /// Mapping of a key to a node is a three-step operation. First step will get an affinity key for 
    /// given key using <c>CacheAffinityKeyMapper</c>. If mapper is not specified, the original key 
    /// will be used. Second step will map affinity key to partition using 
    /// <c>CacheAffinityFunction.partition(Object)</c> method. Third step will map obtained partition 
    /// to nodes for current grid topology version.
    /// <para />
    /// Interface provides various <c>mapKeysToNodes(...)</c> methods which provide node affinity mapping 
    /// for given keys. All <c>mapKeysToNodes(...)</c> methods are not transactional and will not enlist
    /// keys into ongoing transaction.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface ICacheAffinity
    {
        /// <summary>
        /// Gets number of partitions in cache according to configured affinity function.
        /// </summary>
        /// <returns>Number of cache partitions.</returns>
        int Partitions
        {
            get;
        }

        /// <summary>
        /// Gets partition id for the given key.
        /// </summary>
        /// <param name="key">Key to get partition id for.</param>
        /// <returns>Partition id.</returns>
        int Partition<TK>(TK key);

        /// <summary>
        /// Returns 'true' if given node is the primary node for given key.
        /// </summary>
        /// <param name="n">Node.</param>
        /// <param name="key">Key.</param>
        /// <returns>'True' if given node is the primary node for given key.</returns>
        bool IsPrimary<TK>(IClusterNode n, TK key);

        /// <summary>
        /// Returns 'true' if given node is the backup node for given key.
        /// </summary>
        /// <param name="n">Node.</param>
        /// <param name="key">Key.</param>
        /// <returns>'True' if given node is the backup node for given key.</returns>
        bool IsBackup<TK>(IClusterNode n, TK key);

        /// <summary>
        /// Returns 'true' if given node is either primary or backup node for given key.
        /// </summary>
        /// <param name="n">Node.</param>
        /// <param name="key">Key.</param>
        /// <returns>'True' if given node is either primary or backup node for given key.</returns>
        bool IsPrimaryOrBackup<TK>(IClusterNode n, TK key);

        /// <summary>
        /// Gets partition ids for which nodes of the given projection has primary
        /// ownership.
        /// </summary>
        /// <param name="n">Node.</param>
        /// <returns>Partition ids for which given projection has primary ownership.</returns>
        int[] PrimaryPartitions(IClusterNode n);

        /// <summary>
        /// Gets partition ids for which nodes of the given projection has backup
        /// ownership.
        /// </summary>
        /// <param name="n">Node.</param>
        /// <returns>Partition ids for which given projection has backup ownership.</returns>
        int[] BackupPartitions(IClusterNode n);

        /// <summary>
        /// Gets partition ids for which nodes of the given projection has ownership
        /// (either primary or backup).
        /// </summary>
        /// <param name="n">Node.</param>
        /// <returns>Partition ids for which given projection has ownership.</returns>
        int[] AllPartitions(IClusterNode n);

        /// <summary>
        /// Maps passed in key to a key which will be used for node affinity.
        /// </summary>
        /// <param name="key">Key to map.</param>
        /// <returns>Key to be used for node-to-affinity mapping (may be the same key as passed in).</returns>
        TR AffinityKey<TK, TR>(TK key);

        /// <summary>
        /// This method provides ability to detect which keys are mapped to which nodes.
        /// Use it to determine which nodes are storing which keys prior to sending
        /// jobs that access these keys.
        /// </summary>
        /// <param name="keys">Keys to map to nodes.</param>
        /// <returns>Map of nodes to keys or empty map if there are no alive nodes for this cache.</returns>
        IDictionary<IClusterNode, IList<TK>> MapKeysToNodes<TK>(IList<TK> keys);

        /// <summary>
        /// This method provides ability to detect to which primary node the given key
        /// is mapped. Use it to determine which nodes are storing which keys prior to sending
        /// jobs that access these keys.
        /// </summary>
        /// <param name="key">Keys to map to a node.</param>
        /// <returns>Primary node for the key or null if there are no alive nodes for this cache.</returns>
        IClusterNode MapKeyToNode<TK>(TK key);

        /// <summary>
        /// Gets primary and backup nodes for the key. Note that primary node is always
        /// first in the returned collection.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        IList<IClusterNode> MapKeyToPrimaryAndBackups<TK>(TK key);

        /// <summary>
        /// Gets primary node for the given partition.
        /// </summary>
        /// <param name="part">Partition id.</param>
        /// <returns>Primary node for the given partition.</returns>
        IClusterNode MapPartitionToNode(int part);

        /// <summary>
        /// Gets primary nodes for the given partitions.
        /// </summary>
        /// <param name="parts">Partition ids.</param>
        /// <returns>Mapping of given partitions to their primary nodes.</returns>
        IDictionary<int, IClusterNode> MapPartitionsToNodes(IList<int> parts);

        /// <summary>
        /// Gets primary and backup nodes for partition. Note that primary node is always
        /// first in the returned collection.
        /// </summary>
        /// <param name="part">Partition to get affinity nodes for.</param>
        /// <returns>Collection of primary and backup nodes for partition with primary node always first</returns>
        IList<IClusterNode> MapPartitionToPrimaryAndBackups(int part);
    }
}
