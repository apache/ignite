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

 /**
  * @file
  * Declares ignite::cache::CacheAffinity class.
  */

#ifndef _IGNITE_CACHE_AFFINITY
#define _IGNITE_CACHE_AFFINITY

#include <ignite/cluster/cluster_group.h>

#include <ignite/impl/cache/cache_affinity_impl.h>

namespace ignite
{
    namespace cache
    {
        /**
         * Provides affinity information to detect which node is primary and which nodes are backups
         * for a partitioned or replicated cache.
         * You can get an instance of this interface by calling Ignite.GetAffinity(cacheName) method.
         *
         * @tparam K Cache affinity key type.
         */
        template<typename K>
        class IGNITE_IMPORT_EXPORT CacheAffinity
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Pointer to cache affinity implementation.
             */
            CacheAffinity(impl::cache::SP_CacheAffinityImpl impl) :
                impl(impl)
            {
                // No-op.
            }

            /**
             * Get number of partitions in cache according to configured affinity function.
             *
             * @return Number of partitions.
             */
            int32_t GetPartitions()
            {
                return impl.Get()->GetPartitions();
            }

            /**
             * Get partition id for the given key.
             *
             * @param key Key to get partition id for.
             * @return Partition id.
             */
            int32_t GetPartition(const K& key)
            {
                return impl.Get()->GetPartition(key);
            }

            /**
             * Return true if given node is the primary node for given key.
             *
             * @param node Cluster node.
             * @param key Key to check.
             * @return True if given node is primary node for given key.
             */
            bool IsPrimary(cluster::ClusterNode node, const K& key)
            {
                return impl.Get()->IsPrimary(node, key);
            }

            /**
             * Return true if local node is one of the backup nodes for given key.
             *
             * @param node Cluster node.
             * @param key Key to check.
             * @return True if local node is one of the backup nodes for given key.
             */
            bool IsBackup(cluster::ClusterNode node, const K& key)
            {
                return impl.Get()->IsBackup(node, key);
            }

            /**
             * Returns true if local node is primary or one of the backup nodes.
             * This method is essentially equivalent to calling
             * "isPrimary(ClusterNode, Object) || isBackup(ClusterNode, Object))",
             * however it is more efficient as it makes both checks at once.
             *
             * @param node Cluster node.
             * @param key Key to check.
             * @return True if local node is primary or one of the backup nodes.
             */
            bool IsPrimaryOrBackup(cluster::ClusterNode node, const K& key)
            {
                return impl.Get()->IsPrimaryOrBackup(node, key);
            }

            /**
             * Get partition ids for which the given cluster node has primary ownership.
             *
             * @param node Cluster node.
             * @return Container of partition ids for which the given cluster node has primary ownership.
             */
            std::vector<int32_t> GetPrimaryPartitions(cluster::ClusterNode node)
            {
                return impl.Get()->GetPrimaryPartitions(node);
            }

            /**
             * Get partition ids for which given cluster node has backup ownership.
             *
             * @param node Cluster node.
             * @return Container of partition ids for which given cluster node has backup ownership.
             */
            std::vector<int32_t> GetBackupPartitions(cluster::ClusterNode node)
            {
                return impl.Get()->GetBackupPartitions(node);
            }

            /**
             * Get partition ids for which given cluster node has any ownership (either primary or backup).
             *
             * @param node Cluster node.
             * @return Container of partition ids for which given cluster node has any ownership (either primary or backup).
             */
            std::vector<int32_t> GetAllPartitions(cluster::ClusterNode node)
            {
                return impl.Get()->GetAllPartitions(node);
            }

            /**
             * Map passed in key to a key which will be used for node affinity.
             *
             * @tparam TR Key to be used for node-to-affinity mapping type.
             *
             * @param key Key to map.
             * @return Key to be used for node-to-affinity mapping (may be the same key as passed in).
             */
            template <typename TR>
            TR GetAffinityKey(const K& key)
            {
                return impl.Get()->template GetAffinityKey<K, TR>(key);
            }

            /**
             * This method provides ability to detect which keys are mapped to which nodes.
             * Use it to determine which nodes are storing which keys prior to sending
             * jobs that access these keys.
             *
             * @param keys Keys to map to nodes.
             * @return Map of nodes to keys or empty map if there are no alive nodes for this cache.
             */
            std::map<cluster::ClusterNode, std::vector<K> > MapKeysToNodes(const std::vector<K>& keys)
            {
                return impl.Get()->MapKeysToNodes(keys);
            }

            /**
             * This method provides ability to detect to which primary node the given key is mapped.
             * Use it to determine which nodes are storing which keys prior to sending
             * jobs that access these keys.
             *
             * @param key Key to map to node.
             * @return Primary node for the key.
             */
            cluster::ClusterNode MapKeyToNode(const K& key)
            {
                return impl.Get()->MapKeyToNode(key);
            }

            /**
             * Get primary and backup nodes for the key.
             * Note that primary node is always first in the returned collection.
             *
             * @param key Key to map to nodes.
             * @return Collection of cluster nodes.
             */
            std::vector<cluster::ClusterNode> MapKeyToPrimaryAndBackups(const K& key)
            {
                return impl.Get()->MapKeyToPrimaryAndBackups(key);
            }

            /**
             * Get primary node for the given partition.
             *
             * @param part Partition id.
             * @return Primary node for the given partition.
             */
            cluster::ClusterNode MapPartitionToNode(int32_t part)
            {
                return impl.Get()->MapPartitionToNode(part);
            }

            /**
             * Get primary nodes for the given partitions.
             *
             * @param parts Partition ids.
             * @return Mapping of given partitions to their primary nodes.
             */
            std::map<int32_t, cluster::ClusterNode> MapPartitionsToNodes(const std::vector<int32_t>& parts)
            {
                return impl.Get()->MapPartitionsToNodes(parts);
            }

            /**
             * Get primary and backup nodes for partition.
             * Note that primary node is always first in the returned collection.
             *
             * @param part Partition to get affinity nodes for.
             * @return Collection of primary and backup nodes for partition with primary node always first.
             */
            std::vector<cluster::ClusterNode> MapPartitionToPrimaryAndBackups(int32_t part)
            {
                return impl.Get()->MapPartitionToPrimaryAndBackups(part);
            }

        private:
            impl::cache::SP_CacheAffinityImpl impl;
        };
    }
}

#endif //_IGNITE_CACHE_AFFINITY