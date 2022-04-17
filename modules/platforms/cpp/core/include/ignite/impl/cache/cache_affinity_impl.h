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

#ifndef _IGNITE_CACHE_AFFINITY_IMPL
#define _IGNITE_CACHE_AFFINITY_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

#include <ignite/impl/interop/interop_target.h>
#include <ignite/cluster/cluster_node.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            struct Command
            {
                enum Type
                {
                    AFFINITY_KEY = 1,

                    ALL_PARTITIONS = 2,

                    BACKUP_PARTITIONS = 3,

                    IS_BACKUP = 4,

                    IS_PRIMARY = 5,

                    IS_PRIMARY_OR_BACKUP = 6,

                    MAP_KEY_TO_NODE = 7,

                    MAP_KEY_TO_PRIMARY_AND_BACKUPS = 8,

                    MAP_KEYS_TO_NODES = 9,

                    MAP_PARTITION_TO_NODE = 10,

                    MAP_PARTITION_TO_PRIMARY_AND_BACKUPS = 11,

                    MAP_PARTITIONS_TO_NODES = 12,

                    PARTITION = 13,

                    PRIMARY_PARTITIONS = 14,

                    PARTITIONS = 15,
                };
            };

            /* Forward declaration. */
            class CacheAffinityImpl;

            /* Shared pointer. */
            typedef common::concurrent::SharedPointer<CacheAffinityImpl> SP_CacheAffinityImpl;

            /**
             * Cache affinity implementation.
             */
            class IGNITE_FRIEND_EXPORT CacheAffinityImpl : private interop::InteropTarget
            {
                typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                CacheAffinityImpl(SP_IgniteEnvironment env, jobject javaRef);

                /**
                 * Get number of partitions in cache according to configured affinity function.
                 *
                 * @return Number of partitions.
                 */
                int32_t GetPartitions();

                /**
                 * Get partition id for the given key.
                 *
                 * @tparam K Key type.
                 *
                 * @param key Key to get partition id for.
                 * @return Partition id.
                 */
                template <typename K>
                int32_t GetPartition(const K& key)
                {
                    IgniteError err;
                    In1Operation<K> inOp(key);

                    int32_t ret = static_cast<int32_t>(InStreamOutLong(Command::PARTITION, inOp, err));
                    IgniteError::ThrowIfNeeded(err);

                    return ret;
                }

                /**
                 * Return true if given node is the primary node for given key.
                 *
                 * @tparam K Key type.
                 *
                 * @param node Cluster node.
                 * @param key Key to check.
                 * @return True if given node is primary node for given key.
                 */
                template <typename K>
                bool IsPrimary(ignite::cluster::ClusterNode node, const K& key)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteObject(node.GetId());
                    writer.WriteObject<K>(key);

                    out.Synchronize();

                    IgniteError err;

                    bool ret = OutOp(Command::IS_PRIMARY, *memIn.Get(), err);

                    IgniteError::ThrowIfNeeded(err);

                    return ret;
                }

                /**
                 * Return true if local node is one of the backup nodes for given key.
                 *
                 * @tparam K Key type.
                 *
                 * @param node Cluster node.
                 * @param key Key to check.
                 * @return True if local node is one of the backup nodes for given key.
                 */
                template <typename K>
                bool IsBackup(ignite::cluster::ClusterNode node, const K &key)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteObject(node.GetId());
                    writer.WriteObject<K>(key);

                    out.Synchronize();

                    IgniteError err;

                    bool ret = OutOp(Command::IS_BACKUP, *memIn.Get(), err);

                    IgniteError::ThrowIfNeeded(err);

                    return ret;
                }

                /**
                 * Returns true if local node is primary or one of the backup nodes.
                 * This method is essentially equivalent to calling
                 * "isPrimary(ClusterNode, Object) || isBackup(ClusterNode, Object))",
                 * however it is more efficient as it makes both checks at once.
                 *
                 * @tparam K Key type.
                 *
                 * @param node Cluster node.
                 * @param key Key to check.
                 * @return True if local node is primary or one of the backup nodes.
                 */
                template <typename K>
                bool IsPrimaryOrBackup(ignite::cluster::ClusterNode node, const K& key)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteObject(node.GetId());
                    writer.WriteObject<K>(key);

                    out.Synchronize();

                    IgniteError err;

                    bool ret = OutOp(Command::IS_PRIMARY_OR_BACKUP, *memIn.Get(), err);

                    IgniteError::ThrowIfNeeded(err);

                    return ret;
                }

                /**
                 * Get partition ids for which the given cluster node has primary ownership.
                 *
                 * @param node Cluster node.
                 * @return Container of partition ids for which the given cluster node has primary ownership.
                 */
                std::vector<int32_t> GetPrimaryPartitions(ignite::cluster::ClusterNode node);

                /**
                 * Get partition ids for which given cluster node has backup ownership.
                 *
                 * @param node Cluster node.
                 * @return Container of partition ids for which given cluster node has backup ownership.
                 */
                std::vector<int32_t> GetBackupPartitions(ignite::cluster::ClusterNode node);

                /**
                 * Get partition ids for which given cluster node has any ownership (either primary or backup).
                 *
                 * @param node Cluster node.
                 * @return Container of partition ids for which given cluster node has any ownership (either primary or backup).
                 */
                std::vector<int32_t> GetAllPartitions(ignite::cluster::ClusterNode node);

                /**
                 * Map passed in key to a key which will be used for node affinity.
                 *
                 * @tparam TK Key to map type.
                 * @tparam TR Key to be used for node-to-affinity mapping type.
                 *
                 * @param key Key to map.
                 * @return Key to be used for node-to-affinity mapping (may be the same key as passed in).
                 */
                template <typename TK, typename TR>
                TR GetAffinityKey(const TK& key)
                {
                    TR ret;
                    In1Operation<TK> inOp(key);
                    Out1Operation<TR> outOp(ret);

                    IgniteError err;
                    InteropTarget::OutInOp(Command::AFFINITY_KEY, inOp, outOp, err);
                    IgniteError::ThrowIfNeeded(err);

                    return ret;
                }

                /**
                 * This method provides ability to detect which keys are mapped to which nodes.
                 * Use it to determine which nodes are storing which keys prior to sending
                 * jobs that access these keys.
                 *
                 * @tparam TK Key to map type.
                 *
                 * @param keys Keys to map to nodes.
                 * @return Map of nodes to keys or empty map if there are no alive nodes for this cache.
                 */
                template<typename TK>
                std::map<ignite::cluster::ClusterNode, std::vector<TK> > MapKeysToNodes(const std::vector<TK>& keys)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    common::concurrent::SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteInt32(static_cast<int32_t>(keys.size()));
                    for (typename std::vector<TK>::const_iterator it = keys.begin(); it != keys.end(); ++it)
                        writer.WriteObject<TK>(*it);

                    out.Synchronize();

                    IgniteError err;
                    InStreamOutStream(Command::MAP_KEYS_TO_NODES, *memIn.Get(), *memOut.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    interop::InteropInputStream inStream(memOut.Get());
                    binary::BinaryReaderImpl reader(&inStream);

                    std::map<ignite::cluster::ClusterNode, std::vector<TK> > ret;

                    int32_t cnt = reader.ReadInt32();
                    for (int32_t i = 0; i < cnt; i++)
                    {
                        std::vector<TK> val;

                        ignite::cluster::ClusterNode key(GetEnvironment().GetNode(reader.ReadGuid()));
                        reader.ReadCollection<TK>(std::back_inserter(val));

                        ret.insert(std::pair<ignite::cluster::ClusterNode, std::vector<TK> >(key, val));
                    }

                    return ret;
                }

                /**
                 * This method provides ability to detect to which primary node the given key is mapped.
                 * Use it to determine which nodes are storing which keys prior to sending
                 * jobs that access these keys.
                 *
                 * @tparam TK Key to map type.
                 *
                 * @param key Key to map to node.
                 * @return Primary node for the key.
                 */
                template <typename TK>
                ignite::cluster::ClusterNode MapKeyToNode(const TK& key)
                {
                    Guid nodeId;
                    In1Operation<TK> inOp(key);
                    Out1Operation<Guid> outOp(nodeId);

                    IgniteError err;
                    InteropTarget::OutInOp(Command::MAP_KEY_TO_NODE, inOp, outOp, err);
                    IgniteError::ThrowIfNeeded(err);

                    return GetEnvironment().GetNode(nodeId);
                }

                /**
                 * Get primary and backup nodes for the key.
                 * Note that primary node is always first in the returned collection.
                 *
                 * @tparam TK Key to map type.
                 *
                 * @param key Key to map to nodes.
                 * @return Collection of cluster nodes.
                 */
                template <typename TK>
                std::vector<ignite::cluster::ClusterNode> MapKeyToPrimaryAndBackups(const TK& key)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    common::concurrent::SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteObject(key);

                    out.Synchronize();

                    IgniteError err;
                    InStreamOutStream(Command::MAP_KEY_TO_PRIMARY_AND_BACKUPS, *memIn.Get(), *memOut.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    interop::InteropInputStream inStream(memOut.Get());
                    binary::BinaryReaderImpl reader(&inStream);

                    std::vector<ignite::cluster::ClusterNode> ret;
                    int32_t cnt = reader.ReadInt32();
                    ret.reserve(cnt);
                    for (int32_t i = 0; i < cnt; i++)
                        ret.push_back(GetEnvironment().GetNode(reader.ReadGuid()));

                    return ret;
                }

                /**
                 * Get primary node for the given partition.
                 *
                 * @param part Partition id.
                 * @return Primary node for the given partition.
                 */
                ignite::cluster::ClusterNode MapPartitionToNode(int32_t part);

                /**
                 * Get primary nodes for the given partitions.
                 *
                 * @param parts Partition ids.
                 * @return Mapping of given partitions to their primary nodes.
                 */
                std::map<int32_t, ignite::cluster::ClusterNode> MapPartitionsToNodes(const std::vector<int32_t>& parts);

                /**
                 * Get primary and backup nodes for partition.
                 * Note that primary node is always first in the returned collection.
                 *
                 * @param part Partition to get affinity nodes for.
                 * @return Collection of primary and backup nodes for partition with primary node always first.
                 */
                std::vector<ignite::cluster::ClusterNode> MapPartitionToPrimaryAndBackups(int32_t part);

            private:
                /**
                 * Get partition ids for which given cluster node has different ownership.
                 *
                 * @param opType Operation type.
                 * @param node Cluster node.
                 * @return Container of partition ids.
                 */
                std::vector<int32_t> GetPartitions(int32_t opType, ignite::cluster::ClusterNode node);
            };
        }
    }
}

#endif // _IGNITE_CACHE_AFFINITY_IMPL