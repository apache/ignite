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

#include "ignite/impl/cache/cache_affinity_impl.h"

using namespace ignite::common;
using namespace ignite::common::concurrent;
using namespace ignite::cluster;
using namespace ignite::jni::java;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            CacheAffinityImpl::CacheAffinityImpl(SP_IgniteEnvironment env, jobject javaRef)
                : InteropTarget(env, javaRef)
            {
                // No-op.
            }

            int32_t CacheAffinityImpl::GetPartitions()
            {
                IgniteError err;

                int32_t ret = static_cast<int32_t>(OutInOpLong(Command::PARTITIONS, 0, err));

                IgniteError::ThrowIfNeeded(err);

                return ret;
            }

            std::vector<int32_t> CacheAffinityImpl::GetPrimaryPartitions(ClusterNode node)
            {
                return GetPartitions(Command::PRIMARY_PARTITIONS, node);
            }

            std::vector<int32_t> CacheAffinityImpl::GetBackupPartitions(ClusterNode node)
            {
                return GetPartitions(Command::BACKUP_PARTITIONS, node);
            }

            std::vector<int32_t> CacheAffinityImpl::GetAllPartitions(ClusterNode node)
            {
                return GetPartitions(Command::ALL_PARTITIONS, node);
            }

            ClusterNode CacheAffinityImpl::MapPartitionToNode(int32_t part)
            {
                Guid nodeId;
                In1Operation<int32_t> inOp(part);
                Out1Operation<Guid> outOp(nodeId);

                IgniteError err;
                InteropTarget::OutInOp(Command::MAP_PARTITION_TO_NODE, inOp, outOp, err);
                IgniteError::ThrowIfNeeded(err);

                return GetEnvironment().GetNode(nodeId);
            }

            std::map<int32_t, ClusterNode> CacheAffinityImpl::MapPartitionsToNodes(const std::vector<int32_t>& parts)
            {
                SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(memIn.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteInt32(static_cast<int32_t>(parts.size()));
                for (size_t i = 0; i < parts.size(); i++)
                    writer.WriteObject<int32_t>(parts.at(i));

                out.Synchronize();

                IgniteError err;
                InStreamOutStream(Command::MAP_PARTITIONS_TO_NODES, *memIn.Get(), *memOut.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                interop::InteropInputStream inStream(memOut.Get());
                binary::BinaryReaderImpl reader(&inStream);

                std::map<int32_t, ClusterNode> ret;

                int32_t cnt = reader.ReadInt32();
                for (int32_t i = 0; i < cnt; i++)
                {
                    int32_t key = reader.ReadInt32();
                    ClusterNode val(GetEnvironment().GetNode(reader.ReadGuid()));

                    ret.insert(std::pair<int32_t, ClusterNode>(key, val));
                }

                return ret;
            }

            std::vector<ClusterNode> CacheAffinityImpl::MapPartitionToPrimaryAndBackups(int32_t part)
            {
                SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(memIn.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteObject(part);

                out.Synchronize();

                IgniteError err;
                InStreamOutStream(Command::MAP_PARTITION_TO_PRIMARY_AND_BACKUPS, *memIn.Get(), *memOut.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                interop::InteropInputStream inStream(memOut.Get());
                binary::BinaryReaderImpl reader(&inStream);

                int32_t cnt = reader.ReadInt32();
                std::vector<ClusterNode> ret;
                ret.reserve(cnt);
                for (int32_t i = 0; i < cnt; i++)
                    ret.push_back(GetEnvironment().GetNode(reader.ReadGuid()));

                return ret;
            }

            std::vector<int32_t> CacheAffinityImpl::GetPartitions(int32_t opType, ClusterNode node)
            {
                SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(memIn.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteGuid(node.GetId());

                out.Synchronize();

                IgniteError err;
                InStreamOutStream(opType, *memIn.Get(), *memOut.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                interop::InteropInputStream inStream(memOut.Get());
                binary::BinaryReaderImpl reader(&inStream);

                reader.ReadInt8();
                int32_t cnt = reader.ReadInt32();
                std::vector<int32_t> ret;
                ret.reserve(cnt);
                for (int32_t i = 0; i < cnt; i++)
                    ret.push_back(reader.ReadInt32());

                return ret;
            }
        }
    }
}