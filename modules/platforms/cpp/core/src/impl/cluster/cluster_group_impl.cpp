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

#include <ignite/cluster/cluster_group.h>
#include <ignite/cluster/cluster_node.h>

#include <ignite/impl/ignite_impl.h>
#include <ignite/impl/cluster/cluster_node_impl.h>
#include "ignite/impl/cluster/cluster_group_impl.h"

using namespace ignite;
using namespace ignite::common;
using namespace ignite::common::concurrent;
using namespace ignite::cluster;
using namespace ignite::jni::java;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {

            /** Attribute: platform. */
            const std::string attrPlatform = "org.apache.ignite.platform";

            /** Platform. */
            const std::string platform = "cpp";

            struct Command
            {
                enum Type
                {
                    FOR_ATTRIBUTE = 2,

                    FOR_CACHE = 3,

                    FOR_CLIENT = 4,

                    FOR_DATA = 5,

                    FOR_HOST = 6,

                    FOR_NODE_IDS = 7,

                    NODES = 12,

                    PING_NODE = 13,

                    TOPOLOGY = 14,

                    FOR_REMOTES = 17,

                    FOR_DAEMONS = 18,

                    FOR_RANDOM = 19,

                    FOR_OLDEST = 20,

                    FOR_YOUNGEST = 21,

                    RESET_METRICS = 22,

                    FOR_SERVERS = 23,

                    SET_ACTIVE = 28,

                    IS_ACTIVE = 29
                };
            };

            /**
             * Cluster node predicates holder.
             */
            class ClusterNodePredicateHolder : public IgnitePredicate<ClusterNode>
            {
                typedef common::concurrent::SharedPointer<IgnitePredicate<ClusterNode> > SP_Pred;
            public:
                /*
                 * Constructor.
                 */
                ClusterNodePredicateHolder()
                {
                    // No-op.
                }

                /**
                 * Check cluster node predicate.
                 *
                 * @param node Cluster node to check.
                 * @return True in case of positive result.
                 */
                bool operator()(ClusterNode& node)
                {
                    for (size_t i = 0; i < preds.size(); i++)
                        if (!preds.at(i).Get()->operator()(node))
                            return false;

                    return true;
                }

                /**
                 * Insert pointer to new predicate.
                 *
                 * @param pred Pointer to predicate object.
                 */
                void Insert(IgnitePredicate<ClusterNode>* pred)
                {
                    preds.push_back(SP_Pred(pred));
                }

                /**
                 * Insert predicates from another predicate holder.
                 *
                 * @param h Predicate holder object.
                 */
                void Insert(ClusterNodePredicateHolder& h)
                {
                    preds.insert(preds.end(), h.preds.begin(), h.preds.end());
                }

                /**
                 * Check if predicate holder is empty
                 *
                 * @return True if empty.
                 */
                bool IsEmpty()
                {
                    return preds.empty();
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ClusterNodePredicateHolder);

                /* Predicates container. */
                std::vector<SP_Pred> preds;
            };

            ClusterGroupImpl::ClusterGroupImpl(SP_IgniteEnvironment env, jobject javaRef) :
                InteropTarget(env, javaRef),
                predHolder(new ClusterNodePredicateHolder),
                nodes(),
                nodesLock(),
                topVer(0)
            {
                // No-op.
            }

            ClusterGroupImpl::~ClusterGroupImpl()
            {
                // No-op.
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForAttribute(std::string name, std::string val)
            {
                SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(mem.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteString(name);
                writer.WriteString(val);

                out.Synchronize();

                IgniteError err;
                jobject target = InStreamOutObject(Command::FOR_ATTRIBUTE, *mem.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), target));
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForCacheNodes(std::string cacheName)
            {
                return ForCacheNodes(cacheName, Command::FOR_CACHE);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForClientNodes(std::string cacheName)
            {
                return ForCacheNodes(cacheName, Command::FOR_CLIENT);
            }

            class IsClientPredicate : public IgnitePredicate<ClusterNode>
            {
            public:
                bool operator()(ClusterNode& node)
                {
                    return node.IsClient();
                }
            };

            SP_ClusterGroupImpl ClusterGroupImpl::ForClients()
            {
                return ForPredicate(new IsClientPredicate);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForDaemons()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_DAEMONS, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForDataNodes(std::string cacheName)
            {
                return ForCacheNodes(cacheName, Command::FOR_DATA);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForHost(ClusterNode node)
            {
                SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(mem.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteGuid(node.GetId());

                out.Synchronize();

                IgniteError err;
                jobject target = InStreamOutObject(Command::FOR_HOST, *mem.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), target));
            }

            class HostPredicate : public IgnitePredicate<ClusterNode>
            {
            public:
                bool operator()(ClusterNode& node)
                {
                    const std::vector<std::string>& hostNames = node.GetHostNames();
                    for (size_t i = 0; i < hostNames.size(); i++)
                        if (std::find(names.begin(), names.end(), hostNames.at(i)) != names.end())
                            return true;

                    return false;
                }

                HostPredicate(const std::string& hostName)
                {
                    names.push_back(hostName);
                }

                HostPredicate(std::vector<std::string> hostNames) :
                    names(hostNames)
                {
                    // No-op.
                }

            private:
                std::vector<std::string> names;
            };

            SP_ClusterGroupImpl ClusterGroupImpl::ForHost(std::string hostName)
            {
                return ForPredicate(new HostPredicate(hostName));
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForHosts(std::vector<std::string> hostNames)
            {
                return ForPredicate(new HostPredicate(hostNames));
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForNode(ClusterNode node)
            {
                return ForNodeId(node.GetId());
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForNodeId(Guid id)
            {
                std::vector<Guid> ids;

                ids.push_back(id);

                return ForNodeIds(ids);
            }

            struct WriteGuid
            {
                WriteGuid(binary::BinaryWriterImpl& writer) :
                    writer(writer)
                {
                    // No-op.
                }

                void operator()(Guid id)
                {
                    writer.WriteGuid(id);
                }

                binary::BinaryWriterImpl& writer;
            };

            SP_ClusterGroupImpl ClusterGroupImpl::ForNodeIds(std::vector<Guid> ids)
            {
                SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(mem.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteInt32(static_cast<int>(ids.size()));

                std::for_each(ids.begin(), ids.end(), WriteGuid(writer));

                out.Synchronize();

                IgniteError err;
                jobject target = InStreamOutObject(Command::FOR_NODE_IDS, *mem.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), target));
            }

            struct GetGuid
            {
                Guid operator()(ClusterNode& node)
                {
                    return node.GetId();
                }
            };

            SP_ClusterGroupImpl ClusterGroupImpl::ForNodes(std::vector<ClusterNode> nodes)
            {
                std::vector<Guid> ids;

                std::transform(nodes.begin(), nodes.end(), std::back_inserter(ids), GetGuid());

                return ForNodeIds(ids);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForOldest()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_OLDEST, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForPredicate(IgnitePredicate<ClusterNode>* pred)
            {
                SP_PredicateHolder newPredHolder(new ClusterNodePredicateHolder());

                newPredHolder.Get()->Insert(pred);
                newPredHolder.Get()->Insert(*predHolder.Get());

                std::vector<Guid> nodeIds;
                std::vector<ClusterNode> allNodes = GetNodes();
                for (size_t i = 0; i < allNodes.size(); i++)
                    if (newPredHolder.Get()->operator()(allNodes.at(i)))
                        nodeIds.push_back(allNodes.at(i).GetId());

                SP_ClusterGroupImpl ret;
                if (nodeIds.empty())
                    ret = GetEmptyClusterGroup();
                else
                    ret = ForNodeIds(nodeIds);

                ret.Get()->SetPredicate(newPredHolder);

                return ret;
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForRandom()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_RANDOM, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForRemotes()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_REMOTES, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForYoungest()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_YOUNGEST, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForServers()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_SERVERS, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForCpp()
            {
                return ForAttribute(attrPlatform, platform);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForLocal()
            {
                return ForNodeId(GetLocalNode().GetId());
            }

            ClusterNode ClusterGroupImpl::GetLocalNode()
            {
                RefreshNodes();

                return ClusterNode(GetEnvironment().GetLocalNode());
            }

            ClusterNode ClusterGroupImpl::GetNode()
            {
                std::vector<ClusterNode> nodes = GetNodes();
                if (nodes.size())
                    return nodes.at(0);

                const char* msg = "There are no available cluster nodes";
                throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, msg);
            }

            struct FindGuid
            {
                FindGuid(Guid id)
                    : id(id)
                {
                    // No-op.
                }

                bool operator()(ClusterNode& node)
                {
                    return node.GetId() == id;
                }

                Guid id;
            };

            ClusterNode ClusterGroupImpl::GetNode(Guid nid)
            {
                std::vector<ClusterNode> nodes = GetNodes();
                std::vector<ClusterNode>::iterator it = find_if(nodes.begin(), nodes.end(), FindGuid(nid));
                if (it != nodes.end())
                    return *it;

                const char* msg = "There is no cluster node with requested ID";
                throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, msg);
            }

            std::vector<ClusterNode> ClusterGroupImpl::GetNodes()
            {
                return RefreshNodes();
            }

            bool ClusterGroupImpl::IsActive()
            {
                IgniteError err;

                int64_t res = OutInOpLong(Command::IS_ACTIVE, 0, err);

                IgniteError::ThrowIfNeeded(err);

                return res == 1;
            }

            void ClusterGroupImpl::SetActive(bool active)
            {
                IgniteError err;

                OutInOpLong(Command::SET_ACTIVE, active ? 1 : 0, err);

                IgniteError::ThrowIfNeeded(err);
            }

            void ClusterGroupImpl::DisableWal(std::string cacheName)
            {
                IgniteImpl proc(GetEnvironmentPointer());

                proc.DisableWal(cacheName);
            }

            void ClusterGroupImpl::EnableWal(std::string cacheName)
            {
                IgniteImpl proc(GetEnvironmentPointer());

                proc.EnableWal(cacheName);
            }

            bool ClusterGroupImpl::IsWalEnabled(std::string cacheName)
            {
                IgniteImpl proc(GetEnvironmentPointer());

                return proc.IsWalEnabled(cacheName);
            }

            void ClusterGroupImpl::SetBaselineTopologyVersion(int64_t topVer)
            {
                IgniteImpl proc(GetEnvironmentPointer());

                proc.SetBaselineTopologyVersion(topVer);
            }

            void ClusterGroupImpl::SetTxTimeoutOnPartitionMapExchange(int64_t timeout)
            {
                IgniteImpl proc(GetEnvironmentPointer());

                proc.SetTxTimeoutOnPartitionMapExchange(timeout);
            }

            bool ClusterGroupImpl::PingNode(Guid nid)
            {
                IgniteError err;
                In1Operation<Guid> inOp(nid);

                return OutOp(Command::PING_NODE, inOp, err);
            }

            IgnitePredicate<ClusterNode>* ClusterGroupImpl::GetPredicate()
            {
                return predHolder.Get();
            }

            const IgnitePredicate<ClusterNode>* ClusterGroupImpl::GetPredicate() const
            {
                return predHolder.Get();
            }

            std::vector<ClusterNode> ClusterGroupImpl::GetTopology(int64_t version)
            {
                SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(memIn.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteInt64(version);

                out.Synchronize();

                IgniteError err;
                InStreamOutStream(Command::TOPOLOGY, *memIn.Get(), *memOut.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                interop::InteropInputStream inStream(memOut.Get());
                binary::BinaryReaderImpl reader(&inStream);

                return *ReadNodes(reader).Get();
            }

            int64_t ClusterGroupImpl::GetTopologyVersion()
            {
                RefreshNodes();

                return topVer;
            }

            SP_ClusterGroupImpl ClusterGroupImpl::GetEmptyClusterGroup()
            {
                // The empty cluster group could be created using ForNodeId()
                // method with not exist (Guid(0, 0)) Cluster Node Id.
                // It is required for ForPredicate() implementation
                // to avoid situation when two ClusterGroupImpl's uses same jobject.

                return ForNodeId(Guid(0, 0));
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForCacheNodes(std::string name, int32_t op)
            {
                SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(mem.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteString(name);

                out.Synchronize();

                IgniteError err;
                jobject target = InStreamOutObject(op, *mem.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), target));
            }

            SP_ClusterGroupImpl ClusterGroupImpl::FromTarget(jobject javaRef)
            {
                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), javaRef));
            }

            jobject ClusterGroupImpl::GetComputeProcessor()
            {
                return GetEnvironment().GetProcessorCompute(GetTarget());
            }

            ClusterGroupImpl::SP_ClusterNodes ClusterGroupImpl::ReadNodes(binary::BinaryReaderImpl& reader)
            {
                SP_ClusterNodes newNodes(new std::vector<ClusterNode>());

                int cnt = reader.ReadInt32();
                if (cnt < 0)
                    return newNodes;

                newNodes.Get()->reserve(cnt);
                for (int i = 0; i < cnt; i++)
                {
                    SP_ClusterNodeImpl impl = GetEnvironment().GetNode(reader.ReadGuid());
                    ClusterNode node(impl);
                    if (impl.IsValid() && predHolder.Get()->operator()(node))
                        newNodes.Get()->push_back(node);
                }

                return newNodes;
            }

            std::vector<ClusterNode> ClusterGroupImpl::RefreshNodes()
            {
                SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(memIn.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                CsLockGuard mtx(nodesLock);

                writer.WriteInt64(topVer);

                out.Synchronize();

                IgniteError err;
                InStreamOutStream(Command::NODES, *memIn.Get(), *memOut.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                interop::InteropInputStream inStream(memOut.Get());
                binary::BinaryReaderImpl reader(&inStream);

                bool wasUpdated = reader.ReadBool();
                if (wasUpdated)
                {
                    topVer = reader.ReadInt64();
                    nodes = ReadNodes(reader);
                }

                return *nodes.Get();
            }

            void ClusterGroupImpl::SetPredicate(SP_PredicateHolder predHolder)
            {
                this->predHolder = predHolder;
            }
        }
    }
}