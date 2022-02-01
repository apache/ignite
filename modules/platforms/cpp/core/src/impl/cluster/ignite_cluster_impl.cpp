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

#include "ignite/cluster/cluster_group.h"

#include "ignite/impl/cluster/ignite_cluster_impl.h"

using namespace ignite::jni::java;
using namespace ignite::cluster;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {
            IgniteClusterImpl::IgniteClusterImpl(SP_ClusterGroupImpl impl) :
                impl(impl)
            {
                // No-op.
            }

            IgniteClusterImpl::~IgniteClusterImpl()
            {
                // No-op.
            }

            bool IgniteClusterImpl::IsActive()
            {
                return impl.Get()->IsActive();
            }

            void IgniteClusterImpl::SetActive(bool active)
            {
                impl.Get()->SetActive(active);
            }

            void IgniteClusterImpl::DisableWal(std::string cacheName)
            {
                impl.Get()->DisableWal(cacheName);
            }

            void IgniteClusterImpl::EnableWal(std::string cacheName)
            {
                impl.Get()->EnableWal(cacheName);
            }

            bool IgniteClusterImpl::IsWalEnabled(std::string cacheName)
            {
                return impl.Get()->IsWalEnabled(cacheName);
            }

            ClusterGroup IgniteClusterImpl::ForLocal()
            {
                return impl.Get()->ForLocal();
            }

            ClusterNode IgniteClusterImpl::GetLocalNode()
            {
                return impl.Get()->GetLocalNode();
            }

            void IgniteClusterImpl::SetBaselineTopologyVersion(int64_t topVer)
            {
                impl.Get()->SetBaselineTopologyVersion(topVer);
            }

            void IgniteClusterImpl::SetTxTimeoutOnPartitionMapExchange(int64_t timeout)
            {
                impl.Get()->SetTxTimeoutOnPartitionMapExchange(timeout);
            }

            bool IgniteClusterImpl::PingNode(Guid nid)
            {
                return impl.Get()->PingNode(nid);
            }

            std::vector<ClusterNode> IgniteClusterImpl::GetTopology(int64_t version)
            {
                return impl.Get()->GetTopology(version);
            }

            int64_t IgniteClusterImpl::GetTopologyVersion()
            {
                return impl.Get()->GetTopologyVersion();
            }

            SP_ClusterGroupImpl IgniteClusterImpl::AsClusterGroup()
            {
                return impl;
            }
        }
    }
}
