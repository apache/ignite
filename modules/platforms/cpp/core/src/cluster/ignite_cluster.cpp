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

#include "ignite/cluster/ignite_cluster.h"

using namespace ignite::common::concurrent;
using namespace ignite::cluster;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace cluster
    {
        IgniteCluster::IgniteCluster(SharedPointer<IgniteClusterImpl> impl) :
            impl(impl)
        {
            // No-op.
        }

        bool IgniteCluster::IsActive()
        {
            return impl.Get()->IsActive();
        }

        void IgniteCluster::SetActive(bool active)
        {
            impl.Get()->SetActive(active);
        }

        void IgniteCluster::DisableWal(std::string cacheName)
        {
            impl.Get()->DisableWal(cacheName);
        }

        void IgniteCluster::EnableWal(std::string cacheName)
        {
            impl.Get()->EnableWal(cacheName);
        }

        bool IgniteCluster::IsWalEnabled(std::string cacheName)
        {
            return impl.Get()->IsWalEnabled(cacheName);
        }

        ClusterGroup IgniteCluster::ForLocal()
        {
            return impl.Get()->ForLocal();
        }

        ClusterNode IgniteCluster::GetLocalNode()
        {
            return impl.Get()->GetLocalNode();
        }

        void IgniteCluster::SetBaselineTopologyVersion(int64_t topVer)
        {
            impl.Get()->SetBaselineTopologyVersion(topVer);
        }

        void IgniteCluster::SetTxTimeoutOnPartitionMapExchange(int64_t timeout)
        {
            impl.Get()->SetTxTimeoutOnPartitionMapExchange(timeout);
        }

        bool IgniteCluster::PingNode(Guid nid)
        {
            return impl.Get()->PingNode(nid);
        }

        std::vector<ClusterNode> IgniteCluster::GetTopology(int64_t version)
        {
            return impl.Get()->GetTopology(version);
        }

        int64_t IgniteCluster::GetTopologyVersion()
        {
            return impl.Get()->GetTopologyVersion();
        }

        ClusterGroup IgniteCluster::AsClusterGroup()
        {
            return ClusterGroup(impl.Get()->AsClusterGroup());
        }
    }
}
