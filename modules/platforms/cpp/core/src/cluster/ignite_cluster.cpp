/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
        IgniteCluster::IgniteCluster(SharedPointer<ignite::impl::cluster::IgniteClusterImpl> impl) :
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

        void IgniteCluster::SetBaselineTopologyVersion(long topVer)
        {
            impl.Get()->SetBaselineTopologyVersion(topVer);
        }

        void IgniteCluster::SetTxTimeoutOnPartitionMapExchange(long timeout)
        {
            impl.Get()->SetTxTimeoutOnPartitionMapExchange(timeout);
        }

        bool IgniteCluster::PingNode(Guid nid)
        {
            return impl.Get()->PingNode(nid);
        }

        std::vector<ClusterNode> IgniteCluster::GetTopology(long version)
        {
            return impl.Get()->GetTopology(version);
        }

        long IgniteCluster::GetTopologyVersion()
        {
            return impl.Get()->GetTopologyVersion();
        }

        cluster::ClusterGroup IgniteCluster::AsClusterGroup()
        {
            return cluster::ClusterGroup(impl.Get()->AsClusterGroup());
        }
    }
}