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

#include "ignite/cluster/cluster_group.h"

#include "ignite/impl/cluster/cluster_node_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace cluster
    {
        ClusterGroup::ClusterGroup(impl::cluster::SP_ClusterGroupImpl impl) :
            impl(impl)
        {
            // No-op.
        }

        ClusterGroup ClusterGroup::ForAttribute(std::string name, std::string val)
        {
            return ClusterGroup(impl.Get()->ForAttribute(name, val));
        }

        ClusterGroup ClusterGroup::ForDataNodes(std::string cacheName)
        {
            return ClusterGroup(impl.Get()->ForDataNodes(cacheName));
        }

        ClusterGroup ClusterGroup::ForServers()
        {
            return ClusterGroup(impl.Get()->ForServers());
        }

        ClusterGroup ClusterGroup::ForCpp()
        {
            return ClusterGroup(impl.Get()->ForCpp());
        }

        std::vector<ClusterNode> ClusterGroup::GetNodes()
        {
            return impl.Get()->GetNodes();
        }

        impl::cluster::SP_ClusterGroupImpl ClusterGroup::GetImpl()
        {
            return impl;
        }
    }
}