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
        ClusterGroup::ClusterGroup(SP_ClusterGroupImpl impl) :
            impl(impl)
        {
            // No-op.
        }

        ClusterGroup ClusterGroup::ForAttribute(std::string name, std::string val)
        {
            return ClusterGroup(impl.Get()->ForAttribute(name, val));
        }

        ClusterGroup ClusterGroup::ForCacheNodes(std::string cacheName)
        {
            return ClusterGroup(impl.Get()->ForCacheNodes(cacheName));
        }

        ClusterGroup ClusterGroup::ForClientNodes(std::string cacheName)
        {
            return ClusterGroup(impl.Get()->ForClientNodes(cacheName));
        }

        ClusterGroup ClusterGroup::ForDaemons()
        {
            return ClusterGroup(impl.Get()->ForDaemons());
        }

        ClusterGroup ClusterGroup::ForDataNodes(std::string cacheName)
        {
            return ClusterGroup(impl.Get()->ForDataNodes(cacheName));
        }

        ClusterGroup ClusterGroup::ForHost(ClusterNode node)
        {
            return ClusterGroup(impl.Get()->ForHost(node));
        }

        ClusterGroup ClusterGroup::ForNode(ClusterNode node)
        {
            return ClusterGroup(impl.Get()->ForNode(node));
        }

        ClusterGroup ClusterGroup::ForNodeId(Guid id)
        {
            return ClusterGroup(impl.Get()->ForNodeId(id));
        }

        ClusterGroup ClusterGroup::ForNodeIds(std::vector<Guid> ids)
        {
            return ClusterGroup(impl.Get()->ForNodeIds(ids));
        }

        ClusterGroup ClusterGroup::ForNodes(std::vector<ClusterNode> nodes)
        {
            return ClusterGroup(impl.Get()->ForNodes(nodes));
        }

        ClusterGroup ClusterGroup::ForOldest()
        {
            return ClusterGroup(impl.Get()->ForOldest());
        }

        ClusterGroup ClusterGroup::ForRandom()
        {
            return ClusterGroup(impl.Get()->ForRandom());
        }

        ClusterGroup ClusterGroup::ForRemotes()
        {
            return ClusterGroup(impl.Get()->ForRemotes());
        }

        ClusterGroup ClusterGroup::ForServers()
        {
            return ClusterGroup(impl.Get()->ForServers());
        }

        ClusterGroup ClusterGroup::ForYoungest()
        {
            return ClusterGroup(impl.Get()->ForYoungest());
        }

        ClusterGroup ClusterGroup::ForCpp()
        {
            return ClusterGroup(impl.Get()->ForCpp());
        }

        ClusterNode ClusterGroup::GetNode()
        {
            return impl.Get()->GetNode();
        }

        ClusterNode ClusterGroup::GetNode(Guid nid)
        {
            return impl.Get()->GetNode(nid);
        }

        std::vector<ClusterNode> ClusterGroup::GetNodes()
        {
            return impl.Get()->GetNodes();
        }

        SP_ClusterGroupImpl ClusterGroup::GetImpl()
        {
            return impl;
        }
    }
}