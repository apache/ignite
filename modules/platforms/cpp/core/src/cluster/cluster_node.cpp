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

#include "ignite/cluster/cluster_node.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace cluster
    {
        ClusterNode::ClusterNode(SharedPointer<ignite::impl::cluster::ClusterNodeImpl> impl) :
            impl(impl)
        {
            // No-op.
        }

        std::vector<std::string> ClusterNode::GetAddresses()
        {
            return impl.Get()->GetAddresses();
        }

        std::string ClusterNode::GetConsistentId()
        {
            return impl.Get()->GetConsistentId();
        }

        std::vector<std::string> ClusterNode::GetHostNames()
        {
            return impl.Get()->GetHostNames();
        }

        bool ClusterNode::IsAttributeSet(std::string name)
        {
            return impl.Get()->IsAttributeSet(name);
        }

        std::vector<std::string> ClusterNode::GetAttributes()
        {
            return impl.Get()->GetAttributes();
        }

        Guid ClusterNode::GetId()
        {
            return impl.Get()->GetId();
        }

        bool ClusterNode::IsClient()
        {
            return impl.Get()->IsClient();
        }

        bool ClusterNode::IsDaemon()
        {
            return impl.Get()->IsDaemon();
        }

        bool ClusterNode::IsLocal()
        {
            return impl.Get()->IsLocal();
        }

        long ClusterNode::GetOrder()
        {
            return impl.Get()->GetOrder();
        }

        const IgniteProductVersion& ClusterNode::GetVersion()
        {
            return impl.Get()->GetVersion();
        }
    }
}