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

#include "ignite/cluster/cluster_node.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace cluster
    {
        ClusterNode::ClusterNode(SharedPointer<ClusterNodeImpl> impl) :
            impl(impl)
        {
            // No-op.
        }

        const std::vector<std::string>& ClusterNode::GetAddresses() const
        {
            return impl.Get()->GetAddresses();
        }

        std::string ClusterNode::GetConsistentId() const
        {
            return impl.Get()->GetConsistentId();
        }

        const std::vector<std::string>& ClusterNode::GetHostNames() const
        {
            return impl.Get()->GetHostNames();
        }

        bool ClusterNode::IsAttributeSet(std::string name) const
        {
            return impl.Get()->IsAttributeSet(name);
        }

        std::vector<std::string> ClusterNode::GetAttributes() const
        {
            return impl.Get()->GetAttributes();
        }

        Guid ClusterNode::GetId() const
        {
            return impl.Get()->GetId();
        }

        bool ClusterNode::IsClient() const
        {
            return impl.Get()->IsClient();
        }

        bool ClusterNode::IsDaemon() const
        {
            return impl.Get()->IsDaemon();
        }

        bool ClusterNode::IsLocal() const
        {
            return impl.Get()->IsLocal();
        }

        int64_t ClusterNode::GetOrder() const
        {
            return impl.Get()->GetOrder();
        }

        const IgniteProductVersion& ClusterNode::GetVersion() const
        {
            return impl.Get()->GetVersion();
        }
    }
}
