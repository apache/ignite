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


#include "impl/ignite_node.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            IgniteNode::IgniteNode()
            {
                // No-op.
            }

            IgniteNode::IgniteNode(const std::string& host, uint16_t port)
            {
                SetEndPoint(host, port);
            }

            IgniteNode::IgniteNode(const network::EndPoint& endPoint)
            {
                SetEndPoint(endPoint);
            }

            IgniteNode::IgniteNode(const Guid& guid) :
                guid(guid)
            {
                // No-op.
            }

            int64_t IgniteNode::Compare(const IgniteNode& other) const
            {
                if (IsLegacy() && other.IsLegacy())
                    return guid.Compare(other.guid);

                return endPoint.Compare(other.endPoint);
            }

            bool operator==(const IgniteNode& val1, const IgniteNode& val2)
            {
                return val1.Compare(val2) == 0;
            }

            bool operator!=(const IgniteNode& val1, const IgniteNode& val2)
            {
                return val1.Compare(val2) != 0;
            }

            bool operator<(const IgniteNode& val1, const IgniteNode& val2)
            {
                return val1.Compare(val2) < 0;
            }

            bool operator<=(const IgniteNode& val1, const IgniteNode& val2)
            {
                return val1.Compare(val2) <= 0;
            }

            bool operator>(const IgniteNode& val1, const IgniteNode& val2)
            {
                return val1.Compare(val2) > 0;
            }

            bool operator>=(const IgniteNode& val1, const IgniteNode& val2)
            {
                return val1.Compare(val2) >= 0;
            }
        }
    }
}

