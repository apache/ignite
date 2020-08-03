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

#ifndef _IGNITE_IMPL_THIN_IGNITE_NODE
#define _IGNITE_IMPL_THIN_IGNITE_NODE

#include <stdint.h>
#include <vector>

#include <ignite/guid.h>
#include <ignite/network/end_point.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Ignite node info.
             */
            class IgniteNode
            {
            public:
                /**
                 * Default constructor.
                 */
                IgniteNode();

                /**
                 * Constructor.
                 *
                 * Construct with end points.
                 * @param host Host.
                 * @param port TCP port.
                 */
                IgniteNode(const std::string& host, uint16_t port);

                /**
                 * Constructor.
                 *
                 * Construct with end points.
                 * @param endPoint End point.
                 */
                IgniteNode(const network::EndPoint& endPoint);

                /**
                 * Constructor.
                 *
                 * Construct with GUID.
                 * @param guid GUID.
                 */
                IgniteNode(const Guid& guid);

                /**
                 * Set node GUID.
                 *
                 * @param guid GUID.
                 */
                void SetGuid(const Guid& guid)
                {
                    this->guid = guid;
                }

                /**
                 * Get node GUID.
                 *
                 * @return GUID.
                 */
                const Guid& GetGuid() const
                {
                    return guid;
                }

                /**
                 * Set end point.
                 *
                 * @param host Host.
                 * @param port TCP port.
                 */
                void SetEndPoint(const std::string& host, uint16_t port)
                {
                    endPoint = network::EndPoint(host, port);
                }

                /**
                 * Set end point.
                 *
                 * @param endPoint End point.
                 */
                void SetEndPoint(const network::EndPoint& endPoint)
                {
                    this->endPoint = endPoint;
                }

                /**
                 * Get end point.
                 *
                 * @return End point.
                 */
                const network::EndPoint& GetEndPoint() const
                {
                    return endPoint;
                }

                /**
                 * Check if the guid is set for the instance.
                 */
                bool IsLegacy() const
                {
                    return guid.GetMostSignificantBits() == 0 && guid.GetLeastSignificantBits() == 0;
                }

                /**
                 * Compare to another value.
                 *
                 * @param other Instance to compare to.
                 * @return Zero if equals, negative number if less and positive if more.
                 */
                int64_t Compare(const IgniteNode& other) const;

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if equal.
                 */
                friend bool IGNITE_IMPORT_EXPORT operator==(const IgniteNode& val1, const IgniteNode& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if not equal.
                 */
                friend bool IGNITE_IMPORT_EXPORT operator!=(const IgniteNode& val1, const IgniteNode& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if less.
                 */
                friend bool IGNITE_IMPORT_EXPORT operator<(const IgniteNode& val1, const IgniteNode& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if less or equal.
                 */
                friend bool IGNITE_IMPORT_EXPORT operator<=(const IgniteNode& val1, const IgniteNode& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if greater.
                 */
                friend bool IGNITE_IMPORT_EXPORT operator>(const IgniteNode& val1, const IgniteNode& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if greater or equal.
                 */
                friend bool IGNITE_IMPORT_EXPORT operator>=(const IgniteNode& val1, const IgniteNode& val2);

            private:
                /** Address. */
                network::EndPoint endPoint;

                /** Guid. */
                Guid guid;
            };

            /** Ignite nodes. */
            typedef std::vector<IgniteNode> IgniteNodes;
        }
    }
}

#endif // _IGNITE_IMPL_THIN_IGNITE_NODE
