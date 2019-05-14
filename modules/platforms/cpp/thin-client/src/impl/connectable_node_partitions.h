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

#ifndef _IGNITE_IMPL_THIN_CONNECTABLE_NODE_PARTITIONS
#define _IGNITE_IMPL_THIN_CONNECTABLE_NODE_PARTITIONS

#include <stdint.h>

#include "impl/net/end_point.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Address of the node, connectible for the thin client, associated
             * with cache partitions info.
             */
            class ConnectableNodePartitions
            {
            public:
                /**
                 * Default constructor.
                 */
                ConnectableNodePartitions() :
                    endPoints(),
                    partitions()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ConnectableNodePartitions()
                {
                    // No-op.
                }

                /**
                 * Get end points.
                 *
                 * @return End points.
                 */
                const std::vector<net::EndPoint>& GetEndPoints() const
                {
                    return endPoints;
                }

                /**
                 * Get cache partitions for this node.
                 *
                 * @return Cache partitions.
                 */
                const std::vector<int32_t>& GetPartitions() const
                {
                    return partitions;
                }

                /**
                 * Read from data stream, using provided reader.
                 *
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl& reader)
                {
                    int32_t port = reader.ReadInt32();

                    int32_t addrNum = reader.ReadInt32();

                    endPoints.clear();
                    endPoints.reserve(addrNum);

                    for (int32_t i = 0; i < addrNum; ++i)
                    {
                        std::string addr;
                        reader.ReadString(addr);

                        endPoints.push_back(net::EndPoint(addr, static_cast<uint16_t>(port)));
                    }

                    int32_t partsNum = reader.ReadInt32();

                    partitions.clear();
                    partitions.reserve(addrNum);

                    for (int32_t i = 0; i < partsNum; ++i)
                        partitions.push_back(reader.ReadInt32());
                }

            private:
                /** Node end points. */
                std::vector<net::EndPoint> endPoints;

                /** Cache partitions. */
                std::vector<int32_t> partitions;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CONNECTABLE_NODE_PARTITIONS