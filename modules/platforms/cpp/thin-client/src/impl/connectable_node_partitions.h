/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_IMPL_THIN_CONNECTABLE_NODE_PARTITIONS
#define _IGNITE_IMPL_THIN_CONNECTABLE_NODE_PARTITIONS

#include <stdint.h>

#include <ignite/network/end_point.h>

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
                const std::vector<network::EndPoint>& GetEndPoints() const
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

                        endPoints.push_back(network::EndPoint(addr, static_cast<uint16_t>(port)));
                    }

                    int32_t partsNum = reader.ReadInt32();

                    partitions.clear();
                    partitions.reserve(addrNum);

                    for (int32_t i = 0; i < partsNum; ++i)
                        partitions.push_back(reader.ReadInt32());
                }

            private:
                /** Node end points. */
                std::vector<network::EndPoint> endPoints;

                /** Cache partitions. */
                std::vector<int32_t> partitions;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CONNECTABLE_NODE_PARTITIONS