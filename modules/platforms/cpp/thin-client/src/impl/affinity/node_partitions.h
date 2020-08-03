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

#ifndef _IGNITE_IMPL_THIN_NODE_PARTITIONS
#define _IGNITE_IMPL_THIN_NODE_PARTITIONS

#include <stdint.h>
#include <vector>

#include <ignite/guid.h>
#include <ignite/impl/binary/binary_reader_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * GUID of the node associated with cache partitions info.
             */
            class NodePartitions
            {
            public:
                /**
                 * Default constructor.
                 */
                NodePartitions() :
                    guid(),
                    partitions()
                {
                    // No-op.
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
                    guid = reader.ReadGuid();

                    int32_t partsNum = reader.ReadInt32();

                    partitions.clear();
                    partitions.reserve(partsNum);

                    for (int32_t i = 0; i < partsNum; ++i)
                        partitions.push_back(reader.ReadInt32());
                }

            private:
                /** Node GUID. */
                Guid guid;

                /** Cache partitions. */
                std::vector<int32_t> partitions;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_NODE_PARTITIONS