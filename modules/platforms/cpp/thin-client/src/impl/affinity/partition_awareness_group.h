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

#ifndef _IGNITE_IMPL_THIN_PARTITION_AWARENESS_GROUP
#define _IGNITE_IMPL_THIN_PARTITION_AWARENESS_GROUP

#include <stdint.h>
#include <vector>

#include <ignite/impl/binary/binary_reader_impl.h>

#include "impl/affinity/cache_affinity_configs.h"
#include "impl/affinity/node_partitions.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Partition awareness group.
             */
            class PartitionAwarenessGroup
            {
            public:
                /**
                 * Default constructor.
                 */
                PartitionAwarenessGroup() :
                    applicable(true)
                {
                    // No-op.
                }

                /**
                 * Check if the optimization applicable for the group.
                 *
                 * @return @c true if applicable.
                 */
                bool IsApplicable() const
                {
                    return applicable;
                }

                /**
                 * Get caches.
                 *
                 * @return Caches.
                 */
                const std::vector<CacheAffinityConfigs>& GetCaches() const
                {
                    return caches;
                }

                /**
                 * Get node partitions.
                 *
                 * @return Node partitions.
                 */
                const std::vector<NodePartitions>& GetNodePartitions() const
                {
                    return nodeParts;
                }

                /**
                 * Read from data stream, using provided reader.
                 *
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl& reader)
                {
                    applicable = reader.ReadBool();

                    int32_t cachesNum = reader.ReadInt32();

                    caches.clear();
                    caches.resize(static_cast<size_t>(cachesNum));

                    for (int32_t j = 0; j < cachesNum; ++j)
                        caches[j].Read(reader, applicable);

                    if (applicable)
                    {
                        int32_t nodeNum = reader.ReadInt32();

                        nodeParts.clear();
                        nodeParts.resize(static_cast<size_t>(nodeNum));

                        // Node Partitions
                        for (int32_t j = 0; j < nodeNum; ++j)
                            nodeParts[j].Read(reader);
                    }
                }

            private:
                /** Applicable for optimization. */
                bool applicable;

                /** Caches. */
                std::vector<CacheAffinityConfigs> caches; 

                /** Node partitions. */
                std::vector<NodePartitions> nodeParts;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_PARTITION_AWARENESS_GROUP