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

#ifndef _IGNITE_IMPL_THIN_CACHE_CACHE_AFFINITY_INFO
#define _IGNITE_IMPL_THIN_CACHE_CACHE_AFFINITY_INFO

#include <stdint.h>

#include <vector>

#include <ignite/common/concurrent.h>
#include <ignite/guid.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /* Forward declaration. */
            class WritableKey;

            /* Forward declaration. */
            class NodePartitions;

            namespace affinity
            {
                /**
                 * Cache Affinity Info.
                 */
                class AffinityAssignment
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param info Node partitions info.
                     */
                    AffinityAssignment(const std::vector<NodePartitions>& info);

                    /**
                     * Destructor.
                     */
                    ~AffinityAssignment();

                    /**
                     * Get number of partitions.
                     * 
                     * @return Number of partitions.
                     */
                    int32_t GetPartitionsNum() const;

                    /**
                     * Get mapping for the partition.
                     *
                     * @param part Partition.
                     * @return Mapping.
                     */
                    const Guid& GetNodeGuid(int32_t part) const;

                    /**
                     * Get mapping for the partition.
                     *
                     * @param key Key.
                     * @return Mapping.
                     */
                    const Guid& GetNodeGuid(const WritableKey& key) const;

                private:
                    /**
                     * Calculate partition for the key assuming it uses Rendezvous Affinity Function.
                     *
                     * @param key Key.
                     * @return Partition for the key.
                     */
                    int32_t GetPartitionForKey(const WritableKey& key) const;

                    /** Affinity assignment. */
                    std::vector<Guid> assignment;
                };

                /** Shared pointer to Cache Affinity Info. */
                typedef common::concurrent::SharedPointer<AffinityAssignment> SP_AffinityAssignment;
            }
        }
    }
}
#endif // _IGNITE_IMPL_THIN_CACHE_CACHE_AFFINITY_INFO
