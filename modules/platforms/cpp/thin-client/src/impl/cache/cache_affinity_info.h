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

#ifndef _IGNITE_IMPL_THIN_CACHE_CACHE_AFFINITY_INFO
#define _IGNITE_IMPL_THIN_CACHE_CACHE_AFFINITY_INFO

#include <stdint.h>

#include <vector>

#include <ignite/common/concurrent.h>
#include <ignite/network/end_point.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /* Forward declaration. */
            class WritableKey;

            /* Forward declaration. */
            class ConnectableNodePartitions;

            namespace cache
            {
                /** End point collection. */
                typedef std::vector<network::EndPoint> EndPoints;

                /**
                 * Cache Affinity Info.
                 */
                class CacheAffinityInfo
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param info Node partitions info.
                     */
                    CacheAffinityInfo(const std::vector<ConnectableNodePartitions>& info);

                    /**
                     * Destructor.
                     */
                    ~CacheAffinityInfo();

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
                    const EndPoints& GetMapping(int32_t part) const;

                    /**
                     * Get mapping for the partition.
                     *
                     * @param key Key.
                     * @return Mapping.
                     */
                    const EndPoints& GetMapping(const WritableKey& key) const;

                    /**
                     * Calculate partition for the key assuming it uses Rendezvous Affinity Function.
                     *
                     * @param key Key.
                     * @return Partition for the key.
                     */
                    int32_t GetPartitionForKey(const WritableKey& key) const;

                private:
                    /** Affinity mapping. */
                    std::vector<EndPoints> affinityMapping;
                };

                /** Shared pointer to Cache Affinity Info. */
                typedef common::concurrent::SharedPointer<CacheAffinityInfo> SP_CacheAffinityInfo;
            }
        }
    }
}
#endif // _IGNITE_IMPL_THIN_CACHE_CACHE_AFFINITY_INFO
