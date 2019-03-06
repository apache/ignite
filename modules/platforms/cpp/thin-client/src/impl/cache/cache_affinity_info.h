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
