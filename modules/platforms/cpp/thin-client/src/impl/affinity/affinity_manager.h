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

#ifndef _IGNITE_IMPL_THIN_AFFINITY_MANAGER
#define _IGNITE_IMPL_THIN_AFFINITY_MANAGER

#include <stdint.h>
#include <map>
#include <vector>

#include "impl/affinity/affinity_assignment.h"
#include "impl/affinity/affinity_topology_version.h"
#include "impl/affinity/partition_awareness_group.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace affinity
            {
                /**
                 * Affinity manager.
                 */
                class AffinityManager
                {
                public:
                    /**
                     * Default constructor.
                     */
                    AffinityManager();

                    /**
                     * Update affinity.
                     *
                     * @param ver Affinity topology version.
                     */
                    void UpdateAffinity(const AffinityTopologyVersion& ver);

                    /**
                     * Update affinity.
                     *
                     * @param groups Partition awareness groups.
                     * @param ver Affinity topology version.
                     */
                    void UpdateAffinity(const std::vector<PartitionAwarenessGroup>& groups, const AffinityTopologyVersion& ver);

                    /**
                     * Get affinity mapping for the cache.
                     *
                     * @param cacheId Cache ID.
                     * @return Mapping.
                     */
                    SP_AffinityAssignment GetAffinityAssignment(int32_t cacheId) const;

                private:
                    /** Cache affinity map. */
                    typedef std::map<int32_t, SP_AffinityAssignment> CacheAffinityMap;

                    /** Cache affinity map shared pointer. */
                    typedef common::concurrent::SharedPointer<CacheAffinityMap> SP_CacheAffinityMap;

                    /**
                     * Resets affinity mapping.
                     *
                     * @param ver Version.
                     */
                    void ResetAffinity(const AffinityTopologyVersion& ver);

                    /**
                     * Set affinity mapping.
                     *
                     * @param ver Version.
                     * @param cnt Update counter.
                     * @param affinity Affinity mapping.
                     * @return @c true if successful, and @c false, if affinity map was updated concurrently and must be
                     *    re-built.
                     */
                    bool UpdateAffinity(const AffinityTopologyVersion& ver, uint64_t cnt, SP_CacheAffinityMap& affinity);

                    /**
                     * Get affinity mapping.
                     *
                     * @return Affinity mapping.
                     */
                    SP_CacheAffinityMap GetAffinity() const;

                    /**
                     * Get affinity mapping.
                     *
                     * @param cnt Counter value.
                     * @return Affinity mapping.
                     */
                    SP_CacheAffinityMap GetAffinity(uint64_t& cnt) const;

                    /** Current affinity topology version. */
                    AffinityTopologyVersion topologyVersion;

                    /** Cache affinity mapping. */
                    SP_CacheAffinityMap cacheAffinity;

                    /** Cache affinity mapping read-write lock. */
                    mutable common::concurrent::ReadWriteLock affinityRwl;

                    /** Update counter to detect concurrent updates. */
                    mutable uint64_t updateCounter;
                };
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_AFFINITY_MANAGER
