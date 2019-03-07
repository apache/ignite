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

#include "impl/affinity/affinity_manager.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace affinity
            {
                AffinityManager::AffinityManager()
                {
                    // No-op.
                }

                void AffinityManager::UpdateAffinity(const std::vector<AffinityAwarenessGroup>& groups, 
                    AffinityTopologyVersion ver)
                {
                    if (topologyVersion > ver)
                        return;

                    common::concurrent::CsLockGuard lock(mutex);

                    if (topologyVersion < ver)
                    {
                        topologyVersion = ver;

                        cacheAffinityMapping.clear();
                    }

                    std::vector<AffinityAwarenessGroup>::const_iterator group;
                    for (group = groups.begin(); group != groups.end(); ++group)
                    {
                        SP_AffinityAssignment newMapping(new AffinityAssignment(group->GetNodePartitions()));

                        const std::vector<CacheAffinityConfigs>& caches = group->GetCaches();

                        std::vector<CacheAffinityConfigs>::const_iterator cache;
                        for (cache = caches.begin(); cache != caches.end(); ++cache)
                            cacheAffinityMapping[cache->GetCacheId()].Swap(newMapping);
                    }
                }

                SP_AffinityAssignment AffinityManager::GetAffinityAssignment(int32_t cacheId) const
                {
                    common::concurrent::CsLockGuard lock(mutex);
                    
                    std::map<int32_t, SP_AffinityAssignment>::const_iterator it = cacheAffinityMapping.find(cacheId);

                    if (it == cacheAffinityMapping.end())
                        return SP_AffinityAssignment();

                    return it->second;
                }

                void AffinityManager::StopTrackingCache(int32_t cacheId)
                {
                    common::concurrent::CsLockGuard lock(mutex);

                    std::map<int32_t, int32_t>::iterator it = trackedCaches.find(cacheId);

                    if (it == trackedCaches.end())
                        return;

                    --it->second;

                    if (it->second <= 0)
                        trackedCaches.erase(it);
                }

                void AffinityManager::GetTrackedCaches(std::vector<int32_t>& caches) const
                {
                    common::concurrent::CsLockGuard lock(mutex);

                    caches.reserve(trackedCaches.size());

                    std::map<int32_t, int32_t>::const_iterator cache;

                    for (cache = trackedCaches.begin(); cache != trackedCaches.end(); ++cache)
                        caches.push_back(cache->first);
                }
            }
        }
    }
}

