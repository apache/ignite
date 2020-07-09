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
                AffinityManager::AffinityManager() :
                    cacheAffinity(new CacheAffinityMap),
                    updateCounter(0)
                {
                    // No-op.
                }

                void AffinityManager::UpdateAffinity(const AffinityTopologyVersion& ver)
                {
                    if (topologyVersion >= ver)
                        return;

                    ResetAffinity(ver);
                }

                void AffinityManager::UpdateAffinity(const std::vector<PartitionAwarenessGroup>& groups,
                    const AffinityTopologyVersion& ver)
                {
                    if (topologyVersion > ver)
                        return;

                    bool processed = false;

                    while (!processed)
                    {
                        uint64_t cnt;
                        SP_CacheAffinityMap oldAffinityPtr = GetAffinity(cnt);

                        SP_CacheAffinityMap newAffinityPtr(new CacheAffinityMap(*oldAffinityPtr.Get()));

                        CacheAffinityMap& newAffinity = *newAffinityPtr.Get();

                        std::vector<PartitionAwarenessGroup>::const_iterator group;
                        for (group = groups.begin(); group != groups.end(); ++group)
                        {
                            SP_AffinityAssignment newMapping(new AffinityAssignment(group->GetNodePartitions()));

                            const std::vector<CacheAffinityConfigs>& caches = group->GetCaches();

                            std::vector<CacheAffinityConfigs>::const_iterator cache;
                            for (cache = caches.begin(); cache != caches.end(); ++cache)
                                newAffinity[cache->GetCacheId()].Swap(newMapping);
                        }

                        processed = UpdateAffinity(ver, cnt, newAffinityPtr);
                    }
                }

                SP_AffinityAssignment AffinityManager::GetAffinityAssignment(int32_t cacheId) const
                {
                    SP_CacheAffinityMap ptr = GetAffinity();

                    CacheAffinityMap& cacheAffinity0 = *ptr.Get();

                    CacheAffinityMap::const_iterator it = cacheAffinity0.find(cacheId);

                    if (it == cacheAffinity0.end())
                        return SP_AffinityAssignment();

                    return it->second;
                }

                void AffinityManager::ResetAffinity(const AffinityTopologyVersion& ver)
                {
                    common::concurrent::RwExclusiveLockGuard lock(affinityRwl);

                    if (topologyVersion > ver)
                        return;

                    topologyVersion = ver;

                    SP_CacheAffinityMap empty(new CacheAffinityMap);

                    cacheAffinity.Swap(empty);

                    ++updateCounter;
                }

                bool AffinityManager::UpdateAffinity(const AffinityTopologyVersion& ver, uint64_t cnt,
                    SP_CacheAffinityMap& affinity)
                {
                    common::concurrent::RwExclusiveLockGuard lock(affinityRwl);

                    if (topologyVersion > ver)
                        return true;

                    if (updateCounter != cnt)
                        return false;

                    topologyVersion = ver;

                    cacheAffinity.Swap(affinity);

                    ++updateCounter;

                    return true;
                }

                AffinityManager::SP_CacheAffinityMap AffinityManager::GetAffinity() const
                {
                    common::concurrent::RwSharedLockGuard lock(affinityRwl);

                    SP_CacheAffinityMap ptr(cacheAffinity);

                    lock.Reset();

                    return ptr;
                }

                AffinityManager::SP_CacheAffinityMap AffinityManager::GetAffinity(uint64_t& cnt) const
                {
                    common::concurrent::RwSharedLockGuard lock(affinityRwl);

                    SP_CacheAffinityMap ptr(cacheAffinity);

                    cnt = updateCounter;

                    lock.Reset();

                    return ptr;
                }
            }
        }
    }
}

