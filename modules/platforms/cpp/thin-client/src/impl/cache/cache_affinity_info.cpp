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

#include <ignite/impl/thin/writable_key.h>

#include "impl/connectable_node_partitions.h"
#include "impl/cache/cache_affinity_info.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                CacheAffinityInfo::CacheAffinityInfo(const std::vector<NodePartitions>& info)
                {
                    typedef std::vector<NodePartitions>::const_iterator InfoIterator;

                    for (InfoIterator it = info.begin(); it != info.end(); ++it)
                    {
                        const std::vector<int32_t>& parts = it->GetPartitions();
                        const network::EndPoints& endPoints = it->GetEndPoints();

                        for (size_t i = 0; i < parts.size(); ++i)
                        {
                            assert(parts[i] >= 0);

                            size_t uPart = static_cast<size_t>(parts[i]);

                            if (uPart >= affinityMapping.size())
                                affinityMapping.resize(uPart + 1);

                            IgniteNodes& dst = affinityMapping[uPart];

                            for (network::EndPoints::const_iterator ep = endPoints.begin(); ep != endPoints.end(); ++ep)
                                dst.push_back(IgniteNode(*ep));
                        }
                    }
                }

                CacheAffinityInfo::~CacheAffinityInfo()
                {
                    // No-op.
                }

                int32_t CacheAffinityInfo::GetPartitionsNum() const
                {
                    return static_cast<int32_t>(affinityMapping.size());
                }

                const IgniteNodes& CacheAffinityInfo::GetMapping(int32_t part) const
                {
                    assert(part >= 0);
                    assert(static_cast<size_t>(part) < affinityMapping.size());

                    return affinityMapping[part];
                }

                const IgniteNodes& CacheAffinityInfo::GetMapping(const WritableKey& key) const
                {
                    size_t part = static_cast<size_t>(GetPartitionForKey(key));

                    assert(part < affinityMapping.size());

                    return affinityMapping[part];
                }

                int32_t CacheAffinityInfo::GetPartitionForKey(const WritableKey& key) const
                {
                    int32_t hash = key.GetHashCode();
                    uint32_t uHash = static_cast<uint32_t>(hash);

                    int32_t parts = GetPartitionsNum();

                    int32_t part = 0;

                    if ((parts & (parts - 1)) == 0)
                    {
                        int32_t mask = parts - 1;

                        part = (uHash ^ (uHash >> 16)) & mask;
                    }
                    else
                    {
                        part = std::abs(hash % parts);

                        if (part < 0)
                            part = 0;
                    }

                    return part;
                }
            }
        }
    }
}

