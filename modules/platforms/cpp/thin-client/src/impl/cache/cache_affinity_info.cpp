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
                CacheAffinityInfo::CacheAffinityInfo(const std::vector<ConnectableNodePartitions>& info)
                {
                    std::vector<ConnectableNodePartitions>::const_iterator it;

                    for (it = info.begin(); it != info.end(); ++it)
                    {
                        const std::vector<int32_t>& parts = it->GetPartitions();
                        const std::vector<network::EndPoint>& endPoints = it->GetEndPoints();

                        for (size_t i = 0; i < parts.size(); ++i)
                        {
                            assert(parts[i] >= 0);

                            size_t upart = static_cast<size_t>(parts[i]);

                            if (upart >= affinityMapping.size())
                                affinityMapping.resize(upart + 1);

                            EndPoints& dst = affinityMapping[upart];

                            dst.insert(dst.end(), endPoints.begin(), endPoints.end());
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

                const EndPoints& CacheAffinityInfo::GetMapping(int32_t part) const
                {
                    assert(part >= 0);
                    assert(static_cast<size_t>(part) < affinityMapping.size());

                    return affinityMapping[part];
                }

                const EndPoints& CacheAffinityInfo::GetMapping(const WritableKey& key) const
                {
                    int32_t part = GetPartitionForKey(key);

                    return affinityMapping[part];
                }

                int32_t CacheAffinityInfo::GetPartitionForKey(const WritableKey& key) const
                {
                    int32_t hash = key.GetHashCode();
                    uint32_t uhash = static_cast<uint32_t>(hash);

                    int32_t parts = GetPartitionsNum();

                    int32_t part = 0;

                    if ((parts & (parts - 1)) == 0)
                    {
                        int32_t mask = parts - 1;

                        part = (uhash ^ (uhash >> 16)) & mask;
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

