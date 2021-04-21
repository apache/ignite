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

#include "impl/affinity/node_partitions.h"
#include "impl/affinity/affinity_assignment.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace affinity
            {
                AffinityAssignment::AffinityAssignment(const std::vector<NodePartitions>& info)
                {
                    typedef std::vector<NodePartitions>::const_iterator InfoIterator;

                    for (InfoIterator it = info.begin(); it != info.end(); ++it)
                    {
                        const std::vector<int32_t>& parts = it->GetPartitions();
                        const Guid& guid = it->GetGuid();

                        for (size_t i = 0; i < parts.size(); ++i)
                        {
                            assert(parts[i] >= 0);

                            size_t uPart = static_cast<size_t>(parts[i]);

                            if (uPart >= assignment.size())
                                assignment.resize(uPart + 1);

                            assignment[uPart] = guid;
                        }
                    }
                }

                AffinityAssignment::~AffinityAssignment()
                {
                    // No-op.
                }

                int32_t AffinityAssignment::GetPartitionsNum() const
                {
                    return static_cast<int32_t>(assignment.size());
                }

                const Guid& AffinityAssignment::GetNodeGuid(int32_t part) const
                {
                    assert(part >= 0);
                    assert(static_cast<size_t>(part) < assignment.size());

                    return assignment[part];
                }

                const Guid& AffinityAssignment::GetNodeGuid(const WritableKey& key) const
                {
                    size_t part = static_cast<size_t>(GetPartitionForKey(key));

                    assert(part < assignment.size());

                    return assignment[part];
                }

                int32_t AffinityAssignment::GetPartitionForKey(const WritableKey& key) const
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

