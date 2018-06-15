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

#include <ignite/impl/thin/response_status.h>
#include <ignite/impl/thin/message.h>

#include <ignite/impl/thin/cache/cache_client_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                void CacheClientImpl::Put(const Writable& key, const Writable& value)
                {
                    CachePutRequest req(id, binary, key, value);
                    Response rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::Get(const Writable& key, Readable& value)
                {
                    CacheKeyRequest<RequestType::CACHE_GET> req(id, binary, key);
                    CacheGetResponse rsp(value);

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                bool CacheClientImpl::ContainsKey(const Writable& key)
                {
                    CacheKeyRequest<RequestType::CACHE_CONTAINS_KEY> req(id, binary, key);
                    BoolResponse rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                    return rsp.GetValue();
                }

                void CacheClientImpl::UpdatePartitions()
                {
                    std::vector<ConnectableNodePartitions> nodeParts;

                    CacheRequest<RequestType::CACHE_NODE_PARTITIONS> req(id, binary);
                    ClientCacheNodePartitionsResponse rsp(nodeParts);

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                    std::vector<ConnectableNodePartitions>::const_iterator it;

                    for (it = nodeParts.begin(); it != nodeParts.end(); ++it)
                    {
                        const std::vector<int32_t>& parts = it->GetPartitions();
                        const std::vector<net::EndPoint>& endPoints = it->GetEndPoints();

                        for (size_t i = 0; i < parts.size(); ++i)
                        {
                            assert(parts[i] >= 0);

                            size_t upart = static_cast<size_t>(parts[i]);

                            if (upart >= assignment.size())
                                assignment.resize(upart + 1);

                            std::vector<net::EndPoint>& dst = assignment[upart];

                            assert(dst.empty());

                            dst.insert(dst.end(), endPoints.begin(), endPoints.end());
                        }
                    }
                }
            }
        }
    }
}

