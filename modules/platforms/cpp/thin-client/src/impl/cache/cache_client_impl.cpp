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

#include "impl/response_status.h"
#include "impl/message.h"
#include "impl/cache/cache_client_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                CacheClientImpl::CacheClientImpl(
                        const SP_DataRouter& router,
                        const std::string& name,
                        int32_t id) :
                    router(router),
                    name(name),
                    id(id),
                    binary(false)
                {
                    // No-op.
                }

                CacheClientImpl::~CacheClientImpl()
                {
                    // No-op.
                }

                template<typename ReqT, typename RspT>
                void CacheClientImpl::SyncCacheKeyMessage(const WritableKey& key, const ReqT& req, RspT& rsp)
                {
                    SP_CacheAffinityInfo affinityInfo = router.Get()->GetAffinityMapping(id);

                    if (!affinityInfo.IsValid() || affinityInfo.Get()->GetPartitionsNum() == 0)
                    {
                        router.Get()->SyncMessage(req, rsp);
                    }
                    else
                    {
                        const EndPoints& endPoints = affinityInfo.Get()->GetMapping(key);
                        
                        router.Get()->SyncMessage(req, rsp, endPoints);
                    }
                }

                void CacheClientImpl::Put(const WritableKey& key, const Writable& value)
                {
                    CachePutRequest req(id, binary, key, value);
                    Response rsp;

                    SyncCacheKeyMessage(key, req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::Get(const WritableKey& key, Readable& value)
                {
                    CacheKeyRequest<RequestType::CACHE_GET> req(id, binary, key);
                    CacheGetResponse rsp(value);

                    SyncCacheKeyMessage(key, req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                bool CacheClientImpl::ContainsKey(const WritableKey& key)
                {
                    CacheKeyRequest<RequestType::CACHE_CONTAINS_KEY> req(id, binary, key);
                    BoolResponse rsp;

                    SyncCacheKeyMessage(key, req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                    return rsp.GetValue();
                }

                int64_t CacheClientImpl::GetSize(int32_t peekModes)
                {
                    CacheGetSizeRequest req(id, binary, peekModes);
                    Int64Response rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                    return rsp.GetValue();
                }

                bool CacheClientImpl::Remove(const WritableKey& key)
                {
                    CacheKeyRequest<RequestType::CACHE_REMOVE_KEY> req(id, binary, key);
                    BoolResponse rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                    return rsp.GetValue();
                }

                void CacheClientImpl::RemoveAll()
                {
                    CacheRequest<RequestType::CACHE_REMOVE_ALL> req(id, binary);
                    Response rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::Clear(const WritableKey& key)
                {
                    CacheKeyRequest<RequestType::CACHE_CLEAR_KEY> req(id, binary, key);
                    Response rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::Clear()
                {
                    CacheRequest<RequestType::CACHE_CLEAR> req(id, binary);
                    Response rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::LocalPeek(const WritableKey& key, Readable& value)
                {
                    CacheKeyRequest<RequestType::CACHE_LOCAL_PEEK> req(id, binary, key);
                    CacheGetResponse rsp(value);

                    SyncCacheKeyMessage(key, req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::RefreshAffinityMapping()
                {
                    router.Get()->RefreshAffinityMapping(id, binary);
                }
            }
        }
    }
}

