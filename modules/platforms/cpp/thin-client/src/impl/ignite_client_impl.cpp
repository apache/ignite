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

#include "impl/utility.h"
#include "impl/message.h"
#include "impl/response_status.h"

#include "impl/ignite_client_impl.h"
#include "impl/cache/cache_client_impl.h"
#include "impl/compute/compute_client_impl.h"
#include "impl/transactions/transactions_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            IgniteClientImpl::IgniteClientImpl(const ignite::thin::IgniteClientConfiguration& cfg) :
                cfg(cfg),
                router(new DataRouter(cfg)),
                txImpl(new transactions::TransactionsImpl(router)),
                computeImpl(new compute::ComputeClientImpl(router))
            {
                // No-op.
            }

            IgniteClientImpl::~IgniteClientImpl()
            {
                // No-op.
            }

            void IgniteClientImpl::Start()
            {
                router.Get()->Connect();
            }

            cache::SP_CacheClientImpl IgniteClientImpl::GetCache(const char* name) const
            {
                CheckCacheName(name);

                int32_t cacheId = utility::GetCacheId(name);

                return MakeCacheImpl(router, txImpl, name, cacheId);
            }

            cache::SP_CacheClientImpl IgniteClientImpl::GetOrCreateCache(const char* name)
            {
                CheckCacheName(name);

                int32_t cacheId = utility::GetCacheId(name);

                GetOrCreateCacheWithNameRequest req(name);
                Response rsp;

                router.Get()->SyncMessage(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, rsp.GetError().c_str());

                return MakeCacheImpl(router, txImpl, name, cacheId);
            }

            cache::SP_CacheClientImpl IgniteClientImpl::CreateCache(const char* name)
            {
                CheckCacheName(name);

                int32_t cacheId = utility::GetCacheId(name);

                CreateCacheWithNameRequest req(name);
                Response rsp;

                router.Get()->SyncMessage(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, rsp.GetError().c_str());

                return MakeCacheImpl(router, txImpl, name, cacheId);
            }

            void IgniteClientImpl::DestroyCache(const char* name)
            {
                CheckCacheName(name);

                int32_t cacheId = utility::GetCacheId(name);

                DestroyCacheRequest req(cacheId);
                Response rsp;

                router.Get()->SyncMessage(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, rsp.GetError().c_str());
            }

            void IgniteClientImpl::GetCacheNames(std::vector<std::string>& cacheNames)
            {
                Request<RequestType::CACHE_GET_NAMES> req;
                GetCacheNamesResponse rsp(cacheNames);

                router.Get()->SyncMessage(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, rsp.GetError().c_str());
            }

            common::concurrent::SharedPointer<cache::CacheClientImpl> IgniteClientImpl::MakeCacheImpl(
                const SP_DataRouter& router,
                const transactions::SP_TransactionsImpl& tx,
                const std::string& name,
                int32_t id)
            {
                cache::SP_CacheClientImpl cache(new cache::CacheClientImpl(router, tx, name, id));

                return cache;
            }

            void IgniteClientImpl::CheckCacheName(const char* name)
            {
                if (!name || !strlen(name))
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, "Specified cache name is not allowed");
            }
        }
    }
}
