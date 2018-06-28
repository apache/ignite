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

#include <ignite/impl/thin/utility.h>
#include <ignite/impl/thin/ignite_client_impl.h>
#include <ignite/impl/thin/cache/cache_client_impl.h>
#include <ignite/impl/thin/message.h>
#include <ignite/impl/thin/response_status.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            IgniteClientImpl::IgniteClientImpl(const ignite::thin::IgniteClientConfiguration& cfg) :
                cfg(cfg),
                router(new DataRouter(cfg))
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
                ignite::thin::cache::CacheClientConfiguration cacheConfig;

                return GetCache(name, cacheConfig);
            }

            common::concurrent::SharedPointer<cache::CacheClientImpl> IgniteClientImpl::GetCache(const char* name,
                const ignite::thin::cache::CacheClientConfiguration& config) const
            {
                CheckCacheName(name);

                int32_t cacheId = utility::GetCacheId(name);

                return MakeCacheImpl(router, name, config, cacheId);
            }

            cache::SP_CacheClientImpl IgniteClientImpl::GetOrCreateCache(const char* name)
            {
                ignite::thin::cache::CacheClientConfiguration cacheConfig;

                return GetOrCreateCache(name, cacheConfig);
            }

            common::concurrent::SharedPointer<cache::CacheClientImpl> IgniteClientImpl::GetOrCreateCache(const char* name,
                const ignite::thin::cache::CacheClientConfiguration& config)
            {
                CheckCacheName(name);
                
                int32_t cacheId = utility::GetCacheId(name);

                GetOrCreateCacheWithNameRequest req(name);
                Response rsp;

                router.Get()->SyncMessage(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, rsp.GetError().c_str());
                
                return MakeCacheImpl(router, name, config, cacheId);
            }

            cache::SP_CacheClientImpl IgniteClientImpl::CreateCache(const char* name)
            {
                ignite::thin::cache::CacheClientConfiguration cacheConfig;

                return CreateCache(name, cacheConfig);
            }

            common::concurrent::SharedPointer<cache::CacheClientImpl> IgniteClientImpl::CreateCache(const char* name,
                const ignite::thin::cache::CacheClientConfiguration& config)
            {
                CheckCacheName(name);

                int32_t cacheId = utility::GetCacheId(name);

                CreateCacheWithNameRequest req(name);
                Response rsp;

                router.Get()->SyncMessage(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, rsp.GetError().c_str());

                return MakeCacheImpl(router, name, config, cacheId);
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
                const std::string& name,
                const ignite::thin::cache::CacheClientConfiguration& config,
                int32_t id)
            {
                cache::SP_CacheClientImpl cache(new cache::CacheClientImpl(router, name, config, id));

                if (config.IsLessenLatency())
                    cache.Get()->UpdatePartitions();

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
