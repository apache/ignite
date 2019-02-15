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

#include "impl/utility.h"
#include "impl/cache/cache_client_impl.h"
#include "impl/message.h"
#include "impl/response_status.h"

#include "impl/ignite_client_impl.h"

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
                CheckCacheName(name);

                int32_t cacheId = utility::GetCacheId(name);

                return MakeCacheImpl(router, name, cacheId);
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

                return MakeCacheImpl(router, name, cacheId);
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

                return MakeCacheImpl(router, name, cacheId);
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

                router.Get()->ReleaseAffinityMapping(cacheId);
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
                int32_t id)
            {
                cache::SP_CacheClientImpl cache(new cache::CacheClientImpl(router, name, id));

                cache.Get()->RefreshAffinityMapping();

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
