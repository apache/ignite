/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

#include "impl/ignite_client_impl.h"
#include "impl/cache/cache_client_impl.h"

using namespace ignite::impl::thin;
using namespace cache;
using namespace ignite::common::concurrent;

namespace
{
    IgniteClientImpl& GetClientImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<IgniteClientImpl*>(ptr.Get());
    }

    const IgniteClientImpl& GetClientImpl(const SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<const IgniteClientImpl*>(ptr.Get());
    }

    CacheClientImpl& GetCacheImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<CacheClientImpl*>(ptr.Get());
    }

    const CacheClientImpl& GetCacheImpl(const SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<const CacheClientImpl*>(ptr.Get());
    }
}

namespace ignite
{
    namespace thin
    {
        void IgniteClient::DestroyCache(const char* name)
        {
            GetClientImpl(impl).DestroyCache(name);
        }

        void IgniteClient::GetCacheNames(std::vector<std::string>& cacheNames)
        {
            GetClientImpl(impl).GetCacheNames(cacheNames);
        }

        IgniteClient::SP_Void IgniteClient::InternalGetCache(const char* name)
        {
            return GetClientImpl(impl).GetCache(name);
        }

        IgniteClient::SP_Void IgniteClient::InternalGetOrCreateCache(const char* name)
        {
            return GetClientImpl(impl).GetOrCreateCache(name);
        }

        IgniteClient::SP_Void IgniteClient::InternalCreateCache(const char* name)
        {
            return static_cast<SP_Void>(GetClientImpl(impl).CreateCache(name));
        }

        IgniteClient::IgniteClient(SP_Void& impl)
        {
            this->impl.Swap(impl);
        }

        IgniteClient::~IgniteClient()
        {
            // No-op.
        }

        IgniteClient IgniteClient::Start(const IgniteClientConfiguration& cfg)
        {
            SharedPointer<IgniteClientImpl> res(new IgniteClientImpl(cfg));

            res.Get()->Start();

            SP_Void ptr(res);

            return IgniteClient(ptr);
        }
    }
}
