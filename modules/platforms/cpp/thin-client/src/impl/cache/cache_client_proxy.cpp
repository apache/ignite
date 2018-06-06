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

#include <ignite/impl/thin/cache/cache_client_impl.h>
#include <ignite/impl/thin/cache/cache_client_proxy.h>

using namespace ignite::impl::thin;
using namespace cache;
using namespace ignite::common::concurrent;

namespace
{
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
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                void CacheClientProxy::Put(const Writable& key, const Writable& value)
                {
                    GetCacheImpl(impl).Put(key, value);
                }

                void CacheClientProxy::Get(const Writable& key, Readable& value)
                {
                    GetCacheImpl(impl).Get(key, value);
                }
            }
        }
    }
}

