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

#include <ignite/impl/thin/cache/cache_client_proxy.h>

#include <ignite/impl/thin/cache/cache_client_proxy.h>
#include <impl/cache/cache_client_impl.h>

using namespace ignite::impl::thin;
using namespace cache;

namespace
{
    using namespace ignite::common::concurrent;

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
                void CacheClientProxy::Put(const WritableKey& key, const Writable& value)
                {
                    GetCacheImpl(impl).Put(key, value);
                }

                void CacheClientProxy::Get(const WritableKey& key, Readable& value)
                {
                    GetCacheImpl(impl).Get(key, value);
                }

                void CacheClientProxy::PutAll(const Writable& pairs)
                {
                    GetCacheImpl(impl).PutAll(pairs);
                }

                void CacheClientProxy::GetAll(const Writable & keys, Readable & pairs)
                {
                    GetCacheImpl(impl).GetAll(keys, pairs);
                }

                bool CacheClientProxy::Replace(const WritableKey& key, const Writable& value)
                {
                    return GetCacheImpl(impl).Replace(key, value);
                }

                bool CacheClientProxy::ContainsKey(const WritableKey & key)
                {
                    return GetCacheImpl(impl).ContainsKey(key);
                }

                bool CacheClientProxy::ContainsKeys(const Writable & keys)
                {
                    return GetCacheImpl(impl).ContainsKeys(keys);
                }

                int64_t CacheClientProxy::GetSize(int32_t peekModes)
                {
                    return GetCacheImpl(impl).GetSize(peekModes);
                }

                void CacheClientProxy::LocalPeek(const WritableKey& key, Readable& value)
                {
                    GetCacheImpl(impl).LocalPeek(key, value);
                }

                void CacheClientProxy::RefreshAffinityMapping()
                {
                    GetCacheImpl(impl).RefreshAffinityMapping();
                }

                bool CacheClientProxy::Remove(const WritableKey& key)
                {
                    return GetCacheImpl(impl).Remove(key);
                }

                bool CacheClientProxy::Remove(const WritableKey& key, const Writable& val)
                {
                    return GetCacheImpl(impl).Remove(key, val);
                }

                void CacheClientProxy::RemoveAll(const Writable & keys)
                {
                    return GetCacheImpl(impl).RemoveAll(keys);
                }

                void CacheClientProxy::RemoveAll()
                {
                    GetCacheImpl(impl).RemoveAll();
                }

                void CacheClientProxy::Clear(const WritableKey& key)
                {
                    GetCacheImpl(impl).Clear(key);
                }

                void CacheClientProxy::Clear()
                {
                    GetCacheImpl(impl).Clear();
                }

                void CacheClientProxy::ClearAll(const Writable& keys)
                {
                    GetCacheImpl(impl).ClearAll(keys);
                }

                bool CacheClientProxy::Replace(const WritableKey& key, const Writable& oldVal, const Writable& newVal)
                {
                    return GetCacheImpl(impl).Replace(key, oldVal, newVal);
                }

                void CacheClientProxy::GetAndPut(const WritableKey& key, const Writable& valIn, Readable& valOut)
                {
                    GetCacheImpl(impl).GetAndPut(key, valIn, valOut);
                }

                void CacheClientProxy::GetAndRemove(const WritableKey& key, Readable& valOut)
                {
                    GetCacheImpl(impl).GetAndRemove(key, valOut);
                }

                void CacheClientProxy::GetAndReplace(const WritableKey& key, const Writable& valIn, Readable& valOut)
                {
                    GetCacheImpl(impl).GetAndReplace(key, valIn, valOut);
                }

                bool CacheClientProxy::PutIfAbsent(const WritableKey& key, const Writable& val)
                {
                    return GetCacheImpl(impl).PutIfAbsent(key, val);
                }

                void CacheClientProxy::GetAndPutIfAbsent(const WritableKey& key, const Writable& valIn,
                    Readable& valOut)
                {
                    GetCacheImpl(impl).GetAndPutIfAbsent(key, valIn, valOut);
                }
            }
        }
    }
}

