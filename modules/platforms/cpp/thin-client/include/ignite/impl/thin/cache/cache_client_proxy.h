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

#ifndef _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_PROXY
#define _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_PROXY

#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /* Forward declaration. */
            class Writable;
            
            /* Forward declaration. */
            class WritableKey;

            /* Forward declaration. */
            class Readable;

            namespace cache
            {
                /**
                 * Ignite client class proxy.
                 */
                class IGNITE_IMPORT_EXPORT CacheClientProxy
                {
                public:
                    /**
                     * Constructor.
                     */
                    CacheClientProxy(const common::concurrent::SharedPointer<void>& impl) :
                        impl(impl)
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    ~CacheClientProxy()
                    {
                        // No-op.
                    }

                    /**
                     * Put value to cache.
                     *
                     * @param key Key.
                     * @param value Value.
                     */
                    void Put(const WritableKey& key, const Writable& value);

                    /**
                     * Get value from cache.
                     *
                     * @param key Key.
                     * @param value Value.
                     */
                    void Get(const WritableKey& key, Readable& value);

                    /**
                     * Check if the cache contains a value for the specified key.
                     *
                     * @param key Key whose presence in this cache is to be tested.
                     * @return @c true if the cache contains specified key.
                     */
                    bool ContainsKey(const WritableKey& key);

                    /**
                     * Gets the number of all entries cached across all nodes.
                     * @note This operation is distributed and will query all
                     * participating nodes for their cache sizes.
                     *
                     * @param peekModes Peek modes mask.
                     * @return Cache size across all nodes.
                     */
                    int64_t GetSize(int32_t peekModes);

                    /**
                     * Peeks at in-memory cached value using default optional
                     * peek mode. This method will not load value from any
                     * persistent store or from a remote node.
                     *
                     * Use for testing purposes only.
                     *
                     * @param key Key whose presence in this cache is to be tested.
                     * @param value Value.
                     */
                    void LocalPeek(const WritableKey& key, Readable& value);

                    /**
                     * Update cache partitions info.
                     */
                    void UpdatePartitions();

                    /**
                     * Get from CacheClient.
                     * Use for testing purposes only.
                     */
                    template<typename CacheClientT>
                    static CacheClientProxy& GetFromCacheClient(CacheClientT& instance)
                    {
                        return instance.proxy;
                    }

                private:
                    /** Implementation. */
                    common::concurrent::SharedPointer<void> impl;
                };
            }
        }
    }
}
#endif // _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_PROXY
