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

#ifndef _IGNITE_THIN_CACHE_CACHE_CLIENT
#define _IGNITE_THIN_CACHE_CACHE_CLIENT

#include <ignite/common/concurrent.h>

#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/writable_key.h>
#include <ignite/impl/thin/readable.h>

#include <ignite/impl/thin/cache/cache_client_proxy.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            /**
             * Cache client class.
             */
            template<typename K, typename V>
            class CacheClient
            {
                friend class impl::thin::cache::CacheClientProxy;

            public:
                /** Key type. */
                typedef K KeyType;

                /** Value type. */
                typedef V ValueType;

                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                CacheClient(common::concurrent::SharedPointer<void> impl) :
                    proxy(impl)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~CacheClient()
                {
                    // No-op.
                }

                /**
                 * Associate the specified value with the specified key in the cache.
                 *
                 * @param key Key.
                 * @param value Value.
                 */
                void Put(const KeyType& key, const ValueType& value)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrValue(value);

                    proxy.Put(wrKey, wrValue);
                }

                /**
                 * Get value from the cache.
                 *
                 * @param key Key.
                 * @param value Value.
                 */
                void Get(const KeyType& key, ValueType& value)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::ReadableImpl<ValueType> rdValue(value);
                    
                    proxy.Get(wrKey, rdValue);
                }

                /**
                 * Get value from cache.
                 *
                 * @param key Key.
                 * @return Value.
                 */
                ValueType Get(const KeyType& key)
                {
                    ValueType value;

                    Get(key, value);

                    return value;
                }

                /**
                 * Check if the cache contains a value for the specified key.
                 *
                 * @param key Key whose presence in this cache is to be tested.
                 * @return @c true if the cache contains specified key.
                 */
                bool ContainsKey(const KeyType& key)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);

                    return proxy.ContainsKey(wrKey);
                }

                /**
                 * Gets the number of all entries cached across all nodes.
                 * @note This operation is distributed and will query all participating nodes for their cache sizes.
                 *
                 * @param peekModes Peek modes mask.
                 * @return Cache size across all nodes.
                 */
                int64_t GetSize(int32_t peekModes)
                {
                    return proxy.GetSize(peekModes);
                }

                /**
                 * Removes all mappings from cache.
                 * If write-through is enabled, the value will be removed from store.
                 * This method is transactional and will enlist the entry into ongoing transaction if there is one.
                 */
                void RemoveAll()
                {
                    proxy.RemoveAll();
                }

                /**
                 * Clear cache.
                 */
                void Clear()
                {
                    proxy.Clear();
                }

                /**
                 * Update cache partitions info.
                 */
                void UpdatePartitions()
                {
                    proxy.UpdatePartitions();
                }

            private:
                /** Implementation. */
                impl::thin::cache::CacheClientProxy proxy;
            };
        }
    }
}

#endif // _IGNITE_THIN_CACHE_CACHE_CLIENT
