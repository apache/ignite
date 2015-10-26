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

#ifndef _IGNITE_CACHE_MUTABLE_CACHE_ENTRY
#define _IGNITE_CACHE_MUTABLE_CACHE_ENTRY

#include <ignite/common/common.h>
#include <ignite/cache/cache_entry.h>

namespace ignite
{
    namespace cache
    {
        /**
         * Mutable representation of CacheEntry
         */
        template<typename K, typename V>
        class IGNITE_IMPORT_EXPORT MutableCacheEntry
        {
        public:
            /**
             * Default constructor.
             */
            MutableCacheEntry() : key(K()), val(V()), exists(false)
            {
                // No-op.
            }

            /**
             * Constructor.
             *
             * @param key Key.
             * @param val Value.
             */
            MutableCacheEntry(const K& key, const V& val) : key(key), val(val), 
                exists(true)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            MutableCacheEntry(const MutableCacheEntry& other) : key(other.key), val(other.val), 
                exists(other.exists)
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             */
            MutableCacheEntry& operator=(const MutableCacheEntry& other)
            {
                if (this != &other)
                {
                    key = other.key;
                    val = other.val;
                    exists = other.exists;
                }

                return *this;
            }

            /**
             * Gets a value indicating whether cache entry exists in cache.
             */
            bool Exists() const
            {
                return exists;
            }

            /**
             * Removes the entry from the Cache.
             */
            void Remove()
            {
                exists = false;
            }

            /**
             * Get key.
             * 
             * @return Key.
             */
            K GetKey() const
            {
                return key;
            }

            /**
             * Get value.
             *
             * @return Value.
             */
            V GetValue() const
            {
                return val;
            }

            /**
             * Sets or replaces the value associated with the key.
             *
             * After setter invocation "Exists" will return true.
             */
            void SetValue(const V& val)
            {
                this->val = val;
                
                exists = true;
            }

        private:
            /** Key. */
            K key; 

            /** Value. */
            V val; 

            /** Exists. */
            bool exists;
        };
    }
}

#endif
