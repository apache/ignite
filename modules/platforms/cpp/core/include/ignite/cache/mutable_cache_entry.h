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

/**
 * @file
 * Declares ignite::cache::MutableCacheEntry class template.
 */

#ifndef _IGNITE_CACHE_MUTABLE_CACHE_ENTRY
#define _IGNITE_CACHE_MUTABLE_CACHE_ENTRY

namespace ignite
{
    namespace cache
    {
        /**
         * Mutable representation of CacheEntry class template.
         *
         * Both key and value types should be default-constructable,
         * copy-constructable and assignable.
         *
         * Additionally, equality operator should be defined for
         * the value type.
         */
        template<typename K, typename V>
        class MutableCacheEntry
        {
        public:
            /**
             * Default constructor.
             */
            MutableCacheEntry() :
                key(),
                val(),
                exists(false)
            {
                // No-op.
            }

            /**
             * Constructor for non-existing entry.
             *
             * @param key Key.
             */
            MutableCacheEntry(const K& key) :
                key(key),
                val(),
                exists(false)
            {
                // No-op.
            }

            /**
             * Constructor for existing entry.
             *
             * @param key Key.
             * @param val Value.
             */
            MutableCacheEntry(const K& key, const V& val) :
                key(key),
                val(val),
                exists(true)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            MutableCacheEntry(const MutableCacheEntry& other) :
                key(other.key),
                val(other.val),
                exists(other.exists)
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return *this.
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
             * Check whether cache entry exists in cache.
             *
             * @return True if the cache entry exists in cache and false
             *     otherwise.
             */
            bool IsExists() const
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
            const K& GetKey() const
            {
                return key;
            }

            /**
             * Get value.
             *
             * @return Value.
             */
            const V& GetValue() const
            {
                return val;
            }

            /**
             * Sets or replaces the value associated with the key.
             *
             * After setter invocation "IsExists" will return true.
             *
             * @param val Value to set.
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

#endif //_IGNITE_CACHE_MUTABLE_CACHE_ENTRY
