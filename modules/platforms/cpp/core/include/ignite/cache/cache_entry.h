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
 * Declares ignite::cache::CacheEntry class.
 */

#ifndef _IGNITE_CACHE_CACHE_ENTRY
#define _IGNITE_CACHE_CACHE_ENTRY

#include <ignite/common/common.h>

namespace ignite
{
    namespace cache
    {
        /**
         * %Cache entry class template.
         *
         * Both key and value types should be default-constructable,
         * copy-constructable and assignable.
         */
        template<typename K, typename V>
        class CacheEntry
        {
        public:
            /**
             * Default constructor.
             *
             * Creates instance with both key and value default-constructed.
             */
            CacheEntry() :
                key(),
                val(),
                hasValue(false)
            {
                // No-op.
            }

            /**
             * Constructor.
             *
             * @param key Key.
             * @param val Value.
             */
            CacheEntry(const K& key, const V& val) :
                key(key),
                val(val),
                hasValue(true)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            CacheEntry(const CacheEntry& other) :
                key(other.key),
                val(other.val),
                hasValue(other.hasValue)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            virtual ~CacheEntry()
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             */
            CacheEntry& operator=(const CacheEntry& other) 
            {
                if (this != &other)
                {
                    key = other.key;
                    val = other.val;
                    hasValue = other.hasValue;
                }

                return *this;
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
             * Check if the value exists.
             *
             * @return True, if the value exists.
             */
            bool HasValue() const
            {
                return hasValue;
            }

        protected:
            /** Key. */
            K key;

            /** Value. */
            V val;

            /** Indicates whether value exists */
            bool hasValue;
        };
    }
}

#endif //_IGNITE_CACHE_CACHE_ENTRY
