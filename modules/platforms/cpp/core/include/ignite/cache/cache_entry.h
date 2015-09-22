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

#ifndef _IGNITE_CACHE_ENTRY
#define _IGNITE_CACHE_ENTRY

#include <ignite/common/common.h>

namespace ignite
{
    namespace cache
    {
        /**
         * Cache entry.
         */
        template<typename K, typename V>
        class IGNITE_IMPORT_EXPORT CacheEntry
        {
        public:
            /**
             * Default constructor.
             */
            CacheEntry() : key(K()), val(V())
            {
                // No-op.
            }

            /**
             * Constructor.
             *
             * @param key Key.
             * @param val Value.
             */
            CacheEntry(const K& key, const V& val) : key(key), val(val)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            CacheEntry(const CacheEntry& other)
            {
                key = other.key;
                val = other.val;
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
                    CacheEntry tmp(other);

                    K& key0 = key;
                    V& val0 = val;

                    key = tmp.key;
                    val = tmp.val;

                    tmp.key = key0;
                    tmp.val = val0;
                }

                return *this;
            }

            /**
             * Get key.
             * 
             * @return Key.
             */
            K GetKey()
            {
                return key;
            }

            /**
             * Get value.
             *
             * @return Value.
             */
            V GetValue()
            {
                return val;
            }

        private:
            /** Key. */
            K key; 

            /** Value. */
            V val; 
        };
    }
}

#endif
