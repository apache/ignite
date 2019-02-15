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

/**
 * @file
 * Declares ignite::cache::CacheEntry class.
 */

#ifndef _IGNITE_CACHE_CACHE_ENTRY
#define _IGNITE_CACHE_CACHE_ENTRY

#include <utility>
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
             * Constructor.
             *
             * @param p Pair.
             */
            CacheEntry(const std::pair<K, V>& p) :
                key(p.first),
                val(p.second),
                hasValue(true)
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
